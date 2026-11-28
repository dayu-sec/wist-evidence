//! 主机流记录的窗口行（`wist.host_flow/v1`）。
//!
//! 形状的唯一权威在 `wist_contracts::host_flow::HostFlowRecord`（agentd 产出）。
//! 证据服务不依赖那个 crate，也不重建模型：网关已经按契约严格解析并校验过一遍，
//! 到这里只做**落库需要的那部分**——所以这里的结构体：
//!
//! - 只含入库的字段（不落 `port_unavailable` 这种纯标记，它有值时端口已经是 `"0"`）；
//! - `#[serde(default)]` 允许上游加字段而不破这里（加字段 = 升 `record_schema`，
//!   版本闸门在 `validate` 里）；
//! - **重新做不变量检查**：不信任"上游已经查过"，因为这条链路有两个调用方
//!   （网关转发、现场 curl 补投），校验只有一份才算稳。

use serde::Deserialize;

/// v1 记录版本。改字段语义要升它（与契约 crate 同值）。
pub const RECORD_SCHEMA_V1: &str = "wist.host_flow/v1";

/// 一次写入最多接受多少条记录。与 agentd 的单拍上限（4096）一致：
/// 超过这个数说明调用方没按批切，直接拒（**不截断** —— 静默截断是最坏的失败形状）。
pub const MAX_RECORDS_PER_WRITE: usize = 4096;

#[derive(Debug, Clone, Deserialize)]
pub struct FlowRecordIn {
    pub record_schema: String,
    pub source: String,
    pub coverage: String,
    pub capture: String,
    pub collector_instance_id: String,
    pub host_id: String,
    #[serde(default)]
    pub host_name: Option<String>,
    #[serde(default)]
    pub kernel: Option<String>,
    pub ifname: String,
    pub ifindex: i32,
    #[serde(default)]
    pub if_mac: Option<String>,
    #[serde(default)]
    pub local_addrs: Vec<String>,
    pub direction: String,
    pub protocol: String,
    pub srcaddr: String,
    pub dstaddr: String,
    pub srcport: String,
    pub dstport: String,
    #[serde(default)]
    pub icmp_type: Option<String>,
    #[serde(default)]
    pub icmp_code: Option<String>,
    /// Unix 秒（窗口起点，UTC 整分对齐）。
    pub start: i64,
    /// Unix 秒（半开区间：`[start, end)`）。
    pub end: i64,
    pub window_seconds: u32,
    pub packets: u64,
    pub bytes: u64,
    #[serde(default)]
    pub wire_bytes: Option<u64>,
    pub wire_bytes_source: String,
    #[serde(default)]
    pub skb_count: Option<u64>,
    #[serde(default)]
    pub gso_skb_count: Option<u64>,
    pub truncated: bool,
    pub dropped: u32,
    #[serde(default)]
    pub partial: Option<bool>,
    #[serde(default)]
    pub partial_reason: Option<String>,
}

impl FlowRecordIn {
    /// 落库前的不变量检查。返回的是**稳定的错误码**（进响应体的 `failures[]`，
    /// 现场靠它区分"版本不对"与"字段写错"）。
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.record_schema != RECORD_SCHEMA_V1 {
            return Err("unsupported_record_schema");
        }
        if self.direction != "in" && self.direction != "out" {
            return Err("invalid_direction");
        }
        if !matches!(self.protocol.as_str(), "1" | "6" | "17" | "58") {
            return Err("invalid_protocol");
        }
        // 窗口是半开区间，长度必须自洽：`end - start == window_seconds`。
        if self.window_seconds == 0
            || self.end.checked_sub(self.start) != Some(i64::from(self.window_seconds))
        {
            return Err("window_length_mismatch");
        }
        if self.ifindex < 0 || self.start < 0 {
            return Err("invalid_numeric_field");
        }
        if self.collector_instance_id.is_empty()
            || self.ifname.is_empty()
            || self.host_id.is_empty()
        {
            return Err("missing_identity");
        }
        Ok(())
    }

    /// 幂等键在日志/诊断里的可读形式（**不是**入库用的键，入库走唯一约束）。
    pub fn describe(&self) -> String {
        format!(
            "{} {} {}:{}-{}:{} @{}",
            self.ifname,
            self.direction,
            self.srcaddr,
            self.srcport,
            self.dstaddr,
            self.dstport,
            self.start
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> FlowRecordIn {
        FlowRecordIn {
            record_schema: RECORD_SCHEMA_V1.to_string(),
            source: "host_ebpf".to_string(),
            coverage: "host_partial".to_string(),
            capture: "tc_egress".to_string(),
            collector_instance_id: "agent-host-1".to_string(),
            host_id: "host-1".to_string(),
            host_name: None,
            kernel: None,
            ifname: "eth0".to_string(),
            ifindex: 2,
            if_mac: None,
            local_addrs: vec!["10.118.0.14".to_string()],
            direction: "out".to_string(),
            protocol: "6".to_string(),
            srcaddr: "10.118.0.14".to_string(),
            dstaddr: "88.214.25.121".to_string(),
            srcport: "50123".to_string(),
            dstport: "443".to_string(),
            icmp_type: None,
            icmp_code: None,
            start: 1_789_470_120,
            end: 1_789_470_180,
            window_seconds: 60,
            packets: 13,
            bytes: 1460,
            wire_bytes: None,
            wire_bytes_source: "unavailable".to_string(),
            skb_count: Some(4),
            gso_skb_count: Some(1),
            truncated: false,
            dropped: 0,
            partial: None,
            partial_reason: None,
        }
    }

    #[test]
    fn a_documented_record_validates() {
        record().validate().expect("valid");
    }

    #[test]
    fn an_unknown_record_schema_is_rejected() {
        // 版本闸门：加字段必须升版本，不能悄悄改 v1 的语义。
        let mut item = record();
        item.record_schema = "wist.host_flow/v2".to_string();
        assert_eq!(item.validate(), Err("unsupported_record_schema"));
    }

    #[test]
    fn the_window_must_be_self_consistent() {
        let mut item = record();
        item.end = item.start + 30;
        assert_eq!(item.validate(), Err("window_length_mismatch"));

        let mut zero = record();
        zero.window_seconds = 0;
        zero.end = zero.start;
        assert_eq!(zero.validate(), Err("window_length_mismatch"));
    }

    #[test]
    fn only_collected_protocols_and_directions_pass() {
        let mut proto = record();
        proto.protocol = "47".to_string();
        assert_eq!(proto.validate(), Err("invalid_protocol"));

        let mut direction = record();
        direction.direction = "egress".to_string();
        assert_eq!(direction.validate(), Err("invalid_direction"));
    }

    #[test]
    fn identity_fields_are_required() {
        let mut item = record();
        item.ifname = String::new();
        assert_eq!(item.validate(), Err("missing_identity"));
    }
}
