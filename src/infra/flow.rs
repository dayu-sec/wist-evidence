//! 主机流记录的写入：按客户落到各自的 PG（`flow_window` 天分区表）。
//!
//! 三条不变量：
//!
//! 1. **分区按需创建**：`window_start::date` 决定分区名（`flow_window_pYYYYMMDD`）。
//!    并发写同一天的两个批次会撞 `CREATE TABLE` 竞态，`42P07` 当作成功（另一个请求已经建好）。
//! 2. **幂等靠唯一约束**：`on conflict (11 段业务键 + tenant) do update`，**覆盖**而不是累加
//!    —— 记录里是窗口聚合值，重发同一窗口的唯一正确语义就是覆盖。
//! 3. **失败只有一种**：后端错。字段级的错在 [`FlowRecordIn::validate`] 就挡住了，
//!    到这里再报"某条不合法"说明校验漏了一步，按后端错处理更诚实。

use std::collections::HashSet;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::domain::flow::FlowRecordIn;

#[derive(Debug)]
pub enum FlowError {
    Backend(String),
}

impl std::fmt::Display for FlowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backend(message) => write!(f, "flow store error: {message}"),
        }
    }
}

impl std::error::Error for FlowError {}

/// 一次批量写入的结果。`accepted` 是**写进去的行数**（含覆盖），`partitions` 是这次新建的分区数。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FlowWriteOutcome {
    pub accepted: usize,
    pub partitions: usize,
}

#[async_trait]
pub trait FlowStore: Send + Sync {
    /// 自述用（`/healthz` 的客户清单）：`postgres` / `memory`。
    fn kind(&self) -> &'static str;

    /// 批量写入一个客户的窗口记录。
    async fn insert_batch(
        &self,
        tenant_id: &str,
        records: &[FlowRecordIn],
    ) -> Result<FlowWriteOutcome, FlowError>;
}

// ── 内存实现（测试 / 未编 pg 的部署） ─────────────────────────────────────────

/// 内存实现：只保留**业务键集合**用于去重，行数就是"不同窗口"的数量。
///
/// 它存在的意义是让契约测试（幂等、上限、校验）在没有 PG 时也能跑；
/// 它**不是**一个可用的存储后端（重启即失，`/healthz` 会照实报 `memory`）。
#[derive(Debug, Default)]
pub struct MemoryFlowStore {
    keys: Mutex<HashSet<String>>,
}

impl MemoryFlowStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn key(tenant_id: &str, record: &FlowRecordIn) -> String {
        format!(
            "{tenant_id}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            record.collector_instance_id,
            record.ifname,
            record.direction,
            record.protocol,
            record.srcaddr,
            record.srcport,
            record.dstaddr,
            record.dstport,
            record.icmp_type.as_deref().unwrap_or(""),
            record.icmp_code.as_deref().unwrap_or(""),
            record.start
        )
    }
}

#[async_trait]
impl FlowStore for MemoryFlowStore {
    fn kind(&self) -> &'static str {
        "memory"
    }

    async fn insert_batch(
        &self,
        tenant_id: &str,
        records: &[FlowRecordIn],
    ) -> Result<FlowWriteOutcome, FlowError> {
        let mut keys = self.keys.lock().expect("flow store poisoned");
        let mut accepted = 0;
        for record in records {
            if keys.insert(Self::key(tenant_id, record)) {
                accepted += 1;
            }
        }
        Ok(FlowWriteOutcome {
            accepted,
            partitions: 0,
        })
    }
}

#[cfg(feature = "pg")]
pub use pg::PgFlowStore;

#[cfg(feature = "pg")]
mod pg {
    //! PostgreSQL 实现。与索引层共用**同一个连接池**（同一个客户库）：
    //! 分区表的写入是高频小事务，另开一个池只会多占 `max_connections`。

    use async_trait::async_trait;
    use chrono::{DateTime, NaiveDate, Utc};
    use sqlx::postgres::PgPoolOptions;
    use sqlx::types::Json;
    use sqlx::{PgPool, QueryBuilder};

    use crate::domain::flow::FlowRecordIn;

    use super::{FlowError, FlowStore, FlowWriteOutcome};

    #[derive(Debug)]
    pub struct PgFlowStore {
        pool: PgPool,
        /// 已经确认存在的分区（按 UTC 日期缓存，避免每批都发一条 DDL）。
        known_partitions: std::sync::Mutex<std::collections::HashSet<NaiveDate>>,
    }

    impl PgFlowStore {
        pub async fn connect(database_url: &str) -> Result<Self, FlowError> {
            let pool = PgPoolOptions::new()
                .max_connections(8)
                .connect(database_url)
                .await
                .map_err(|err| FlowError::Backend(format!("connect postgres: {err}")))?;
            Ok(Self::from_pool(pool))
        }

        /// 复用索引层的池（同一客户库）。`PgPool` 是 `Arc` 内部结构，克隆很便宜。
        pub fn from_pool(pool: PgPool) -> Self {
            Self {
                pool,
                known_partitions: std::sync::Mutex::new(std::collections::HashSet::new()),
            }
        }

        /// 窗口起点所在的 UTC 日期（分区粒度）。
        fn partition_day(timestamp: i64) -> NaiveDate {
            DateTime::<Utc>::from_timestamp(timestamp, 0)
                .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).expect("epoch"))
                .date_naive()
        }

        /// 确保某天的分区存在。并发撞 `42P07`（duplicate_table）视为成功。
        async fn ensure_partition(&self, day: NaiveDate) -> Result<bool, FlowError> {
            {
                let known = self
                    .known_partitions
                    .lock()
                    .expect("partition set poisoned");
                if known.contains(&day) {
                    return Ok(false);
                }
            }

            let next = day.succ_opt().ok_or_else(|| {
                FlowError::Backend(format!("partition day {day} has no successor"))
            })?;
            // 名字里的日期与边界都来自 `NaiveDate`（已格式化成 `%Y-%m-%d`），不可能带引号。
            let name = day.format("flow_window_p%Y%m%d");
            let ddl = format!(
                "create table if not exists {name} partition of flow_window \
                 for values from ('{day} 00:00:00+00') to ('{next} 00:00:00+00')"
            );

            // 动态 SQL 必须显式走 `raw_sql`：sqlx 把 `query()` 限定为静态串（编译期审计注入面）。
            // 这里的全部插值都来自 `NaiveDate::format`（`%Y-%m-%d` 与 `%Y%m%d`），
            // 不可能带引号或分号，也不需要绑定参数（PG 不允许 DDL 用占位符）。
            match sqlx::raw_sql(sqlx::AssertSqlSafe(ddl))
                .execute(&self.pool)
                .await
            {
                Ok(_) => {}
                Err(err) => {
                    // 并发建同一天的分区：另一个请求赢了，这不是错误。
                    let duplicate = err
                        .as_database_error()
                        .and_then(|db_err| db_err.code())
                        .is_some_and(|code| code.as_ref() == "42P07");
                    if !duplicate {
                        return Err(FlowError::Backend(format!("create partition: {err}")));
                    }
                    log::debug!("flow partition {name} already created concurrently");
                }
            }

            self.known_partitions
                .lock()
                .expect("partition set poisoned")
                .insert(day);
            Ok(true)
        }
    }

    #[async_trait]
    impl FlowStore for PgFlowStore {
        fn kind(&self) -> &'static str {
            "postgres"
        }

        async fn insert_batch(
            &self,
            tenant_id: &str,
            records: &[FlowRecordIn],
        ) -> Result<FlowWriteOutcome, FlowError> {
            let mut partitions = 0;
            let mut days: Vec<NaiveDate> = records
                .iter()
                .map(|record| Self::partition_day(record.start))
                .collect();
            days.sort_unstable();
            days.dedup();
            for day in days {
                if self.ensure_partition(day).await? {
                    partitions += 1;
                }
            }

            let mut builder: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
                "insert into flow_window (\
                 tenant_id, collector_instance_id, ifname, direction, protocol, \
                 srcaddr, srcport, dstaddr, dstport, icmp_type, icmp_code, \
                 window_start, window_seconds, host_id, host_name, ifindex, if_mac, \
                 local_addrs, kernel, packets, bytes, wire_bytes, skb_count, gso_skb_count, \
                 truncated, dropped, partial, partial_reason, coverage, source, capture) ",
            );

            builder.push_values(records, |mut row, record| {
                let window_start = DateTime::<Utc>::from_timestamp(record.start, 0)
                    .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).expect("epoch"));
                row.push_bind(tenant_id)
                    .push_bind(&record.collector_instance_id)
                    .push_bind(&record.ifname)
                    .push_bind(&record.direction)
                    .push_bind(&record.protocol)
                    .push_bind(&record.srcaddr)
                    .push_bind(&record.srcport)
                    .push_bind(&record.dstaddr)
                    .push_bind(&record.dstport)
                    .push_bind(record.icmp_type.as_deref().unwrap_or(""))
                    .push_bind(record.icmp_code.as_deref().unwrap_or(""))
                    .push_bind(window_start)
                    .push_bind(i32::try_from(record.window_seconds).unwrap_or(60))
                    .push_bind(&record.host_id)
                    .push_bind(record.host_name.as_deref())
                    .push_bind(record.ifindex)
                    .push_bind(record.if_mac.as_deref())
                    .push_bind(Json(&record.local_addrs))
                    .push_bind(record.kernel.as_deref())
                    .push_bind(record.packets as i64)
                    .push_bind(record.bytes as i64)
                    .push_bind(record.wire_bytes.map(|value| value as i64))
                    .push_bind(record.skb_count.map(|value| value as i64))
                    .push_bind(record.gso_skb_count.map(|value| value as i64))
                    .push_bind(record.truncated)
                    .push_bind(i32::try_from(record.dropped).unwrap_or(i32::MAX))
                    .push_bind(record.partial.unwrap_or(false))
                    .push_bind(record.partial_reason.as_deref())
                    .push_bind(&record.coverage)
                    .push_bind(&record.source)
                    .push_bind(&record.capture);
            });

            builder.push(
                " on conflict (tenant_id, collector_instance_id, ifname, direction, protocol, \
                 srcaddr, srcport, dstaddr, dstport, icmp_type, icmp_code, window_start) \
                 do update set \
                 window_seconds = excluded.window_seconds, \
                 host_id = excluded.host_id, \
                 host_name = excluded.host_name, \
                 ifindex = excluded.ifindex, \
                 if_mac = excluded.if_mac, \
                 local_addrs = excluded.local_addrs, \
                 kernel = excluded.kernel, \
                 packets = excluded.packets, \
                 bytes = excluded.bytes, \
                 wire_bytes = excluded.wire_bytes, \
                 skb_count = excluded.skb_count, \
                 gso_skb_count = excluded.gso_skb_count, \
                 truncated = excluded.truncated, \
                 dropped = excluded.dropped, \
                 partial = excluded.partial, \
                 partial_reason = excluded.partial_reason, \
                 coverage = excluded.coverage, \
                 source = excluded.source, \
                 capture = excluded.capture, \
                 ingested_at = now()",
            );

            builder
                .build()
                .execute(&self.pool)
                .await
                .map_err(|err| FlowError::Backend(format!("upsert flow_window: {err}")))?;

            Ok(FlowWriteOutcome {
                accepted: records.len(),
                partitions,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::flow::RECORD_SCHEMA_V1;

    fn record(start: i64, dst_port: &str) -> FlowRecordIn {
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
            local_addrs: Vec::new(),
            direction: "out".to_string(),
            protocol: "6".to_string(),
            srcaddr: "10.0.0.1".to_string(),
            dstaddr: "1.1.1.1".to_string(),
            srcport: "50000".to_string(),
            dstport: dst_port.to_string(),
            icmp_type: None,
            icmp_code: None,
            start,
            end: start + 60,
            window_seconds: 60,
            packets: 1,
            bytes: 60,
            wire_bytes: None,
            wire_bytes_source: "unavailable".to_string(),
            skb_count: None,
            gso_skb_count: None,
            truncated: false,
            dropped: 0,
            partial: None,
            partial_reason: None,
        }
    }

    #[tokio::test]
    async fn resending_the_same_window_does_not_grow_the_store() {
        let store = MemoryFlowStore::new();
        let batch = [record(1_789_470_120, "443")];

        let first = store.insert_batch("c-001", &batch).await.expect("first");
        assert_eq!(first.accepted, 1);
        let second = store.insert_batch("c-001", &batch).await.expect("second");
        assert_eq!(second.accepted, 0, "同键重发应当被覆盖而不是新增");
    }

    /// 租户是幂等键的第一段：两个客户写同一份记录是两条不同的行。
    #[tokio::test]
    async fn the_same_record_under_two_tenants_is_two_rows() {
        let store = MemoryFlowStore::new();
        let batch = [record(1_789_470_120, "443")];
        assert_eq!(
            store.insert_batch("c-001", &batch).await.unwrap().accepted,
            1
        );
        assert_eq!(
            store.insert_batch("c-002", &batch).await.unwrap().accepted,
            1
        );
    }

    #[tokio::test]
    async fn different_ports_are_different_windows() {
        let store = MemoryFlowStore::new();
        let batch = [record(1_789_470_120, "443"), record(1_789_470_120, "80")];
        assert_eq!(
            store.insert_batch("c-001", &batch).await.unwrap().accepted,
            2
        );
    }
}
