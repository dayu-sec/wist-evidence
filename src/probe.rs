//! 写探针：每 `interval` 往**自己的写端点**为**每个已登记客户**投一条证据，验证整条写路径。
//!
//! 为什么走 HTTP 而不直接调 [`EvidenceService::write_simple`]：绕开 HTTP 层就验不到
//! 路由、序列化、错误映射。所以探针就是一个小号的 wparse：同样的 URL、同样的三字段形状。
//!
//! 每个 cell 各探一条：默认后端已经移除，只探一个保留客户就证明不了「每个客户的库都能写」。
//! 探针产物 `subject_id = "_probe"`，证据列表里**不显示**（见 `EvidenceService::list`），
//! 超过保留期的由 [`crate::service::EvidenceService::purge_probes`] 清掉。

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;

use crate::domain::artifact::new_artifact_id;
use crate::service::{EvidenceService, PROBE_SUBJECT};

/// 探针的投递目标：写端点的完整 URL。
#[derive(Debug, Clone)]
pub struct ProbeTarget {
    pub url: String,
}

/// 由监听地址推导探针 URL。
///
/// 监听可能是 `0.0.0.0:8080` / `[::]:8080` —— 那是「听谁」，不是「连谁」，
/// 探针要从回环打进去。
pub fn loopback_sink_url(listen_addr: &str) -> String {
    let port = listen_addr.rsplit(':').next().unwrap_or("8080").trim();
    format!("http://127.0.0.1:{port}/internal/evidence")
}

pub struct ProbeRunner {
    service: Arc<EvidenceService>,
    http: reqwest::Client,
    target: ProbeTarget,
    /// 要探的客户（= 所有已登记 cell 的 cust_id）；写入与清理同一口径。
    customers: Vec<String>,
    interval: Duration,
    retention: Duration,
}

impl ProbeRunner {
    pub fn new(
        service: Arc<EvidenceService>,
        target: ProbeTarget,
        customers: Vec<String>,
        interval: Duration,
        retention: Duration,
    ) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_default();
        Self {
            service,
            http,
            target,
            customers,
            interval,
            retention,
        }
    }

    /// 为每个客户各投一条探针。
    ///
    /// 全部成功才算成功，延迟取最大值；任一失败都记失败，并在日志里点名是哪个 cust_id。
    pub async fn tick(&self) -> Result<(), String> {
        let mut max_latency_ms = 0u64;
        let mut failures = Vec::new();
        for cust_id in &self.customers {
            let started = Instant::now();
            // 记录形状与 wparse 的 http_sink 完全一致：三字段 + 可选 `subject`。
            // `cmd_id` 用新 id：每次探针都必须是一次**真写入**，不能被去重掉。
            let record = json!({
                "cust_id": cust_id,
                "cmd_id": new_artifact_id(),
                "subject": PROBE_SUBJECT,
                "content": {
                    "probe": true,
                    "at": chrono::Utc::now().to_rfc3339(),
                },
            });

            let result = self.http.post(&self.target.url).json(&record).send().await;
            let latency_ms = started.elapsed().as_millis() as u64;
            max_latency_ms = max_latency_ms.max(latency_ms);
            match result {
                Ok(response) if response.status().is_success() => {}
                Ok(response) => failures.push(format!(
                    "{cust_id}: write endpoint returned HTTP {}",
                    response.status().as_u16()
                )),
                Err(err) => failures.push(format!("{cust_id}: write endpoint unreachable: {err}")),
            }
        }

        let ok = failures.is_empty();
        self.service.record_probe_result(ok, max_latency_ms);
        if ok {
            Ok(())
        } else {
            let detail = failures.join("; ");
            log::warn!("evidence_probe_failed url={}: {detail}", self.target.url);
            Err(detail)
        }
    }

    /// 常驻循环：第一拍立即执行；清理按保留窗口（最多每小时扫一次）。
    pub async fn run(self) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let purge_every = self.retention.min(Duration::from_secs(3600));
        let mut last_purge = Instant::now();
        loop {
            ticker.tick().await;
            // 失败明细已由 `tick` 按客户记进日志，这里只报成功。
            if let Ok(()) = self.tick().await {
                log::info!("evidence_probe_ok url={}", self.target.url);
            }
            if last_purge.elapsed() >= purge_every {
                last_purge = Instant::now();
                self.purge_expired().await;
            }
        }
    }

    /// 按客户逐个清理过期探针：探针写在哪个后端就清哪个后端。
    async fn purge_expired(&self) {
        let mut purged_total = 0u64;
        for cust_id in &self.customers {
            match self.service.purge_probes(cust_id, self.retention).await {
                Ok(0) => {}
                Ok(purged) => {
                    purged_total += purged;
                    log::info!("evidence_probe_purged cust_id={cust_id} count={purged}");
                }
                Err(err) => {
                    log::warn!("evidence_probe_purge_failed cust_id={cust_id}: {err:?}");
                }
            }
        }
        if purged_total > 0 {
            log::info!("evidence_probe_purged_total count={purged_total}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_url_replaces_the_wildcard_host() {
        assert_eq!(
            loopback_sink_url("0.0.0.0:8080"),
            "http://127.0.0.1:8080/internal/evidence"
        );
        assert_eq!(
            loopback_sink_url("127.0.0.1:19173"),
            "http://127.0.0.1:19173/internal/evidence"
        );
        assert_eq!(
            loopback_sink_url("[::]:8080"),
            "http://127.0.0.1:8080/internal/evidence"
        );
    }
}
