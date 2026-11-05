//! 写探针：每 `interval` 往**自己的写端点**投一条证据，验证整条写路径。
//!
//! 为什么走 HTTP 而不直接调 [`EvidenceService::write_simple`]：绕开 HTTP 层就验不到
//! 路由、序列化、错误映射。所以探针就是一个小号的 wparse：同样的 URL、同样的三字段形状。
//!
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

/// 探针的客户 id：探针产物按 `subject = _probe` 标记，客户用一个保留值。
pub const PROBE_CUSTOMER: &str = "c-probe";

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
    /// 探针写到哪个客户（与清理同一口径）。
    customer_id: String,
    interval: Duration,
    retention: Duration,
}

impl ProbeRunner {
    pub fn new(
        service: Arc<EvidenceService>,
        target: ProbeTarget,
        customer_id: impl Into<String>,
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
            customer_id: customer_id.into(),
            interval,
            retention,
        }
    }

    /// 投一次探针。`Ok` 表示写端点回了 2xx；失败原因会进日志与 `/healthz`。
    pub async fn tick(&self) -> Result<(), String> {
        let started = Instant::now();
        // 记录形状与 wparse 的 http_sink 完全一致：三字段 + 可选 `subject`。
        // `cmd_id` 用新 id：每次探针都必须是一次**真写入**，不能被去重掉。
        let record = json!({
            "cust_id": PROBE_CUSTOMER,
            "cmd_id": new_artifact_id(),
            "subject": PROBE_SUBJECT,
            "content": {
                "probe": true,
                "at": chrono::Utc::now().to_rfc3339(),
            },
        });

        let result = self.http.post(&self.target.url).json(&record).send().await;

        let latency_ms = started.elapsed().as_millis() as u64;
        let outcome = match result {
            Ok(response) if response.status().is_success() => Ok(()),
            Ok(response) => Err(format!(
                "write endpoint returned HTTP {}",
                response.status().as_u16()
            )),
            Err(err) => Err(format!("write endpoint unreachable: {err}")),
        };
        self.service
            .record_probe_result(outcome.is_ok(), latency_ms);
        outcome
    }

    /// 常驻循环：第一拍立即执行；清理按保留窗口（最多每小时扫一次）。
    pub async fn run(self) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let purge_every = self.retention.min(Duration::from_secs(3600));
        let mut last_purge = Instant::now();
        loop {
            ticker.tick().await;
            match self.tick().await {
                Ok(()) => log::info!("evidence_probe_ok url={}", self.target.url),
                Err(err) => log::warn!("evidence_probe_failed url={}: {err}", self.target.url),
            }
            if last_purge.elapsed() >= purge_every {
                last_purge = Instant::now();
                // 探针只写一个客户，清理也只扫那一个。
                match self
                    .service
                    .purge_probes(&self.customer_id, self.retention)
                    .await
                {
                    Ok(0) => {}
                    Ok(purged) => log::info!("evidence_probe_purged count={purged}"),
                    Err(err) => log::warn!("evidence_probe_purge_failed: {err:?}"),
                }
            }
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
