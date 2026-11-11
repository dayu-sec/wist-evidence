//! `wist-evidence` 进程入口：两张监听（写 / 读）各自独立。

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use wist_evidence::api::{self, ReadState, SinkState};
use wist_evidence::config::{Config, IndexKind, ObjectsKind};
use wist_evidence::infra::FileObjectStore;
use wist_evidence::infra::Index;
use wist_evidence::infra::MemoryIndex;
use wist_evidence::infra::ObjectStore;
use wist_evidence::probe::{self, ProbeRunner, ProbeTarget};
use wist_evidence::service::EvidenceService;

#[cfg(feature = "pg")]
use wist_evidence::infra::PgIndex;
#[cfg(feature = "s3")]
use wist_evidence::infra::{S3Config, S3ObjectStore};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let config_path = std::env::args().nth(1);
    let config = Config::load(config_path.as_deref().map(Path::new))?;

    let backends = build_backends(&config).await?;
    let mut service = EvidenceService::with_backends(
        backends,
        config.sink.allowed_kinds.clone(),
        config.sink.max_body_bytes,
    );
    // 写探针：先登记状态位，再由下面 spawn 的循环投递。
    if config.probe.enabled {
        service.enable_probe();
    }
    let service = Arc::new(service);

    let sink_state = SinkState {
        service: service.clone(),
        default_kind: config.sink.default_kind,
        default_schema: config.sink.default_schema.clone(),
        default_sensitivity: config.sink.default_sensitivity,
    };
    let read_state = ReadState {
        service: service.clone(),
        service_token: config.read.service_token.clone(),
    };

    let sink_app = api::sink_router(sink_state, config.sink.max_body_bytes);
    let read_app = api::read_router(read_state);

    log::info!(
        "wist-evidence starting: sink={} read={} index={:?} objects={:?} kinds={:?} probe={}",
        config.sink.listen_addr,
        config.read.listen_addr,
        config.index.kind,
        config.objects.kind,
        config.sink.allowed_kinds,
        if config.probe.enabled {
            format!(
                "every {}s / keep {}h",
                config.probe.interval_secs, config.probe.retention_hours
            )
        } else {
            "off".to_string()
        }
    );

    if config.read.service_token.is_none() {
        log::warn!(
            "wist-evidence read endpoint has NO service token: identity headers are trusted as-is \
             (any in-network caller can claim any tenant); set `[read] service_token` to enable the check"
        );
    }

    let sink_listener = tokio::net::TcpListener::bind(&config.sink.listen_addr)
        .await
        .with_context(|| format!("bind sink listener {}", config.sink.listen_addr))?;
    let read_listener = tokio::net::TcpListener::bind(&config.read.listen_addr)
        .await
        .with_context(|| format!("bind read listener {}", config.read.listen_addr))?;

    let sink = tokio::spawn(async move {
        axum::serve(sink_listener, sink_app)
            .with_graceful_shutdown(shutdown_signal())
            .await
    });
    let read = tokio::spawn(async move {
        axum::serve(read_listener, read_app)
            .with_graceful_shutdown(shutdown_signal())
            .await
    });

    // 探针必须在两张监听都起来之后再启动：第一拍是立即执行的，抢跑只会记一次假失败。
    if config.probe.enabled {
        spawn_probe(
            service.clone(),
            probe::PROBE_CUSTOMER.to_string(),
            // 探针打的是自己的写端点：默认由 sink 监听地址推回环 URL，
            // 需要时可用 `[probe] url` 覆盖（例如前面有反代）。
            config
                .probe
                .url
                .clone()
                .unwrap_or_else(|| probe::loopback_sink_url(&config.sink.listen_addr)),
            Duration::from_secs(config.probe.interval_secs),
            Duration::from_secs(config.probe.retention_hours * 3600),
        );
    }

    tokio::select! {
        result = sink => result.context("sink server task")??,
        result = read => result.context("read server task")??,
    }
    Ok(())
}

/// 组装存储路由表：`default` 是顶层 `[index]` / `[objects]`，每个 `[[storage.cell]]` 一个后端。
///
/// 每个后端的 PG 连接池是**独立**的（`max_connections` 默认 8），所以客户数 × 8 不能超过
/// PG 实例的 `max_connections` —— 单客户一个 PG 实例时自然满足。
async fn build_backends(config: &Config) -> anyhow::Result<Arc<wist_evidence::infra::Backends>> {
    let default = wist_evidence::infra::Backend {
        index: build_index(&config.index).await?,
        objects: build_objects(&config.objects)?,
        bucket: config.objects.bucket.clone(),
    };

    let mut cells = std::collections::BTreeMap::new();
    for cell in &config.cells {
        let backend = wist_evidence::infra::Backend {
            index: build_index(&cell.index).await?,
            objects: build_objects(&cell.objects)?,
            bucket: cell.objects.bucket.clone(),
        };
        cells.insert(cell.cust_id.clone(), backend);
    }

    if !cells.is_empty() {
        log::info!(
            "storage: {} customer cell(s) configured, others fall back to the default backend",
            cells.len()
        );
    }
    Ok(Arc::new(wist_evidence::infra::Backends::new(
        default, cells,
    )))
}

async fn build_index(
    config: &wist_evidence::config::IndexConfig,
) -> anyhow::Result<Arc<dyn Index>> {
    match config.kind {
        IndexKind::Memory => Ok(Arc::new(MemoryIndex::new())),
        IndexKind::Postgres => {
            #[cfg(feature = "pg")]
            {
                let url = config
                    .database_url
                    .as_deref()
                    .context("index.database_url is required for postgres")?;
                let index = PgIndex::connect(url).await?;
                index.migrate().await?;
                Ok(Arc::new(index))
            }
            #[cfg(not(feature = "pg"))]
            {
                anyhow::bail!("built without the `pg` feature: cannot use index.kind = postgres")
            }
        }
    }
}

fn build_objects(
    config: &wist_evidence::config::ObjectsConfig,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    match config.kind {
        ObjectsKind::File => {
            let dir = config
                .dir
                .as_deref()
                .context("objects.dir is required for objects.kind = file")?;
            let store =
                FileObjectStore::open(dir).with_context(|| format!("open objects.dir {dir}"))?;
            log::info!(
                "objects.kind = file: evidence bodies under {} (no presigned URLs)",
                store.root().display()
            );
            Ok(Arc::new(store))
        }
        ObjectsKind::S3 => {
            #[cfg(feature = "s3")]
            {
                let s3 = S3Config {
                    endpoint: config.endpoint.clone().unwrap_or_default(),
                    bucket: config.bucket.clone(),
                    region: config.region.clone().unwrap_or_default(),
                    access_key: config.access_key.clone().unwrap_or_default(),
                    secret_key: config.secret_key.clone().unwrap_or_default(),
                };
                Ok(Arc::new(S3ObjectStore::new(&s3)))
            }
            #[cfg(not(feature = "s3"))]
            {
                anyhow::bail!("built without the `s3` feature: cannot use objects.kind = s3")
            }
        }
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

/// 写探针：每 `interval` 打一次自己的写端点，每过一个保留窗口清一次过期探针。
///
/// 投递本身就验了「三字段请求 → 对象 → 索引」这一整条链路；
/// 失败只记日志，不拖垮进程 —— 结果反映在 `/healthz` 的 `probe` 块里。
fn spawn_probe(
    service: Arc<EvidenceService>,
    customer_id: String,
    url: String,
    interval: Duration,
    retention: Duration,
) {
    let runner = ProbeRunner::new(
        service,
        ProbeTarget { url },
        customer_id,
        interval,
        retention,
    );
    tokio::spawn(runner.run());
}
