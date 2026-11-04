//! `wist-evidence` 进程入口：两张监听（写 / 读）各自独立。

use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use wist_evidence::api::{self, ReadState, SinkState};
use wist_evidence::config::{Config, IndexKind, ObjectsKind};
use wist_evidence::infra::FileObjectStore;
use wist_evidence::infra::Index;
use wist_evidence::infra::MemoryIndex;
use wist_evidence::infra::ObjectStore;
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

    let index = build_index(&config).await?;
    let objects = build_objects(&config)?;
    let service = Arc::new(EvidenceService::new(
        index,
        objects,
        config.objects.bucket.clone(),
        config.sink.allowed_kinds.clone(),
        config.sink.max_body_bytes,
    ));

    let sink_state = SinkState {
        service: service.clone(),
        tokens: Arc::new(config.sink.tokens.clone()),
    };
    let read_state = ReadState {
        service: service.clone(),
    };

    let sink_app = api::sink_router(sink_state, config.sink.max_body_bytes);
    let read_app = api::read_router(read_state);

    log::info!(
        "wist-evidence starting: sink={} read={} index={:?} objects={:?} kinds={:?}",
        config.sink.listen_addr,
        config.read.listen_addr,
        config.index.kind,
        config.objects.kind,
        config.sink.allowed_kinds
    );

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

    tokio::select! {
        result = sink => result.context("sink server task")??,
        result = read => result.context("read server task")??,
    }
    Ok(())
}

async fn build_index(config: &Config) -> anyhow::Result<Arc<dyn Index>> {
    match config.index.kind {
        IndexKind::Memory => Ok(Arc::new(MemoryIndex::new())),
        IndexKind::Postgres => {
            #[cfg(feature = "pg")]
            {
                let url = config
                    .index
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

fn build_objects(config: &Config) -> anyhow::Result<Arc<dyn ObjectStore>> {
    match config.objects.kind {
        ObjectsKind::File => {
            let dir = config
                .objects
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
                    endpoint: config.objects.endpoint.clone().unwrap_or_default(),
                    bucket: config.objects.bucket.clone(),
                    region: config.objects.region.clone().unwrap_or_default(),
                    access_key: config.objects.access_key.clone().unwrap_or_default(),
                    secret_key: config.objects.secret_key.clone().unwrap_or_default(),
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
