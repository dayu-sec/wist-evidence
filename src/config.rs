//! 分层配置：主配置 + `${VAR}` 展开。
//!
//! 与 `wist-center` 同口径：`${VAR}` 先查密钥表（`WIST_EVIDENCE_SEC_FILE` 指向的 YAML，
//! 缺省走 orion 的 `.galaxy/sec_value.yml`），再回落系统环境变量；**展开后仍含占位符就报错**，
//! 绝不把 `${SEC_...}` 当字面值用。

use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use orion_sec::sec::NoSecConv;
use orion_sec::{SecFileFmt, load_sec_dict, load_secfile_by};
use orion_variate::vars::{EnvChecker, EnvDict, EnvEvaluable, ValueType};
use serde::Deserialize;

use crate::domain::Kind;
use crate::service::{SinkCaller, TokenMap};

const SEC_FILE_ENV: &str = "WIST_EVIDENCE_SEC_FILE";
const CONFIG_ENV: &str = "WIST_EVIDENCE_CONFIG";

/// 请求体硬上限（含 `body` 字符串的转义膨胀）。§12.1。
pub const HARD_MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct Config {
    pub sink: SinkConfig,
    pub read: ReadConfig,
    pub index: IndexConfig,
    pub objects: ObjectsConfig,
}

#[derive(Debug, Clone)]
pub struct SinkConfig {
    pub listen_addr: String,
    pub max_body_bytes: usize,
    pub allowed_kinds: Vec<Kind>,
    pub tokens: TokenMap,
}

#[derive(Debug, Clone)]
pub struct ReadConfig {
    pub listen_addr: String,
}

#[derive(Debug, Clone)]
pub struct IndexConfig {
    pub kind: IndexKind,
    pub database_url: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    Memory,
    Postgres,
}

#[derive(Debug, Clone)]
pub struct ObjectsConfig {
    pub kind: ObjectsKind,
    /// `kind = file`：本地目录的根（证据正文落在这里，方便测试与现场核对）。
    pub dir: Option<String>,
    pub bucket: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

/// 证据正文的两种落点：
///
/// - [`ObjectsKind::File`]：本地目录。测试 / 现场验证用，重启不丢、能直接看文件；
/// - [`ObjectsKind::S3`]：S3 兼容对象存储（腾讯 COS / MinIO）。生产用。
///
/// 没有「内存」这一档：正文丢了就再也要不回来，不该成为一个部署选项。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectsKind {
    File,
    S3,
}

impl Config {
    /// 读配置：显式路径 > `WIST_EVIDENCE_CONFIG` > `./wist-evidence.toml`。
    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        let path = match path {
            Some(path) => path.to_path_buf(),
            None => match std::env::var_os(CONFIG_ENV).filter(|value| !value.is_empty()) {
                Some(value) => PathBuf::from(value),
                None => PathBuf::from("wist-evidence.toml"),
            },
        };
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("read config {}", path.display()))?;
        let raw: RawConfig =
            toml::from_str(&raw).with_context(|| format!("parse config {}", path.display()))?;
        let env = secret_dict()?;
        raw.resolve(&env)
            .with_context(|| format!("resolve config {}", path.display()))
    }
}

/// 展开 `${VAR}`；展开后仍含占位符 → 报错（fail-closed）。
fn expand(env: &EnvDict, value: &str) -> anyhow::Result<String> {
    let expanded: String = value.to_string().env_eval(env);
    if expanded.needs_env_eval() {
        bail!(
            "unresolved environment variable(s) in {value:?}: {}",
            expanded.list_env_vars().join(", ")
        );
    }
    Ok(expanded)
}

/// 进程环境变量优先，其下是密钥文件里的 `SEC_*`。
fn secret_dict() -> anyhow::Result<EnvDict> {
    let mut dict = match std::env::var_os(SEC_FILE_ENV).filter(|path| !path.is_empty()) {
        Some(path) => load_sec_file(Path::new(&path))?,
        None => load_sec_dict().map_err(|err| anyhow::anyhow!("failed to load secrets: {err}"))?,
    };
    for (key, value) in std::env::vars() {
        dict.insert(key, ValueType::from(value.as_str()));
    }
    Ok(dict)
}

fn load_sec_file(path: &Path) -> anyhow::Result<EnvDict> {
    if !path.exists() {
        bail!(
            "secrets file not found: {} ({SEC_FILE_ENV})",
            path.display()
        );
    }
    let space = load_secfile_by(path.to_path_buf(), SecFileFmt::Yaml)
        .map_err(|err| anyhow::anyhow!("failed to load secrets {}: {err}", path.display()))?;
    let mut dict = EnvDict::new();
    for (key, value) in space.no_sec() {
        dict.insert(key, value);
    }
    Ok(dict)
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    #[serde(default)]
    sink: RawSink,
    #[serde(default)]
    read: RawRead,
    #[serde(default)]
    index: RawIndex,
    #[serde(default)]
    objects: RawObjects,
}

#[derive(Debug, Deserialize)]
struct RawSink {
    #[serde(default = "default_sink_addr")]
    listen_addr: String,
    #[serde(default = "default_max_body_bytes")]
    max_body_bytes: usize,
    #[serde(default = "default_allowed_kinds")]
    allowed_kinds: Vec<String>,
    #[serde(default)]
    tokens: std::collections::BTreeMap<String, RawToken>,
}

impl Default for RawSink {
    fn default() -> Self {
        Self {
            listen_addr: default_sink_addr(),
            max_body_bytes: default_max_body_bytes(),
            allowed_kinds: default_allowed_kinds(),
            tokens: Default::default(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawToken {
    tenant_id: String,
    customer_id: String,
}

#[derive(Debug, Deserialize)]
struct RawRead {
    #[serde(default = "default_read_addr")]
    listen_addr: String,
}

impl Default for RawRead {
    fn default() -> Self {
        Self {
            listen_addr: default_read_addr(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawIndex {
    #[serde(default = "default_index_kind")]
    kind: String,
    #[serde(default)]
    database_url: String,
}

impl Default for RawIndex {
    fn default() -> Self {
        Self {
            kind: default_index_kind(),
            database_url: String::new(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawObjects {
    #[serde(default = "default_objects_kind")]
    kind: String,
    #[serde(default = "default_objects_dir")]
    dir: String,
    #[serde(default = "default_bucket")]
    bucket: String,
    #[serde(default)]
    endpoint: String,
    #[serde(default)]
    region: String,
    #[serde(default)]
    access_key: String,
    #[serde(default)]
    secret_key: String,
}

impl Default for RawObjects {
    fn default() -> Self {
        Self {
            kind: default_objects_kind(),
            dir: default_objects_dir(),
            bucket: default_bucket(),
            endpoint: String::new(),
            region: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
        }
    }
}

fn default_sink_addr() -> String {
    "127.0.0.1:8080".to_string()
}
fn default_read_addr() -> String {
    "127.0.0.1:8081".to_string()
}
fn default_max_body_bytes() -> usize {
    HARD_MAX_BODY_BYTES
}
fn default_allowed_kinds() -> Vec<String> {
    vec![Kind::InvestigationReport.as_str().to_string()]
}
fn default_index_kind() -> String {
    "memory".to_string()
}
fn default_objects_kind() -> String {
    "file".to_string()
}
fn default_objects_dir() -> String {
    "data/objects".to_string()
}
fn default_bucket() -> String {
    "wist-evidence-dev".to_string()
}

impl RawConfig {
    fn resolve(self, env: &EnvDict) -> anyhow::Result<Config> {
        if self.sink.max_body_bytes == 0 || self.sink.max_body_bytes > HARD_MAX_BODY_BYTES {
            bail!(
                "sink.max_body_bytes must be in 1..={HARD_MAX_BODY_BYTES}, got {}",
                self.sink.max_body_bytes
            );
        }
        if self.sink.tokens.is_empty() {
            bail!("sink.tokens is empty: without a token every write would be rejected");
        }

        let mut tokens = TokenMap::new();
        for (raw_token, raw) in self.sink.tokens {
            // 站点 token 本身就是**键**：允许多写一个 `${SEC_...}`，避免把 bearer 值写进配置文件。
            let token = expand(env, &raw_token)?;
            if token.trim().is_empty() {
                bail!("sink.tokens has an empty token");
            }
            if !crate::domain::artifact::is_safe_segment(raw.tenant_id.trim()) {
                bail!("sink.tokens.<token>.tenant_id must be a safe key segment");
            }
            if raw.customer_id.trim().is_empty() {
                bail!("sink.tokens.<token>.customer_id must not be empty");
            }
            tokens.insert(
                token,
                SinkCaller {
                    tenant_id: raw.tenant_id.trim().to_string(),
                    customer_id: raw.customer_id.trim().to_string(),
                },
            );
        }

        let mut allowed_kinds = Vec::new();
        for raw in &self.sink.allowed_kinds {
            let kind = Kind::parse(raw.trim())
                .ok_or_else(|| anyhow::anyhow!("sink.allowed_kinds has unknown kind {raw:?}"))?;
            if !allowed_kinds.contains(&kind) {
                allowed_kinds.push(kind);
            }
        }
        if allowed_kinds.is_empty() {
            bail!("sink.allowed_kinds must list at least one kind");
        }

        let index_kind = match self.index.kind.trim() {
            "memory" => IndexKind::Memory,
            "postgres" => IndexKind::Postgres,
            other => bail!("index.kind must be memory|postgres, got {other:?}"),
        };
        let database_url = if self.index.database_url.trim().is_empty() {
            None
        } else {
            Some(expand(env, &self.index.database_url)?)
        };
        if index_kind == IndexKind::Postgres && database_url.is_none() {
            bail!("index.kind = postgres requires index.database_url");
        }

        let objects_kind = match self.objects.kind.trim() {
            "file" => ObjectsKind::File,
            "s3" => ObjectsKind::S3,
            other => bail!(
                "objects.kind must be file|s3, got {other:?} \
                 （from 0.1.7 the memory backend is gone）"
            ),
        };
        if self.objects.bucket.trim().is_empty() {
            bail!("objects.bucket must not be empty");
        }
        let dir = optional_expanded(env, &self.objects.dir)?;
        let endpoint = optional_expanded(env, &self.objects.endpoint)?;
        let region = optional_expanded(env, &self.objects.region)?;
        let access_key = optional_expanded(env, &self.objects.access_key)?;
        let secret_key = optional_expanded(env, &self.objects.secret_key)?;
        if objects_kind == ObjectsKind::File && dir.is_none() {
            // 相对路径相对进程工作目录；容器里给绝对路径（挂载点）。
            bail!("objects.kind = file requires objects.dir");
        }
        if objects_kind == ObjectsKind::S3 {
            if endpoint.is_none() || region.is_none() {
                bail!("objects.kind = s3 requires objects.endpoint and objects.region");
            }
            if access_key.is_none() || secret_key.is_none() {
                bail!("objects.kind = s3 requires objects.access_key and objects.secret_key");
            }
        }

        Ok(Config {
            sink: SinkConfig {
                listen_addr: self.sink.listen_addr,
                max_body_bytes: self.sink.max_body_bytes,
                allowed_kinds,
                tokens,
            },
            read: ReadConfig {
                listen_addr: self.read.listen_addr,
            },
            index: IndexConfig {
                kind: index_kind,
                database_url,
            },
            objects: ObjectsConfig {
                kind: objects_kind,
                dir,
                bucket: self.objects.bucket,
                endpoint,
                region,
                access_key,
                secret_key,
            },
        })
    }
}

fn optional_expanded(env: &EnvDict, value: &str) -> anyhow::Result<Option<String>> {
    if value.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(expand(env, value)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(raw: &str) -> anyhow::Result<Config> {
        let raw: RawConfig = toml::from_str(raw)?;
        raw.resolve(&EnvDict::new())
    }

    #[test]
    fn token_key_can_come_from_the_environment() {
        let mut env = EnvDict::new();
        env.insert("SEC_EVIDENCE_SINK_TOKEN", ValueType::from("tok_secret"));
        let raw: RawConfig = toml::from_str(
            r#"
            [sink.tokens."${SEC_EVIDENCE_SINK_TOKEN}"]
            tenant_id = "t-001"
            customer_id = "c-001"
            "#,
        )
        .expect("toml");
        let config = raw
            .resolve(&env)
            .expect("token key should expand from the secret dict");
        assert!(config.sink.tokens.contains_key("tok_secret"));
        assert_eq!(config.sink.tokens.len(), 1);
    }

    #[test]
    fn defaults_are_dev_safe() {
        let config = parse(
            r#"
            [sink.tokens.tok_dev]
            tenant_id = "t-001"
            customer_id = "c-001"
            "#,
        )
        .expect("dev config should parse");
        assert_eq!(config.index.kind, IndexKind::Memory);
        // 没有「内存对象存储」这一档：默认就是本地目录，重启不丢。
        assert_eq!(config.objects.kind, ObjectsKind::File);
        assert_eq!(config.objects.dir.as_deref(), Some("data/objects"));
        assert_eq!(config.sink.allowed_kinds, vec![Kind::InvestigationReport]);
        assert_eq!(config.sink.tokens.len(), 1);
    }

    #[test]
    fn file_objects_require_a_directory() {
        let error = parse(
            r#"
            [sink.tokens.tok_dev]
            tenant_id = "t-001"
            customer_id = "c-001"
            [objects]
            kind = "file"
            dir = ""
            "#,
        )
        .expect_err("file without dir must fail");
        assert!(error.to_string().contains("requires objects.dir"));
    }

    #[test]
    fn s3_objects_require_credentials() {
        let error = parse(
            r#"
            [sink.tokens.tok_dev]
            tenant_id = "t-001"
            customer_id = "c-001"
            [objects]
            kind = "s3"
            endpoint = "https://cos.ap-shanghai.myqcloud.com"
            region = "ap-shanghai"
            "#,
        )
        .expect_err("s3 without keys must fail");
        assert!(error.to_string().contains("objects.access_key"));
    }

    #[test]
    fn blank_objects_dir_is_rejected() {
        let error = parse(
            r#"
            [sink.tokens.tok_dev]
            tenant_id = "t-001"
            customer_id = "c-001"
            [objects]
            kind = "file"
            dir = "   "
            "#,
        )
        .expect_err("blank dir must fail");
        assert!(error.to_string().contains("requires objects.dir"));
    }

    #[test]
    fn s3_objects_require_endpoint_and_region() {
        let error = parse(
            r#"
            [sink.tokens.tok_dev]
            tenant_id = "t-001"
            customer_id = "c-001"
            [objects]
            kind = "s3"
            access_key = "id"
            secret_key = "key"
            "#,
        )
        .expect_err("s3 without endpoint must fail");
        assert!(
            error
                .to_string()
                .contains("objects.endpoint and objects.region")
        );
    }

    #[test]
    fn empty_bucket_is_rejected() {
        for kind in ["file", "s3"] {
            let error = parse(&format!(
                r#"
                [sink.tokens.tok_dev]
                tenant_id = "t-001"
                customer_id = "c-001"
                [objects]
                kind = "{kind}"
                bucket = ""
                endpoint = "https://cos.example.com"
                region = "ap-shanghai"
                access_key = "id"
                secret_key = "key"
                "#
            ))
            .expect_err("empty bucket must fail");
            assert!(error.to_string().contains("objects.bucket"));
        }
    }

    #[test]
    fn objects_dir_is_kept_verbatim() {
        // 库里的路径决定数据落点：配置层不做任何规范化（不补斜杠、不 trim）。
        let config = parse(
            r#"
            [sink.tokens.tok_dev]
            tenant_id = "t-001"
            customer_id = "c-001"
            [objects]
            kind = "file"
            dir = "./data/objects"
            "#,
        )
        .expect("relative dir is allowed");
        assert_eq!(config.objects.dir.as_deref(), Some("./data/objects"));
    }

    #[test]
    fn unknown_objects_kind_is_rejected() {
        let error = parse(
            r#"
            [sink.tokens.tok_dev]
            tenant_id = "t-001"
            customer_id = "c-001"
            [objects]
            kind = "memory"
            "#,
        )
        .expect_err("memory is no longer a deployable kind");
        assert!(error.to_string().contains("file|s3"));
    }

    #[test]
    fn empty_tokens_are_rejected() {
        let error = parse("").expect_err("no token must fail");
        assert!(error.to_string().contains("sink.tokens is empty"));
    }

    #[test]
    fn postgres_without_url_is_rejected() {
        let error = parse(
            r#"
            [sink.tokens.tok_dev]
            tenant_id = "t-001"
            customer_id = "c-001"
            [index]
            kind = "postgres"
            "#,
        )
        .expect_err("postgres without url must fail");
        assert!(error.to_string().contains("requires index.database_url"));
    }

    #[test]
    fn tenant_must_be_a_safe_key_segment() {
        let error = parse(
            r#"
            [sink.tokens.tok_dev]
            tenant_id = "../other"
            customer_id = "c-001"
            "#,
        )
        .expect_err("traversal tenant must fail");
        assert!(error.to_string().contains("safe key segment"));
    }

    #[test]
    fn unknown_kind_is_rejected() {
        let error = parse(
            r#"
            [sink]
            allowed_kinds = ["mystery_report"]

            [sink.tokens.tok_dev]
            tenant_id = "t-001"
            customer_id = "c-001"
            "#,
        )
        .expect_err("unknown kind must fail");
        assert!(error.to_string().contains("unknown kind"));
    }
}
