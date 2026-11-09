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
use crate::domain::kind::Sensitivity;

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
    pub probe: ProbeConfig,
}

/// 写探针：每隔 `interval_secs` 往自己的写路径投一条探针证据，
/// 验证「对象 + 索引」真的能写；探针产物按 `subject_id = _probe` 标记，
/// 过了 `retention_hours` 的每 `retention_hours` 清一次。
#[derive(Debug, Clone)]
pub struct ProbeConfig {
    pub enabled: bool,
    pub interval_secs: u64,
    pub retention_hours: u64,
    /// 覆盖探针要打的写端点 URL；默认由 `sink.listen_addr` 推回环地址。
    pub url: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SinkConfig {
    pub listen_addr: String,
    pub max_body_bytes: usize,
    pub allowed_kinds: Vec<Kind>,
    /// 三字段形状没带的维度，用这三个默认值补。
    pub default_kind: Kind,
    pub default_schema: String,
    pub default_sensitivity: Sensitivity,
}

#[derive(Debug, Clone)]
pub struct ReadConfig {
    pub listen_addr: String,
    /// 读口最小鉴权：配了就必须带 `x-evidence-service-token`。
    ///
    /// P0 的身份头（`x-evidence-tenant` / `x-evidence-scopes`）是调用方自述 —— 内网里
    /// 任何进程改个头就能读任何租户。加一道服务级 token 把「谁可以调读口」收到发钥方，
    /// 比把身份头当授权可靠。没配 = 保持旧行为（仅内网隔离），启动日志会提醒。
    pub service_token: Option<String>,
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
    #[serde(default)]
    probe: RawProbe,
}

#[derive(Debug, Deserialize)]
struct RawSink {
    #[serde(default = "default_sink_addr")]
    listen_addr: String,
    #[serde(default = "default_max_body_bytes")]
    max_body_bytes: usize,
    #[serde(default = "default_allowed_kinds")]
    allowed_kinds: Vec<String>,
    #[serde(default = "default_sink_kind")]
    default_kind: String,
    #[serde(default = "default_sink_schema")]
    default_schema: String,
    #[serde(default = "default_sink_sensitivity")]
    default_sensitivity: String,
}

impl Default for RawSink {
    fn default() -> Self {
        Self {
            listen_addr: default_sink_addr(),
            max_body_bytes: default_max_body_bytes(),
            allowed_kinds: default_allowed_kinds(),
            default_kind: default_sink_kind(),
            default_schema: default_sink_schema(),
            default_sensitivity: default_sink_sensitivity(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawRead {
    #[serde(default = "default_read_addr")]
    listen_addr: String,
    /// 读口服务 token（通常写 `${SEC_EVIDENCE_READ_TOKEN}`）。
    #[serde(default)]
    service_token: Option<String>,
}

impl Default for RawRead {
    fn default() -> Self {
        Self {
            listen_addr: default_read_addr(),
            service_token: None,
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
struct RawProbe {
    #[serde(default)]
    enabled: bool,
    #[serde(default = "default_probe_interval")]
    interval_secs: u64,
    #[serde(default = "default_probe_retention")]
    retention_hours: u64,
    #[serde(default)]
    url: String,
}

impl Default for RawProbe {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_secs: default_probe_interval(),
            retention_hours: default_probe_retention(),
            url: String::new(),
        }
    }
}

fn default_probe_interval() -> u64 {
    300
}
fn default_probe_retention() -> u64 {
    24
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
fn default_sink_kind() -> String {
    Kind::InvestigationReport.as_str().to_string()
}
fn default_sink_schema() -> String {
    "wist.evidence/v1".to_string()
}
fn default_sink_sensitivity() -> String {
    "paths".to_string()
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
        let default_kind = Kind::parse(self.sink.default_kind.trim()).ok_or_else(|| {
            anyhow::anyhow!(
                "sink.default_kind has unknown kind {:?}",
                self.sink.default_kind
            )
        })?;
        let default_sensitivity = Sensitivity::parse(self.sink.default_sensitivity.trim())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "sink.default_sensitivity is unknown: {:?}",
                    self.sink.default_sensitivity
                )
            })?;
        let default_schema = self.sink.default_schema.trim().to_string();
        if default_schema.is_empty() {
            bail!("sink.default_schema must not be empty");
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
        if !allowed_kinds.contains(&default_kind) {
            bail!(
                "sink.default_kind {:?} must be listed in sink.allowed_kinds",
                default_kind.as_str()
            );
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

        // 存储后端也由环境变量选（`EVIDENCE_STORE_KIND=file|s3`）：切换模式不需要改配置文件。
        let raw_kind = expand(env, &self.objects.kind)?;
        let objects_kind = match raw_kind.trim() {
            "file" => ObjectsKind::File,
            "s3" => ObjectsKind::S3,
            other => bail!(
                "objects.kind must be file|s3, got {other:?} \
                 （from 0.1.7 the memory backend is gone）"
            ),
        };
        let bucket = expand(env, &self.objects.bucket)?;
        if bucket.trim().is_empty() {
            bail!("objects.bucket must not be empty");
        }
        let dir = optional_expanded(env, &self.objects.dir)?;
        // COS 四个字段在配置文件里**常驻**（不然切模式还得改文件），所以 file 模式下
        // 它们解析不出来只当「没配」；真选 s3 时再 fail-closed。
        let cos_required = objects_kind == ObjectsKind::S3;
        let endpoint = optional_expanded_lax(env, &self.objects.endpoint, cos_required)?;
        let region = optional_expanded_lax(env, &self.objects.region, cos_required)?;
        let access_key = optional_expanded_lax(env, &self.objects.access_key, cos_required)?;
        let secret_key = optional_expanded_lax(env, &self.objects.secret_key, cos_required)?;
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

        let probe_url = optional_expanded(env, &self.probe.url)?;
        if self.probe.enabled {
            if self.probe.interval_secs == 0 {
                bail!("probe.interval_secs must be > 0 when probe.enabled");
            }
            if self.probe.retention_hours == 0 {
                // 0 会让「清理」变成「每轮删刚写的东西」。
                bail!("probe.retention_hours must be > 0 when probe.enabled");
            }
        }

        Ok(Config {
            sink: SinkConfig {
                // 监听地址也允许 `${VAR}`：容器里让编排决定端口，避免端口写死在
                // 配置、compose 健康检查、上游 base_url 三处各一份。
                listen_addr: expand(env, &self.sink.listen_addr)?,
                max_body_bytes: self.sink.max_body_bytes,
                allowed_kinds,
                default_kind,
                default_schema,
                default_sensitivity,
            },
            read: ReadConfig {
                listen_addr: expand(env, &self.read.listen_addr)?,
                service_token: match self.read.service_token.as_deref() {
                    Some(raw) if !raw.trim().is_empty() => Some(expand(env, raw)?),
                    _ => None,
                },
            },
            index: IndexConfig {
                kind: index_kind,
                database_url,
            },
            objects: ObjectsConfig {
                kind: objects_kind,
                dir,
                bucket,
                endpoint,
                region,
                access_key,
                secret_key,
            },
            probe: ProbeConfig {
                enabled: self.probe.enabled,
                interval_secs: self.probe.interval_secs,
                retention_hours: self.probe.retention_hours,
                url: probe_url,
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

/// 可选字段的宽容版：解析不出来时，`required` 为真才报错，否则当成「没配」。
///
/// 用于 `[objects]` 里常驻的 COS 四字段：本地文件模式下不该逼着部署方
/// 去定义一堆用不到的 COS 凭据，但真选了 `kind = s3` 就必须 fail-closed。
fn optional_expanded_lax(
    env: &EnvDict,
    value: &str,
    required: bool,
) -> anyhow::Result<Option<String>> {
    if value.trim().is_empty() {
        return Ok(None);
    }
    let expanded: String = value.to_string().env_eval(env);
    if expanded.needs_env_eval() {
        if required {
            bail!(
                "unresolved environment variable(s) in {value:?}: {}",
                expanded.list_env_vars().join(", ")
            );
        }
        return Ok(None);
    }
    Ok(Some(expanded))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(raw: &str) -> anyhow::Result<Config> {
        let raw: RawConfig = toml::from_str(raw)?;
        raw.resolve(&EnvDict::new())
    }

    #[test]
    fn probe_defaults_are_off() {
        let config = parse(
            r#"
            "#,
        )
        .expect("minimal config");
        assert!(!config.probe.enabled, "probe must be opt-in");
        assert_eq!(config.probe.interval_secs, 300);
        assert_eq!(config.probe.retention_hours, 24);
    }

    #[test]
    fn enabled_probe_rejects_zero_interval() {
        let error = parse(
            r#"
            [probe]
            enabled = true
            interval_secs = 0
            "#,
        )
        .expect_err("zero interval must fail");
        assert!(error.to_string().contains("probe.interval_secs"));
    }

    #[test]
    fn enabled_probe_rejects_zero_retention() {
        // 0 会让清理变成「每轮删刚写的东西」。
        let error = parse(
            r#"
            [probe]
            enabled = true
            retention_hours = 0
            "#,
        )
        .expect_err("zero retention must fail");
        assert!(error.to_string().contains("probe.retention_hours"));
    }

    #[test]
    fn objects_kind_and_bucket_can_come_from_the_environment() {
        // 切换存储后端不改配置文件：两个栈级变量说了算。
        let mut env = EnvDict::new();
        env.insert("EVIDENCE_STORE_KIND", ValueType::from("s3"));
        env.insert(
            "EVIDENCE_OBJECTS_BUCKET",
            ValueType::from("wist-evidence-1250000000"),
        );
        env.insert(
            "SEC_COS_ENDPOINT",
            ValueType::from("https://cos.ap-shanghai.myqcloud.com"),
        );
        env.insert("SEC_COS_REGION", ValueType::from("ap-shanghai"));
        env.insert("SEC_COS_SECRET_ID", ValueType::from("id"));
        env.insert("SEC_COS_SECRET_KEY", ValueType::from("key"));
        let raw: RawConfig = toml::from_str(
            r#"
            [objects]
            kind = "${EVIDENCE_STORE_KIND}"
            bucket = "${EVIDENCE_OBJECTS_BUCKET}"
            endpoint = "${SEC_COS_ENDPOINT}"
            region = "${SEC_COS_REGION}"
            access_key = "${SEC_COS_SECRET_ID}"
            secret_key = "${SEC_COS_SECRET_KEY}"
            "#,
        )
        .expect("toml");
        let config = raw.resolve(&env).expect("s3 should resolve from env");
        assert_eq!(config.objects.kind, ObjectsKind::S3);
        assert_eq!(config.objects.bucket, "wist-evidence-1250000000");
        assert_eq!(
            config.objects.endpoint.as_deref(),
            Some("https://cos.ap-shanghai.myqcloud.com")
        );
    }

    #[test]
    fn cos_placeholders_are_ignored_in_file_mode() {
        // `[objects]` 里常驻着 COS 的四个 `${SEC_...}`：file 模式下没人定义它们，
        // 不应该因此起不来。
        let mut env = EnvDict::new();
        env.insert("EVIDENCE_STORE_KIND", ValueType::from("file"));
        let raw: RawConfig = toml::from_str(
            r#"
            [objects]
            kind = "${EVIDENCE_STORE_KIND}"
            dir = "/var/lib/wist-evidence/objects"
            endpoint = "${SEC_COS_ENDPOINT}"
            region = "${SEC_COS_REGION}"
            access_key = "${SEC_COS_SECRET_ID}"
            secret_key = "${SEC_COS_SECRET_KEY}"
            "#,
        )
        .expect("toml");
        let config = raw
            .resolve(&env)
            .expect("file mode must not need COS creds");
        assert_eq!(config.objects.kind, ObjectsKind::File);
        assert!(config.objects.endpoint.is_none());
        assert!(config.objects.access_key.is_none());
    }

    #[test]
    fn cos_placeholders_are_required_in_s3_mode() {
        let mut env = EnvDict::new();
        env.insert("EVIDENCE_STORE_KIND", ValueType::from("s3"));
        let raw: RawConfig = toml::from_str(
            r#"
            [objects]
            kind = "${EVIDENCE_STORE_KIND}"
            endpoint = "${SEC_COS_ENDPOINT}"
            region = "ap-shanghai"
            access_key = "id"
            secret_key = "key"
            "#,
        )
        .expect("toml");
        let error = raw.resolve(&env).expect_err("s3 must fail closed");
        assert!(
            error
                .to_string()
                .contains("unresolved environment variable"),
            "{error}"
        );
        assert!(error.to_string().contains("SEC_COS_ENDPOINT"), "{error}");
    }

    #[test]
    fn unresolved_store_kind_is_rejected() {
        // 栈级变量没传进来时不能静默退回某个后端。
        let error = parse(
            r#"
            [objects]
            kind = "${EVIDENCE_STORE_KIND}"
            "#,
        )
        .expect_err("unresolved kind must fail");
        assert!(
            error
                .to_string()
                .contains("unresolved environment variable"),
            "{error}"
        );
    }

    #[test]
    fn listen_addr_can_come_from_the_environment() {
        // 容器里让编排决定端口：配置、compose 健康检查、上游 base_url 只认一个来源。
        let mut env = EnvDict::new();
        env.insert("EVIDENCE_READ_PORT", ValueType::from("9443"));
        env.insert("EVIDENCE_SINK_PORT", ValueType::from("9080"));
        let raw: RawConfig = toml::from_str(
            r#"
            [sink]
            listen_addr = "0.0.0.0:${EVIDENCE_SINK_PORT}"
            [read]
            listen_addr = "0.0.0.0:${EVIDENCE_READ_PORT}"
            "#,
        )
        .expect("toml");
        let config = raw.resolve(&env).expect("listen addrs should expand");
        assert_eq!(config.sink.listen_addr, "0.0.0.0:9080");
        assert_eq!(config.read.listen_addr, "0.0.0.0:9443");
    }

    #[test]
    fn unresolved_listen_addr_is_rejected() {
        let error = parse(
            r#"
            [read]
            listen_addr = "0.0.0.0:${EVIDENCE_READ_PORT}"
            "#,
        )
        .expect_err("unresolved port must not become a literal bind address");
        assert!(
            error
                .to_string()
                .contains("unresolved environment variable"),
            "{error}"
        );
    }

    #[test]
    fn defaults_are_dev_safe() {
        let config = parse(
            r#"
            "#,
        )
        .expect("dev config should parse");
        assert_eq!(config.index.kind, IndexKind::Memory);
        // 没有「内存对象存储」这一档：默认就是本地目录，重启不丢。
        assert_eq!(config.objects.kind, ObjectsKind::File);
        assert_eq!(config.objects.dir.as_deref(), Some("data/objects"));
        assert_eq!(config.sink.allowed_kinds, vec![Kind::InvestigationReport]);
        assert_eq!(config.sink.default_kind, Kind::InvestigationReport);
    }

    #[test]
    fn file_objects_require_a_directory() {
        let error = parse(
            r#"
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
            [objects]
            kind = "memory"
            "#,
        )
        .expect_err("memory is no longer a deployable kind");
        assert!(error.to_string().contains("file|s3"));
    }

    #[test]
    fn postgres_without_url_is_rejected() {
        let error = parse(
            r#"
            [index]
            kind = "postgres"
            "#,
        )
        .expect_err("postgres without url must fail");
        assert!(error.to_string().contains("requires index.database_url"));
    }

    #[test]
    fn unknown_kind_is_rejected() {
        let error = parse(
            r#"
            [sink]
            allowed_kinds = ["mystery_report"]

            "#,
        )
        .expect_err("unknown kind must fail");
        assert!(error.to_string().contains("unknown kind"));
    }
}
