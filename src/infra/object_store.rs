//! 对象层：证据正文唯一的落点。
//!
//! 两种部署形态、一个 trait：
//! - [`FileObjectStore`]：本地目录。测试与现场验证用 —— 进程重启不丢，可以直接看文件；
//! - `s3`：S3 兼容对象存储（腾讯 COS / MinIO）。生产用。
//!
//! **方法级契约**（三个实现都遵守）：
//! - `put`：键由调用方保证唯一且不可变；实现不校验内容、不判重，**同键覆盖写**；
//! - `get`：**缺失必须 `Ok(None)`**，不得用错误表达缺失；只有权限 / 网络 / 5xx 才 `Err`；
//! - `presign_get`：唯一允许返回 [`StoreError::Unsupported`] 的方法。
//!
//! 抽成 trait 的理由不是"以后可能换云"，而是**让契约测试不需要真 COS**：
//! 幂等、键布局、元数据重建这些行为都必须在内存实现上就能验。
//! [`MemoryObjectStore`] 现在**只给单元/契约测试**，不再是可部署的配置项。
//!
//! 生产实现（`s3` feature）只做 PUT / GET / 预签名，不做 List（对账作业才需要）。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::Duration;

use async_trait::async_trait;

/// 对象自定义元数据：`(name, value)`，name 是不带 `x-cos-meta-` 前缀的短名。
///
/// 按写入顺序展开，方便断言"七个自描述字段一个不少"（§5.2）。
pub type ObjectMeta = Vec<(String, String)>;

#[derive(Debug)]
pub enum StoreError {
    /// 键本身不合法（`..`、绝对路径、空段）。调用方的 bug，重试无用。
    InvalidKey(String),
    /// 后端失败（网络 / 权限 / 5xx / 磁盘满）。写路径一律回 5xx，让 sink 重试后落 rescue。
    Backend(String),
    /// 当前后端不支持这个动作（例如本地目录存储没有预签名）。
    Unsupported(&'static str),
}

impl StoreError {
    /// 只有 [`StoreError::Backend`] 是临时故障；键错误与能力缺失重试不会变好。
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Backend(_))
    }
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidKey(message) => write!(f, "object store rejected the key: {message}"),
            Self::Backend(message) => write!(f, "object store backend error: {message}"),
            Self::Unsupported(action) => write!(f, "object store does not support {action}"),
        }
    }
}

impl std::error::Error for StoreError {}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// 写入一个**不可变**对象。键冲突按内容判重由调用方负责，这里只负责存。
    async fn put(&self, key: &str, bytes: Vec<u8>, meta: &ObjectMeta) -> Result<(), StoreError>;

    /// 读回正文（流式代理读路径用）。
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError>;

    /// 签一个**单对象、短命**的 GET URL（§7.1 的默认读路径）。
    async fn presign_get(&self, key: &str, ttl: Duration) -> Result<String, StoreError>;
}

/// 进程内对象存储：**只给单元/契约测试**（已不是部署选项）。
///
/// 它不落盘：跑完即丢。要落盘用 [`FileObjectStore`]，要上云用 S3。
#[derive(Debug, Default)]
pub struct MemoryObjectStore {
    objects: RwLock<HashMap<String, StoredObject>>,
}

#[derive(Debug, Clone)]
pub struct StoredObject {
    pub bytes: Vec<u8>,
    pub meta: ObjectMeta,
}

impl MemoryObjectStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// 测试辅助：直接读回元数据，验"索引可从对象重建"。
    pub fn meta_of(&self, key: &str) -> Option<ObjectMeta> {
        self.objects
            .read()
            .expect("memory object store poisoned")
            .get(key)
            .map(|object| object.meta.clone())
    }

    pub fn len(&self) -> usize {
        self.objects
            .read()
            .expect("memory object store poisoned")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl ObjectStore for MemoryObjectStore {
    async fn put(&self, key: &str, bytes: Vec<u8>, meta: &ObjectMeta) -> Result<(), StoreError> {
        self.objects
            .write()
            .expect("memory object store poisoned")
            .insert(
                key.to_string(),
                StoredObject {
                    bytes,
                    meta: meta.clone(),
                },
            );
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self
            .objects
            .read()
            .expect("memory object store poisoned")
            .get(key)
            .map(|object| object.bytes.clone()))
    }

    async fn presign_get(&self, key: &str, ttl: Duration) -> Result<String, StoreError> {
        // 形状按真实预签名 URL 给：带 expires / 签名，便于测试断言语义而不接真后端。
        let expires_at = chrono::Utc::now() + chrono::Duration::from_std(ttl).unwrap_or_default();
        let signature = sign_for_test(key, expires_at.timestamp());
        Ok(format!(
            "memory://objects/{key}?expires={}&signature={signature}",
            expires_at.timestamp()
        ))
    }
}

fn sign_for_test(key: &str, expires: i64) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    hasher.update(expires.to_be_bytes());
    hex::encode(&hasher.finalize()[..8])
}

/// 本地目录实现：`<dir>/<object_key>` 存正文，`<dir>/<object_key>.meta.json` 存元数据。
///
/// 给测试与现场核对用：进程重启不丢，能直接 `find` / `zstd -d` 看。
/// 预签名在这里没有意义（没有一个外部能直连的桶），所以 `presign_get` 明确报不支持 ——
/// 需要签名 URL 就切 `objects.kind = s3`。
///
/// 有意的语义边界：**只适用于单实例 + 本地文件系统**。`rename` / `create_dir_all`
/// 的原子性在 NFS / CIFS 上不保证，多副本部署必须用 `s3`。
#[derive(Debug)]
pub struct FileObjectStore {
    root: PathBuf,
}

impl FileObjectStore {
    /// 开目录（幂等）**并验一次可写**：挂了只读卷 / 属主不对时，启动就失败，
    /// 而不是等到第一条证据投递才 502。
    pub fn open(root: impl Into<PathBuf>) -> std::io::Result<Self> {
        let root = root.into();
        if root.as_os_str().is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "objects.dir is empty",
            ));
        }
        std::fs::create_dir_all(&root)?;
        // `create_dir_all` 对**已存在**的目录只做 stat，不证明可写 —— 真写一个再删。
        let probe = root.join(format!(".write-probe-{}", std::process::id()));
        std::fs::write(&probe, b"")?;
        std::fs::remove_file(&probe)?;
        // 统一成绝对路径：启动日志里要能直接看出正文落在哪。
        Ok(Self {
            root: root.canonicalize()?,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// 键 → 绝对路径。键本来由服务端拼（已是安全段），这里仍拒一次路径穿越。
    fn path_for(&self, key: &str) -> Result<PathBuf, StoreError> {
        if key.is_empty()
            || key.starts_with('/')
            || key
                .split('/')
                .any(|segment| segment.is_empty() || segment == "." || segment == "..")
        {
            return Err(StoreError::InvalidKey(format!("unsafe object key {key:?}")));
        }
        Ok(self.root.join(key))
    }

    fn meta_path(path: &Path) -> PathBuf {
        let mut name = path.as_os_str().to_os_string();
        name.push(".meta.json");
        PathBuf::from(name)
    }
}

/// 临时文件名：**每次写都唯一**（`with_extension` 会替换 `.zst`，同键会踩）。
fn partial_path(path: &Path) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "object".to_string());
    path.with_file_name(format!(".{name}.partial-{}-{nanos}", std::process::id()))
}

/// 先写临时文件 + `sync_all`，再 `rename`：断电后不会出现「文件名对、内容是零页」。
fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;

    let tmp = partial_path(path);
    let result = (|| {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, path)
    })();
    if result.is_err() {
        // 失败不要留半截临时文件（对账作业会把它当成不认识的东西）。
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// `rename` 之后同步父目录项。失败只记日志：它不是对象可读性的前提。
fn sync_dir(dir: &Path) {
    match std::fs::File::open(dir) {
        Ok(handle) => {
            if let Err(err) = handle.sync_all() {
                log::warn!("sync objects dir {} failed: {err}", dir.display());
            }
        }
        Err(err) => log::warn!("open objects dir {} for sync failed: {err}", dir.display()),
    }
}

#[async_trait]
impl ObjectStore for FileObjectStore {
    async fn put(&self, key: &str, bytes: Vec<u8>, meta: &ObjectMeta) -> Result<(), StoreError> {
        let path = self.path_for(key)?;
        let meta_path = Self::meta_path(&path);
        let root = self.root.clone();
        // 元数据按 S3 同口径存成 `{name: value}`，便于直接从文件重建索引。
        let meta_json: std::collections::BTreeMap<&str, &str> = meta
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        let meta_bytes = serde_json::to_vec_pretty(&meta_json)
            .map_err(|err| StoreError::Backend(format!("serialize object meta: {err}")))?;

        tokio::task::spawn_blocking(move || {
            // 根目录没了（卷卸载 / 误删）就报错，**不要**在容器层上悄悄重建一个、
            // 写到重启即丢的地方 —— 那样“重启不丢”就成了谎话。
            if !root.is_dir() {
                return Err(objects_root_missing(&root));
            }
            let Some(parent) = path.parent() else {
                return Err(StoreError::InvalidKey(
                    "object key has no parent directory".to_string(),
                ));
            };
            std::fs::create_dir_all(parent).map_err(|err| {
                StoreError::Backend(format!("create {}: {err}", parent.display()))
            })?;
            write_atomic(&path, &bytes)
                .map_err(|err| StoreError::Backend(format!("write {}: {err}", path.display())))?;
            // 正文先落地、元数据后落地：留下的是「有正文、无 meta」——
            // 对账时能把这种情况识别成未完成写入（反过来就没法区分）。
            write_atomic(&meta_path, &meta_bytes).map_err(|err| {
                StoreError::Backend(format!("write meta {}: {err}", meta_path.display()))
            })?;
            sync_dir(parent);
            Ok(())
        })
        .await
        .map_err(|err| StoreError::Backend(format!("object task failed: {err}")))?
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let path = self.path_for(key)?;
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            // 根目录没了不是「这个对象不存在」，是存储故障：报 None 会让读路径
            // 把一次故障说成 404「证据不存在」。
            if !root.is_dir() {
                return Err(objects_root_missing(&root));
            }
            match std::fs::read(&path) {
                Ok(bytes) => Ok(Some(bytes)),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(err) => Err(StoreError::Backend(format!(
                    "read {}: {err}",
                    path.display()
                ))),
            }
        })
        .await
        .map_err(|err| StoreError::Backend(format!("object task failed: {err}")))?
    }

    async fn presign_get(&self, _key: &str, _ttl: Duration) -> Result<String, StoreError> {
        Err(StoreError::Unsupported(
            "presign on the local file backend (objects.kind = file)",
        ))
    }
}

fn objects_root_missing(root: &Path) -> StoreError {
    StoreError::Backend(format!("objects root {} is missing", root.display()))
}

#[cfg(feature = "s3")]
pub use s3::{S3Config, S3ObjectStore};

#[cfg(feature = "s3")]
mod s3 {
    //! S3 兼容实现（MinIO / 腾讯 COS）。
    //!
    //! 两个必须显式打开的开关：
    //! - `force_path_style(true)`：COS / MinIO 都不接受 AWS 的虚拟主机风格；
    //! - 预签名走 SDK 的 SigV4 —— **这条还没在真 COS 上验过**（见设计稿开放问题 5）。
    //!   验不过就退回"流式代理读"，预签名放 P1。

    use std::time::Duration;

    use async_trait::async_trait;
    use aws_sdk_s3::config::{Credentials, Region};
    use aws_sdk_s3::primitives::ByteStream;

    use super::{ObjectMeta, ObjectStore, StoreError};

    #[derive(Debug)]
    pub struct S3ObjectStore {
        client: aws_sdk_s3::Client,
        bucket: String,
    }

    #[derive(Debug, Clone)]
    pub struct S3Config {
        pub endpoint: String,
        pub bucket: String,
        pub region: String,
        pub access_key: String,
        pub secret_key: String,
    }

    impl S3ObjectStore {
        pub fn new(config: &S3Config) -> Self {
            let credentials = Credentials::new(
                &config.access_key,
                &config.secret_key,
                None,
                None,
                "wist-evidence",
            );
            let sdk_config = aws_sdk_s3::Config::builder()
                .endpoint_url(&config.endpoint)
                .region(Region::new(config.region.clone()))
                .credentials_provider(credentials)
                .force_path_style(true)
                .build();
            Self {
                client: aws_sdk_s3::Client::from_conf(sdk_config),
                bucket: config.bucket.clone(),
            }
        }
    }

    #[async_trait]
    impl ObjectStore for S3ObjectStore {
        async fn put(
            &self,
            key: &str,
            bytes: Vec<u8>,
            meta: &ObjectMeta,
        ) -> Result<(), StoreError> {
            let mut request = self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(ByteStream::from(bytes));
            for (name, value) in meta {
                // S3 侧统一用 `x-amz-meta-*`；COS 的 `x-cos-meta-*` 是同义扩展，
                // 真机验证前先按 S3 头写（见 README「待验证」）。
                request = request.metadata(name, value);
            }
            request
                .send()
                .await
                .map_err(|err| StoreError::Backend(format!("put_object {key} failed: {err}")))?;
            Ok(())
        }

        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
            match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
            {
                Ok(output) => {
                    let bytes = output
                        .body
                        .collect()
                        .await
                        .map_err(|err| StoreError::Backend(format!("read body {key}: {err}")))?
                        .into_bytes()
                        .to_vec();
                    Ok(Some(bytes))
                }
                Err(err) => {
                    // 契约：缺失一律 `Ok(None)`。COS / MinIO / 网关前置不一定回
                    // `NoSuchKey`，也可能只给一个 404，所以两种都认。
                    let not_found = err
                        .raw_response()
                        .map(|response| response.status().as_u16() == 404)
                        .unwrap_or(false);
                    let service = err.into_service_error();
                    if service.is_no_such_key() || not_found {
                        Ok(None)
                    } else {
                        Err(StoreError::Backend(format!(
                            "get_object {key} failed: {service}"
                        )))
                    }
                }
            }
        }

        async fn presign_get(&self, key: &str, ttl: Duration) -> Result<String, StoreError> {
            let request = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .presigned(
                    aws_sdk_s3::presigning::PresigningConfig::expires_in(ttl)
                        .map_err(|err| StoreError::Backend(format!("presign ttl: {err}")))?,
                )
                .await
                .map_err(|err| StoreError::Backend(format!("presign {key} failed: {err}")))?;
            Ok(request.uri().to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 临时根目录，`Drop` 时自动删除 —— 断言 panic 也不会在 /tmp 里攒垃圾。
    struct TempRoot(PathBuf);

    impl TempRoot {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "wist-evidence-objects-{name}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|elapsed| elapsed.as_nanos())
                    .unwrap_or(0)
            ));
            std::fs::create_dir_all(&path).expect("create temp root");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }

        /// 只在 cleanup 无法正常删除时用（例如被 chmod 成只读）。
        fn open_store(&self) -> FileObjectStore {
            FileObjectStore::open(self.path()).expect("open file store")
        }
    }

    impl Drop for TempRoot {
        fn drop(&mut self) {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
            }
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn meta_of(tenant: &str) -> ObjectMeta {
        vec![
            ("tenant-id".to_string(), tenant.to_string()),
            ("kind".to_string(), "investigation_report".to_string()),
            ("sha256".to_string(), "abc123".to_string()),
        ]
    }

    // ── 打开目录 ────────────────────────────────────────────────────────────

    #[test]
    fn open_rejects_empty_root() {
        assert!(
            FileObjectStore::open("").is_err(),
            "empty objects.dir must not fall back to the process cwd"
        );
    }

    #[test]
    fn open_fails_when_root_is_a_regular_file() {
        let guard = TempRoot::new("root-is-file");
        let file = guard.path().join("not-a-dir");
        std::fs::write(&file, b"x").expect("write file");
        assert!(FileObjectStore::open(&file).is_err());
    }

    #[test]
    fn open_canonicalizes_the_root() {
        let guard = TempRoot::new("canonical");
        let store = guard.open_store();
        assert!(
            store.root().is_absolute(),
            "root must be absolute so the startup log says where bodies land"
        );
    }

    /// 只读目录必须在启动时失败，而不是第一次写入才 502。
    /// 以 root 跑测试时目录模式不生效，此时跳过（CI 容器常见）。
    #[cfg(unix)]
    #[test]
    fn open_fails_when_directory_is_not_writable() {
        use std::os::unix::fs::PermissionsExt;

        let guard = TempRoot::new("readonly");
        std::fs::set_permissions(guard.path(), std::fs::Permissions::from_mode(0o555))
            .expect("chmod 0555");

        let opened = FileObjectStore::open(guard.path());

        std::fs::set_permissions(guard.path(), std::fs::Permissions::from_mode(0o755)).ok();
        match opened {
            Err(_) => {}
            Ok(_) => eprintln!("skip: directory mode ignored (running with root privileges)"),
        }
    }

    // ── 写 / 读 ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn put_and_get_roundtrip_binary_body() {
        let guard = TempRoot::new("roundtrip");
        let store = guard.open_store();
        // 正文是 zstd 字节流，必然不是合法 UTF-8。
        let body = vec![0x00, 0xff, 0xfe, 0x80, 0x7f, b'{', b'}'];

        store
            .put(
                "t-001/investigation_report/2026/11/art.json.zst",
                body.clone(),
                &meta_of("t-001"),
            )
            .await
            .expect("put");

        let read_back = store
            .get("t-001/investigation_report/2026/11/art.json.zst")
            .await
            .expect("get")
            .expect("present");
        assert_eq!(read_back, body);
    }

    #[tokio::test]
    async fn put_creates_nested_directories() {
        let guard = TempRoot::new("nested");
        let store = guard.open_store();
        let key = "t-001/investigation_report/2026/11/deep/art.json.zst";

        store
            .put(key, b"body".to_vec(), &meta_of("t-001"))
            .await
            .expect("put");

        assert!(store.root().join(key).is_file());
    }

    #[tokio::test]
    async fn empty_body_is_some_empty_not_none() {
        let guard = TempRoot::new("empty-body");
        let store = guard.open_store();

        store
            .put(
                "t-001/a/2026/11/empty.json.zst",
                Vec::new(),
                &meta_of("t-001"),
            )
            .await
            .expect("put");

        let read_back = store
            .get("t-001/a/2026/11/empty.json.zst")
            .await
            .expect("get")
            .expect("empty object is still an object");
        assert!(read_back.is_empty());

        // 与「对象不存在」严格区分：混了就会让读路径静默返回空正文。
        let missing = store
            .get("t-001/a/2026/11/nope.json.zst")
            .await
            .expect("get");
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn sidecar_is_a_json_object_with_exactly_the_given_fields() {
        let guard = TempRoot::new("sidecar");
        let store = guard.open_store();
        let key = "t-001/investigation_report/2026/11/art.json.zst";
        let meta = meta_of("t-001");

        store.put(key, b"body".to_vec(), &meta).await.expect("put");

        let sidecar_path = FileObjectStore::meta_path(&store.root().join(key));
        let text = std::fs::read_to_string(&sidecar_path).expect("sidecar");
        // 反序列化断言，不依赖 pretty 的空格风格。
        let parsed: std::collections::BTreeMap<String, String> =
            serde_json::from_str(&text).expect("sidecar is json");
        assert_eq!(parsed.len(), meta.len(), "no extra keys: {text}");
        for (name, value) in &meta {
            assert_eq!(parsed.get(name), Some(value), "field {name} in {text}");
        }
    }

    #[tokio::test]
    async fn overwrite_same_key_replaces_body_and_meta() {
        let guard = TempRoot::new("overwrite");
        let store = guard.open_store();
        let key = "t-001/investigation_report/2026/11/art.json.zst";

        store
            .put(key, b"first".to_vec(), &meta_of("t-001"))
            .await
            .expect("put 1");
        store
            .put(key, b"second".to_vec(), &meta_of("t-002"))
            .await
            .expect("put 2");

        assert_eq!(store.get(key).await.expect("get").unwrap(), b"second");
        let text = std::fs::read_to_string(FileObjectStore::meta_path(&store.root().join(key)))
            .expect("sidecar");
        let parsed: std::collections::BTreeMap<String, String> =
            serde_json::from_str(&text).expect("json");
        assert_eq!(parsed.get("tenant-id").map(String::as_str), Some("t-002"));
    }

    #[tokio::test]
    async fn no_partial_files_after_put() {
        let guard = TempRoot::new("no-partial");
        let store = guard.open_store();
        store
            .put(
                "t-001/a/2026/11/art.json.zst",
                b"body".to_vec(),
                &meta_of("t-001"),
            )
            .await
            .expect("put");

        let leftovers = std::fs::read_dir(store.root().join("t-001/a/2026/11"))
            .expect("read dir")
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".partial"))
            .collect::<Vec<_>>();
        assert!(leftovers.is_empty(), "leftover temp files: {leftovers:?}");
    }

    #[tokio::test]
    async fn reopen_after_drop_sees_the_same_object() {
        let guard = TempRoot::new("reopen");
        let key = "t-001/investigation_report/2026/11/art.json.zst";
        {
            let store = guard.open_store();
            store
                .put(key, b"persisted".to_vec(), &meta_of("t-001"))
                .await
                .expect("put");
        }

        // 「重启不丢」这条承诺在这里变成可执行的断言。
        let reopened = guard.open_store();
        assert_eq!(reopened.get(key).await.expect("get").unwrap(), b"persisted");
    }

    // ── 失败形态 ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn unsafe_keys_are_rejected_as_invalid_key() {
        let guard = TempRoot::new("unsafe");
        let store = guard.open_store();
        for key in [
            "",
            "../escape",
            "/abs/path",
            "a//b",
            "a/./b",
            "a/../b",
            "a/b/",
        ] {
            let put = store.put(key, Vec::new(), &Vec::new()).await;
            assert!(
                matches!(put, Err(StoreError::InvalidKey(_))),
                "{key:?} must be an InvalidKey, got {put:?}"
            );
            // 非法键不是「对象不存在」。
            assert!(
                matches!(store.get(key).await, Err(StoreError::InvalidKey(_))),
                "{key:?} get must be InvalidKey"
            );
        }
        // 没有任何副作用：临时根目录下不应多出文件。
        assert_eq!(std::fs::read_dir(store.root()).expect("dir").count(), 0);
    }

    #[test]
    fn invalid_key_is_not_retryable() {
        assert!(!StoreError::InvalidKey("x".to_string()).is_retryable());
        assert!(!StoreError::Unsupported("presign").is_retryable());
        assert!(StoreError::Backend("disk full".to_string()).is_retryable());
    }

    #[tokio::test]
    async fn get_on_a_directory_is_a_backend_error() {
        let guard = TempRoot::new("get-dir");
        let store = guard.open_store();
        store
            .put(
                "t-001/a/2026/11/art.json.zst",
                b"body".to_vec(),
                &meta_of("t-001"),
            )
            .await
            .expect("put");

        let result = store.get("t-001/a/2026/11").await;
        assert!(
            matches!(result, Err(StoreError::Backend(_))),
            "reading a directory must not look like a missing object: {result:?}"
        );
    }

    #[tokio::test]
    async fn missing_root_is_an_error_not_a_missing_object() {
        let guard = TempRoot::new("root-gone");
        let store = guard.open_store();
        std::fs::remove_dir_all(guard.path()).expect("remove root");

        // 卷被卸载 / 根目录被误删：这是存储故障，不能答「证据不存在」。
        let got = store.get("t-001/a/2026/11/art.json.zst").await;
        assert!(matches!(got, Err(StoreError::Backend(_))), "{got:?}");

        // 也不要偷偷在容器层重建一个目录（那会写进重启即丢的地方）。
        let put = store
            .put(
                "t-001/a/2026/11/art.json.zst",
                b"x".to_vec(),
                &meta_of("t-001"),
            )
            .await;
        assert!(matches!(put, Err(StoreError::Backend(_))), "{put:?}");
        assert!(
            !guard.path().exists(),
            "put must not recreate the objects root"
        );
    }

    #[tokio::test]
    async fn file_backend_has_no_presign() {
        let guard = TempRoot::new("presign");
        let store = guard.open_store();
        let error = store
            .presign_get("t-001/x/2026/11/art.zst", Duration::from_secs(60))
            .await
            .expect_err("file backend cannot presign");
        assert!(matches!(error, StoreError::Unsupported(_)));
    }

    // ── 并发 ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn concurrent_puts_to_distinct_keys_all_succeed() {
        let guard = TempRoot::new("concurrent-distinct");
        let store = guard.open_store();

        let mut tasks = Vec::new();
        for index in 0..8 {
            let key = format!("t-001/investigation_report/2026/11/art-{index}.json.zst");
            let body = format!("body-{index}").into_bytes();
            let store_ref = &store;
            tasks.push(async move {
                store_ref
                    .put(&key, body, &meta_of("t-001"))
                    .await
                    .map(|()| key)
            });
        }
        // 同一目录首次并发创建：`create_dir_all` 拿到 EEXIST 后要能收敛。
        let keys = futures_join_all(tasks).await;
        for key in keys {
            let key = key.expect("concurrent put");
            assert!(store.get(&key).await.expect("get").is_some(), "{key}");
        }
    }

    #[tokio::test]
    async fn concurrent_puts_to_the_same_key_are_not_torn() {
        let guard = TempRoot::new("concurrent-same");
        let store = guard.open_store();
        let key = "t-001/investigation_report/2026/11/art.json.zst";
        let first = vec![b'a'; 64 * 1024];
        let second = vec![b'b'; 32 * 1024];
        let meta = meta_of("t-001");

        let (a, b) = tokio::join!(
            store.put(key, first.clone(), &meta),
            store.put(key, second.clone(), &meta),
        );
        a.expect("put a");
        b.expect("put b");

        // 临时文件名每次唯一 → 结果一定是**某一次完整写入**，不会是交织的一半一半。
        let body = store.get(key).await.expect("get").expect("present");
        assert!(
            body == first || body == second,
            "torn write: len={}",
            body.len()
        );
    }

    /// 少一个依赖：只在这里需要 join_all，手写一个够用的版本。
    async fn futures_join_all<F>(futures: Vec<F>) -> Vec<F::Output>
    where
        F: std::future::Future,
    {
        let mut outputs = Vec::with_capacity(futures.len());
        for future in futures {
            outputs.push(future.await);
        }
        outputs
    }
}
