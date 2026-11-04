//! 对象层：证据正文唯一的落点。
//!
//! 两种部署形态、一个 trait：
//! - [`FileObjectStore`]：本地目录。测试与现场验证用 —— 重启不丢，可以直接看文件；
//! - `s3`：S3 兼容对象存储（腾讯 COS / MinIO）。生产用。
//!
//! 抽成 trait 的理由不是"以后可能换云"，而是**让契约测试不需要真 COS**：
//! 幂等、键布局、元数据重建这些行为都必须在内存实现上就能验。
//! [`MemoryObjectStore`] 现在**只给单元测试**，不再是可部署的配置项。
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
    /// 后端失败（网络 / 权限 / 5xx）。写路径一律回 5xx，让 sink 重试后落 rescue。
    Backend(String),
    /// 当前后端不支持这个动作（例如内存实现无法给真客户端发 URL）。
    Unsupported(&'static str),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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
/// 给测试与现场核对用：重启不丢，能直接 `find` / `zstd -d` 看。
/// 预签名在这里没有意义（没有一个外部能直连的桶），所以 `presign_get` 明确报不支持 ——
/// 需要签名 URL 就切 `objects.kind = s3`。
#[derive(Debug)]
pub struct FileObjectStore {
    root: PathBuf,
}

impl FileObjectStore {
    /// 开目录（幂等）。拿不到写权限就在启动时失败，而不是到第一次写入才炸。
    pub fn open(root: impl Into<PathBuf>) -> std::io::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
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
            return Err(StoreError::Backend(format!("unsafe object key {key:?}")));
        }
        Ok(self.root.join(key))
    }

    fn meta_path(path: &Path) -> PathBuf {
        let mut name = path.as_os_str().to_os_string();
        name.push(".meta.json");
        PathBuf::from(name)
    }
}

#[async_trait]
impl ObjectStore for FileObjectStore {
    async fn put(&self, key: &str, bytes: Vec<u8>, meta: &ObjectMeta) -> Result<(), StoreError> {
        let path = self.path_for(key)?;
        let meta_path = Self::meta_path(&path);
        // 元数据按 S3 同口径存成 `{name: value}`，便于直接从文件重建索引。
        let meta_json: std::collections::BTreeMap<&str, &str> = meta
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        let meta_bytes = serde_json::to_vec_pretty(&meta_json)
            .map_err(|err| StoreError::Backend(format!("serialize object meta: {err}")))?;

        tokio::task::spawn_blocking(move || {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|err| {
                    StoreError::Backend(format!("create {}: {err}", parent.display()))
                })?;
            }
            // 先写临时文件再 rename：半截文件不会被当成完整对象读到。
            let tmp = path.with_extension("partial");
            std::fs::write(&tmp, &bytes)
                .map_err(|err| StoreError::Backend(format!("write {}: {err}", tmp.display())))?;
            std::fs::rename(&tmp, &path)
                .map_err(|err| StoreError::Backend(format!("rename {}: {err}", path.display())))?;
            std::fs::write(&meta_path, &meta_bytes).map_err(|err| {
                StoreError::Backend(format!("write {}: {err}", meta_path.display()))
            })?;
            Ok(())
        })
        .await
        .map_err(|err| StoreError::Backend(format!("object task failed: {err}")))?
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let path = self.path_for(key)?;
        tokio::task::spawn_blocking(move || match std::fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(StoreError::Backend(format!(
                "read {}: {err}",
                path.display()
            ))),
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
                    let service = err.into_service_error();
                    if service.is_no_such_key() {
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

    fn temp_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "wist-evidence-objects-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos())
                .unwrap_or(0)
        ))
    }

    #[tokio::test]
    async fn file_backend_writes_body_and_metadata_sidecar() {
        let root = temp_root("roundtrip");
        let store = FileObjectStore::open(&root).expect("open");
        let key = "t-001/investigation_report/2026/11/art_01.zst";
        let meta = vec![
            ("tenant-id".to_string(), "t-001".to_string()),
            ("sha256".to_string(), "abc".to_string()),
        ];

        store
            .put(key, b"compressed-body".to_vec(), &meta)
            .await
            .expect("put");

        // 正文落在磁盘上（重启不丢），元数据在同一目录的 sidecar 里。
        let body = std::fs::read(root.join(key)).expect("body file");
        assert_eq!(body, b"compressed-body");
        let sidecar = std::fs::read_to_string(FileObjectStore::meta_path(&root.join(key)))
            .expect("meta sidecar");
        assert!(sidecar.contains("\"tenant-id\": \"t-001\""));

        let read_back = store.get(key).await.expect("get").expect("present");
        assert_eq!(read_back, b"compressed-body");
        assert!(
            store
                .get("t-001/missing/2026/11/nope.zst")
                .await
                .expect("get")
                .is_none()
        );

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn file_backend_rejects_unsafe_keys() {
        let root = temp_root("unsafe");
        let store = FileObjectStore::open(&root).expect("open");
        for key in ["../escape", "/abs/path", "a//b", "a/./b"] {
            assert!(
                store.put(key, Vec::new(), &Vec::new()).await.is_err(),
                "{key} must be rejected"
            );
        }
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn file_backend_has_no_presign() {
        let root = temp_root("presign");
        let store = FileObjectStore::open(&root).expect("open");
        let error = store
            .presign_get("t-001/x/2026/11/art.zst", Duration::from_secs(60))
            .await
            .expect_err("file backend cannot presign");
        assert!(matches!(error, StoreError::Unsupported(_)));
        std::fs::remove_dir_all(&root).ok();
    }
}
