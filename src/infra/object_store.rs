//! 对象层：证据正文唯一的落点。
//!
//! 抽成 trait 的理由不是"以后可能换云"，而是**让契约测试不需要真 COS**：
//! 幂等、键布局、元数据重建这些行为都必须在内存实现上就能验。
//!
//! 生产实现（`s3` feature）只做 PUT / GET / 预签名，不做 List（对账作业才需要）。

use std::collections::HashMap;
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

/// 进程内对象存储：只给测试与本地联调用。
///
/// 它**故意**不落盘：跑完即丢，避免有人拿它当"本地证据库"（那条路会退化成第二个数据库）。
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
