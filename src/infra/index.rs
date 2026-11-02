//! 索引层：一份产物一行，`(tenant, kind, subject, dedupe_key)` 唯一。
//!
//! 两条不变量来自设计稿：
//! - **先对象、后索引**（D6）：索引写失败只会留下"孤儿对象"，可由对账补回；
//!   反过来会出现"索引说有、点开 404"；
//! - **同键同 `sha256` 是重投**：sink 的 `max_retries` 会让重复投递成为常态，
//!   所以这条路必须回成功，而不是报冲突。

use std::collections::HashMap;
use std::sync::RwLock;

use async_trait::async_trait;

use crate::domain::{Artifact, Kind};

#[derive(Debug)]
pub enum IndexError {
    Backend(String),
    /// 唯一键冲突（并发写入时才会出现；调用方重新 `find` 一次即可）。
    Conflict,
}

impl std::fmt::Display for IndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backend(message) => write!(f, "index backend error: {message}"),
            Self::Conflict => write!(f, "index unique key conflict"),
        }
    }
}

impl std::error::Error for IndexError {}

/// 列表筛选条件。**没有** `tenant_id`：租户永远来自调用方身份。
#[derive(Debug, Clone, Default)]
pub struct ListFilter {
    pub kind: Option<Kind>,
    pub subject_id: Option<String>,
    pub since: Option<chrono::DateTime<chrono::Utc>>,
}

#[async_trait]
pub trait Index: Send + Sync {
    /// 按唯一键找当前行。`subject_id` 与 `dedupe_key` 都来自**服务端推导**，不取正文自述。
    async fn find_by_dedupe(
        &self,
        tenant_id: &str,
        kind: Kind,
        subject_id: &str,
        dedupe_key: &str,
    ) -> Result<Option<Artifact>, IndexError>;

    /// 按 id 读一行；**必须带租户**，否则跨租户会变成"能猜就能读"。
    async fn get(&self, tenant_id: &str, artifact_id: &str)
    -> Result<Option<Artifact>, IndexError>;

    /// 时间倒序列表。`cursor` 是**不透明**字符串，P0 实现成十进制 offset。
    async fn list(
        &self,
        tenant_id: &str,
        filter: &ListFilter,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Vec<Artifact>, IndexError>;

    async fn insert(&self, artifact: &Artifact) -> Result<(), IndexError>;

    /// 同键新内容：更新当前行指向新对象，`artifact_id` 不变（它代表"这次动作的产物"）。
    async fn update(&self, artifact: &Artifact) -> Result<(), IndexError>;
}

/// 进程内索引：测试与本地联调用。
#[derive(Debug, Default)]
pub struct MemoryIndex {
    rows: RwLock<HashMap<String, Artifact>>,
}

impl MemoryIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.rows.read().expect("memory index poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn all(&self) -> Vec<Artifact> {
        self.rows
            .read()
            .expect("memory index poisoned")
            .values()
            .cloned()
            .collect()
    }
}

#[async_trait]
impl Index for MemoryIndex {
    async fn find_by_dedupe(
        &self,
        tenant_id: &str,
        kind: Kind,
        subject_id: &str,
        dedupe_key: &str,
    ) -> Result<Option<Artifact>, IndexError> {
        Ok(self
            .rows
            .read()
            .expect("memory index poisoned")
            .values()
            .find(|row| {
                row.tenant_id == tenant_id
                    && row.kind == kind
                    && row.subject_id == subject_id
                    && row.dedupe_key == dedupe_key
            })
            .cloned())
    }

    async fn get(
        &self,
        tenant_id: &str,
        artifact_id: &str,
    ) -> Result<Option<Artifact>, IndexError> {
        Ok(self
            .rows
            .read()
            .expect("memory index poisoned")
            .get(artifact_id)
            .filter(|row| row.tenant_id == tenant_id)
            .cloned())
    }

    async fn insert(&self, artifact: &Artifact) -> Result<(), IndexError> {
        let mut rows = self.rows.write().expect("memory index poisoned");
        let duplicate = rows.values().any(|row| {
            row.tenant_id == artifact.tenant_id
                && row.kind == artifact.kind
                && row.subject_id == artifact.subject_id
                && row.dedupe_key == artifact.dedupe_key
        });
        if duplicate {
            return Err(IndexError::Conflict);
        }
        rows.insert(artifact.artifact_id.clone(), artifact.clone());
        Ok(())
    }

    async fn list(
        &self,
        tenant_id: &str,
        filter: &ListFilter,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Vec<Artifact>, IndexError> {
        let offset: usize = cursor.and_then(|raw| raw.parse().ok()).unwrap_or(0);
        let mut matched: Vec<Artifact> = self
            .rows
            .read()
            .expect("memory index poisoned")
            .values()
            .filter(|row| row.tenant_id == tenant_id)
            .filter(|row| filter.kind.is_none_or(|kind| row.kind == kind))
            .filter(|row| {
                filter
                    .subject_id
                    .as_deref()
                    .is_none_or(|subject| row.subject_id == subject)
            })
            .filter(|row| filter.since.is_none_or(|since| row.reported_at >= since))
            .cloned()
            .collect();
        matched.sort_by_key(|row| std::cmp::Reverse(row.reported_at));
        Ok(matched.into_iter().skip(offset).take(limit).collect())
    }

    async fn update(&self, artifact: &Artifact) -> Result<(), IndexError> {
        let mut rows = self.rows.write().expect("memory index poisoned");
        match rows.get_mut(&artifact.artifact_id) {
            Some(slot) if slot.tenant_id == artifact.tenant_id => {
                *slot = artifact.clone();
                Ok(())
            }
            _ => Err(IndexError::Backend(format!(
                "artifact {} not found for update",
                artifact.artifact_id
            ))),
        }
    }
}

#[cfg(feature = "pg")]
pub use pg::PgIndex;

#[cfg(feature = "pg")]
mod pg {
    //! PostgreSQL 实现。查询全部走运行时绑定（不用 `query!`），
    //! 这样 `cargo check` 不需要连数据库。

    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use sqlx::postgres::PgPoolOptions;
    use sqlx::types::Json;
    use sqlx::{FromRow, PgPool};

    use crate::domain::{
        Artifact, ArtifactState, Durability, Kind, RetentionClass, Sensitivity, Verdict,
    };

    use super::{Index, IndexError, ListFilter};

    #[derive(Debug)]
    pub struct PgIndex {
        pool: PgPool,
    }

    impl PgIndex {
        pub async fn connect(database_url: &str) -> Result<Self, IndexError> {
            let pool = PgPoolOptions::new()
                .max_connections(8)
                .connect(database_url)
                .await
                .map_err(|err| IndexError::Backend(format!("connect postgres: {err}")))?;
            Ok(Self { pool })
        }

        /// 建表 + 索引（`if not exists`，可重复执行）。
        pub async fn migrate(&self) -> Result<(), IndexError> {
            sqlx::migrate!("./migrations")
                .run(&self.pool)
                .await
                .map_err(|err| IndexError::Backend(format!("migrate: {err}")))
        }
    }

    #[derive(Debug, FromRow)]
    struct Row {
        artifact_id: String,
        tenant_id: String,
        customer_id: String,
        kind: String,
        subtype: Option<String>,
        subject_type: String,
        subject_id: String,
        gateway_id: Option<String>,
        agent_id: Option<String>,
        command_id: Option<String>,
        dedupe_key: String,
        schema_id: String,
        sensitivity: String,
        retention_class: String,
        bucket: String,
        object_key: String,
        size_bytes: i64,
        sha256: String,
        verdict: String,
        marker_ids: Json<Vec<String>>,
        source_ref: Option<String>,
        source_sha256: Option<String>,
        durability: String,
        reported_at: DateTime<Utc>,
        stored_at: DateTime<Utc>,
        retention_until: DateTime<Utc>,
        legal_hold: bool,
        state: String,
    }

    impl From<Row> for Artifact {
        fn from(row: Row) -> Self {
            Self {
                artifact_id: row.artifact_id,
                tenant_id: row.tenant_id,
                customer_id: row.customer_id,
                kind: Kind::parse(&row.kind).unwrap_or(Kind::InvestigationReport),
                subtype: row.subtype,
                subject_type: row.subject_type,
                subject_id: row.subject_id,
                gateway_id: row.gateway_id,
                agent_id: row.agent_id,
                command_id: row.command_id,
                dedupe_key: row.dedupe_key,
                schema_id: row.schema_id,
                sensitivity: Sensitivity::parse(&row.sensitivity).unwrap_or(Sensitivity::Paths),
                retention_class: RetentionClass::parse(&row.retention_class)
                    .unwrap_or(RetentionClass::R30d),
                bucket: row.bucket,
                object_key: row.object_key,
                size_bytes: row.size_bytes,
                sha256: row.sha256,
                verdict: Verdict::parse(&row.verdict),
                marker_ids: row.marker_ids.0,
                source_ref: row.source_ref,
                source_sha256: row.source_sha256,
                durability: Durability::parse(&row.durability),
                reported_at: row.reported_at,
                stored_at: row.stored_at,
                retention_until: row.retention_until,
                legal_hold: row.legal_hold,
                state: ArtifactState::parse(&row.state),
            }
        }
    }

    #[async_trait]
    impl Index for PgIndex {
        async fn find_by_dedupe(
            &self,
            tenant_id: &str,
            kind: Kind,
            subject_id: &str,
            dedupe_key: &str,
        ) -> Result<Option<Artifact>, IndexError> {
            // 列名写死而不是拼字符串：sqlx 0.9 要求 SQL 是静态字面量（`SqlSafeStr`），
            // 拼出来的语句直接不给编。字段变多时宁可在两条查询里各改一次。
            sqlx::query_as::<_, Row>(
                "select artifact_id, tenant_id, customer_id, kind, subtype, subject_type, \
                 subject_id, gateway_id, agent_id, command_id, dedupe_key, schema_id, sensitivity, \
                 retention_class, bucket, object_key, size_bytes, sha256, verdict, marker_ids, \
                 source_ref, source_sha256, durability, reported_at, stored_at, retention_until, \
                 legal_hold, state \
                 from evidence_artifact \
                 where tenant_id = $1 and kind = $2 and subject_id = $3 and dedupe_key = $4",
            )
            .bind(tenant_id)
            .bind(kind.as_str())
            .bind(subject_id)
            .bind(dedupe_key)
            .fetch_optional(&self.pool)
            .await
            .map(|row| row.map(Artifact::from))
            .map_err(|err| IndexError::Backend(format!("find by dedupe: {err}")))
        }

        async fn get(
            &self,
            tenant_id: &str,
            artifact_id: &str,
        ) -> Result<Option<Artifact>, IndexError> {
            sqlx::query_as::<_, Row>(
                "select artifact_id, tenant_id, customer_id, kind, subtype, subject_type, \
                 subject_id, gateway_id, agent_id, command_id, dedupe_key, schema_id, sensitivity, \
                 retention_class, bucket, object_key, size_bytes, sha256, verdict, marker_ids, \
                 source_ref, source_sha256, durability, reported_at, stored_at, retention_until, \
                 legal_hold, state \
                 from evidence_artifact where tenant_id = $1 and artifact_id = $2",
            )
            .bind(tenant_id)
            .bind(artifact_id)
            .fetch_optional(&self.pool)
            .await
            .map(|row| row.map(Artifact::from))
            .map_err(|err| IndexError::Backend(format!("get artifact: {err}")))
        }

        async fn list(
            &self,
            tenant_id: &str,
            filter: &ListFilter,
            limit: usize,
            cursor: Option<&str>,
        ) -> Result<Vec<Artifact>, IndexError> {
            let offset: i64 = cursor
                .and_then(|raw| raw.parse().ok())
                .unwrap_or(0_i64)
                .max(0);
            // 一个静态语句盖住四种组合：`$n is null or …` 让可选筛选不需要拼 SQL
            // （sqlx 0.9 不接受拼出来的字符串）。
            let rows = sqlx::query_as::<_, Row>(
                "select artifact_id, tenant_id, customer_id, kind, subtype, subject_type, \
             subject_id, gateway_id, agent_id, command_id, dedupe_key, schema_id, sensitivity, \
             retention_class, bucket, object_key, size_bytes, sha256, verdict, marker_ids, \
             source_ref, source_sha256, durability, reported_at, stored_at, retention_until, \
             legal_hold, state \
             from evidence_artifact \
             where tenant_id = $1 \
               and ($2::text is null or kind = $2) \
               and ($3::text is null or subject_id = $3) \
               and ($4::timestamptz is null or reported_at >= $4) \
             order by reported_at desc \
             limit $5 offset $6",
            )
            .bind(tenant_id)
            .bind(filter.kind.map(|kind| kind.as_str()))
            .bind(filter.subject_id.as_deref())
            .bind(filter.since)
            .bind(limit as i64)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
            .map_err(|err| IndexError::Backend(format!("list artifacts: {err}")))?;
            Ok(rows.into_iter().map(Artifact::from).collect())
        }

        async fn insert(&self, artifact: &Artifact) -> Result<(), IndexError> {
            sqlx::query(
                "insert into evidence_artifact (artifact_id, tenant_id, customer_id, kind, subtype, \
                 subject_type, subject_id, gateway_id, agent_id, command_id, dedupe_key, schema_id, \
                 sensitivity, retention_class, bucket, object_key, size_bytes, sha256, verdict, \
                 marker_ids, source_ref, source_sha256, durability, reported_at, retention_until, \
                 state) \
                 values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,\
                 $21,$22,$23,$24,$25,$26)",
            )
            .bind(&artifact.artifact_id)
            .bind(&artifact.tenant_id)
            .bind(&artifact.customer_id)
            .bind(artifact.kind.as_str())
            .bind(&artifact.subtype)
            .bind(&artifact.subject_type)
            .bind(&artifact.subject_id)
            .bind(&artifact.gateway_id)
            .bind(&artifact.agent_id)
            .bind(&artifact.command_id)
            .bind(&artifact.dedupe_key)
            .bind(&artifact.schema_id)
            .bind(artifact.sensitivity.as_str())
            .bind(artifact.retention_class.as_str())
            .bind(&artifact.bucket)
            .bind(&artifact.object_key)
            .bind(artifact.size_bytes)
            .bind(&artifact.sha256)
            .bind(artifact.verdict.as_str())
            .bind(Json(artifact.marker_ids.clone()))
            .bind(&artifact.source_ref)
            .bind(&artifact.source_sha256)
            .bind(artifact.durability.as_str())
            .bind(artifact.reported_at)
            .bind(artifact.retention_until)
            .bind(artifact.state.as_str())
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|err| {
                if let sqlx::Error::Database(db) = &err
                    && db.is_unique_violation()
                {
                    return IndexError::Conflict;
                }
                IndexError::Backend(format!("insert artifact: {err}"))
            })
        }

        async fn update(&self, artifact: &Artifact) -> Result<(), IndexError> {
            let affected = sqlx::query(
                "update evidence_artifact set object_key = $3, size_bytes = $4, sha256 = $5, \
                 verdict = $6, marker_ids = $7, reported_at = $8, retention_until = $9, \
                 retention_class = $10, sensitivity = $11, state = $12 \
                 where tenant_id = $1 and artifact_id = $2",
            )
            .bind(&artifact.tenant_id)
            .bind(&artifact.artifact_id)
            .bind(&artifact.object_key)
            .bind(artifact.size_bytes)
            .bind(&artifact.sha256)
            .bind(artifact.verdict.as_str())
            .bind(Json(artifact.marker_ids.clone()))
            .bind(artifact.reported_at)
            .bind(artifact.retention_until)
            .bind(artifact.retention_class.as_str())
            .bind(artifact.sensitivity.as_str())
            .bind(artifact.state.as_str())
            .execute(&self.pool)
            .await
            .map_err(|err| IndexError::Backend(format!("update artifact: {err}")))?
            .rows_affected();
            if affected == 0 {
                return Err(IndexError::Backend(format!(
                    "artifact {} not found for update",
                    artifact.artifact_id
                )));
            }
            Ok(())
        }
    }
}
