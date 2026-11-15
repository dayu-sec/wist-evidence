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
    /// 调查命令 id：发起调查的人手里只有这个，也是页面上最自然的入口。
    pub command_id: Option<String>,
    pub since: Option<chrono::DateTime<chrono::Utc>>,
    /// 要**排除**的主体：自检探针（`_probe`）写的行不是证据。
    ///
    /// 必须在查询层排除（而不是取完一页再跳过）：探针 5 分钟一条，
    /// 后过滤会让它占满第一页，真正的证据一条都进不来。
    pub exclude_subject: Option<String>,
}

/// 窗口内的接收计数（看板用）：索引表在 `stored_at` 上的聚合。
///
/// 只算**已入库的行**（每行 = 一份产物 = 数据面发来的一条），不区分 kind：
/// 看板要回答的是“有没有数据进来”，不是“进来的是哪类”。
#[derive(Debug, Clone, Copy, Default)]
pub struct IndexStats {
    pub received: u64,
    pub last_stored_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[async_trait]
pub trait Index: Send + Sync {
    /// 索引类型（自述用，`/healthz` 的客户清单里会用到）：`memory` / `postgres`。
    fn kind(&self) -> &'static str;

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

    /// 按调查命令 id（`cmd_id`）找当前行。
    ///
    /// 发起调查的人手里只有这个 id（证据 id 是不透明的、也不该由人转抄），
    /// 所以读接口要能拿它直接定位。唯一键仍然是
    /// `(tenant, kind, subject, dedupe_key)`：极端情况下同一个命令可以在不同 kind 下落
    /// 多行（目前只有 `investigation_report` 帧带命令 id），这里取**最近写入的一行**。
    async fn find_by_command(
        &self,
        tenant_id: &str,
        command_id: &str,
    ) -> Result<Option<Artifact>, IndexError>;

    /// 时间倒序列表。`cursor` 是**不透明**字符串，P0 实现成十进制 offset。
    async fn list(
        &self,
        tenant_id: &str,
        filter: &ListFilter,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Vec<Artifact>, IndexError>;

    async fn insert(&self, artifact: &Artifact) -> Result<(), IndexError>;

    /// 窗口内的接收计数（看板用）。`since` 之前的不算；
    /// `exclude_subject` 给自检探针用 —— 探针不是「数据进来了」。
    async fn stats(
        &self,
        tenant_id: &str,
        since: chrono::DateTime<chrono::Utc>,
        exclude_subject: Option<&str>,
    ) -> Result<IndexStats, IndexError>;

    /// `before` 之前、某个主体的行（按时间升序，清理用）。
    ///
    /// 为什么要单独一个方法：清理不能「取最新一页、里面没有过期行就收工」——
    /// 探针 5 分钟一条，200 条只覆盖 16 小时，永远盖不到 24 小时保留期的边界，
    /// 于是过期行一行也删不掉（只增不减）。从**最旧**端取就没有这个问题。
    async fn expired(
        &self,
        tenant_id: &str,
        subject_id: &str,
        before: chrono::DateTime<chrono::Utc>,
        limit: usize,
    ) -> Result<Vec<Artifact>, IndexError>;

    /// 同键新内容：更新当前行指向新对象，`artifact_id` 不变（它代表"这次动作的产物"）。
    async fn update(&self, artifact: &Artifact) -> Result<(), IndexError>;

    /// 删除一行（对账 / 探针清理用）。**不存在视为成功**，且必须限在同一个租户内。
    async fn delete(&self, tenant_id: &str, artifact_id: &str) -> Result<(), IndexError>;
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
    fn kind(&self) -> &'static str {
        "memory"
    }

    async fn stats(
        &self,
        tenant_id: &str,
        since: chrono::DateTime<chrono::Utc>,
        exclude_subject: Option<&str>,
    ) -> Result<IndexStats, IndexError> {
        let rows = self.rows.read().expect("memory index poisoned");
        let mut stats = IndexStats::default();
        for row in rows.values() {
            if row.tenant_id != tenant_id || row.stored_at < since {
                continue;
            }
            if exclude_subject.is_some_and(|hidden| row.subject_id == hidden) {
                continue;
            }
            stats.received += 1;
            stats.last_stored_at = Some(match stats.last_stored_at {
                Some(current) if current >= row.stored_at => current,
                _ => row.stored_at,
            });
        }
        Ok(stats)
    }

    async fn expired(
        &self,
        tenant_id: &str,
        subject_id: &str,
        before: chrono::DateTime<chrono::Utc>,
        limit: usize,
    ) -> Result<Vec<Artifact>, IndexError> {
        let rows = self.rows.read().expect("memory index poisoned");
        let mut matched: Vec<Artifact> = rows
            .values()
            .filter(|row| {
                row.tenant_id == tenant_id
                    && row.subject_id == subject_id
                    && row.reported_at < before
            })
            .cloned()
            .collect();
        // 从**最旧**端取：每次都能推进，不会像「取最新一页」那样卡在保留期边界上。
        matched.sort_by_key(|row| (row.reported_at, row.stored_at));
        matched.truncate(limit);
        Ok(matched)
    }

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

    async fn find_by_command(
        &self,
        tenant_id: &str,
        command_id: &str,
    ) -> Result<Option<Artifact>, IndexError> {
        Ok(self
            .rows
            .read()
            .expect("memory index poisoned")
            .values()
            .filter(|row| {
                row.tenant_id == tenant_id && row.command_id.as_deref() == Some(command_id)
            })
            // 同命令多行（不同 kind）时取最近写入的那行：人要看的是「这次调查的结果」。
            .max_by_key(|row| (row.reported_at, row.stored_at))
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
            .filter(|row| {
                filter
                    .command_id
                    .as_deref()
                    .is_none_or(|command| row.command_id.as_deref() == Some(command))
            })
            .filter(|row| filter.since.is_none_or(|since| row.reported_at >= since))
            .filter(|row| {
                filter
                    .exclude_subject
                    .as_deref()
                    .is_none_or(|hidden| row.subject_id != hidden)
            })
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

    async fn delete(&self, tenant_id: &str, artifact_id: &str) -> Result<(), IndexError> {
        let mut rows = self.rows.write().expect("memory index poisoned");
        if rows
            .get(artifact_id)
            .is_some_and(|row| row.tenant_id == tenant_id)
        {
            rows.remove(artifact_id);
        }
        Ok(())
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

    use super::{Index, IndexError, IndexStats, ListFilter};

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
        fn kind(&self) -> &'static str {
            "postgres"
        }

        async fn stats(
            &self,
            tenant_id: &str,
            since: DateTime<Utc>,
            exclude_subject: Option<&str>,
        ) -> Result<IndexStats, IndexError> {
            let row: (i64, Option<DateTime<Utc>>) = sqlx::query_as(
                "select count(*), max(stored_at) from evidence_artifact \
                 where tenant_id = $1 and stored_at >= $2 \
                   and ($3::text is null or subject_id <> $3)",
            )
            .bind(tenant_id)
            .bind(since)
            .bind(exclude_subject)
            .fetch_one(&self.pool)
            .await
            .map_err(|err| IndexError::Backend(format!("stats: {err}")))?;
            Ok(IndexStats {
                received: row.0.max(0) as u64,
                last_stored_at: row.1,
            })
        }

        async fn expired(
            &self,
            tenant_id: &str,
            subject_id: &str,
            before: DateTime<Utc>,
            limit: usize,
        ) -> Result<Vec<Artifact>, IndexError> {
            sqlx::query_as::<_, Row>(
                "select artifact_id, tenant_id, customer_id, kind, subtype, subject_type, \
                 subject_id, gateway_id, agent_id, command_id, dedupe_key, schema_id, sensitivity, \
                 retention_class, bucket, object_key, size_bytes, sha256, verdict, marker_ids, \
                 source_ref, source_sha256, durability, reported_at, stored_at, retention_until, \
                 legal_hold, state \
                 from evidence_artifact \
                 where tenant_id = $1 and subject_id = $2 and reported_at < $3 \
                 order by reported_at asc \
                 limit $4",
            )
            .bind(tenant_id)
            .bind(subject_id)
            .bind(before)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map(|rows| rows.into_iter().map(Artifact::from).collect())
            .map_err(|err| IndexError::Backend(format!("expired artifacts: {err}")))
        }

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

        async fn find_by_command(
            &self,
            tenant_id: &str,
            command_id: &str,
        ) -> Result<Option<Artifact>, IndexError> {
            // 同命令多行（不同 kind）时取最近写入的那行；`command_id` 可空，
            // 无命令的产物靠 `artifact_id` 读，不在这条路里。
            sqlx::query_as::<_, Row>(
                "select artifact_id, tenant_id, customer_id, kind, subtype, subject_type, \
                 subject_id, gateway_id, agent_id, command_id, dedupe_key, schema_id, sensitivity, \
                 retention_class, bucket, object_key, size_bytes, sha256, verdict, marker_ids, \
                 source_ref, source_sha256, durability, reported_at, stored_at, retention_until, \
                 legal_hold, state \
                 from evidence_artifact \
                 where tenant_id = $1 and command_id = $2 \
                 order by reported_at desc, stored_at desc limit 1",
            )
            .bind(tenant_id)
            .bind(command_id)
            .fetch_optional(&self.pool)
            .await
            .map(|row| row.map(Artifact::from))
            .map_err(|err| IndexError::Backend(format!("find by command: {err}")))
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
            // 一个静态语句盖住五种组合：`$n is null or …` 让可选筛选不需要拼 SQL
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
               and ($5::text is null or command_id = $5) \
               and ($6::text is null or subject_id <> $6) \
             order by reported_at desc \
             limit $7 offset $8",
            )
            .bind(tenant_id)
            .bind(filter.kind.map(|kind| kind.as_str()))
            .bind(filter.subject_id.as_deref())
            .bind(filter.since)
            .bind(filter.command_id.as_deref())
            .bind(filter.exclude_subject.as_deref())
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

        async fn delete(&self, tenant_id: &str, artifact_id: &str) -> Result<(), IndexError> {
            sqlx::query("delete from evidence_artifact where tenant_id = $1 and artifact_id = $2")
                .bind(tenant_id)
                .bind(artifact_id)
                .execute(&self.pool)
                .await
                .map_err(|err| IndexError::Backend(format!("delete artifact: {err}")))?;
            Ok(())
        }
    }
}
