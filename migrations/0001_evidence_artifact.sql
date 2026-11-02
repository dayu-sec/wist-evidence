-- 证据索引：一份产物一行。
--
-- 设计见 `wist-design/doc/design/center/evidence-service.md` §5.1。
-- 刻意**不存**正文、自由文本摘要、具体路径 / 用户名 / 命令行、凭据；
-- `marker_ids` 只记闭集标识（如 `disk.top_dirs`），不含值。
--
-- 索引是**衍生物**：对象上的 `x-cos-meta-*` 足以重建这张表（D6），
-- 所以这里的任何一行都不该是唯一事实来源。

create table if not exists evidence_artifact (
  artifact_id      text primary key,
  tenant_id        text not null,
  customer_id      text not null,
  kind             text not null,
  subtype          text,
  subject_type     text not null,
  subject_id       text not null,
  gateway_id       text,
  agent_id         text,
  command_id       text,
  dedupe_key       text not null,
  schema_id        text not null,
  sensitivity      text not null,
  retention_class  text not null,
  bucket           text not null,
  object_key       text not null,
  size_bytes       bigint not null,
  sha256           text not null,
  verdict          text not null,
  marker_ids       jsonb not null default '[]'::jsonb,
  source_ref       text,
  source_sha256    text,
  durability       text not null,
  reported_at      timestamptz not null,
  stored_at        timestamptz not null default now(),
  retention_until  timestamptz not null,
  legal_hold       boolean not null default false,
  state            text not null default 'stored',
  unique (tenant_id, kind, subject_id, dedupe_key)
);

create index if not exists evidence_artifact_tenant_kind_reported
  on evidence_artifact (tenant_id, kind, reported_at desc);

create index if not exists evidence_artifact_tenant_subject_reported
  on evidence_artifact (tenant_id, subject_id, reported_at desc);

create index if not exists evidence_artifact_tenant_kind_subtype_reported
  on evidence_artifact (tenant_id, kind, subtype, reported_at desc);

create index if not exists evidence_artifact_due
  on evidence_artifact (retention_until)
  where state = 'stored' and not legal_hold;
