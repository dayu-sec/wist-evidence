-- 主机流记录的运行时观测表（`wist.host_flow/v1` 的落库形状）。
--
-- 设计见 `wist-design/doc/design/center/agent-flow-storage.md`。
--
-- 为什么**不是** `evidence_artifact`：那张表是"一份产物一行 + 正文进对象存储 + 索引可从
-- 对象元数据重建"，适合 KB~MB 级的、一次一份的东西。流记录是每主机每 60s 每五元组一条，
-- 一天上百万条 —— 按对象存会造出百万个小对象，按行存索引表会让"可重建"的假设失效。
--
-- 保留期靠**分区 drop**（见 5.3）：行级 delete 在百万级表上会拖垮 vacuum。
-- 分区由写入端按 `window_start::date` 按需创建（`flow_window_pYYYYMMDD`）。
--
-- 幂等键 = 记录规范 §7 的 11 段 + `tenant_id`（`icmp_*` 用 '' 占位以参与唯一约束）。
-- 记录是**窗口聚合值**，重发是覆盖（`on conflict do update`），不是累加。

create table if not exists flow_window (
  tenant_id             text        not null,
  collector_instance_id text        not null,
  ifname                text        not null,
  direction             text        not null,   -- in | out
  protocol              text        not null,   -- 6 | 17 | 1 | 58
  srcaddr               text        not null,
  srcport               text        not null,
  dstaddr               text        not null,
  dstport               text        not null,
  icmp_type             text        not null default '',
  icmp_code             text        not null default '',
  window_start          timestamptz not null,
  window_seconds        integer     not null,
  host_id               text        not null,
  host_name             text,
  ifindex               integer     not null,
  if_mac                text,
  local_addrs           jsonb       not null default '[]'::jsonb,
  kernel                text,
  packets               bigint      not null,
  bytes                 bigint      not null,
  wire_bytes            bigint,
  skb_count             bigint,
  gso_skb_count         bigint,
  truncated             boolean     not null default false,
  dropped               integer     not null default 0,
  partial               boolean     not null default false,
  partial_reason        text,
  coverage              text        not null,
  source                text        not null,
  capture               text        not null,
  ingested_at           timestamptz not null default now(),
  unique (tenant_id, collector_instance_id, ifname, direction, protocol,
          srcaddr, srcport, dstaddr, dstport, icmp_type, icmp_code, window_start)
) partition by range (window_start);

-- 三条查询路径：按主机看时间线、按对端反查"谁连过它"（入站看 dstaddr、出站看 srcaddr）。
-- 在分区父表上建索引会传播到之后创建的分区（PG 11+ 的 partitioned index）。
create index if not exists flow_window_host_time
  on flow_window (tenant_id, host_id, window_start desc);
create index if not exists flow_window_src_time
  on flow_window (tenant_id, srcaddr, window_start desc);
create index if not exists flow_window_dst_time
  on flow_window (tenant_id, dstaddr, window_start desc);
