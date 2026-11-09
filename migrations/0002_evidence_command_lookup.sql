-- 读接口可以按**调查命令 id**（`cmd_id`）定位：
-- `GET /internal/evidence/{key}` / `...:presign` / `.../content` 先按 `artifact_id` 找，
-- 找不到再按 `command_id` 找。
--
-- `command_id` 可空（没有命令的产物只能按 `artifact_id` 读），所以用部分索引；
-- 同一命令可能在不同 kind 下落多行（目前只有 `investigation_report` 带命令 id），
-- 查询按 `reported_at desc, stored_at desc limit 1` 取最近写入的一行。
create index if not exists evidence_artifact_tenant_command_reported
  on evidence_artifact (tenant_id, command_id, reported_at desc)
  where command_id is not null;
