# CHANGELOG

## 0.1.3 · 2026-11-02

- 修正镜像构建：`gx adm v_patch` 只改 `Cargo.toml`，`Cargo.lock` 未同步，`--locked` 直接失败；
  已同步锁文件，并让 CI 也带 `--locked`，同类漂移以后在 CI 就会暴露。

## 0.1.2 · 2026-11-02

- `[sink.tokens]` 的**键**也支持 `${SEC_...}`：站点 token 不再需要以明文写进配置文件
  （值仍是租户 / 客户声明）。展开后仍含占位符则启动失败（fail-closed）。

## 0.1.1 · 2026-11-02

- 新增 `GET /internal/evidence` 列表接口：时间倒序，支持 `kind` / `subject_id` / `since` 筛选；
  列表同样受 scope、`max_sensitivity`、状态与保留期约束，跨租户返回空列表（不泄露存在性）。
- 发布镜像（ghcr，`linux/amd64` + `linux/arm64`），供计算区 stack 按固定 tag 拉取；
  Dockerfile 改为多阶段镜像内构建（与 `wist-atlas` 同构），不再依赖 `dist/`。

## 0.1.0 · 2026-11-02

首个骨架：证据服务 P0（索引 + sink 写入 + 元数据读 + 短签名）。

- 写端点按数据面 `http_sink` 的记录形状收（单对象 / 数组），站点 token 映射租户；
- 幂等：同 `(tenant, kind, subject, dedupe_key)` 同 `sha256` 的重投回 `200` + 同一 `artifact_id`；
- 跨租户 / 低敏感度 / 已过期一律 `404`；预签名夹到 60s 且只签单个对象；
- 对象带 7 个自描述元数据字段，索引可由此重建；
- 索引与对象均可在内存实现上跑契约测试；`pg` / `s3` feature 提供 Postgres 与 COS(S3 兼容) 实现。
