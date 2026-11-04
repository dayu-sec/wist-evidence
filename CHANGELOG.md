# CHANGELOG

## 0.1.6 · 2026-11-02

- 写端点新增 **Basic 认证**：`username` = 站点 token（`password` 留空）。
  这是给 warp-parse http 连接器用的 —— 它只有 `username` / `password` 两个稳定字段，
  不依赖 header 值的 `${VAR}` 展开；`Bearer` 继续支持。
- 文档补齐写入契约：请求形状、字段缺省、幂等语义、错误码，以及可直接 copy 的 wparse
  连接器 / sink 分组位置（见 `wist-console-stack/examples/warp-parse/`）。

## 0.1.5 · 2026-11-02

- 镜像构建改用 `wist-gateway` 的同一套做法：release 流水线按平台**原生**编译二进制，
  镜像只打包 `docker/amd64` / `docker/arm64`，不再在镜像内编译（QEMU 下编 sqlx / aws-sdk 极慢）。
- 运行镜像改为 `ubuntu:24.04` + `busybox-static`（不再依赖 `busybox:stable` 的 glibc 版本），
  uid 固定 10001。
- GitHub Release 继续附带三个平台的原生二进制 tarball。

## 0.1.4 · 2026-11-02

- 修正运行时镜像：`busybox:stable` 现在要求 glibc 2.38，而 debian bookworm 只有 2.36，
  容器内健康检查会直接报 `GLIBC_2.38 not found`；改用静态的 `busybox:stable-musl`。
- 运行时阶段不装任何包（复制 CA），没有外网的环境也能构建镜像。

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
