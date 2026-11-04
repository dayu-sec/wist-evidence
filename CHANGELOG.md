# CHANGELOG

## 0.1.10 · 2026-11-04

- 新增 `GET /internal/evidence/{artifact_id}/content`：**代理读正文**（设计稿 §7.2 预留的那条）。
  可见性判断与元数据读一致（跨租户 / 低敏感度 / 已过期 → 404）；对象层是 zstd 压缩的，
  服务解压后回原文（`application/json`、`cache-control: no-store`、`nosniff`）。
  本地文件存储（`objects.kind = file`）没有预签名 URL，这是它**唯一**的读正文通路。
- 索引里有、对象没了 → `502 evidence_read_failed`（不当成「证据不存在」），细节只进日志。
- 每次代理读记一行 `evidence_content` 审计日志（与 `evidence_presign` 同口径）。
- 补 3 个契约测试：原文回读、跨租户/不存在 404、对象消失 502。

## 0.1.9 · 2026-11-04

- 5xx 响应体不再回显后端原文：`evidence_write_failed` / `evidence_read_failed` 现在只回一句
  通用文案，具体原因（宿主绝对路径、errno、桶名）只进 `log::error!`。
  之前持有效 token 的调用方可以从 502 正文里读出目录结构与磁盘状态。
- 补两条回归测试：写/读两侧的后端失败都不泄露路径与 errno。

## 0.1.8 · 2026-11-04

本轮是对象存储的「写完再读一遍」：6 轮独立评审后的修正 + 补齐测试（31 → 57 个）。

修正（均为本地文件后端的行为）：

- `FileObjectStore::open` 真正做到启动即失败：空 `dir`、路径是普通文件、目录不可写都在启动时报错
  （之前 `create_dir_all` 对已存在目录只做 stat，只读挂载要到第一次写入才 502）。
  `root` 统一 canonicalize，启动日志里的落点变绝对路径。
- 临时文件改成**每次写唯一**（`.<name>.partial-<pid>-<nanos>`）。旧实现用
  `with_extension("partial")`，同键并发写会踩同一个临时文件。
- 正文与 `.meta.json` 都走「tmp + `sync_all` + rename」，失败不留半截文件，
  也不再出现「sidecar 截断」这种静默损坏。
- 根目录消失（卷卸载 / 误删）不再答成 `Ok(None)`：那是存储故障，不是「对象不存在」；
  也不会在容器层悄悄重建一个重启即丢的目录。
- 新增 `StoreError::InvalidKey`：非法键（`..`、绝对路径、空段）不再伪装成后端故障。
  写路径回 `400 evidence_object_key_invalid`（不重试），读路径回后端错误。
- 配置：`objects.bucket` 必须非空；`resolve` 出错带上配置路径；
  `objects.kind = memory` 的报错补一句「0.1.7 起已取消」。
- systemd unit 补 `StateDirectory` / `WorkingDirectory` / `ReadWritePaths`：
  裸机按默认 `file` 模式跑时不会再因只读根目录起不来。

测试（新增 26 个）：

- 文件后端：二进制正文回读、空正文 vs 不存在、sidecar 逐字段、同键覆盖、
  重启（drop 后重开）仍可读、无 `.partial` 残留、6 种非法键、目录/根目录消失、
  并发写不同键与同键不撕裂。
- 契约（file 后端首次有端到端覆盖）：正文落盘能解压回原文、七字段与索引行一致、
  重投不写第二个对象、supersede 保留旧对象、`:presign` 回 501 且不泄露路径、
  不存在的 artifact 仍是 404；另用恒失败的后端验「对象失败不留索引行」与
  「非法键 400 / 后端故障 502」的分级。
- 配置：空白 `dir`、s3 缺 endpoint/region、空 `bucket`、相对 `dir` 原样保留。

## 0.1.7 · 2026-11-04

- 对象存储改成**两种模式**，去掉可部署的「内存」档（正文丢了就再也要不回来）：
  - `objects.kind = "file"`（默认）：正文落本地目录 `<objects.dir>/<键>`，
    同目录 `<键>.meta.json` 存元数据；重启不丢，测试与现场核对直接看文件。
  - `objects.kind = "s3"`：S3 兼容桶（腾讯 COS / MinIO），行为不变。
- 本地文件模式**不支持预签名**（没有外部可直连的桶）：`:presign` 回
  `501 evidence_presign_unsupported` 并说明正文在哪里；要签名 URL 就切 `s3`。
- 写入先写临时文件再 `rename`，半截文件不会被当成完整对象读到；对象键再做一次
  路径穿越拒绝（`..`、绝对路径、空段）。
- **不兼容**：`objects.kind = "memory"` 现在启动即报错（允许值 `file|s3`）。

## 0.1.6 · 2026-11-04

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
