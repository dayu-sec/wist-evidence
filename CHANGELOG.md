# CHANGELOG

## 0.2.0 · 2026-11-11

- **多客户：客户级存储路由（`[[storage.cell]]`）**。一个进程可以同时服务多个客户，
  每个客户落**独立的 PG 与独立的对象目录**；写入按请求 body 里的 `cust_id` 分流，
  读按 `x-evidence-tenant` 分流。未登记的 `cust_id` 落 default 并打 WARN（不丢数据）。
  - 不配 `[[storage.cell]]` 时行为与 0.1.x **完全一致**（单后端），升级无感。
  - `[[storage.cell]]` 的 `[storage.cell.index]` / `[storage.cell.objects]` 都是**必填**：
    静默继承 default 会让两个客户写进同一个库，比启动失败难查得多。
  - 重复的 `cust_id`、路径穿越形态的 `cust_id` 在启动期直接拒。
  - `/healthz` 多一个 `cells: [cust_id, …]`，现场能一眼看出路由表。
- 存储后端抽象（`infra::Backends`）替代单 `Index` + `ObjectStore`：
  `service` 的读写、探针清理都按客户取后端；`EvidenceService::new` 保持原签名（单后端）。


## 0.1.16 · 2026-11-10

- **读端点最小鉴权（`[read] service_token`）**：配了就必须带
  `x-evidence-service-token`；没带 / 不对一律 401。
  原因：P0 的身份头（`x-evidence-tenant` / `x-evidence-scopes`）是**调用方自述**，
  边界只有网络隔离 —— 内网里任何进程改个头就能读任何租户。
- **没配 token 时保持旧行为**并打一条启动警告（`warn`）：现有部署升级不会被拒，
  但日志里会直接给出待办。写端点（sink）本轮不变。
- 比较用常量时间，不长短泄露。

## 0.1.15 · 2026-11-09

- **读端点可以用调查命令 id（`cmd_id`）定位**：`GET /internal/evidence/{id}`、
  `POST /internal/evidence/{id}:presign`、`GET /internal/evidence/{id}/content` 的
  `{id}` 先当 `artifact_id` 找，找不到再当 `command_id` 找。
  发起调查的人手里只有 `cmd_id`（证据 id 是不透明的、也不该由人转抄），
  之前只能先列一遍索引再从行里取 `artifact_id`。两套 id 指向同一行，
  租户 / scope / 敏感度 / 保留期闸全部不变；跨租户仍然 404。
- 索引新增 `find_by_command`（PG 按 `command_id` + `reported_at desc limit 1`；
  同命令多行时取最近写入的一行）。

## 0.1.14 · 2026-11-06

- **`/healthz` 自述对象后端**：读写两张监听都回 `"objects": {"kind": "file"|"s3"}`。
  上游（console / 页面）据此决定正文怎么读：本地文件存储没有预签名 URL，直接代理读，
  不必先发一个必然 501 的 `:presign`。新增字段，老调用方不受影响。

## 0.1.13 · 2026-11-05

- 无代码变更：**仅为把镜像同步到 TCR** 而重发。0.1.12 的 tag 早于 release 工作流
  增加 TCR 推送，所以 `dy-sec.tencentcloudcr.com/cloud/wist-evidence:v0.1.12-alpha` 一直不存在
  —— 栈切到 TCR 时就是 `manifest unknown`。之后的版本两个源（ghcr / TCR）同进退。

## 0.1.12 · 2026-11-05

- **写入契约简化成三个字段**：`{cust_id, cmd_id, content}`（可选 `subject`）。
  数据面不再构造信封，也不用学 `kind` / `schema` / `sensitivity`（取部署默认值）。
  `cust_id` 同时就是租户：对象键第一段、索引分区列都用它。
- **写端点不再校验身份**（内网可信），HTTPS 双向认证在 P1 接管。站点 token 表、
  `[sink].auth`、旧信封（`IVSREP`）整条路都删掉了。
- 新增**写探针**：每 5 分钟往自己的写端点投一条真证据，验「路由 → 对象 → 索引」，
  结果在 `/healthz` 的 `probe` 块里；探针产物按 `_probe` 标记，过期每小时清一次，
  且不出现在证据列表里。
- `objects.kind` / `objects.bucket` 支持 `${VAR}`：用 `EVIDENCE_STORE_KIND=file|s3`
  切存储不用改配置文件；file 模式下解析不出的 COS 占位符会被忽略（s3 仍 fail-closed）。

## 0.1.11 · 2026-11-04

- `[sink].listen_addr` 与 `[read].listen_addr` 支持 `${VAR}` 展开（与其它字段同口径）。
  容器里可以由编排决定端口，不必把 `8443` 同时写进配置、compose 健康检查和上游 `base_url`。
  未解析仍然 fail-closed（报 `unresolved environment variable`，不会把字面占位符当地址去 bind）。

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
