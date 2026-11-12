# wist-evidence

平台里**所有"某次动作的产物"**的统一落点：调查报告、体检报告、命令日志、`ActionPlan` 原始输出……
正文进对象存储（COS 私有桶），索引 / 授权 / 短签名 / 保留留在计算区的薄服务，
控制面只持 `evidence_ref`。

设计稿：[`wist-design/doc/design/center/evidence-service.md`](../../wist-design/doc/design/center/evidence-service.md)。

## 当前覆盖（P0）

| 能力 | 状态 |
|---|---|
| `POST /internal/evidence` sink 写入（单对象 / 数组、幂等） | ✅ |
| 索引写入（`(tenant, kind, subject, dedupe_key)` 唯一） | ✅ |
| 对象写入（先对象后索引；7 个自描述元数据字段） | ✅ |
| 对象存储：**本地目录** / **S3（COS / MinIO）** | ✅ |
| `GET /internal/evidence/{id}` 元数据读（跨租户/低敏感度 → 404） | ✅ |
| `POST /internal/evidence/{id}:presign` 短签名（≤ 60s、单对象） | ✅（仅 `s3`） |
| `GET /internal/evidence/{id}/content` 代理读正文 | ✅ |
| `GET /healthz` 部署自述（`objects.kind` + 客户 cell 清单） | ✅ |
| PG 索引 / S3(COS) 对象 | ✅ 代码就绪（`pg` / `s3` feature），**未接真机** |
| 清理作业、删除凭证、对账、索引重建、前缀级 STS、每客户 CMK | ⬜ P1+ |

> 读端点的 `{id}` 可以是**调查命令 id**（`cmd_id`）——发起调查的人手里只有这个；
> 也可以是内部 `artifact_id`（`art_…`，列表行里给）。证据服务先按 `artifact_id` 再按
> `command_id` 找，两者指向同一行；可见性 / 租户 / 敏感度闸不变。
> 列表接口 `GET /internal/evidence?cmd_id=...` 也支持按命令 id 直接筛。

## 两张监听，两套信任模型

| 面 | 端口 | 身份 | 为什么分开 |
|---|---|---|---|
| 写 | `sink.listen_addr`（明文） | 先放宽（内网可信），P1 换 HTTPS 双向认证 | wparse 的 `http_sink` **没有 TLS 参数**，只能明文 + 内网 |
| 读 / 删 | `read.listen_addr` | mTLS + scope（P0 由内网 TLS 终结层注入请求头） | 预签名 URL 是 bearer，读侧必须能证明调用方是谁 |

> 写端点**不能**和读端点共用端口：混在一起就得让读路径容忍"没有证书"的流量。

## 写入接口（数据面 / 脚本）

```text
POST /internal/evidence
Content-Type: application/json
# 无认证（内网可信）；认证在 P1 由 HTTPS 双向认证接管
```

### 三字段

数据面只需要说清三件事：**谁的、哪次动作、产物是什么**。

```json
{
  "cust_id": "c-001",
  "cmd_id": "cmd-20261105-001",
  "content": { "任意证据内容": "字符串或 JSON 都行" }
}
```

| 字段 | 必填 | 说明 |
|---|---|---|
| `cust_id` | ✅ | 客户。**它同时就是租户**：对象键第一段、索引分区键都用它 |
| `cmd_id` | ✅ | 命令/动作 id，**幂等键**（同客户同 `cust_id` + `cmd_id` 且内容相同 → 重投） |
| `content` | ✅ | 证据正文；给字符串原样存，给 JSON 对象则序列化后存 |
| `subject` | — | 覆盖主体标识，缺省 = `cust_id`（探针用保留值 `_probe`） |

`kind` / `schema_id` / `sensitivity` 这些"我们自己的口径"不让接入方学：
取 `[sink]` 的 `default_kind` / `default_schema` / `default_sensitivity`。

对象键是 `{cust_id}/{kind}/{yyyy}/{mm}/{object_id}.{ext}.zst`，正文 zstd 压缩后落库。

单对象或数组都收（`batch_size > 1` 是数据面的常态）。

**幂等**：同 `(cust_id, kind, subject, cmd_id)` 且 `sha256` 相同 → `200` + 同一个
`artifact_id`，不写第二个对象；同键不同正文 → 新对象 + 行内切换（旧对象保留待清）。

响应：

```json
{ "count": 1, "duplicates": 0,
  "artifacts": [{ "artifact_id": "art_...", "durability": "best_effort", "duplicate": false, "superseded": false }] }
```

### 错误码

| 状态 | code | 含义 |
|---|---|---|
| 400 | `evidence_record_invalid` / `evidence_body_invalid` / `evidence_subject_invalid` / `evidence_customer_invalid` / `evidence_object_key_invalid` | 参数不对，重试无用 |
| 400 | `evidence_customer_unregistered` | 该 `cust_id` 没有在本站点登记 `[[storage.cell]]`（检查 cells 清单与网关绑定的 `tenant_id`） |
| 403 | `evidence_kind_not_allowed` | 部署未开该 kind |
| 413 | `evidence_body_too_large` | 超过 `[sink].max_body_bytes` |
| 5xx | `evidence_write_failed` | **只有这类才该重试**（sink 会重试后落 rescue 文件） |

> 5xx 的 `message` **只有一句通用文案**：后端原文（宿主绝对路径、errno、桶名）只进服务日志。
> 数据面拿到 `evidence_write_failed` 就该重试，细节去看服务日志——不要把路径回给调用方，
> 那等于送一个目录与磁盘状态探测器。

### 可直接 copy 的 wparse 配置

见 [`wist-console-stack/examples/warp-parse/`](https://github.com/dayu-sec/wist-console-stack/tree/main/examples/warp-parse)：
一个连接器（只负责 POST、不做字段映射）+ 一个 sink 分组；只需要 `EVIDENCE_SINK_URL`。
OML 的输出必须是三字段 `cust_id` / `cmd_id` / `content`。

## 客户存储（`[[storage.cell]]`，必填）

每个客户一套独立的索引 + 对象存储；**没有默认后端**。配置里至少要有
一条 `[[storage.cell]]`，否则进程启动直接失败——单客户站点也一样。

```toml
[[storage.cell]]
cust_id = "c-001"

[storage.cell.index]
kind = "postgres"
database_url = "${SEC_EVIDENCE_DB_URL}"

[storage.cell.objects]
kind = "s3"
bucket = "wist-evidence-c-001"
endpoint = "${SEC_COS_ENDPOINT}"
region = "${SEC_COS_REGION}"
access_key = "${SEC_COS_SECRET_ID}"
secret_key = "${SEC_COS_SECRET_KEY}"
```

- **写**：body 里的 `cust_id` 决定落哪个 cell；未登记客户回 `400`
  `evidence_customer_unregistered`，**不会**落到别的客户库里。
- **读**：`x-evidence-tenant` 决定去哪个 cell；未登记租户的列表为空、按 key 读 `404`、
  探针清理返回 `0`，不泄露“这个客户存不存在”。
- `/healthz` **永远**回显客户清单（`cust_id` + `index` / `objects` 类型），现场用它核对
  路由表；清单不含连接串 / 口令。

## 对象存储：两种模式

每个 cell 的 `[storage.cell.objects]` 只有两个落点，**没有「内存」这一档**
（正文丢了就再也要不回来，不该成为部署选项）：

| `[storage.cell.objects] kind` | 用在哪 | 落点 | 预签名 |
|---|---|---|---|
| `file` | 测试 / 现场验证 | `<dir>/<键>`，元数据在同目录 `<键>.meta.json` | ❌（回 501，正文直接看文件） |
| `s3` | 生产 | S3 兼容桶（腾讯 COS / MinIO） | ✅ SDK SigV4 |

后端类型对外自述在 `/healthz` 的 `objects.kind` 里（两张监听同一口径）；
`mixed` 表示各客户的 `objects` 类型不同，读端会保守地走代理读：

```json
{
  "status": "ok",
  "objects": { "kind": "file" },
  "cells": [{ "cust_id": "c-001", "index": "memory", "objects": "file" }]
}
```

上游（console BFF / 页面）读这个字段决定正文怎么打开 —— `file` 直接走代理读，
不必先发一个必然 501 的 `:presign`。字段不认识（老实例）时上游按「先试签名」处理。

```toml
# 每个 cell 各写一份；单客户最小示例见 wist-evidence.example.toml。
# 本地文件（重启不丢，能直接看）
[storage.cell.objects]
kind = "file"
dir = "data/objects"

# 腾讯 COS / MinIO
[storage.cell.objects]
kind = "s3"
bucket = "wist-evidence"
endpoint = "${SEC_COS_ENDPOINT}"
region = "${SEC_COS_REGION}"
access_key = "${SEC_COS_SECRET_ID}"
secret_key = "${SEC_COS_SECRET_KEY}"
```

本地模式的文件布局就是对象键：`<租户>/<kind>/<年>/<月>/<object_id>.<ext>.zst`，
同目录下 `.meta.json` 是那次写入的 7 个自描述字段。正文是 **zstd** 压缩的，
直接看内容：`zstd -dc <文件> | jq .`。

写入协议的几条约定（有测试固定）：

- **启动即验可写**：`dir` 为空、路径是个普通文件、目录不可写 —— 进程直接起不来，
  不会「启动全绿、第一条证据才 502」；
- **正文与元数据都原子落盘**：临时文件 `sync_all` 后 `rename`，失败不留半截文件；
  断电最多留下「有正文、无 `.meta.json`」，对账时可识别为未完成写入；
- **同键 = 覆盖写**：键唯一由调用方（每次写入新生成 object id）保证；
- **根目录消失 ≠ 对象不存在**：卷被卸载 / 目录被删时报后端故障，不答 404；
- **非法键回 400**（`evidence_object_key_invalid`），不当作可重试的 5xx。

## 写探针（自检）

每 `interval_secs` 往**自己的写端点**为**每个已登记客户**各投一条探针证据 ——
相当于一个内置的小号 wparse：同样的 URL、同样的三字段形状。它验的是整条写链路：
路由 → 序列化 → 对象 → 索引。某个客户写不进去时，日志会点名是哪个 `cust_id`。

```toml
[probe]
enabled = true           # 默认关；测试/验证环境打开
interval_secs = 300      # 5 分钟一条
retention_hours = 24     # 探针产物活多久
# url = "http://127.0.0.1:8080/internal/evidence"   # 可选；默认由 sink 监听地址推开环地址
```

- 探针产物 `subject_id = "_probe"`，**不出现在证据列表**里（按 id 仍可读）；
- 过期探针每小时扫一次，**按客户逐个扫**：先删对象、再删索引行；
- 结果在 `/healthz` 的 `probe` 块里（`last_ok` / `last_at_ms` / `last_latency_ms` /
  `consecutive_failures`），星象台的「接口可用性」页直接展示。

## 构建

```bash
# 开发 / 契约测试（内存后端，不依赖 PG、COS）
cargo test --no-default-features

# 生产二进制（默认就把 `pg` + `s3` 编进去）
cargo build --release
```

## 运行

```bash
./target/debug/wist-evidence wist-evidence.example.toml
# 或 WIST_EVIDENCE_CONFIG=/etc/wist-evidence/wist-evidence.toml wist-evidence
```

> 示例配置用 `19173/19174`：本机的 `8080` 常被代理 / 容器转发占用，撞上只会得到一句
> `bind sink listener ... Address already in use`。

```bash
# 写一条（三字段）
curl -sS -X POST http://127.0.0.1:19173/internal/evidence \
  -H 'Content-Type: application/json' \
  -d '{"cust_id":"c-001","cmd_id":"cmd-1","content":{"report_schema":"wist.investigation.disk/v1","summary":{"overall":"warn"}}}'

# 读元数据（P0 由 TLS 终结层注入身份；本机联调直接带头）
# 路径里的 `cmd-1` 是调查命令 id；换成列表行里的 `art_...` 也一样能读。
curl -sS http://127.0.0.1:19174/internal/evidence/cmd-1 \
  -H 'x-evidence-tenant: c-001' \
  -H 'x-evidence-scopes: evidence:read' \
  -H 'x-evidence-max-sensitivity: paths'

# 短签名（60s 上限）
curl -sS -X POST 'http://127.0.0.1:19174/internal/evidence/cmd-1:presign' \
  -H 'x-evidence-tenant: c-001' -H 'x-evidence-scopes: evidence:read' \
  -H 'x-evidence-max-sensitivity: paths' -H 'Content-Type: application/json' \
  -d '{"ttl_seconds":600}'
```

## 部署

### systemd（裸机 / VM）

```bash
install -D -m 0755 target/release/wist-evidence /usr/local/bin/wist-evidence
install -D -m 0644 systemd/wist-evidence.service /etc/systemd/system/wist-evidence.service
# 配置与密钥：属主=运行账号，0640
install -D -m 0640 -o wisteve -g wisteve wist-evidence.toml /etc/wist-evidence/wist-evidence.toml
systemctl daemon-reload && systemctl enable --now wist-evidence
```

单位文件里只用 `WIST_EVIDENCE_CONFIG` / `WIST_EVIDENCE_SEC_FILE` 两个环境变量；
`${SEC_...}` 的真值放 `/etc/wist-evidence/.wist/sec-value.yml`，配置本身不含密钥。

### 容器

镜像**不在里面编译**（与 `wist-gateway` 同构）：二进制由 release 流水线按平台**原生**编译，
build context 是 `docker/`，按架构提供 `amd64/` `arm64/` 产物，镜像只负责打包：

```bash
# 在 Linux 开发机上准备二进制（macOS 编不了 linux-gnu 目标）
cargo build --release --locked --target aarch64-unknown-linux-gnu
mkdir -p docker/arm64 && cp target/aarch64-unknown-linux-gnu/release/wist-evidence docker/arm64/
docker build -t wist-evidence:dev docker
```

日常直接拉 CI 产物即可（多架构由 release 流水线发布到 ghcr）：

```bash
docker run --rm -p 8080:8080 -p 8443:8443 \
  -v /etc/wist-evidence:/etc/wist-evidence:ro \
  -e WIST_EVIDENCE_CONFIG=/etc/wist-evidence/wist-evidence.toml \
  -e WIST_EVIDENCE_SEC_FILE=/etc/wist-evidence/.wist/sec-value.yml \
  ghcr.io/dayu-sec/wist-evidence:v0.1.5-alpha
```

端口约定：`8080` = 写（明文，只在内网）/ `8443` = 读。读端点 P0 靠内网 TLS 终结层保护，
服务自带的 mTLS 在 P1 接入（设计稿 §16 / §19-10）。

镜像以 **10001:10001** 运行；挂进来的配置 0644 即可，`sec-value.yml` 0640 且属主/属组为 10001。
运行时镜像里带**静态 busybox**（`busybox-static`，`/bin/busybox`）供编排做健康检查。

## 待真机验证（不要当成已完成）

1. **COS 预签名**：目前走 SDK 的 S3 SigV4。COS 是否接受这条预签名还没验过；
   不接受就退回"流式代理读"（设计稿 §7.1），预签名放 P1。
2. **SSE-KMS 头**：现在只写标准 S3 自定义元数据（`x-amz-meta-*`）。COS 的
   `x-cos-server-side-encryption` 与每客户 CMK 需要在真机上对齐。
3. **路径风格**：已强制 `force_path_style(true)`（COS / MinIO 必须）。
4. **mTLS**：读端点的 TLS 终结与 CA-I 签发还没接（P0 用请求头过渡）。

## 与设计稿的一处实现差异

对象键的最后一段用的是**每次写入新生成的不透明 id**，而索引行的 `artifact_id` 是
"这次动作的产物"的逻辑身份。原因：同键不同正文要保留旧对象待清（键不可覆盖），
而 `(tenant, kind, subject, dedupe_key)` 又要保持唯一 —— 两者只能靠"行身份 ≠ 对象 id"共存。
