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
| `GET /internal/evidence/{id}` 元数据读（跨租户/低敏感度 → 404） | ✅ |
| `POST /internal/evidence/{id}:presign` 短签名（≤ 60s、单对象） | ✅ |
| PG 索引 / S3(COS) 对象 | ✅ 代码就绪（`pg` / `s3` feature），**未接真机** |
| 清理作业、删除凭证、对账、索引重建、前缀级 STS、每客户 CMK | ⬜ P1+ |

## 两张监听，两套信任模型

| 面 | 端口 | 身份 | 为什么分开 |
|---|---|---|---|
| 写 | `sink.listen_addr`（明文） | 站点 token（`Authorization`） | wparse 的 `http_sink` **没有 TLS 参数**，只能明文 + 内网 |
| 读 / 删 | `read.listen_addr` | mTLS + scope（P0 由内网 TLS 终结层注入请求头） | 预签名 URL 是 bearer，读侧必须能证明调用方是谁 |

> 写端点**不能**和读端点共用端口：混在一起就得让写路径容忍"没有证书"的流量。

## 写入语义（给数据面配置的人看）

`agentd --IVSREP--> 网关数据面 --> 平台数据面（wparse） --http_sink--> wist-evidence`

请求体是 wparse 的记录（单对象或数组）：

```json
{
  "schema": "IVSREP",
  "agent": "agent-7f3",
  "ts": "2026-11-01T00:00:00Z",
  "seq": 1,
  "sensitivity": "paths",
  "body": "{\"command_id\":\"cmd-...\",\"report_schema\":\"wist.investigation.disk/v1\",...}"
}
```

- `body` 是契约对象的 **JSON 字符串**（与网关 `api/ingest.rs` 同一条纪律）；
- `kind` 由 `schema` 帧标记推导（只认 `IVSREP`），`subject_id` 取信封 `agent`；
- **同键同 `sha256` = 重投** → `200` + 同一个 `artifact_id`，不写第二个对象；
- 只有"真的没存下"才回 5xx —— 那会让 sink 重试并最终把失败落成可见的 rescue 文件。

响应：

```json
{ "count": 1, "duplicates": 0,
  "artifacts": [{ "artifact_id": "art_...", "durability": "best_effort", "duplicate": false, "superseded": false }] }
```

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
# 写一条（示例配置里的 tok_dev → t-001）
curl -sS -X POST http://127.0.0.1:19173/internal/evidence \
  -H 'Authorization: Bearer tok_dev' -H 'Content-Type: application/json' \
  -d '{"schema":"IVSREP","agent":"agent-7f3","sensitivity":"paths","body":"{\"command_id\":\"cmd-1\",\"report_schema\":\"wist.investigation.disk/v1\",\"summary\":{\"overall\":\"warn\"},\"findings\":[{\"id\":\"disk.filesystems\"}]}"}'

# 读元数据（P0 由 TLS 终结层注入身份；本机联调直接带头）
curl -sS http://127.0.0.1:19174/internal/evidence/art_... \
  -H 'x-evidence-tenant: t-001' \
  -H 'x-evidence-scopes: evidence:read' \
  -H 'x-evidence-max-sensitivity: paths'

# 短签名（60s 上限）
curl -sS -X POST 'http://127.0.0.1:19174/internal/evidence/art_...:presign' \
  -H 'x-evidence-tenant: t-001' -H 'x-evidence-scopes: evidence:read' \
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
