# wist-evidence — 计算区证据服务（索引 + 对象存储）
#
# 与 `wist-atlas` 同一套做法：**镜像内构建**（多阶段），运行时只装二进制与 CA。
# 交叉编译放到 buildx 的 `platforms` 里做 —— 本地直接 `docker build .` 就是本机架构。
FROM rust:1.99-bookworm AS builder
WORKDIR /src

# 先把依赖清单与源码分开，命中 Docker 层缓存时能跳过依赖重编。
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
COPY README.md LICENSE ./

# 默认 feature 就含 pg + s3（生产后端）；--locked 保证和仓库里的 Cargo.lock 一致。
RUN cargo build --release --locked

# 健康检查用的 busybox（debian slim 里没有 wget/curl）。
FROM busybox:stable AS busybox

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -s /usr/sbin/nologin wist

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=busybox /bin/busybox /bin/busybox
COPY --from=builder /src/target/release/wist-evidence /usr/local/bin/wist-evidence

# 配置与密钥**不进镜像**：由 stack 挂载到 /etc/wist-evidence（0640，属主=wist）。
ENV WIST_EVIDENCE_CONFIG=/etc/wist-evidence/wist-evidence.toml \
    WIST_EVIDENCE_SEC_FILE=/etc/wist-evidence/.wist/sec-value.yml

USER wist
# 8080 = 写（数据面 http_sink 直投，明文，只在内网）/ 8443 = 读（mTLS，P1 接入）。
# 这两个只是**约定**：真实监听地址由配置决定。
EXPOSE 8080 8443

# 不在这里写死 HEALTHCHECK：端口是配置项，健康检查应由编排（compose）按实际端口定义。
ENTRYPOINT ["wist-evidence"]
