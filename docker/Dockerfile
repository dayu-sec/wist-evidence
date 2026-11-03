# ===========================================================================
# wist-evidence — 运行镜像（多架构）
#
# 与 `wist-gateway` 同一套做法：**二进制由 release 流水线按平台原生编译**，
# build context（本目录）按 `amd64/` `arm64/` 提供，镜像只负责打包，不在里面编译。
#
# 为什么不在镜像里编译：buildx 多架构会把 arm64 丢给 QEMU，sqlx / aws-sdk 这种
# 重依赖能编到 40 分钟以上，而且构建机没有外网时 apt / crates.io 都会挂。
# ===========================================================================
FROM ubuntu:24.04

ARG TARGETARCH

LABEL org.opencontainers.image.source="https://github.com/dayu-sec/wist-evidence"
LABEL org.opencontainers.image.description="Evidence service: index + object storage for action artifacts"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# 运行时只需要根证书（COS/PG 的 TLS）与静态 busybox（供编排做健康检查）。
# uid 固定 10001：stack 里的配置与 sec-value.yml 按这个 uid 授权。
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates busybox-static \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 10001 -s /usr/sbin/nologin wist

# 预编译二进制（build context 按架构提供：amd64/wist-evidence、arm64/wist-evidence）
COPY ${TARGETARCH}/wist-evidence /usr/local/bin/wist-evidence

# 配置与密钥**不进镜像**：由 stack 挂载到 /etc/wist-evidence。
ENV WIST_EVIDENCE_CONFIG=/etc/wist-evidence/wist-evidence.toml \
    WIST_EVIDENCE_SEC_FILE=/etc/wist-evidence/.wist/sec-value.yml

USER 10001:10001

# 8080 = 写（数据面 http_sink 直投，明文，只在内网）/ 8443 = 读（mTLS，P1 接入）。
# 这两个只是**约定**：真实监听地址由配置决定。
EXPOSE 8080 8443

# 健康检查由编排（compose）按实际端口定义；镜像里只提供 /bin/busybox。
ENTRYPOINT ["wist-evidence"]
