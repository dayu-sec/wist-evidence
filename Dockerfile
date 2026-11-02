# 运行镜像只装「已经编好的二进制」。
#
# 与 wist-ingest-ctrl 同一套做法：二进制由 CI 在各平台**原生**编译（Linux 走静态 musl），
# 这里按 $TARGETARCH 从 dist/ 取对应那一份。
#
# 两个必须注意的点：
#   1. 不要在镜像里 `cargo build`：buildx 多架构时 arm64 走 QEMU，一个 Rust 项目要很久；
#   2. 不要换成 gnu 目标：ubuntu-24.04 构建出来的二进制需要更新的 glibc，
#      而本镜像基础是 bookworm，会直接 `GLIBC_… not found`。musl 静态无此问题。
#
# 本地手工构建（只能用于本机架构，产物必须是 linux 二进制）：
#   cargo build --release --locked --target aarch64-unknown-linux-musl   # 需要 musl 工具链
#   mkdir -p dist/arm64 && cp target/aarch64-unknown-linux-musl/release/wist-evidence version.txt dist/arm64/
#   docker build -t wist-evidence:dev .
FROM debian:bookworm-slim

# buildx 会为每个 platform 注入 TARGETARCH（amd64 / arm64）。
ARG TARGETARCH

# ca-certificates：COS 走 HTTPS；tini 不引入 —— 进程自己就是 PID 1，信号处理已在代码里。
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -s /usr/sbin/nologin wist

COPY dist/${TARGETARCH}/wist-evidence /usr/local/bin/wist-evidence
COPY dist/${TARGETARCH}/version.txt /usr/local/share/wist-evidence/version.txt

# 配置与密钥**不进镜像**：由部署挂载到 /etc/wist-evidence（0640，属主=wist）。
ENV WIST_EVIDENCE_CONFIG=/etc/wist-evidence/wist-evidence.toml \
    WIST_EVIDENCE_SEC_FILE=/etc/wist-evidence/.wist/sec-value.yml

USER wist
# 写端点（明文，只在内网）+ 读端点。读端点 P0 由内网 TLS 终结层保护，
# 服务自带 mTLS 在 P1 接入，见 README。
EXPOSE 8080 8443
ENTRYPOINT ["wist-evidence"]
