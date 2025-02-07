FROM --platform=$BUILDPLATFORM debian:bookworm-slim AS builder
USER root

RUN apt-get update && \
  apt-get install -y build-essential clang cmake protobuf-compiler curl \
  openssl libssl-dev pkg-config git-core ca-certificates && \
  update-ca-certificates

# Get Rust
RUN curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain stable -y
ENV PATH="/root/.cargo/bin:${PATH}"

ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
  CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc \
  CXX_aarch64_unknown_linux_gnu=aarch64-linux-gnu-g++

WORKDIR /app

# Update the version here if our `rust-toolchain.toml` would cause something new to be fetched every time.
ARG RUST_VERSION=1.84.0
RUN \
  rustup install ${RUST_VERSION} && \
  rustup default ${RUST_VERSION} && \
  rustup target add aarch64-unknown-linux-gnu

# Defined here so anything above it can be cached as a common dependency.
ARG TARGETARCH

# Only installing MacOS specific libraries if necessary.
RUN if [ "${TARGETARCH}" = "arm64" ]; then \
  apt-get install -y g++-aarch64-linux-gnu libc6-dev-arm64-cross; \
  rustup target add aarch64-unknown-linux-gnu; \
  rustup toolchain install ${RUST_VERSION}-aarch64-unknown-linux-gnu; \
  fi


COPY /root-config /root/
RUN sed -E 's|/home/ghrunner[0-9]?|/root|g' -i.bak /root/.ssh/config
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

RUN rm -rf ~/.ssh/known_hosts && \
    umask 077; mkdir -p ~/.ssh && \
    ssh-keyscan github.com >> ~/.ssh/known_hosts

COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
COPY ./lib ./lib

RUN \
  --mount=type=ssh \
  --mount=type=cache,target=/root/.cargo/registry,sharing=locked \
  --mount=type=cache,target=/root/.cargo/git,sharing=locked \
  --mount=type=cache,target=/app/target,sharing=locked \
  set -eux; \
  case "${TARGETARCH}" in \
  amd64) ARCH='x86_64'  ;; \
  arm64) ARCH='aarch64' ;; \
  esac; \
  rustup show ; \
  cargo build --features binary --release --locked --target ${ARCH}-unknown-linux-gnu; \
  mv ./target/${ARCH}-unknown-linux-gnu/release/recall_s3 ./

FROM debian:bookworm-slim

COPY --from=builder /etc/ssl/certs /etc/ssl/certs
COPY --from=builder /app/recall_s3 .

EXPOSE 8014

CMD ["./recall_s3"]
