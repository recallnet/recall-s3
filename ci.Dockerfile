FROM rust:1.80-slim-bookworm as builder
USER root

COPY /root-config /root/
RUN sed -E 's|/home/ghrunner[0-9]?|/root|g' -i.bak /root/.ssh/config
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

RUN apt-get update && apt-get install --yes pkg-config libssl-dev ca-certificates ssh git
RUN update-ca-certificates
RUN mkdir /basin-s3
WORKDIR /basin-s3

RUN rm -rf ~/.ssh/known_hosts && \
    ssh-keyscan github.com >> ~/.ssh/known_hosts

COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
COPY ./lib ./lib

RUN rustup component add rustfmt
RUN \
  --mount=type=ssh \
  --mount=type=cache,target=/root/.cargo/registry,sharing=locked \
  --mount=type=cache,target=/root/.cargo/git,sharing=locked \
  --mount=type=cache,target=/app/target,sharing=locked \
  cargo build --features binary --release

FROM debian:bookworm-slim

# copy the build artifact from the build stage
COPY --from=builder /usr/bin/openssl /usr/bin/openssl
COPY --from=builder /usr/lib/ /usr/lib/
COPY --from=builder /usr/share/ca-certificates/ /usr/share/ca-certificates/
COPY --from=builder /usr/local/share/ca-certificates/ /usr/local/share/ca-certificates/
COPY --from=builder /etc/ssl/certs /etc/ssl/certs

COPY --from=builder /basin-s3/target/release/basin_s3 .

EXPOSE 8014

CMD ["./basin_s3"]
