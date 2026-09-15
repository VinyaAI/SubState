# Multi-stage build for the SubState sidecar (`substate serve`).

FROM rust:bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY components ./components
RUN cargo build -p substate-cli --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/substate /usr/local/bin/substate
EXPOSE 8080
ENTRYPOINT ["substate"]
CMD ["serve"]
