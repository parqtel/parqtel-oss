# syntax=docker/dockerfile:1
# ─────────────────────────────────────────────────────────────────────────────
# Parqtel — Multi-stage build with cargo-chef for optimal layer caching
# Final image: distroless/static (~15MB), nonroot, no shell
# ─────────────────────────────────────────────────────────────────────────────

ARG RUST_VERSION=1.85
ARG CHEF_VERSION=0.1.71

# ── Stage 1: Chef planner ────────────────────────────────────────────────────
FROM rust:${RUST_VERSION}-slim AS chef
RUN cargo install cargo-chef --version ${CHEF_VERSION} --locked
WORKDIR /app

# ── Stage 2: Compute dependency recipe ───────────────────────────────────────
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 3: Build dependencies (cached unless Cargo.toml/lock changes) ──────
FROM chef AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev cmake g++ \
    && rm -rf /var/lib/apt/lists/*

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json -p parqtel-server

# Build the actual binary
COPY . .
RUN cargo build --release -p parqtel-server \
    && strip target/release/parqtel \
    && cp target/release/parqtel /usr/local/bin/parqtel

# ── Stage 4: Final distroless image ─────────────────────────────────────────
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

LABEL org.opencontainers.image.source="https://github.com/parqtel/parqtel-oss"
LABEL org.opencontainers.image.description="Ultra-lightweight SRE observability engine"
LABEL org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder --chown=nonroot:nonroot /usr/local/bin/parqtel /usr/local/bin/parqtel

EXPOSE 8080
VOLUME ["/data"]

USER nonroot:nonroot
ENTRYPOINT ["parqtel"]
CMD ["serve"]
