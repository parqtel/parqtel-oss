# syntax=docker/dockerfile:1
# ─────────────────────────────────────────────────────────────────────────────
# Parqtel — Multi-stage build with cargo-chef for optimal layer caching
# Final image: distroless/static (~15MB), nonroot, no shell
#
# Optimization goals:
#   • cargo-chef so only crates that changed recompile
#   • Cache mounts for cargo registry/git/target — never re-download deps
#   • Parallel build with CARGO_BUILD_JOBS
# ─────────────────────────────────────────────────────────────────────────────

ARG RUST_VERSION=1.86
ARG CHEF_VERSION=0.1.71

# ── Stage 1: Chef planner ────────────────────────────────────────────────────
FROM rust:${RUST_VERSION}-slim AS chef
ARG CHEF_VERSION
RUN cargo install cargo-chef --version ${CHEF_VERSION} --locked
WORKDIR /app

# ── Stage 2: Compute dependency recipe ───────────────────────────────────────
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 3: Build workspace dependencies + binary in one cached stage ──────
FROM chef AS builder
ARG CARGO_BUILD_JOBS=8
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev cmake g++ protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Build dependencies only (cached unless Cargo.toml/lock change).
# Cache mounts keep the cargo registry / git index between runs so
# incremental CI builds don't re-download megabytes of metadata.
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo chef cook --release --recipe-path recipe.json -j ${CARGO_BUILD_JOBS} -p parqtel-server

# Build the binary. Cargo reuses the cooked deps in /app/target and only
# recompiles workspace crates that changed since the previous layer.
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo build --release -j ${CARGO_BUILD_JOBS} -p parqtel-server \
    && strip -s target/release/parqtel \
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
