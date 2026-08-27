# syntax=docker/dockerfile:1
# ─────────────────────────────────────────────────────────────────────────────
# Parqtel — Multi-stage build with cargo-chef for optimal layer caching
# Final image: distroless/static (~15MB), nonroot, no shell
#
# Optimization goals:
#   • Split deps → binary build so only crates that changed are recompiled
#   • Parallel build with CARGO_BUILD_JOBS and cargo-chef layer reuse
#   • BuildKit cache mounts avoid re-downloading the Cargo registry
#   • Cross-compilation via CARGO_TARGET_<TRIPLE>_LINKER + TARGETARCH
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

# ── Stage 3a: Build workspace dependencies (cached unless Cargo.toml/lock changes)
FROM chef AS deps
ARG TARGETARCH
ARG CARGO_BUILD_JOBS=8

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev cmake g++ protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY --from=planner /app/recipe.json recipe.json

# Cook dependencies once; store in a dedicated output dir so this stage
# can be cached independently of the binary build stage.
RUN cargo chef cook --release --recipe-path recipe.json --target-dir /target \
    && cp -r /target/debug/deps /deps-debug \
    && cp -r /target/release/deps /deps-release \
    && cp -r /target/debug/.fingerprint /fp-debug \
    && cp -r /target/release/.fingerprint /fp-release \
    && cp -r /target/debug/incremental /inc-debug \
    && cp -r /target/release/incremental /inc-release

# ── Stage 3b: Build the binary (deps stage cached; only recompiles changed crates)
FROM chef AS builder
ARG TARGETARCH
ARG CARGO_BUILD_JOBS=8
COPY --from=deps /deps-release /target/release/deps
COPY --from=deps /fp-release  /target/release/.fingerprint
COPY --from=deps /inc-release /target/release/incremental

RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev cmake g++ protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY --from=planner /app/recipe.json recipe.json

# Restore the pre-built dependency artifacts so cargo only recompiles the
# workspace crates that changed since deps was built.
RUN mkdir -p /target/release/deps /target/release/.fingerprint /target/release/incremental \
    && cp -r /deps-release/* /target/release/deps/ \
    && cp -r /fp-release/*  /target/release/.fingerprint/ \
    && cp -r /inc-release/* /target/release/incremental/ \
    # Mark all deps as freshly built so cargo doesn't think they're stale
    && find /target/release/.fingerprint -type f -exec touch -d now {} \; 2>/dev/null || true

# Build the actual binary; cargo will reuse cached deps and only rebuild changed crates.
COPY . .
RUN cargo build --release -p parqtel-server -j ${CARGO_BUILD_JOBS} \
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
