# syntax=docker/dockerfile:1
# ─────────────────────────────────────────────────────────────────────────────
# Parqtel — Multi-stage build with cargo-chef for optimal layer caching.
# Final image: gcr.io/distroless/cc-debian12:nonroot (~15MB, glibc-based, no shell).
#
# Linkage note: parqtel-server links rustls, NOT OpenSSL. openssl-sys is only
# pulled by the standalone parqtel-mcp-* servers (not compiled into this binary),
# so the cc-debian12 base (glibc + libstdc++) is sufficient and libssl-dev is
# intentionally omitted from the builder.
#
# Healthcheck note: parqtel is an HTTP (axum) service with NO gRPC health
# service, so grpc_health_probe cannot be used. The `probe` stage compiles a
# tiny std-only Rust binary that does an HTTP GET on /health instead.
#
# Optimization goals:
#   • cargo-chef so only crates that changed recompile
#   • Cache mounts for cargo registry/git/target — never re-download deps
#   • Build context trimmed via .dockerignore (no data/, graphs, charts, ...)
#   • COPY --link for faster, cache-friendly layer population (BuildKit)
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
COPY --link . .
RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 3: Build workspace dependencies + binary in one cached stage ──────
FROM chef AS builder
ARG CARGO_BUILD_JOBS=8
ARG DEBIAN_FRONTEND=noninteractive
# Resilience for flaky package mirrors / crates.io during CI builds.
ENV CARGO_NET_RETRY=10 \
    CARGO_NET_TIMEOUT=60

# protoc (prost-build), cmake+g++ (ring/zstd -sys crates), pkg-config (-sys probes).
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config cmake g++ protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Build dependencies only (cached unless Cargo.toml/lock change).
# Cache mounts keep the cargo registry / git index / target between runs so
# incremental CI builds don't re-download or recompile from scratch.
COPY --link --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo chef cook --release --recipe-path recipe.json -p parqtel-server

# Build the real binary; deps are already cooked, so only parqtel-server
# (and anything it touches) recompiles. [profile.release] sets
# strip = "symbols", so no separate strip step is needed.
COPY --link . .
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo build --release -j ${CARGO_BUILD_JOBS} -p parqtel-server \
    && cp target/release/parqtel /usr/local/bin/parqtel

# ── Stage 3b: Tiny dependency-free HTTP health probe ─────────────────────────
# distroless has no curl/wget, and parqtel is HTTP-only (no gRPC health), so a
# ~15-line std-only Rust binary compiled with rustc is the minimal correct
# probe. No new language, no crates, fully reproducible, runs on the glibc base.
FROM rust:${RUST_VERSION}-slim AS probe
RUN mkdir -p /src && cat > /src/healthcheck.rs <<'EOF'
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::exit;
fn main() {
    let port = std::env::var("HEALTH_PORT").unwrap_or_else(|_| "8080".into());
    let mut s = match TcpStream::connect(("127.0.0.1", port.parse::<u16>().unwrap_or(8080))) {
        Ok(s) => s,
        Err(_) => exit(1),
    };
    let _ = s.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    let mut buf = [0u8; 512];
    let n = s.read(&mut buf).unwrap_or(0);
    let resp = String::from_utf8_lossy(&buf[..n]);
    if resp.starts_with("HTTP/1.1 2") || resp.starts_with("HTTP/1.0 2") {
        exit(0);
    }
    exit(1);
}
EOF
RUN rustc -O /src/healthcheck.rs -o /usr/local/bin/healthcheck

# ── Stage 4: Final distroless image ─────────────────────────────────────────
# Pinned by digest for supply-chain reproducibility. Refresh with:
#   docker buildx imagetools inspect gcr.io/distroless/cc-debian12:nonroot
FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f AS runtime

LABEL org.opencontainers.image.source="https://github.com/parqtel/parqtel-oss"
LABEL org.opencontainers.image.description="Ultra-lightweight SRE observability engine"
LABEL org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder --chown=nonroot:nonroot /usr/local/bin/parqtel /usr/local/bin/parqtel
COPY --from=probe --chown=nonroot:nonroot /usr/local/bin/healthcheck /usr/local/bin/healthcheck

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/healthcheck"]

EXPOSE 8080
VOLUME ["/data"]

USER nonroot:nonroot
ENTRYPOINT ["parqtel"]
CMD ["serve"]
