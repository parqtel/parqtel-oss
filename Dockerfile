FROM rust:1.85-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev cmake g++ make autoconf automake libtool \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN cargo build --release -p parqtel-server && \
    strip target/release/parqtel

FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=builder /app/target/release/parqtel /usr/local/bin/parqtel

EXPOSE 8080
USER nonroot:nonroot

ENTRYPOINT ["parqtel"]
CMD ["serve"]
