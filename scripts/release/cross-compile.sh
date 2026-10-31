#!/usr/bin/env bash
set -euo pipefail

# This script cross-compiles parqtel for all supported targets,
# packages them into tar.gz archives along with the config and systemd unit,
# and generates sha256 checksums.

DIST_DIR="dist"
mkdir -p "$DIST_DIR"

if [ -z "${TARGETS:-}" ]; then
    TARGETS=(
        "x86_64-unknown-linux-musl"
        "aarch64-unknown-linux-musl"
        "x86_64-apple-darwin"
        "aarch64-apple-darwin"
    )
else
    # Parse space-separated targets from env var
    read -r -a TARGETS <<< "$TARGETS"
fi

# Use cross if available, otherwise fallback to cargo
if command -v cross &> /dev/null; then
    BUILD_CMD="cross"
else
    BUILD_CMD="cargo"
    echo "Warning: 'cross' not found. Falling back to 'cargo'. Cross-compilation may fail if targets are not installed."
fi

for TARGET in "${TARGETS[@]}"; do
    echo "========================================"
    echo "Building for target: $TARGET"
    echo "========================================"

    # Build
    $BUILD_CMD build --release --target "$TARGET" --package parqtel-server

    BIN_PATH="target/$TARGET/release/parqtel"
    
    if [ ! -f "$BIN_PATH" ]; then
        echo "Error: Binary not found at $BIN_PATH after build."
        exit 1
    fi

    # Check size
    SIZE=$(stat -c%s "$BIN_PATH" 2>/dev/null || stat -f%z "$BIN_PATH")
    echo "Binary size for $TARGET: $SIZE bytes"
    if [ "$SIZE" -gt 20971520 ]; then
        echo "ERROR: Binary exceeds 20MB limit!"
        exit 1
    fi

    # Package
    ARCHIVE_NAME="parqtel-$TARGET.tar.gz"
    ARCHIVE_PATH="$DIST_DIR/$ARCHIVE_NAME"
    
    # Create a temporary staging directory
    STAGING_DIR=$(mktemp -d)
    cp "$BIN_PATH" "$STAGING_DIR/"
    cp config/default.toml "$STAGING_DIR/parqtel.toml"
    cp parqtel.service "$STAGING_DIR/"
    
    tar -czf "$ARCHIVE_PATH" -C "$STAGING_DIR" .
    rm -rf "$STAGING_DIR"

    # Checksum
    (cd "$DIST_DIR" && sha256sum "$ARCHIVE_NAME" > "$ARCHIVE_NAME.sha256" || shasum -a 256 "$ARCHIVE_NAME" > "$ARCHIVE_NAME.sha256")

    echo "Successfully packaged $ARCHIVE_NAME"
done

echo "All targets built successfully. Artifacts are in $DIST_DIR/"
