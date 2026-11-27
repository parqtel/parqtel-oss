#!/usr/bin/env bash
# Collect the exact runtime rootfs for the parqtel binaries into /out.
#
# parqtel NEEDs only glibc + libgcc (verified with readelf), but the distroless
# base also ships OpenSSL, libstdc++ and gconv modules that a later `rm` cannot
# remove. So the image is built from a scratch rootfs that carries exactly the
# shared libraries the given binaries resolve, plus the CA bundle rustls/S3
# needs and the /dev nodes a scratch image lacks.
#
# Usage: collect-runtime-libs.sh /usr/local/bin/parqtel [/path/to/healthcheck]
#
# Called from two Dockerfile stages: the source build (`runtime-libs`) and the
# release build that copies already-compiled binaries (`prebuilt-libs`). One
# implementation keeps both images byte-identical in layout.
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <binary>..." >&2
  exit 64
fi

out=/out
rm -rf "$out"
mkdir -p "$out"

for binary in "$@"; do
  ldd "$binary" | awk '($2 == "=>") { print $3; next } ($1 ~ /ld-linux/) { print $1 }'
done | sort -u | while read -r lib; do
  [ -e "$lib" ] || continue
  mkdir -p "$out$(dirname "$lib")"
  cp -L "$lib" "$out$lib"
done

mkdir -p "$out/dev" "$out/tmp" "$out/etc"
mknod "$out/dev/null" c 1 3
mknod "$out/dev/zero" c 1 5
mknod "$out/dev/urandom" c 1 9
mknod "$out/dev/random" c 1 8
chmod 666 "$out/dev/null" "$out/dev/zero" "$out/dev/urandom" "$out/dev/random"
chmod 1777 "$out/tmp"

mkdir -p "$out/data"
chown 65532:65532 "$out/data"
printf 'nonroot:x:65532:65532::/nonexistent:/sbin/nologin\n' > "$out/etc/passwd"
printf 'nonroot:x:65532:\n' > "$out/etc/group"

if [ -d /etc/ssl/certs ]; then
  mkdir -p "$out/etc/ssl/certs"
  cp -L /etc/ssl/certs/* "$out/etc/ssl/certs/"
fi

echo '--- runtime rootfs ---'
find "$out" -type f | sort
