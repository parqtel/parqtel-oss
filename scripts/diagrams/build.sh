#!/usr/bin/env bash
# Regenerate the animated architecture diagrams in docs/assets/diagrams/.
#
# Sources: docs/assets/diagrams/src/*.mmd  (Mermaid, the editable truth)
# Output:  docs/assets/diagrams/*.svg     (animated, embedded by the docs)
#
# Styling comes from parqtel-mermaid-config.json (palette) and
# parqtel-motion.css (the animation vocabulary) — see those files for the
# motion system. Runs offline; requires @mermaid-js/mermaid-cli.
#
#   npm install -g @mermaid-js/mermaid-cli
#   ./scripts/diagrams/build.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../.." && pwd)"
assets="$root/docs/assets/diagrams"

if ! command -v mmdc >/dev/null 2>&1; then
  echo "mmdc not found — npm install -g @mermaid-js/mermaid-cli" >&2
  exit 1
fi

# White background keeps a mermaid diagram readable in GitHub's light *and*
# dark themes (GitHub does not re-theme SVG images).
for src in "$assets"/src/*.mmd; do
  name="$(basename "$src" .mmd)"
  mmdc \
    --input "$src" \
    --output "$assets/$name.svg" \
    --configFile "$assets/parqtel-mermaid-config.json" \
    --cssFile "$assets/parqtel-motion.css" \
    --backgroundColor "#ffffff" \
    --quiet
  printf '  %-22s %6s\n' "$name.svg" "$(du -h "$assets/$name.svg" | cut -f1)"
done

echo "Done. Embed with a relative path from the doc, e.g. ../assets/diagrams/system-context.svg"