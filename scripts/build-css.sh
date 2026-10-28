#!/usr/bin/env bash
# Build docs/assets/app.css from the page sources, and publish assets/js/chart-tokens.js and
# assets/js/portfolio.js to docs/assets/js/. Run after ANY class-name change or any edit to either.
#
# Tailwind v3 standalone binary — no Node, no npm, no package.json, no build server.
# Both outputs are committed because GitHub Pages serves static files only (docs/ on main):
# there is no build step at deploy time. assets/ is the source; docs/assets/ is the published copy.
# tests/test_design_assets.py fails if a published copy drifts from its source.
set -euo pipefail
cd "$(dirname "$0")/.."

BIN="tools/tailwindcss"
VERSION="v3.4.17"
OUT="docs/assets/app.css"

if [ ! -x "$BIN" ]; then
  echo "==> fetching Tailwind $VERSION standalone binary"
  mkdir -p tools
  curl -fsSL -o "$BIN" \
    "https://github.com/tailwindlabs/tailwindcss/releases/download/$VERSION/tailwindcss-macos-arm64"
  chmod +x "$BIN"
fi

"$BIN" -c tailwind.config.js -i assets/css/tailwind.src.css -o "$OUT" --minify
echo "==> wrote $OUT ($(wc -c < "$OUT" | tr -d ' ') bytes)"

for name in chart-tokens.js portfolio.js; do
  JS_OUT="docs/assets/js/$name"
  mkdir -p "$(dirname "$JS_OUT")"
  cp "assets/js/$name" "$JS_OUT"
  echo "==> published $JS_OUT"
done
echo "    remember to commit both: GitHub Pages serves the repo, not a build"
