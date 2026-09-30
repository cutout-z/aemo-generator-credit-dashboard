#!/usr/bin/env bash
# Build docs/assets/app.css from the page sources, and publish assets/js/chart-tokens.js to
# docs/assets/js/. Run after ANY class-name change or any edit to chart-tokens.js.
#
# Tailwind v3 standalone binary — no Node, no npm, no package.json, no build server.
# Both outputs are committed because GitHub Pages serves static files only (docs/ on main):
# there is no build step at deploy time. assets/ is the source; docs/assets/ is the published copy.
# tests/test_design_assets.py fails if the published chart-tokens.js drifts from its source.
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

JS_SRC="assets/js/chart-tokens.js"
JS_OUT="docs/assets/js/chart-tokens.js"
mkdir -p "$(dirname "$JS_OUT")"
cp "$JS_SRC" "$JS_OUT"
echo "==> published $JS_OUT"
echo "    remember to commit both: GitHub Pages serves the repo, not a build"
