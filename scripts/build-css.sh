#!/usr/bin/env bash
# Build docs/assets/app.css from the page sources. Run after ANY class-name change.
#
# Tailwind v3 standalone binary — no Node, no npm, no package.json, no build server.
# The compiled file is committed because GitHub Pages serves static files only:
# there is no build step at deploy time.
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
echo "    remember to commit it: GitHub Pages serves the repo, not a build"
