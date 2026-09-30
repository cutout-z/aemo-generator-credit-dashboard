# Design tokens — the rules

The values live in **`assets/css/tailwind.src.css`** (one `:root` block, dark + light) and compile to
`docs/assets/app.css`. Charts get their colours from the same variables through
`assets/js/chart-tokens.js`. This file states the rules that the CSS cannot.

## The rules

1. **No raw hex in `docs/index.html` or any page.** Use the token names (`--accent`, `.card`, `.badge`,
   `.seg`, `.th`/`.td`). The only file with literal colours is the token source.
2. **Colour belongs to an entity, not to a series index.** A metric keeps its colour when the chart
   changes, and the legend swatch uses the same token (`ChartTokens.swatchStyle(name)`), so a mark and
   its legend can never disagree.
3. **Status colour is never the only signal.** `pill-good/warn/bad` carry a dot **and** a word.
4. **`--faint` is the floor for 12px text** (4.9:1 on a card in dark, 4.7:1 on white in light). Nothing
   dimmer carries text.
5. **Two themes, one code path.** Dark is the default; `<html data-theme="light">` flips it. Anything
   that hard-codes a dark-only colour breaks the flip — call `ChartTokens.restyle()` after flipping.
6. **Charts always get an explicit height** (`ChartTokens.HEIGHTS`). Plotly renders a 0px chart when it
   is created inside a hidden panel; that failure is silent and looks like a broken panel.
7. **The card is the unit of composition**: `.card-head` owns the title, `.card-body` the content,
   `.card-foot` the provenance (source, lag, caveat). Provenance is not decoration on this dashboard —
   every panel's data has a publication lag worth stating.

## Adding classes

Tailwind only emits component classes it finds in the sources (`content` in `tailwind.config.js`).
After adding any new class use:

```bash
./scripts/build-css.sh      # rewrites docs/assets/app.css — commit it
```

The compiled CSS is committed because GitHub Pages serves static files: **there is no build step at
deploy time.** A class that exists only in a template string the scanner cannot see will silently do
nothing.
