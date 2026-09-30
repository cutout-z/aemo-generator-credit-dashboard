# BRIEF — AEMO credit dashboard redesign, pass 1

Paste this whole file as the FIRST message to the coding agent, with the project folder
`/Users/zalen/Design/aemo-credit-design` open. Read `CLAUDE.md` → `AGENTS.md` (the contract) before
writing code. They load automatically; this file is the task.

## The goal, stated as an outcome

The dashboard is functionally complete and visually poor. Make it **look like it was designed by
someone who cares**: modern, restrained, scannable, and consistent panel to panel. Data pages that
look good — not a new product, not new features, no data changes.

**Definition of done = the real page, rendering real data, looking right when screenshotted.** Adopting
a token file, adding a stylesheet, "styling centralised", or a theme module is NOT done. An earlier
pass on a sibling dashboard delivered exactly that and nothing visible changed on 37 of 39 pages.

## Hard constraints (these define the pass)

- **It stays static.** GitHub Pages serves `docs/` — one page, no server, no build step at deploy, no
  self-hosting. Everything you build must work as plain files a browser loads.
- **Interactivity is client-side only.** Filters, search, period switching, sorting, fullscreen and the
  CSV/XLSX exports already work without a server; they must still work when you are done. Nothing you
  add may need a backend.
- **Presentation only.** No data, schema, pipeline or contract changes (`AGENTS.md` forbids the trees).
- **One page.** `docs/index.html`. Keep it one page; the CSS is the asset that gets split out (`app.css`
  is already wired as a separate compiled file — do not inline it back).

## The design language — adopt it, do not invent a second one

| Where | What |
|---|---|
| `assets/css/tailwind.src.css` | **The tokens.** Surfaces, text, status colours, radii, shadows, two themes. The only file with literal colours. |
| `design/design-tokens.md` | The seven rules (colour = entity, contrast floor, explicit chart heights, …) |
| `design/tokens.html` | **The proof page** — palette, type scale and every component class rendered live. Open it first. |
| `assets/js/chart-tokens.js` | How Plotly gets its colours: `ChartTokens.layout({height})`, `.color(name)`, `.series(...)`, `.swatchStyle(name)`, `.HEIGHTS` |
| `~/Design/ai-dc-frontend` (:9300) | The reference gallery where these blocks were chosen and the variants decided. Same language, different app — match it, do not copy its markup wholesale. |

Rules that matter most here: **colour belongs to an entity, not to a series index**; status is never
colour alone (dot **and** word); `--faint` is the floor for 12px text; every chart gets an explicit
height.

## Baseline: what it looks like now, and what is wrong with it

Look at `design/screens/before-*.png` (empty, top, mid, low, foot, phone). Real view: 9 of 14 charts
render by default, 5.7 screens tall, no JS errors. The concrete defects:

1. **Competing accents with no semantic role** — neon-green bars, a purple `BATTERY` pill, a bright-blue
   active period button, coral red, lavender lines. Nothing tells you what a colour means.
2. **Dotted underlines under panel titles.** That convention reads as "tooltip/abbreviation"; here it is
   static decoration.
3. **The period strip (`3M 6M 12M 3Y 5Y`) floats in dead space** between the entity card and the chart
   card, governing charts it is not attached to.
4. **X-axis labels rotated -45°** across 24 months — strenuous to scan. Thin the ticks instead.
5. **Legend treatment differs panel to panel**: one top-left legend that omits its own primary series,
   one inline right-edge annotation, one panel with no legend at all.
6. **Marker and cadence inconsistency**: node dots on one line chart, none on another; monthly rotated
   labels in one panel, bi-monthly horizontal in the next.
7. **The same lavender is used for two unrelated metrics** (a 0–5% volatility series and a 1.002–1.0035
   MLF index).
8. **Faint gridlines and tick labels** against the card fill — a contrast risk (see the token floor).
9. **An unlabelled dashed line** sits at the top of the Capacity Factor panel with no legend entry.
10. **Structure**: single column, no shell, no navigation, no KPI row, no table component. 5.7 screens
    of scroll with nothing to orient by, and the entity's own facts (DUID, region, fuel, technology,
    capacity, connection point) are a flat 6-column label row rather than a header.

## Panels that exist (map your work onto these)

Entity header (station, fuel badge, CSV/XLSX) · filter bar (search, region, fuel, counts) · period
control · **Generation** · **Generation (Last 12 Months)** · **Capacity Factor** · **Curtailment
Proxy** · **Estimated Economic Curtailment** · **MLF Trajectory (Annual)** · **Price Capture** ·
**Spot Price Exposure** · **Energy offers — this unit** · **Regional FCAS Prices** · **Regional Price
Spread (BESS Arbitrage)** · **Binding Network Constraints** — 14 Plotly charts across 12 panel headings.

## Order of work

One commit per step, and stop after step 1 to report before scaling:

1. **Wire the token layer and the shell.** Add the `app.css` link *before* the existing inline
   `<style>` so nothing shifts by accident, then convert the page chrome: background, typography, the
   app bar, the filter bar (`.input`, `.seg`, `.chip`) and the entity card (`.card`/`.card-head`).
   Once the old inline rules for those elements are gone, turn `corePlugins.preflight` on in
   `tailwind.config.js`, rebuild, and delete what it replaced. Report here.
2. **Entity header + KPI row** — DUID/region/fuel/technology/capacity/connection point as a proper
   header with a KPI row, not a label strip.
3. **Period control** — attach it to the panels it governs (panel header), not a floating strip.
4. **The 14 charts** — all through `ChartTokens.layout()`; no hand-set colours; explicit heights; one
   legend convention; unresolvable panels state why (the `state` pattern).
5. **Tables and filters**, if any numbers deserve a table rather than a chart (`.th`/`.td`, `dt-stack`
   on phones).
6. **States** — before-selection, "no data for this unit/window", and a quiet loading state.
7. **Phone (390px)** — single column, tables stack, nothing clipped, tap targets ≥ 32px.
8. **The interactions you must not lose**: search, filters, period switch, fullscreen, CSV/XLSX export,
   and the hand-written `initAxisRangeDrag()` axis-drag behaviour.

## Evidence (part of done, not optional)

- `design/screens/after-*.png` at the **same scroll positions** as `before-*.png`, plus the empty state
  and the phone width. (Screenshots live under `design/` — `docs/` is the published site.)
- `scripts/verify-design.py` exits **0** (it checks the proof page and a real generator view).
- `pytest -q` result stated. The fresh-clone baseline is `301 passed, 12 failed, 1 skipped, 9 errors`,
  and the 21 non-passes are all `tests/test_outputs.py` needing generated data that git ignores — so
  report it as "301 passed, unchanged environmental set", not as a new failure. Three test files parse
  this page's DOM — `#offersBox`, `#offerCurveWrap` and `populateFilters()`'s region options are pinned
  (`AGENTS.md` → Pitfalls). Keep them working, or change the test deliberately and say why.
- `./scripts/build-css.sh` run after the last class change and `docs/assets/app.css` committed.

## Report back

Surfaces changed of the ones that exist; what is half-done; any decision the brief did not cover. Do
not claim a panel is done because the HTML changed — it is done when a browser renders it with real
data.
