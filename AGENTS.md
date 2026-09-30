# Design-pass contract — aemo-generator-credit-dashboard

Read this before touching anything. The design brief (`BRIEF.md`) says *what* to build; this says how
work here is allowed to happen.

## The five hard rules

1. **Work on a branch, never on `main`.** A design pass gets its own branch (`design/<month>`).
   `main` is published to the public GitHub Pages site and is what the data lane pushes to.
2. **Never touch the data or the pipeline.** `docs/data/**`, `src/**`, `deploy/**`, `.github/**`,
   `data/**` are off limits. This pass changes presentation only.
3. **Never invent data.** Every figure on screen traces to a file in `docs/data/`. No mock arrays in
   the page, no placeholder numbers, no "for now" sample series. An empty panel is a state, not a gap
   to fill (see the `state` pattern).
4. **Verify in a browser, not by grep.** A change is done when a real browser renders it with real
   data: element present, height > 0, content non-empty. An HTTP 200 on the HTML file proves nothing.
5. **Keep the tests green.** 19 test files under `tests/` (`pytest -q`). Three of them parse
   `docs/index.html` and pin its DOM — see *Pitfalls*. If a change breaks one, the change or the test
   is wrong, deliberately: say which in the commit message.

   **Baseline on a fresh clone: `301 passed, 12 failed, 1 skipped, 9 errors`.** Every one of the 21
   non-passes is in `tests/test_outputs.py` and is a `FileNotFoundError` on *generated* data that git
   ignores (`data/**`, `docs/data/processed-cache/**`) — it validates pipeline output that only exists
   on the lane machine. That is environmental, not a regression. Your bar: the 301 keep passing and you
   add no new failure.

## Facts

| | |
|---|---|
| What it is | One static page — `docs/index.html` (2,521 lines; one inline `<style>`, one inline `<script>`) |
| Served by | **GitHub Pages from `docs/` on `main`** — public, no server, no login, no build step at deploy |
| Stack | Plotly **2.35.2** (CDN), SheetJS **0.20.3** (CDN, XLSX export), Tailwind **v3.4.17 standalone** (compiled CSS, committed) |
| Data | `docs/data/**` — 31 MB, 1,088 generator JSONs + market/quarterly/FCAS/outage/reference files |
| Presentation entry point | `docs/index.html` → `assets/css/tailwind.src.css` (tokens) → `docs/assets/app.css` (compiled) |
| Chart colours | `assets/js/chart-tokens.js` (reads the CSS variables) → published to `docs/assets/js/chart-tokens.js` by `./scripts/build-css.sh` |
| Deep links | selection is by URL hash: `#/<DUID>` — every one of the 630 units is directly addressable |
| Data lane | `deploy/run-update.sh` on the NAS writes **`docs/data/**` only** |
| Export | CSV + XLSX are client-side (SheetJS). Keep them working; they are used. |

**The lane never writes `docs/index.html`** — that is a recorded owner decision (`src/config.py`,
`src/aer_qa.py` both state it). Your work on the page cannot be clobbered by the daily run, and you
must not start writing data into `docs/data/` either.

## Local preview

```bash
# GitHub Pages equivalent (serves docs/ as the site root — this is the one that matters)
cd <repo>/docs && /opt/anaconda3/bin/python3 -m http.server 9350 --bind 127.0.0.1
# open http://127.0.0.1:9350/  then  http://127.0.0.1:9350/#ADPBA1

# design/ pages (the proof page, anything unpublished)
cd <repo> && /opt/anaconda3/bin/python3 -m http.server 9351 --bind 127.0.0.1
# open http://127.0.0.1:9351/design/tokens.html
```

Screenshot and verify with **Playwright** — cloud browsers cannot reach `127.0.0.1`.
`design/screens/before-*.png` are the pass baseline (`before-empty`, `before-top`, `before-mid`,
`before-low`, `before-foot`, `before-phone`); deliver `after-*.png` at the same scroll positions.

**Screenshots never go in `docs/`** — that tree is the published site. Design evidence lives in
`design/`, alongside the proof page.

**Run the checker, not your eyes:**

```bash
cd <repo> && /opt/anaconda3/bin/python3 scripts/verify-design.py --dashboard ADPBA1
```

It writes nothing by default. Add `--screens` to write the evidence screenshots
(`design/screens/tokens-proof-*.png`, `after-<DUID>-*.png`) — do that when you mean to commit them.

It catches the three failures that are invisible in a diff — a component class Tailwind purged, a chart
created inside a hidden panel (0px), a chart still on hard-coded colours instead of the tokens — plus
theme-flip breakage. Exit code 1 means something is wrong; it must be 0 before you report.

## Pitfalls

- **Plotly renders 0px inside a hidden panel.** Always pass an explicit height
  (`ChartTokens.HEIGHTS`), and re-layout on reveal if a panel starts collapsed.
- **`initAxisRangeDrag()`** in `docs/index.html` is a hand-written capture-phase axis-drag handler
  (grab an axis to scale the range; it preserves box-zoom, wheel and double-click reset). It is
  load-bearing for how this page is used — if you restructure the charts, keep it working.
- **Three tests pin the page's DOM**: `tests/test_offer_curve_panel.py` asserts `#offerCurveWrap` sits
  inside `#offersBox`, `tests/test_s306_genset_removal.py` mirrors `populateFilters()`, and
  `tests/test_aer_qa.py` asserts the AER QA lane adds **no** panel or chart. Renaming those ids or
  adding an AER panel breaks the build, on purpose.
- **Tailwind purges unused component classes.** After adding classes, run `./scripts/build-css.sh` and
  commit `docs/assets/app.css`, or the styling silently does nothing. The same script republishes
  `chart-tokens.js`; edit the source in `assets/js/`, never the copy in `docs/assets/js/`.
- **Preflight is off** (`tailwind.config.js`) so the existing inline styles keep working. Forgetting to
  turn it on at the end means the page keeps two competing resets — the brief's step 1 covers this.

## Working alongside Hermes

Hermes owns the design copy prep, the mirrors/verification, and the brief you are executing. The
branch is the boundary: two agents editing the same *files* on different branches is fine; on the same
branch it is not. If you need a data file that does not exist, stop and say so — do not synthesise it.

## Finishing a design pass (the handback)

- [ ] Working tree clean; all work committed **on the branch**.
- [ ] `pytest -q` run, result stated (counts, and any failure named).
- [ ] `./scripts/build-css.sh` run after the last class change; `docs/assets/app.css` and
      `docs/assets/js/chart-tokens.js` committed.
- [ ] Screenshots for every surface you changed, at desktop and phone width, in `design/screens/`
      (never `docs/` — that is the published site).
- [ ] Presentation-only: `git diff --stat main` shows no `docs/data/**`, `src/**` or `deploy/**`.
- [ ] A short report: surfaces changed of the ones that exist, what you did not get to, and anything
      you had to decide that the brief did not cover.

If you run out of time mid-change: commit what works, leave the branch pushed, and say plainly what is
half-done. A half-migrated panel that is labelled is worth more than a clean-looking one that is not.
