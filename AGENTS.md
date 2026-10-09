# Design-pass contract — aemo-generator-credit-dashboard

Read this before touching anything. The design brief (`BRIEF.md`) says *what* to build; this says how
work here is allowed to happen.

<!-- BEGIN agent-contracts:family -->
<!-- source: family/AGENTS.family.md sha256:116b1391e2bb — edit in cutout-z/agent-contracts, not here -->
## Family conventions (every repo, every agent)

*Generated from `agent-contracts/family/AGENTS.family.md`. Edit it there, never here: a drift check
reports any local edit.* These apply to every agent (Hermes, Claude Code, Codex or any other),
whatever app drives it. Where this repo's own rules (above or below this block) are stricter, they
win. Machine-specific conventions (which checkout is which, the lane runtime, where memory lives)
are in the owner's private contract, which reaches agents through their user-level instructions
where a harness supports it.

### Branches, concurrency, cleanup

- Work in a working clone, on a branch, never in a live or serving checkout: a live checkout is what
  serves or publishes, so an edit there goes out unreviewed. Merge to `main` only if this repo's
  contract says the agent may; otherwise push the branch and hand back for review.
- Other agents may be working in this repo right now. Fetch before you act. If a branch moved
  unexpectedly, or files you didn't touch changed, stop and report rather than reconcile.
- Use a worktree for parallel work, not a second clone. The exception is a live checkout: use a
  separate clone, because adding a worktree writes into the live checkout's `.git`. At session end,
  remove the worktrees you created and leave each checkout on the branch it was on when you arrived:
  a leftover worktree pins its branch, and a checkout left on your branch changes what the next
  agent, or a server running from it, sees.
- Push every branch you want kept. An unpushed branch is one disk failure from gone.

### Instructions and automation

- **`AGENTS.md` is the only instruction file.** `CLAUDE.md`, or any other harness-specific file, holds
  just a comment line and `@AGENTS.md`. Other harnesses never read those files, so a rule or fact put
  there reaches only one agent.
- **Nothing you depend on lives only in one harness or app.** Hooks, slash commands, plugins, app
  quick actions and app-scheduled tasks may speed things up. The only copy of any automation or rule
  goes in this repo (scripts, `AGENTS.md`) or the owner's scheduler, because the harness or app may be
  swapped.
- **Name the tier, not the model.** Briefs, skills and procedures say a tier and an effort (tier 0
  mechanical, 1 routine, 2 standard, 3 hard or review, 4 vision); `agent-contracts/tiers.yaml` maps
  each tier to a model per harness. Models change often, so a name in a skill goes stale.
- **A fact other agents need goes where they load it**: this repo's `AGENTS.md` Facts, with source
  and date. If it applies across repos, propose it for the shared facts block in your handback. Your
  own memory or skills are invisible to the other agents.

### What needs the owner's yes

An explicit instruction from the owner in the current session covers that action only, not similar
later ones: the yes was for what the owner saw, not for what follows. A repo contract may record a
standing permission from the owner (e.g. "the agent merges to `main` once `check.sh` is green"). It
counts as the owner's yes only for the actions and the agents it names, only in the repo whose
contract records it, and only as that text stands on `origin/main` when you start. It never covers
adding, widening or rewording a standing permission, or any change to `AGENTS.md` or this block:
those always need the owner's yes in the current session. Without either, declare these and wait
for a yes:

- **Publishing**: anything that changes what other people can see (`main` on a published repo,
  Pages, public data files).
- **Data and ETL**: data files, pipeline code, data contracts (columns, keys, paths, schemas).
  Lanes, dashboards and other repos read them, and a change breaks them silently.
- **The instruments**: `check.sh`, guard tests, audit and verify scripts. They are how anyone,
  including you, knows a change works. Never weaken one to make something pass. If one is wrong,
  say so and leave it.
- **Infra**: ports, scheduled jobs, servers, publish pipelines. Other jobs, and the owner, rely on
  them running as they are.
- **Another agent's state**: another agent's memory, config or notes. With the owner's yes any agent
  may change them; no agent is the only writer. The owning agent can't see your edit, so commit it
  where it's visible, and edit the source rather than a generated copy.

Never read, quote or commit secrets (`.env`, auth files, keys, tokens): repos, transcripts and
handback notes get copied and published.

### Verification standard

- A change is done when you have seen the evidence yourself: tests run (exact counts, failures
  named, pre-existing failures shown to exist on `main`), and for UI, a real browser render, or a
  plain statement that it wasn't rendered and why. A reviewer can check evidence, not belief.
- For a fix, show its test fails with the fix reverted and passes with it applied; otherwise nothing
  shows the test exercises the fix.
- Don't relay another agent's or subagent's numbers. Re-run or read the evidence yourself: a relayed
  number can't be traced, and you are the one signing the handback.

### Attribution and handback

- **Every commit names its agent** in a trailer, so audits and the owner's digest can tell which agent
  made each change, whatever app drove it: `Co-Authored-By: <Agent> <model> <email>`, e.g.
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. `<Agent>` is one word (`Claude`,
  `Codex`, `Hermes`, …); `<model>` is the model name without a provider prefix (e.g. `Opus 5.5`,
  `deepseek-v4.1-flash`). The email goes in angle brackets: Claude `noreply@anthropic.com`, Hermes
  `noreply@nousresearch.com` (as in existing commits), any other agent `<agent>@agents.invalid`.
  Audits match the key case-insensitively and the first word of the value.
- **Commit with the repo's configured identity.** Never set or override `user.name` or `user.email`,
  and never pass `--author`: on a public repo, author fields are published.
- **End every piece of work with a handback note.** It is the durable record; app session lists and
  an agent's memory are not, and both may be swapped. Use the repo's own path and branch convention
  if it has one; it wins, including whether notes merge. Otherwise use
  `handbacks/<YYYY-MM-DD>-<topic>.md`. On a repo whose `main` is published (a public repo, or one that
  deploys or builds a site from `main`), commit the note on its own branch, `handback/<your-branch>`:
  push it, and never merge or delete it, so the record survives the work branch's merge without
  landing on `main`. On a public repo every pushed branch is public too, so keep notes there free of
  private details. The frontmatter goes above the first line of any repo template:

  ```yaml
  ---
  project: <project name as on the owner's dashboard>
  agent: <the trailer's Agent, lowercase: claude, codex, hermes, …>
  branch: <branch>
  merged: false
  status: <one line>
  outstanding:
    - "to-do [mac-local] <item>"   # [mac-local] = only actionable on the owner's Mac
  ---
  ```

  Then cover what changed, the evidence, what's left, and any decision the brief didn't cover.
<!-- END agent-contracts:family -->

<!-- BEGIN agent-contracts:aemo-facts -->
<!-- source: facts/AEMO-FACTS.md sha256:c32c596c4a5d — edit in cutout-z/agent-contracts, not here -->
## AEMO shared facts — every agent, every repo

One canonical home for cross-repo AEMO domain facts, so a fact established in one repo is never
invisible to an agent working in another (the battery-MLF lesson, 2026-10-05: the TLF orientation
was established inside one repo's rollout doc while other agents recomputed with the wrong factor).

**Who reads this:** any agent (Hermes, Claude Code, Codex) working in an AEMO repo. It is stamped
into each AEMO repo's `AGENTS.md` between `agent-contracts:aemo-facts` markers, and the `aemo-audit`
and `aemo-logic-pass` skills point here instead of duplicating facts. A fact here overrides anything
remembered or re-derived.

**Maintenance:** append with date + source; supersede in place (`~~SUPERSEDED~~ <date> <reason>`).
Location: `agent-contracts/facts/AEMO-FACTS.md` (git, `cutout-z/agent-contracts`). Edit here only; `scripts/stamp.py` copies it into each AEMO repo's AGENTS.md.

| Fact | Detail | Verified | Source |
|---|---|---|---|
| TLF orientation | `TRANSMISSIONLOSSFACTOR` = **Import** MLF; `SECONDARY_TLF` = **Export** MLF for BIDIRECTIONAL (battery) units. A battery's export MLF comes from `SECONDARY_TLF`, never `TRANSMISSIONLOSSFACTOR`. Confirmed 51/51 differing batteries against AEMO's 2026-27 workbook. The MLF Tracker used the import factor for FY24-25/FY25-26 battery export values until fix `dbaf6f7` (2026-10-05); downstream battery revenue was restated (~−$13.1M across 26 batteries' months) after the fix. | 2026-10-05 | AEMO 2026-27 MLF workbook; `cutout-z/aemo-generator-credit-dashboard`: `audits/Logic Pass Rollout 2026-10-05.md` |
| FCAS regime | FCAS causer-pays contribution factors are DEAD — replaced by the Frequency Performance Payment (FPP) on 8 June 2025 (5-minute contribution factors on NEMWEB). Never recommend or resurrect causer-pays factor tracking; FPP cost-allocation factors per DUID are the future extension. | 2026-09-01 | AEMO/NEMWEB; `aemo-audit` skill; credit repo `docs/FUTURE_DATA_SOURCES.md` |
<!-- END agent-contracts:aemo-facts -->

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
| Shared sidebar | `docs/index.html` loads `https://cutout-z.github.io/aemo-dashboards/nav.js` (repo `cutout-z/aemo-dashboards`, added 2026-10-10): the sidebar and phone top bar every AEMO dashboard shares. It changes there, not here, and a change there reaches this page with no PR here. It pads `body` by 232px at 1024px and wider, so check layout changes at that width. Keep the tag plain (not `defer`) at the end of `<head>` |
| Data | `docs/data/**` — 31 MB, 1,088 generator JSONs + market/quarterly/FCAS/outage/reference files |
| Presentation entry point | `docs/index.html` → `assets/css/tailwind.src.css` (tokens) → `docs/assets/app.css` (compiled) |
| Chart colours | `assets/js/chart-tokens.js` (reads the CSS variables) → published to `docs/assets/js/chart-tokens.js` by `./scripts/build-css.sh` |
| Deep links | selection is by URL hash: `#/<DUID>` — every one of the 630 units is directly addressable |
| Themes | dark default; the app-bar toggle flips to light and is remembered per viewer (localStorage); `?theme=light` / `?theme=dark` forces one (screenshots, shared links). A flip redraws the charts from the tokens. `tests/test_design_assets.py` fails on any literal colour in the page |
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

And the interactions a design pass must not lose (search, filters, period, fullscreen, CSV/XLSX,
axis drag on both axes, deep links, duration switch, offer-day select, reorder, resize, phone scroll):

```bash
cd <repo> && /opt/anaconda3/bin/python3 scripts/verify-interactions.py
```

`verify-design.py` writes nothing by default. Add `--screens` to write the evidence screenshots
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
- [ ] `scripts/verify-design.py --dashboard ADPBA1` and `scripts/verify-interactions.py` both exit 0.
- [ ] `./scripts/build-css.sh` run after the last class change; `docs/assets/app.css` and
      `docs/assets/js/chart-tokens.js` committed.
- [ ] Screenshots for every surface you changed, at desktop and phone width, in `design/screens/`
      (never `docs/` — that is the published site).
- [ ] Presentation-only: `git diff --stat main` shows no `docs/data/**`, `src/**` or `deploy/**`.
- [ ] A short report: surfaces changed of the ones that exist, what you did not get to, and anything
      you had to decide that the brief did not cover.

If you run out of time mid-change: commit what works, leave the branch pushed, and say plainly what is
half-done. A half-migrated panel that is labelled is worth more than a clean-looking one that is not.
