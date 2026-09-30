# CLAUDE.md

**Before anything else, read `AGENTS.md`** — it is the contract for work in this repo: the five hard
rules, the facts about how this dashboard is served, the local preview commands, the pitfalls that
have bitten before (Plotly 0px heights, the hand-written axis-drag handler, the three tests that pin
this page's DOM), and the handback checklist.

Then read **`BRIEF.md`** — the design brief for the pass you are running.

Summary of the two, so you cannot miss the shape of it:

- This is a **static** dashboard: one page (`docs/index.html`) + `docs/data/**`, served by GitHub
  Pages. No server, no build at deploy. Presentation work only.
- The design language is already decided and transplanted: tokens in `assets/css/tailwind.src.css`,
  compiled to `docs/assets/app.css`, chart colours via `assets/js/chart-tokens.js`. Adopt it; do not
  invent a second one.
- Never touch `docs/data/**`, `src/**`, `deploy/**`. Never invent data. Verify in a browser.
- Work on the design branch, commit as you go, and finish with the handback checklist in `AGENTS.md`.
