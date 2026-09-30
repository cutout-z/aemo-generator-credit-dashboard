#!/usr/bin/env python3
"""Verify the design pack in a real browser — the check the pass should rerun after every change.

    # proof page (tokens, components, one chart, theme flip)
    cd <repo> && python3 -m http.server 9351 --bind 127.0.0.1 &
    /opt/anaconda3/bin/python3 scripts/verify-design.py

    # a dashboard generator view (needs the docs/ server on 9350)
    /opt/anaconda3/bin/python3 scripts/verify-design.py --dashboard ADPBA1

    # ...and write the evidence screenshots into design/screens/ (off by default, so a routine
    # check never dirties the tree)
    /opt/anaconda3/bin/python3 scripts/verify-design.py --dashboard ADPBA1 --screens

Why a script and not a screenshot-by-eye: "looks done" and "is done" diverge on this page in known
ways — a component class that Tailwind purged, a chart created inside a hidden panel (0px), a chart
still using hard-coded colours instead of the tokens. All three are invisible in a diff and obvious here.

Exit code is 1 if any check fails, so it can gate a pass.
"""
from __future__ import annotations

import argparse
import pathlib
import sys

from playwright.sync_api import sync_playwright

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCREENS = ROOT / "design" / "screens"   # unpublished: docs/ is the public GitHub Pages tree


def rgb(value: str) -> tuple[int, int, int]:
    """Normalise '#171717' and 'rgb(23, 23, 23)' to the same tuple."""
    v = value.strip()
    if v.startswith("#"):
        v = v.lstrip("#")
        if len(v) == 3:
            v = "".join(c * 2 for c in v)
        return tuple(int(v[i : i + 2], 16) for i in (0, 2, 4))  # type: ignore[return-value]
    if v.startswith("rgb"):
        parts = v[v.index("(") + 1 : v.index(")")].split(",")
        return tuple(int(float(p)) for p in parts[:3])  # type: ignore[return-value]
    return (-1, -1, -1)


def check_proof_page(pg, failures: list[str], shots: bool) -> None:
    pg.goto("http://127.0.0.1:9351/design/tokens.html", wait_until="networkidle", timeout=60000)
    pg.wait_for_timeout(2000)

    surface = pg.evaluate("getComputedStyle(document.documentElement).getPropertyValue('--surface').trim()")
    card_bg = pg.evaluate("getComputedStyle(document.querySelector('.card')).backgroundColor")
    print(f"  --surface {surface} vs .card {card_bg}")
    if rgb(surface) != rgb(card_bg):
        failures.append(f"card background {card_bg} is not the surface token {surface}")

    for cls, label in ((".chip", "chips"), (".seg-item", "segment items"), (".kpi-value", "KPI value")):
        n = pg.eval_on_selector_all(cls, "e => e.length")
        h = pg.eval_on_selector_all(cls, "e => Math.max(...e.map(x => x.getBoundingClientRect().height))")
        print(f"  {cls:12} count={n} height={h:.0f}px")
        if n == 0:
            failures.append(f"{label} missing — did you forget ./scripts/build-css.sh? (Tailwind purges unused classes)")

    charts = pg.eval_on_selector_all(".js-plotly-plot", "e => e.length")
    zero = pg.eval_on_selector_all(".js-plotly-plot", "e => e.filter(x => x.getBoundingClientRect().height < 5).length")
    fill = pg.evaluate("() => { const el = document.getElementById('demo-chart'); return el && el.data ? el.data[0].marker.color : null; }")
    token = pg.evaluate("ChartTokens.color('info')")
    print(f"  charts {charts} (zero-height {zero}), bar fill {fill} vs token {token}")
    if charts and zero:
        failures.append(f"{zero} chart(s) rendered 0px — pass an explicit height from ChartTokens.HEIGHTS")
    if charts and fill and fill.lower() != token.lower():
        failures.append(f"chart colour {fill} is not the token colour {token} — a hard-coded colour survived")

    if shots:
        pg.screenshot(path=str(SCREENS / "tokens-proof-dark.png"), full_page=True)
    dark_card = pg.evaluate("getComputedStyle(document.querySelector('.card')).backgroundColor")
    pg.evaluate("document.documentElement.setAttribute('data-theme','light'); ChartTokens.restyle()")
    pg.wait_for_timeout(1000)
    light_card = pg.evaluate("getComputedStyle(document.querySelector('.card')).backgroundColor")
    print(f"  theme flip: card {dark_card} -> {light_card}")
    if rgb(dark_card) == rgb(light_card):
        failures.append("the theme flip changed nothing — the page is not using the token variables")
    if shots:
        pg.screenshot(path=str(SCREENS / "tokens-proof-light.png"), full_page=True)


def check_dashboard(pg, duid: str, failures: list[str], shots: bool) -> None:
    pg.goto(f"http://127.0.0.1:9350/#{duid}", wait_until="networkidle", timeout=60000)
    pg.wait_for_timeout(1500)
    if pg.eval_on_selector_all(".js-plotly-plot", "e => e.length") == 0:
        pg.evaluate(f"selectGenerator('{duid}')")
    try:
        pg.wait_for_selector(".js-plotly-plot", timeout=20000)
    except Exception:  # noqa: BLE001
        failures.append(f"{duid}: no chart ever rendered")
        return
    pg.wait_for_timeout(2000)
    charts = pg.eval_on_selector_all(".js-plotly-plot", "e => e.length")
    zero = pg.eval_on_selector_all(".js-plotly-plot", "e => e.filter(x => x.getBoundingClientRect().height < 5).length")
    height = pg.evaluate("document.body.scrollHeight")
    print(f"  {duid}: charts {charts}, zero-height {zero}, page {height}px")
    if zero:
        failures.append(f"{duid}: {zero} chart(s) at 0px")
    if not shots:
        return
    pg.screenshot(path=str(SCREENS / f"after-{duid}-top.png"))
    for frac, tag in ((0.3, "mid"), (0.6, "low"), (0.9, "foot")):
        pg.evaluate(f"window.scrollTo(0, Math.floor(document.body.scrollHeight * {frac}))")
        pg.wait_for_timeout(900)
        pg.screenshot(path=str(SCREENS / f"after-{duid}-{tag}.png"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dashboard", metavar="DUID", help="also verify the dashboard at #<DUID> (server on 9350)")
    ap.add_argument("--screens", action="store_true",
                    help="write screenshots to design/screens/ (default: check only, write nothing)")
    args = ap.parse_args()
    if args.screens:
        SCREENS.mkdir(parents=True, exist_ok=True)
    failures: list[str] = []
    with sync_playwright() as p:
        b = p.chromium.launch()
        pg = b.new_page(viewport={"width": 1440, "height": 960})
        pg.on("pageerror", lambda e: failures.append(f"page error: {str(e)[:100]}"))
        pg.on("response", lambda r: failures.append(f"HTTP {r.status} {r.url[-50:]}") if r.status >= 400 else None)
        print("proof page (design/tokens.html on :9351)")
        try:
            check_proof_page(pg, failures, args.screens)
        except Exception as exc:  # noqa: BLE001
            failures.append(f"proof page could not be checked: {type(exc).__name__}: {exc}")
        if args.dashboard:
            print(f"dashboard (#{args.dashboard} on :9350)")
            try:
                check_dashboard(pg, args.dashboard, failures, args.screens)
            except Exception as exc:  # noqa: BLE001
                failures.append(f"dashboard could not be checked: {type(exc).__name__}: {exc}")
        b.close()
    if failures:
        print("\n  FAILED:")
        for f in failures:
            print(f"    - {f}")
        return 1
    print("\n  all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
