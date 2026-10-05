#!/usr/bin/env python3
"""Verify the dashboard's interactions in a real browser — the ones BRIEF.md step 8 says a design
pass must not lose: search (typing + keyboard), filters, period switch, fullscreen, CSV/XLSX export,
the hand-written axis-drag (initAxisRangeDrag), deep links, the battery-duration switch, the
offer-day select, drag-to-reorder and panel resize — plus touch scrolling on a phone.

    cd <repo>/docs && python3 -m http.server 9350 --bind 127.0.0.1 &
    /opt/anaconda3/bin/python3 scripts/verify-interactions.py

Reads only; writes nothing. Exit code 1 if any interaction is broken.
"""
from __future__ import annotations

import json
import pathlib
import sys

from playwright.sync_api import sync_playwright

ROOT = pathlib.Path(__file__).resolve().parent.parent
BASE = "http://127.0.0.1:9350/"
UNIT = "CLRKCWF1"   # wind farm: every panel type, constraints, offer curve, FCAS
results: list[tuple[bool, str, str]] = []


def check(ok: bool, name: str, detail: str = "") -> None:
    results.append((bool(ok), name, detail))


def plot_range(pg, chart_id: str, axis: str):
    return pg.evaluate(f"document.getElementById('{chart_id}')._fullLayout.{axis}axis.range.slice()")


SWEEP_AXES = """(id) => { const L = document.getElementById(id)._fullLayout;
    return Object.keys(L).filter(k => /^[xy]axis\\d*$/.test(k) && L[k]._length).map(k => ({ k, type: L[k].type,
        labels: L[k].showticklabels !== false, matches: L[k].matches || null })); }"""
RANGES = """(id) => { const L = document.getElementById(id)._fullLayout;
    return Object.fromEntries(Object.keys(L).filter(k => /^[xy]axis\\d*$/.test(k) && L[k]._length).map(k => [k, L[k].range.map(v => L[k].r2l(v))])); }"""
STRIP = """([id, k]) => { const gd = document.getElementById(id), r = gd.getBoundingClientRect(), L = gd._fullLayout, a = L[k], s = L._size;
    if (k[0] === 'x') { const ya = L[String(a.anchor || 'y').replace(/^y/, 'yaxis')];
        return [r.left + a._offset + a._length / 2, r.top + ya._offset + ya._length + 10]; }
    return [a.side === 'right' ? r.right - s.r / 2 : r.left + s.l / 2, r.top + a._offset + a._length / 2]; }"""


def axis_sweep(pg) -> list[str]:
    """Drag every draggable axis strip of every chart on screen. Each drag must expand exactly the
    grabbed axis around its current view (plus any axis Plotly links to it with `matches`);
    returns a line per problem."""
    problems = []
    ids = pg.evaluate("[...document.querySelectorAll('.js-plotly-plot')].filter(e => e.data && e.getBoundingClientRect().height > 50).map(e => e.id)")
    for cid in ids:
        pg.evaluate(f"document.getElementById('{cid}').scrollIntoView({{block: 'center'}})")
        pg.wait_for_timeout(250)
        axes = pg.evaluate(SWEEP_AXES, cid)
        for a in axes:
            k = a["k"]
            if not a["labels"] or (k[0] == "y" and a["type"] in ("category", "date", "log")) or a["type"] == "log":
                continue
            linked = {k} | {b["k"] for b in axes if b["matches"] and b["matches"].replace("x", "xaxis").replace("y", "yaxis") == k}
            if a["matches"]:
                linked.add(a["matches"].replace("x", "xaxis", 1) if a["matches"][0] == "x" else a["matches"].replace("y", "yaxis", 1))
            before = pg.evaluate(RANGES, cid)
            x, y = pg.evaluate(STRIP, [cid, k])
            pg.mouse.move(x, y)
            pg.mouse.down()
            pg.mouse.move(x + (80 if k[0] == "x" else 0), y + (60 if k[0] == "y" else 0), steps=6)
            pg.mouse.up()
            pg.wait_for_timeout(300)
            after = pg.evaluate(RANGES, cid)
            # An axis "moved" if either end shifted by more than 0.1% of its span (Plotly re-pads
            # autoranged axes by a hair when another axis changes; that is not a drag leaking).
            span = lambda r: abs(r[1] - r[0]) or 1
            moved = {n for n in before if max(abs(after[n][0] - before[n][0]), abs(after[n][1] - before[n][1])) > 0.001 * span(before[n])}
            grew = after[k][0] < before[k][0] and after[k][1] > before[k][1]
            if not grew or not moved <= linked:
                problems.append(f"{cid}.{k}: {'not expanded' if not grew else 'also moved ' + str(sorted(moved - linked))}")
            pg.evaluate(f"Plotly.relayout('{cid}', Object.fromEntries({list(before)}.map(n => [n + '.autorange', true])))")
            pg.wait_for_timeout(150)
    return problems


def main() -> int:
    with sync_playwright() as p:
        b = p.chromium.launch()
        errors: list[str] = []
        pg = b.new_page(viewport={"width": 1440, "height": 960}, accept_downloads=True)
        pg.on("pageerror", lambda e: errors.append(str(e)[:160]))

        # Search: type, keyboard-select, deep link written
        pg.goto(BASE, wait_until="networkidle")
        pg.wait_for_timeout(500)
        pg.fill("#searchInput", "clarke creek")
        pg.wait_for_timeout(300)
        n = pg.eval_on_selector_all("#searchDropdown .search-item[data-index]", "e => e.length")
        pg.keyboard.press("ArrowDown")
        pg.keyboard.press("Enter")
        pg.wait_for_selector(".js-plotly-plot", timeout=20000)
        pg.wait_for_timeout(2500)
        check(n > 0 and pg.evaluate("location.hash").startswith("#CLRKCWF"), "search + keyboard select",
              f"{n} results, hash {pg.evaluate('location.hash')}")

        # Filters narrow the results
        pg.fill("#searchInput", "")
        pg.click("#regionSeg .seg-item[data-value='SA1']")
        pg.select_option("#filterFuel", "Battery")
        pg.wait_for_timeout(300)
        badges = pg.eval_on_selector_all("#searchDropdown .search-item[data-index] .badge", "e => [...new Set(e.map(x => x.textContent))]")
        check(set(badges) <= {"SA1", "Battery", "Station"} and "SA1" in badges, "region + fuel filters", str(badges))
        pg.click("#regionSeg .seg-item[data-value='']")
        pg.select_option("#filterFuel", "")
        pg.keyboard.press("Escape")

        # Deep link
        pg.goto(BASE + "#" + UNIT, wait_until="networkidle")
        pg.wait_for_timeout(3000)
        charts = pg.eval_on_selector_all(".js-plotly-plot", "e => e.filter(x => x.data && x.getBoundingClientRect().height > 50).length")
        check(charts >= 9, "deep link renders the unit", f"{charts} charts")

        # Period switch slices the monthly charts
        pg.click(".time-btn[data-months='6']")
        pg.wait_for_timeout(700)
        six = pg.evaluate("document.getElementById('chartGeneration').data[0].x.length")
        pg.click(".time-btn[data-months='36']")
        pg.wait_for_timeout(700)
        full = pg.evaluate("document.getElementById('chartGeneration').data[0].x.length")
        check(six == 6 and full > 6, "period switch", f"6M → {six} months, 3Y → {full}")

        # Axis drag (initAxisRangeDrag): every axis of every chart, at its tick-label strip
        n_axes = pg.evaluate("[...document.querySelectorAll('.js-plotly-plot')].filter(e => e.data).length")
        problems = axis_sweep(pg)
        check(not problems, "axis drag — every axis, every chart", "; ".join(problems) or f"{n_axes} charts swept")

        # Fullscreen: button opens and grows the chart; Escape and the overlay both close
        panel = "#chartCapacity >> xpath=ancestor::div[contains(@class,'chart-panel')][1]"
        h0 = pg.evaluate("document.getElementById('chartCapacity').getBoundingClientRect().height")
        pg.hover("#chartCapacity")
        pg.click(panel + "//button[contains(@class,'chart-expand-btn')]")
        pg.wait_for_timeout(800)
        h1 = pg.evaluate("document.getElementById('chartCapacity').getBoundingClientRect().height")
        pg.keyboard.press("Escape")
        pg.wait_for_timeout(800)
        esc = pg.evaluate("document.querySelectorAll('.chart-panel.fullscreen').length")
        pg.hover("#chartCapacity")
        pg.click(panel + "//button[contains(@class,'chart-expand-btn')]")
        pg.wait_for_timeout(500)
        pg.click(".chart-panel.fullscreen .chart-expand-btn")   # the panel covers the viewport: ✕ closes
        pg.wait_for_timeout(800)
        closed = pg.evaluate("document.querySelectorAll('.chart-panel.fullscreen').length")
        h2 = pg.evaluate("document.getElementById('chartCapacity').getBoundingClientRect().height")
        check(h1 > h0 + 100 and esc == 0 and closed == 0 and abs(h2 - h0) < 2, "fullscreen open / Esc / close button",
              f"{h0:.0f}px → {h1:.0f}px → {h2:.0f}px")

        # Exports
        for label in ("CSV", "XLSX"):
            with pg.expect_download(timeout=15000) as d:
                pg.click(f"#downloadButtons button:has-text('{label}')")
            path = d.value.path()
            size = path.stat().st_size if path else 0
            check(size > 1000, f"{label} export", f"{d.value.suggested_filename}, {size} bytes")

        # Battery-duration switch
        pg.click("#spreadDuration button[data-dur='4h']")
        pg.wait_for_timeout(600)
        title = pg.evaluate("document.getElementById('chartMarketSpread').layout.yaxis.title.text")
        check("4h" in title, "battery-duration switch", title)

        # Offer-day select redraws the bid stack. The target day is chosen from the unit's own offer
        # file so its stack really differs from the day on screen: a wind farm often bids the same
        # stack for weeks (CLRKCWF1's first and last days are identical), and comparing those two
        # proves nothing either way. Selecting back must restore the first drawing.
        days = pg.eval_on_selector_all("#offerCurveDay option", "e => e.map(o => o.value)")
        shown_day = pg.input_value("#offerCurveDay") if days else None
        raw_days = {d["date"]: d["stack"] for d in json.loads(
            (ROOT / "docs" / "data" / "offer_curves" / f"{UNIT}.json").read_text())["days"]}
        stacks = {k: json.dumps(v) for k, v in raw_days.items()}
        # What drawBidStack plots for a day: a step from (first price, 0) through each (price, cumulative MW).
        expect = lambda day: [[[raw_days[day][0][0]] + [p for p, _ in raw_days[day]], [0] + [c for _, c in raw_days[day]]]]
        other = next((d for d in days if shown_day in stacks and d in stacks and stacks[d] != stacks[shown_day]), None)
        draw = "JSON.stringify(document.getElementById('offerCurveChart').data.map(t => [t.x, t.y]))"
        if other is None:
            check(False, "offer-day select", f"{len(days)} days on the page, none with a stack unlike {shown_day}: pick another UNIT")
        else:
            before = pg.evaluate(draw)
            pg.select_option("#offerCurveDay", other)
            pg.wait_for_timeout(600)
            after = pg.evaluate(draw)
            pg.select_option("#offerCurveDay", shown_day)
            pg.wait_for_timeout(600)
            back = pg.evaluate(draw)
            check(before != after and back == before, "offer-day select",
                  f"{shown_day} → {other} redraws: {before != after}; back restores: {back == before}")
            check(json.loads(before) == expect(shown_day) and json.loads(after) == expect(other),
                  "offer-day bid stack equals the unit's offer file",
                  f"{shown_day}: {len(raw_days[shown_day])} bands, {other}: {len(raw_days[other])} bands")

        # Drag-to-reorder within a group (synthetic HTML5 drag events), refused across groups
        order = "[...document.querySelectorAll('.panel-group')].map(g => [...g.querySelectorAll('.chart-panel')].filter(p => getComputedStyle(p).display !== 'none').map(p => p.id || p.querySelector('h3').textContent))"
        drag_js = """([src, dst]) => {
            const find = n => [...document.querySelectorAll('.chart-panel h3')].find(h => h.textContent === n);
            const s = find(src), d = find(dst), dt = new DataTransfer();
            s.dispatchEvent(new MouseEvent('mousedown', {bubbles: true}));
            s.closest('.chart-panel').dispatchEvent(new DragEvent('dragstart', {bubbles: true, dataTransfer: dt}));
            d.dispatchEvent(new DragEvent('dragover', {bubbles: true, cancelable: true, dataTransfer: dt}));
            d.dispatchEvent(new DragEvent('drop', {bubbles: true, cancelable: true, dataTransfer: dt}));
            s.closest('.chart-panel').dispatchEvent(new DragEvent('dragend', {bubbles: true, dataTransfer: dt}));
        }"""
        o0 = pg.evaluate(order)
        pg.evaluate(drag_js, ["Price Capture", "Generation"])
        o1 = pg.evaluate(order)
        pg.evaluate(drag_js, ["Generation", "MLF Trajectory (Annual)"])
        o2 = pg.evaluate(order)
        check(o1 != o0 and o2 == o1, "drag-to-reorder (within a group only)", f"{o0[0][:2]} → {o1[0][:2]}")

        # Theme: the toggle flips the page, redraws the charts from the light tokens, is remembered
        def theme_state():
            return pg.evaluate("""() => ({ theme: document.documentElement.getAttribute('data-theme') || 'dark',
                body: getComputedStyle(document.body).backgroundColor,
                bar: document.getElementById('chartGeneration').data[0].marker.color, token: ChartTokens.color('info'),
                saved: (() => { try { return localStorage.getItem('theme'); } catch (e) { return null; } })() })""")
        t0 = theme_state()
        pg.click("#themeToggle")
        pg.wait_for_timeout(2500)
        t1 = theme_state()
        pg.reload(wait_until="networkidle")
        pg.wait_for_timeout(3000)
        t2 = theme_state()
        pg.click("#themeToggle")
        pg.wait_for_timeout(2500)
        t3 = theme_state()
        check(t0["theme"] == "dark" and t1["theme"] == "light" and t1["body"] != t0["body"]
              and t1["bar"] == t1["token"] != t0["bar"] and t2["theme"] == "light" and t2["bar"] == t1["bar"]
              and t3["theme"] == "dark" and t3["bar"] == t0["bar"] and t3["saved"] == "dark",
              "theme toggle (flip, chart redraw, remembered)", f"bar {t0['bar']} → {t1['bar']} → reload {t2['bar']} → {t3['bar']}")

        # Corner resize handle
        pnl = pg.locator("#panelMarketSpread")
        pg.evaluate("document.getElementById('panelMarketSpread').scrollIntoView({block: 'center'})")
        pg.wait_for_timeout(300)
        bb = pnl.bounding_box()
        pnl.hover()
        pg.mouse.move(bb["x"] + bb["width"] - 6, bb["y"] + bb["height"] - 6)
        pg.mouse.down()
        pg.mouse.move(bb["x"] + bb["width"] - 300, bb["y"] + bb["height"] + 80, steps=8)
        pg.mouse.up()
        pg.wait_for_timeout(500)
        bb2 = pnl.bounding_box()
        check(bb2["width"] < bb["width"] - 200 and bb2["height"] > bb["height"] + 40, "panel resize (corner)",
              f"{bb['width']:.0f}x{bb['height']:.0f} → {bb2['width']:.0f}x{bb2['height']:.0f}")
        pg.close()

        # Phone: a swipe that starts on a chart scrolls the page
        ctx = b.new_context(viewport={"width": 390, "height": 844}, has_touch=True, is_mobile=True)
        ph = ctx.new_page()
        ph.on("pageerror", lambda e: errors.append(str(e)[:160]))
        ph.goto(BASE + "#" + UNIT, wait_until="networkidle")
        ph.wait_for_timeout(2500)
        ph.evaluate("document.getElementById('chartCapacity').scrollIntoView({block: 'center'})")
        ph.wait_for_timeout(300)
        x, y = ph.evaluate("(() => { const r = document.querySelector('#chartCapacity .nsewdrag').getBoundingClientRect(); return [r.x + r.width / 2, r.y + r.height / 2]; })()")
        y0 = ph.evaluate("scrollY")
        cdp = ctx.new_cdp_session(ph)
        cdp.send("Input.dispatchTouchEvent", {"type": "touchStart", "touchPoints": [{"x": x, "y": y}]})
        for i in range(1, 11):
            cdp.send("Input.dispatchTouchEvent", {"type": "touchMove", "touchPoints": [{"x": x, "y": y - 20 * i}]})
        cdp.send("Input.dispatchTouchEvent", {"type": "touchEnd", "touchPoints": []})
        ph.wait_for_timeout(600)
        check(ph.evaluate("scrollY") > y0 + 100, "phone: swipe over a chart scrolls the page", f"scrolled {ph.evaluate('scrollY') - y0}px")

        check(not errors, "no page errors", "; ".join(errors))
        b.close()

    width = max(len(n) for _, n, _ in results)
    for ok, name, detail in results:
        print(f"  {'ok  ' if ok else 'FAIL'}  {name:<{width}}  {detail}")
    failed = [n for ok, n, _ in results if not ok]
    print(f"\n  {len(results) - len(failed)}/{len(results)} interactions ok")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
