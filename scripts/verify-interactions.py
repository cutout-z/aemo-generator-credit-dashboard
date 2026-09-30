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

import sys

from playwright.sync_api import sync_playwright

BASE = "http://127.0.0.1:9350/"
UNIT = "CLRKCWF1"   # wind farm: every panel type, constraints, offer curve, FCAS
results: list[tuple[bool, str, str]] = []


def check(ok: bool, name: str, detail: str = "") -> None:
    results.append((bool(ok), name, detail))


def plot_range(pg, chart_id: str, axis: str):
    return pg.evaluate(f"document.getElementById('{chart_id}')._fullLayout.{axis}axis.range.slice()")


def axis_drag(pg, chart_id: str, axis: str) -> tuple[list, list]:
    """Grab the axis strip where its tick labels actually are, and drag along it."""
    pg.evaluate(f"document.getElementById('{chart_id}').scrollIntoView({{block: 'center'}})")
    pg.wait_for_timeout(300)
    box = pg.evaluate(f"""(() => {{
        const gd = document.getElementById('{chart_id}'), r = gd.getBoundingClientRect(), s = gd._fullLayout._size;
        return '{axis}' === 'y'
            ? [r.left + s.l / 2, r.top + s.t + s.h / 2]
            : [r.left + s.l + s.w / 2, r.top + s.t + s.h + 12];
    }})()""")
    before = plot_range(pg, chart_id, axis)
    pg.mouse.move(*box)
    pg.mouse.down()
    dx, dy = (0, 60) if axis == "y" else (80, 0)   # down / right = expand
    pg.mouse.move(box[0] + dx, box[1] + dy, steps=8)
    pg.mouse.up()
    pg.wait_for_timeout(400)
    after = plot_range(pg, chart_id, axis)
    # Expanded around the same view — in Plotly's linear units, so date axes are compared as dates
    # (a range scaled as strings once jumped to Plotly's 2000–2001 default and still "changed").
    expanded = pg.evaluate(f"""(([b, a]) => {{ const ax = document.getElementById('{chart_id}')._fullLayout.{axis}axis;
        return ax.r2l(a[0]) < ax.r2l(b[0]) && ax.r2l(a[1]) > ax.r2l(b[1]); }})""", [before, after])
    return before, after if expanded else ["NOT EXPANDED", *after]


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

        # Axis drag (initAxisRangeDrag), both axes, at the tick-label strips
        yb, ya = axis_drag(pg, "chartCapacity", "y")
        check(ya[0] != "NOT EXPANDED", "axis drag — y (linear)", f"{yb} → {ya}")
        xb, xa = axis_drag(pg, "chartMarketSpread", "x")
        check(xa[0] != "NOT EXPANDED", "axis drag — x (date)", f"{xb} → {xa}")
        pg.evaluate("Plotly.relayout('chartCapacity', {'yaxis.autorange': true}); Plotly.relayout('chartMarketSpread', {'xaxis.autorange': true})")

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

        # Offer-day select redraws the bid stack
        days = pg.eval_on_selector_all("#offerCurveDay option", "e => e.map(o => o.value)")
        if len(days) > 1:
            before = pg.evaluate("JSON.stringify(document.getElementById('offerCurveChart').data[0].x)")
            pg.select_option("#offerCurveDay", days[-1])
            pg.wait_for_timeout(600)
            after = pg.evaluate("JSON.stringify(document.getElementById('offerCurveChart').data[0].x)")
            check(before != after or len(days) == 1, "offer-day select", f"{days[0]} → {days[-1]}")

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

        # Corner resize handle
        pnl = pg.locator("#panelMarketSpread")
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
