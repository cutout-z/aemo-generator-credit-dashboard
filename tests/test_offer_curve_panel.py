"""Offer-curve panel visibility contract: unit-level blocks vs regional FCAS.

House rule: a data-layer JSON is not a user-visible panel. The per-DUID daily
bid-stack (#offerCurveWrap) and the offers summary (#offersBox) are UNIT-level
blocks; #panelFCAS is REGIONAL market context that renderCharts() hides
whenever the unit's fcas block has fewer than three non-null monthly points
(or no fcas block at all).

Regression guarded here: both blocks used to sit INSIDE #panelFCAS, so a unit
with a complete offers/offer-curve payload but a thin or absent FCAS block kept
its child at display:block behind a display:none ancestor — the panel never
reached the screen (doc['offers'] and docs/data/offer_curves/{DUID}.json both
present). Observed instances in the Sep-2026 vintage: PLBESS1 (Pine Lodge BESS)
and ERB02 (Eraring BESS 2) — both batteries, which is the battery-DUID case;
units with enough FCAS history (ADPBA1, ER01) rendered fine either way.

ADPBA1G, the DUID the report named, is a different matter and its absence is
intentional: it is a retired 7 MW genset at the Adelaide Desalination Plant
(MLF tracker STATUS=Retired, last MLF FY23-24; it has no row in the current NEM
Registration and Exemption List). S3-06 removed the unvalidated GENSETID->DUID
metadata append, so a unit with no NEM registration is not in index.json and
cannot be selected at all; with no bid rows in the published window there is
also no docs/data/offer_curves/ADPBA1G.json. No panel, because no publication —
not because a CSS condition hid one.
"""

import json
import re
from html.parser import HTMLParser
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INDEX_HTML = ROOT / "docs" / "index.html"
DOCS_DATA = ROOT / "docs" / "data"
GENERATORS_DIR = DOCS_DATA / "generators"
OFFER_CURVES_DIR = DOCS_DATA / "offer_curves"

# FCAS render gate, mirrored from docs/index.html renderCharts():
# at least one service needs >= 3 non-null monthly points.
FCAS_MIN_POINTS = 3

VOID_ELEMENTS = {
    "area", "base", "br", "col", "embed", "hr", "img", "input", "link",
    "meta", "param", "source", "track", "wbr",
}


class _NestingParser(HTMLParser):
    """Element-id -> ancestor-id stack, so nesting claims are checkable."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack: list[tuple[str, str | None]] = []
        self.ancestors: dict[str, tuple[str, ...]] = {}

    def _record(self, tag, attrs):
        element_id = dict(attrs).get("id")
        if element_id:
            self.ancestors[element_id] = tuple(i for _, i in self.stack if i)
        if tag not in VOID_ELEMENTS:
            self.stack.append((tag, element_id))

    def handle_starttag(self, tag, attrs):
        self._record(tag, attrs)

    def handle_startendtag(self, tag, attrs):
        self._record(tag, attrs)
        if tag not in VOID_ELEMENTS:
            self.stack.pop()

    def handle_endtag(self, tag):
        for i in range(len(self.stack) - 1, -1, -1):
            if self.stack[i][0] == tag:
                del self.stack[i:]
                return


def _nesting() -> _NestingParser:
    parser = _NestingParser()
    parser.feed(INDEX_HTML.read_text())
    return parser


def _published_curve_units() -> list[dict]:
    """Indexed units that have a published daily bid-stack file."""
    index = json.loads((DOCS_DATA / "index.json").read_text())
    rows = []
    for entry in index:
        if entry.get("type") == "station":
            continue
        duid = entry["duid"]
        curve_path = OFFER_CURVES_DIR / f"{duid}.json"
        if not curve_path.exists():
            continue
        doc = json.loads((GENERATORS_DIR / f"{entry['file']}.json").read_text())
        services = ((doc.get("fcas") or {}).get("services")) or {}
        rows.append({
            "duid": duid,
            "fuel_category": entry.get("fuel_category"),
            "curve_days": len(json.loads(curve_path.read_text()).get("days") or []),
            "has_offers": bool(doc.get("offers")),
            "has_curve_doc": bool(doc.get("offer_curve")),
            "fcas_renderable": any(
                len([v for v in values if v is not None]) >= FCAS_MIN_POINTS
                for values in services.values()
            ),
        })
    return rows


class TestPanelMarkup:
    """Static contract on the file the dashboard actually serves."""

    def test_offer_blocks_are_not_nested_inside_the_fcas_panel(self):
        nesting = _nesting()
        assert "offerCurveWrap" in nesting.ancestors, "#offerCurveWrap missing from docs/index.html"
        assert "offersBox" in nesting.ancestors, "#offersBox missing from docs/index.html"
        assert "panelFCAS" not in nesting.ancestors["offerCurveWrap"], (
            "#offerCurveWrap is nested inside #panelFCAS: renderCharts() sets "
            "#panelFCAS to display:none when the unit's regional FCAS block is "
            "thin, which leaves a live offer-curve payload invisible"
        )
        assert "panelFCAS" not in nesting.ancestors["offersBox"], (
            "#offersBox is nested inside #panelFCAS — same visibility trap"
        )

    def test_offer_blocks_live_in_their_own_panel(self):
        nesting = _nesting()
        assert nesting.ancestors["offerCurveWrap"][-1] == "panelOffers"
        assert nesting.ancestors["offersBox"][-1] == "panelOffers"

    def test_panel_visibility_synced_at_every_offer_display_branch(self):
        html = INDEX_HTML.read_text()
        assert re.search(r"function\s+syncOffersPanel\s*\(", html), "syncOffersPanel() helper missing"
        # call sites: purge, offers box, curve success, curve empty, curve
        # failure, no-DUID fallback.
        assert html.count("syncOffersPanel();") >= 5, (
            "syncOffersPanel() must follow each #offersBox/#offerCurveWrap "
            "display switch or the panel can stay stale"
        )


class TestPublishedOfferCurveVisibility:
    """The published vintage must be renderable without regional FCAS data."""

    def test_every_published_curve_has_a_renderable_payload(self):
        rows = _published_curve_units()
        assert rows, "no indexed unit has a published offer-curve file — data missing?"
        broken = [
            r["duid"] for r in rows
            if not (r["has_offers"] and r["has_curve_doc"] and r["curve_days"] > 0)
        ]
        assert broken == [], f"units with an unrenderable offer payload: {broken}"

    def test_battery_duids_without_regional_fcas_are_still_complete(self):
        """The reported battery-DUID case, at the data layer.

        A unit whose regional FCAS block is too thin to chart must keep a full
        unit-level offer payload, so the (now FCAS-independent) panel has
        content. Sep-2026 vintage: PLBESS1 (Pine Lodge BESS), ERB02 (Eraring
        BESS 2) — both batteries.
        """
        rows = {r["duid"]: r for r in _published_curve_units()}
        thin = [r for r in rows.values() if not r["fcas_renderable"]]
        for r in thin:
            assert r["has_offers"], r
            assert r["has_curve_doc"], r
            assert r["curve_days"] > 0, r
        # The battery shape reported against ADPBA1G, when the vintage has it.
        batteries = [r for r in thin if r["fuel_category"] == "Battery"]
        for r in batteries:
            assert r["has_offers"] and r["curve_days"] > 0, r

    def test_retired_genset_duids_stay_unpublished(self):
        """ADPBA1G's missing panel is absence-by-publication, not a hide bug.

        Only registration-list DUIDs (which carry a NEM region) are indexed
        (S3-06); a retired genset such as ADPBA1G is not offered by the UI and
        has no offer-curve file. If AEMO ever re-registers it this test is the
        reminder that the panel then needs a curve file too.
        """
        index = json.loads((DOCS_DATA / "index.json").read_text())
        indexed = {e["duid"] for e in index}
        assert "ADPBA1G" not in indexed
        assert not (OFFER_CURVES_DIR / "ADPBA1G.json").exists()
