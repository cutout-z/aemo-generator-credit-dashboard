"""Portfolio view (docs/assets/js/portfolio.js): the browser combiner must agree
with the pipeline's station rules.

Oracle: every published station file is a portfolio the pipeline already built.
Rebuilding each one in node from its members' unit files must reproduce it,
within the rounding the unit files carry. Runs the page's real JavaScript, so
it needs node; skipped where node is absent (the NAS lane).
"""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
JS = ROOT / "assets" / "js" / "portfolio.js"   # the source; test_design_assets pins the published copy
DATA = ROOT / "docs" / "data"
NODE = shutil.which("node")

pytestmark = pytest.mark.skipif(NODE is None, reason="node not installed")


def run_node(script: str) -> object:
    out = subprocess.run(
        [NODE, "-e", script], capture_output=True, text=True, timeout=120,
        env={"PORTFOLIO_JS": str(JS), "DATA": str(DATA)},
    )
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


ORACLE = r"""
const fs = require('fs'), path = require('path');
const P = require(process.env.PORTFOLIO_JS);
const D = process.env.DATA;
const index = JSON.parse(fs.readFileSync(path.join(D, 'index.json')));
const units = new Map(index.filter(g => g.type !== 'station').map(g => [g.duid, g]));
const load = f => JSON.parse(fs.readFileSync(path.join(D, 'generators', f + '.json')));
const near = (a, b, tol) => (a == null && b == null) || (a != null && b != null && Math.abs(a - b) <= tol);
const bad = [];
let stations = 0, noMonthly = 0, econCompared = 0, econMarked = 0;
for (const st of index.filter(g => g.type === 'station')) {
    const s = load(st.file);
    if (!s.monthly) { noMonthly++; continue; }
    stations++;
    if (s.monthly.econ_curtailment_weighting === 'energy') econMarked++;
    const members = s.duids.map(d => ({ entry: units.get(d), doc: load(units.get(d).file || d) }));
    const p = P.combine(members, { batteriesInCF: true });
    const n = members.length, sm = s.monthly, pm = p.monthly;
    const flag = (what, i, a, b) => bad.push(`${st.file} ${sm.months[i] || ''} ${what}: station ${a} vs portfolio ${b}`);
    if (JSON.stringify(sm.months) !== JSON.stringify(pm.months)) { bad.push(`${st.file} months differ`); continue; }
    sm.months.forEach((m, i) => {
        const hours = new Date(Date.UTC(+m.slice(0, 4), +m.slice(5, 7), 0)).getUTCDate() * 24;
        const genTol = 0.5 * n + 1;
        if (!near(sm.generation_mwh[i], pm.generation_mwh[i], genTol)) flag('generation', i, sm.generation_mwh[i], pm.generation_mwh[i]);
        if (!near(sm.revenue_aud[i], pm.revenue_aud[i], genTol + 1)) flag('revenue', i, sm.revenue_aud[i], pm.revenue_aud[i]);
        // Unit files publish nameplate to 0.1 MW; the pipeline sums unrounded MW.
        const capTol = 0.05 * n + 1e-3;
        if (!near(sm.capacity_basis_mw[i], pm.capacity_basis_mw[i], capTol)) flag('capacity basis', i, sm.capacity_basis_mw[i], pm.capacity_basis_mw[i]);
        const basis = sm.capacity_basis_mw[i] || 1;
        const cfTol = genTol / (basis * hours) + Math.abs(sm.capacity_factor[i] || 0) * capTol / basis + 1e-4;
        if (!near(sm.capacity_factor[i], pm.capacity_factor[i], cfTol)) flag('capacity factor', i, sm.capacity_factor[i], pm.capacity_factor[i]);
        if (sm.curtailment_pct) {
            if (!near(sm.curtailment_pct[i], pm.curtailment_pct[i], 2e-4)) flag('curtailment', i, sm.curtailment_pct[i], pm.curtailment_pct[i]);
            if (sm.curtailment_potential_mwh) {
                if (!near(sm.curtailment_potential_mwh[i], pm.curtailment_potential_mwh[i], 0.05 * n + 0.1)) flag('potential', i, sm.curtailment_potential_mwh[i], pm.curtailment_potential_mwh[i]);
                if (!near(sm.curtailment_actual_mwh[i], pm.curtailment_actual_mwh[i], 0.05 * n + 0.1)) flag('actual', i, sm.curtailment_actual_mwh[i], pm.curtailment_actual_mwh[i]);
            }
        }
        // Stations published before the energy-weighted rule carry no marker.
        if (sm.econ_curtailment_weighting === 'energy') {
            econCompared++;
            if (!near(sm.econ_curtailment_pct[i], pm.econ_curtailment_pct[i], 2e-4)) flag('economic curtailment', i, sm.econ_curtailment_pct[i], pm.econ_curtailment_pct[i]);
        }
        if (sm.captured_price) {
            // Unit files publish generation to whole MWh, so each member's weight can be
            // 0.5 MWh out: bound the error that shift can cause. A month whose priced
            // generation rounds to zero cannot be rebuilt from the unit files at all.
            const cp = sm.captured_price[i], pcp = pm.captured_price[i];
            const priced = members.map(x => { const mm = x.doc.monthly || {}; const k = (mm.months || []).indexOf(m);
                return k < 0 ? null : { cp: (mm.captured_price || [])[k], gen: mm.generation_mwh[k] }; })
                .filter(r => r && r.cp != null);
            const pricedGen = priced.reduce((a, r) => a + (r.gen || 0), 0);
            const ref = cp != null ? cp : pcp;
            const spread = priced.reduce((a, r) => a + Math.abs(r.cp - (ref || 0)), 0);
            const cpTol = 0.011 + (pricedGen > 0.5 * priced.length ? 0.5 * spread / (pricedGen - 0.5 * priced.length) : Infinity);
            const unrebuildable = pcp == null && pricedGen === 0;
            if (!unrebuildable && !near(cp, pcp, cpTol)) flag('captured price', i, cp, pcp);
            if (!near(sm.avg_rrp[i], pm.avg_rrp[i], 0.011)) flag('average RRP', i, sm.avg_rrp[i], pm.avg_rrp[i]);
        }
        if (sm.revenue_mlf_status && sm.revenue_mlf_status[i] !== pm.revenue_mlf_status[i]) flag('MLF status', i, sm.revenue_mlf_status[i], pm.revenue_mlf_status[i]);
        if (sm.revenue_loss_adjusted_aud && !near(sm.revenue_loss_adjusted_aud[i], pm.revenue_loss_adjusted_aud[i], genTol + 1)) flag('loss-adjusted revenue', i, sm.revenue_loss_adjusted_aud[i], pm.revenue_loss_adjusted_aud[i]);
    });
    if (s.daily) {
        const sd = s.daily, pd = p.daily;
        if (!pd || JSON.stringify(sd.dates) !== JSON.stringify(pd.dates)) bad.push(`${st.file} daily dates differ`);
        else sd.dates.forEach((d, i) => {
            if (!near(sd.generation_mwh[i], pd.generation_mwh[i], 0.05 * n + 0.1)) bad.push(`${st.file} ${d} daily generation ${sd.generation_mwh[i]} vs ${pd.generation_mwh[i]}`);
            // Same 0.1 MW nameplate rounding: the day's basis is generation ÷ (24 × CF).
            const dBasis = pd.capacity_factor[i] > 0 ? pd.generation_mwh[i] / (24 * pd.capacity_factor[i]) : Infinity;
            if (!near(sd.capacity_factor[i], pd.capacity_factor[i], 2e-4 + Math.abs(sd.capacity_factor[i] || 0) * (0.05 * n + 1e-3) / dBasis)) bad.push(`${st.file} ${d} daily CF ${sd.capacity_factor[i]} vs ${pd.capacity_factor[i]}`);
            if (sd.intervals_observed && sd.intervals_observed[i] !== pd.intervals_observed[i]) bad.push(`${st.file} ${d} intervals observed`);
        });
    }
    const sk = Object.keys(s.mlf_by_duid || {}).sort().join(), pk = Object.keys(p.mlf_by_duid || {}).sort().join();
    if (sk !== pk) bad.push(`${st.file} MLF members ${sk} vs ${pk}`);
}
const kinds = {}; bad.forEach(b => { const k = b.split(' ').slice(2).join(' ').split(':')[0]; kinds[k] = (kinds[k] || 0) + 1; }); console.log(JSON.stringify({ stations, noMonthly, econCompared, econMarked, kinds, bad: bad.slice(0, 15), badCount: bad.length }));
"""


def test_every_station_rebuilds_from_its_unit_files():
    res = run_node(ORACLE)
    assert res["stations"] >= 60, res
    assert res["badCount"] == 0, "\n".join(res["bad"])
    # Economic curtailment is compared only on stations published under the energy-weighted
    # rule (the first lane run after it landed); once any carries the marker, it must bite.
    if res["econMarked"]:
        assert res["econCompared"] > 0, res


# The pipeline and the combiner on the same units: generate_all writes the unit files and the
# station file, node combines the unit files. Covers what the published-data oracle cannot until
# stations carry the energy-weighted rule: energy weighting, the per-unit cap, legacy rows that
# recover potential from generation, and the nameplate fallback when no member has energy.
PIPELINE_VS_JS = r"""
const fs = require('fs'), path = require('path');
const P = require(process.env.PORTFOLIO_JS);
const dir = process.env.GEN_DIR;
const load = f => JSON.parse(fs.readFileSync(path.join(dir, f + '.json')));
const station = load('station_Mix_Station');
const members = station.duids.map(d => ({ entry: JSON.parse(process.env.ENTRIES)[d], doc: load(d) }));
const p = P.combine(members, { batteriesInCF: true });
console.log(JSON.stringify({ months: station.monthly.months, pipeline: station.monthly.econ_curtailment_pct,
    browser: p.monthly.econ_curtailment_pct, pMonths: p.monthly.months }));
"""


def test_economic_curtailment_matches_pipeline(tmp_path):
    import pandas as pd

    from src.generate_json import generate_all

    caps = {"GEN_A": 100.0, "GEN_B": 300.0, "GEN_C": 50.0}
    generators = pd.DataFrame({
        "DUID": list(caps), "STATION_NAME": ["Mix Station"] * 3, "REGION": ["QLD1"] * 3,
        "FUEL_CATEGORY": ["Wind", "Wind", "Solar"], "CAPACITY_MW": list(caps.values()),
        "TECHNOLOGY": ["Wind", "Wind", "Solar"], "CONNECTION_POINT": ["CP_A", "CP_B", "CP_C"],
    })
    nan = float("nan")
    rows = [
        # 2026-03: energy columns; GEN_C's economic % (0.3) is above its total (0.2) and is capped.
        # GEN_B's potential is not 10× its nameplate, so energy and nameplate weights differ.
        ("GEN_A", "2026-03", 900, 0.10, 0.02, 900, 1000),
        ("GEN_B", "2026-03", 2400, 0.40, 0.15, 2400, 4000),
        ("GEN_C", "2026-03", 400, 0.20, 0.30, 400, 500),
        # 2026-04: legacy rows, potential recovered as generation / (1 − pct).
        ("GEN_A", "2026-04", 800, 0.20, 0.05, nan, nan),
        ("GEN_B", "2026-04", 2700, 0.40, 0.08, nan, nan),
        ("GEN_C", "2026-04", 450, 0.10, 0.01, nan, nan),
        # 2026-05: no total, so no derivable potential: nameplate-weighted fallback.
        ("GEN_A", "2026-05", 1000, nan, 0.04, nan, nan),
        ("GEN_B", "2026-05", 3000, nan, 0.12, nan, nan),
        ("GEN_C", "2026-05", 500, nan, nan, nan, nan),
        # 2026-06: GEN_B beats its availability, so the station total clips to 0 and the
        # energy-weighted economic figure (0.5·1000/4000) is capped to it.
        ("GEN_A", "2026-06", 500, 0.50, 0.50, 500, 1000),
        ("GEN_B", "2026-06", 3600, 0.0, 0.0, 3600, 3000),
    ]
    monthly = pd.DataFrame(rows, columns=["duid", "month", "generation_mwh", "curtailment_pct",
                                          "econ_curtailment_pct", "curtailment_actual_mwh",
                                          "curtailment_potential_mwh"])
    monthly = monthly.astype({c: float for c in monthly.columns if c not in ("duid", "month")})
    monthly["revenue_aud"] = monthly["generation_mwh"] * 50.0
    monthly["capacity_factor"] = 0.1
    out = tmp_path / "generators"
    generate_all(generators, monthly_aggregates=monthly, output_dir=str(out))

    entries = {d: {"duid": d, "fuel_category": f, "capacity_mw": caps[d], "region": "QLD1"}
               for d, f in zip(caps, ["Wind", "Wind", "Solar"])}
    proc = subprocess.run(
        [NODE, "-e", PIPELINE_VS_JS], capture_output=True, text=True, timeout=60,
        env={"PORTFOLIO_JS": str(JS), "GEN_DIR": str(out), "ENTRIES": json.dumps(entries)},
    )
    assert proc.returncode == 0, proc.stderr
    r = json.loads(proc.stdout)
    assert r["pMonths"] == r["months"] == ["2026-03", "2026-04", "2026-05", "2026-06"]
    # March: (0.02·1000 + 0.15·4000 + 0.2·500) / 5500 = 0.1309 (GEN_C capped at 0.2);
    # nameplate weights would give 57/450 = 0.1267.
    assert r["pipeline"][0] == pytest.approx(0.1309, abs=1e-4)
    # April, legacy: potentials 800/0.8, 2700/0.6, 450/0.9 = 1000, 4500, 500 →
    # (0.05·1000 + 0.08·4500 + 0.01·500) / 6000 = 0.0692 (nameplate: 0.0656).
    assert r["pipeline"][1] == pytest.approx(0.0692, abs=1e-4)
    # May: nameplate fallback (0.04·100 + 0.12·300) / 400 = 0.1.
    assert r["pipeline"][2] == pytest.approx(0.1, abs=1e-4)
    assert r["pipeline"][3] == 0.0
    assert r["browser"] == [pytest.approx(v, abs=1e-4) for v in r["pipeline"]]


UNIT = r"""
const P = require(process.env.PORTFOLIO_JS);
const month = (gen, extra) => Object.assign({ generation_mwh: gen, revenue_aud: gen * 50 }, extra || {});
const doc = (months, cols, extra) => Object.assign({ monthly: Object.assign({ months }, cols) }, extra || {});
const wind = { duid: 'WIND1', fuel_category: 'Wind', capacity_mw: 100, region: 'VIC1' };
const solar = { duid: 'SOLAR1', fuel_category: 'Solar', capacity_mw: 50, region: 'NSW1' };
const batt = { duid: 'BATT1', fuel_category: 'Battery', capacity_mw: 50, region: 'VIC1' };
const w = doc(['2026-04', '2026-05'], { generation_mwh: [7200, 7440], revenue_aud: [1, 1],
    curtailment_pct: [0.1, 0], curtailment_actual_mwh: [900, 1100], curtailment_potential_mwh: [1000, 1000],
    econ_curtailment_pct: [0.05, 0] });
const s = doc(['2026-04', '2026-05'], { generation_mwh: [3600, 3720], revenue_aud: [1, 1],
    curtailment_pct: [0.5, 0], curtailment_actual_mwh: [1000, 1000], curtailment_potential_mwh: [2000, 900],
    econ_curtailment_pct: [0.5, 0] },
    { gen_info: { units: [{ commitment_status: 'In Commissioning', full_commercial_use_date: '2026-05-15' }] } });
const b = doc(['2026-04', '2026-05'], { generation_mwh: [3600, 3720], revenue_aud: [1, 1] });
const all = P.combine([{ entry: wind, doc: w }, { entry: solar, doc: s }, { entry: batt, doc: b }]);
const withB = P.combine([{ entry: wind, doc: w }, { entry: solar, doc: s }, { entry: batt, doc: b }], { batteriesInCF: true });
const noComm = P.combine([{ entry: wind, doc: w }, { entry: solar, doc: s }], { excludeCommissioning: true });
const dup = P.combine([{ entry: wind, doc: w }, { entry: wind, doc: w }]);
const st = (statuses) => doc(['2026-04'], { generation_mwh: [1], revenue_aud: [1], revenue_mlf_status: statuses });
const status = [['exact', 'prior-carry'], ['prior-carry', 'unknown'], ['exact', 'exact'], [null, 'exact']].map(([a, c]) =>
    P.combine([{ entry: wind, doc: st([a]) }, { entry: solar, doc: st([c]) }]).monthly.revenue_mlf_status[0]);
const tricky = ['TARONG#1', 'W/HOE#2', 'BRYB1WF1'];
console.log(JSON.stringify({
    gen: all.monthly.generation_mwh, cf: all.monthly.capacity_factor, basis: all.monthly.capacity_basis_mw,
    cfB: withB.monthly.capacity_factor, curt: all.monthly.curtailment_pct, econ: all.monthly.econ_curtailment_pct,
    regions: all.regions, region: all.region, fuel: all.fuel_category,
    noCommGen: noComm.monthly.generation_mwh, noCommCurt: noComm.monthly.curtailment_pct,
    assetComm: all.assets.map(a => [a.duid, a.commissioning, a.full_commercial_use, a.in_cf]),
    dupGen: dup.monthly.generation_mwh, dupMembers: dup.duids,
    hash: P.buildHash(tricky), parsed: P.parseHash('#' + P.buildHash(tricky)),
    parsedDup: P.parseHash('#portfolio=A,B,A'), notPortfolio: P.parseHash('#BRYB1WF1'), status,
}));
"""


def test_combine_rules_and_link_format():
    r = run_node(UNIT)
    # Generation counts every member, battery discharge included.
    assert r["gen"] == [14400, 14880]
    # CF leaves the battery out of numerator and capacity: (7200+3600)/(150·720).
    assert r["basis"] == [150, 150]
    assert r["cf"] == [pytest.approx(0.1), pytest.approx(0.1)]
    assert r["cfB"] == [pytest.approx(14400 / (200 * 720), abs=1e-4), pytest.approx(14880 / (200 * 744), abs=1e-4)]
    # Curtailment sums energy then clips: 1 − 1900/3000; May overshoots (2100/1900) → 0.
    assert r["curt"] == [pytest.approx(0.3667, abs=1e-4), 0.0]
    # Economic curtailment is energy-weighted: (0.05·1000 + 0.5·2000)/3000 = 0.35.
    assert r["econ"] == [0.35, 0.0]
    assert r["regions"] == ["NSW1", "VIC1"] and r["region"] is None and r["fuel"] == "Mixed"
    # Commissioning off by default; on, the solar farm's April (before 15 May FCU) drops out.
    assert r["noCommGen"] == [7200, 11160]
    assert r["noCommCurt"][0] == pytest.approx(0.1)
    assert r["assetComm"] == [["WIND1", False, None, True], ["SOLAR1", True, "2026-05-15", True],
                              ["BATT1", False, None, False]]
    # A unit added twice (directly and through its station) counts once.
    assert r["dupGen"] == [7200, 7440] and r["dupMembers"] == ["WIND1"]
    # DUIDs with '#' and '/' survive the link; duplicates collapse; unit links are not portfolios.
    assert "#" not in r["hash"].split("=", 1)[1]
    assert r["parsed"] == ["TARONG#1", "W/HOE#2", "BRYB1WF1"]
    assert r["parsedDup"] == ["A", "B"]
    assert r["notPortfolio"] is None
    # MLF status: the most conservative member's (unknown > prior-carry > exact).
    assert r["status"] == ["prior-carry", "unknown", "exact", "exact"]


FCU = r"""
const P = require(process.env.PORTFOLIO_JS);
const row = (name, status, fcu) => ({ unit_name: name, commitment_status: status, full_commercial_use_date: fcu });
const at = (duid, rows) => P.fullCommercialUse({ duid, gen_info: { units: rows } });
console.log(JSON.stringify({
    // ERB01's block: a Committed stage 2 due 2027 beside the in-service stage 1 (no date).
    erb: at('ERB01', [row('ERARNGB2', 'Committed', '2027-03-31'), row('ERARNGB3', 'In Service', null), row('ERARNGB1', 'In Service', null)]),
    // WAGGNSF1's block: two stages in commissioning, one undated: which is this unit is unknown.
    wagga: at('WAGGNSF1', [row('WNS23-34', 'In Commissioning', '2023-07-01'), row('WNSF1-22', 'In Commissioning', null)]),
    // A row named after the DUID wins over the project's other stages.
    own: at('ABC1', [row('ABC2', 'In Commissioning', '2026-09-01'), row('abc1', 'In Service', '2024-05-10')]),
    // Several dated operating stages, none named: the earliest (leaves out the fewest months).
    stages: at('XYZ1', [row('X2', 'In Service', '2025-02-01'), row('X1', 'In Commissioning', '2024-11-15'), row('X3', 'Anticipated', '2028-01-01')]),
    none: at('NONE1', []),
    // WTAHB1: one Committed row, generating since 2024-09: a one-row block is this unit whatever its status.
    oneRow: at('WTAHB1', [row('WSB_G1', 'Committed', '2026-09-01')]),
    // MP2: in service since before our data, yet dated 2027 in the register: no usable date.
    future: P.fullCommercialUse({ duid: 'MP2', monthly: { months: ['2021-09', '2026-09'] },
        gen_info: { units: [row('Mt Piper 2', 'In Service', '2027-01-31')] } }),
    // In commissioning and dated past our data: every month so far is commissioning.
    commissioningAhead: P.fullCommercialUse({ duid: 'MCINTYR1', monthly: { months: ['2024-08', '2026-09'] },
        gen_info: { units: [row('MIWF', 'In Commissioning', '2026-12-01')] } }),
}));
"""


def test_full_commercial_use_reads_the_units_own_stage():
    r = run_node(FCU)
    assert r["erb"] == {"date": None, "commissioning": False}
    assert r["wagga"] == {"date": None, "commissioning": True}
    assert r["own"] == {"date": "2024-05-10", "commissioning": False}
    assert r["stages"] == {"date": "2024-11-15", "commissioning": True}
    assert r["none"] == {"date": None, "commissioning": False}
    assert r["oneRow"] == {"date": "2026-09-01", "commissioning": False}
    assert r["future"] == {"date": None, "commissioning": False}
    assert r["commissioningAhead"] == {"date": "2026-12-01", "commissioning": True}
