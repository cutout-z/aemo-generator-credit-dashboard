/* Portfolio combiner for the generator credit dashboard.
 *
 * A portfolio is a set of units the viewer picks. The page fetches each unit's public file
 * (data/generators/<file>.json) and this module combines them into a doc shaped like a station
 * file, so every existing chart, KPI, period control and export renders it unchanged.
 *
 * Privacy: membership lives only in the viewer's browser. The link carries unit DUIDs after the
 * '#', which browsers never send to the server; the portfolio's name stays in localStorage.
 *
 * The rules mirror _aggregate_station_monthly in src/generate_json.py. tests/test_portfolio_js.py
 * rebuilds every published station file from its members with this module and checks the match.
 * Two deliberate differences from a station page, both options:
 *   - batteriesInCF (default false): a battery's file reports discharge only, so batteries stay
 *     out of the capacity factor (numerator and capacity basis). Stations count them.
 *   - excludeCommissioning (default false): drop each member's months before its AEMO full
 *     commercial use month. Off by default: the owner's financial models already account for
 *     commissioning (decision 2026-10-28).
 *
 * Pure functions, no DOM: loaded by the page (window.Portfolio) and by node in the tests.
 */
(function (root) {
    'use strict';

    const HASH_KEY = 'portfolio=';
    const STATUS_RANK = { 'unknown': 2, 'prior-carry': 1, 'exact': 0 };
    const CURTAILABLE = new Set(['Solar', 'Wind']);

    const isNum = v => typeof v === 'number' && isFinite(v);
    const round = (v, dp) => {
        if (!isNum(v)) return null;
        const f = Math.pow(10, dp);
        return Math.round(v * f) / f;
    };
    const isBattery = entry => (entry && entry.fuel_category) === 'Battery';

    // ── Link format: #portfolio=DUID1,DUID2 (each DUID URI-encoded: DUIDs can hold '#' and '/')
    function parseHash(hash) {
        const h = String(hash || '').replace(/^#/, '');
        if (!h.startsWith(HASH_KEY)) return null;
        const seen = new Set();
        const out = [];
        for (const part of h.slice(HASH_KEY.length).split(',')) {
            let d;
            try { d = decodeURIComponent(part).trim(); } catch (e) { d = part.trim(); }
            if (d && !seen.has(d)) { seen.add(d); out.push(d); }
        }
        return out;
    }

    function buildHash(duids) {
        return HASH_KEY + [...new Set(duids)].map(encodeURIComponent).join(',');
    }

    function daysInMonth(month) {
        const [y, m] = month.split('-').map(Number);
        return new Date(Date.UTC(y, m, 0)).getUTCDate();
    }

    // AEMO full commercial use date for a unit, from its Generation Information block. That
    // block lists the unit's whole project, later stages included (ERB01 carries a Committed
    // stage 2 due 2027), so: the row named after the DUID when there is one; otherwise only rows
    // already In Service or In Commissioning, and no date at all if one of those has none (an
    // undated stage is running, so which row is this unit can't be told); else the earliest.
    // An In Service row dated after the unit's latest published month contradicts itself
    // (MP2: in service, dated 2027) and counts as undated, so the switch can't wipe its history.
    const OPERATING = /^in (service|commissioning)$/i;
    function fullCommercialUse(doc) {
        const rows = (doc && doc.gen_info && doc.gen_info.units) || [];
        const duid = String((doc && doc.duid) || '').toUpperCase();
        const months = (doc && doc.monthly && doc.monthly.months) || [];
        const last = months.length ? months[months.length - 1] : null;
        const own = rows.filter(u => duid && String(u.unit_name || '').toUpperCase() === duid);
        const candidates = own.length ? own : rows.filter(u => OPERATING.test(u.commitment_status || ''));
        const dates = candidates.map(u => {
            const d = u.full_commercial_use_date;
            return d && last && /^in service$/i.test(u.commitment_status || '') && d.slice(0, 7) > last ? null : d;
        });
        return {
            date: dates.length && dates.every(Boolean) ? [...dates].sort()[0] : null,
            commissioning: candidates.some(u => /commission/i.test(u.commitment_status || '')),
        };
    }

    // Per-member monthly rows keyed by month, after the commissioning filter.
    function memberRows(doc, opts) {
        const m = doc.monthly || {};
        const months = m.months || [];
        const fcu = fullCommercialUse(doc);
        const fcuMonth = fcu.date ? fcu.date.slice(0, 7) : null;
        const rows = new Map();
        months.forEach((month, i) => {
            if (opts.excludeCommissioning && fcuMonth && month < fcuMonth) return;
            const at = key => (m[key] ? m[key][i] : undefined);
            rows.set(month, {
                gen: at('generation_mwh'),
                revenue: at('revenue_aud'),
                revenueLoss: at('revenue_loss_adjusted_aud'),
                mlfStatus: at('revenue_mlf_status'),
                curtPct: at('curtailment_pct'),
                act: at('curtailment_actual_mwh'),
                pot: at('curtailment_potential_mwh'),
                econ: at('econ_curtailment_pct'),
                cp: at('captured_price'),
                rrp: at('avg_rrp'),
                cf: at('capacity_factor'),
            });
        });
        return { rows, fcu };
    }

    /* Combine unit docs into a station-shaped doc.
     * members: [{ entry, doc }] — entry is the unit's index.json row, doc its generator file.
     * opts: { name, batteriesInCF, excludeCommissioning }
     */
    function combine(members, opts) {
        opts = Object.assign({ batteriesInCF: false, excludeCommissioning: false }, opts || {});
        const seen = new Set();
        members = members.filter(x => x && x.doc && x.entry && !seen.has(x.entry.duid) && seen.add(x.entry.duid));
        const prepared = members.map(x => Object.assign({}, x, memberRows(x.doc, opts)));

        const months = [...new Set(prepared.flatMap(p => [...p.rows.keys()]))].sort();
        const fuels = new Set(members.map(x => x.entry.fuel_category));
        const curtailable = [...fuels].some(f => CURTAILABLE.has(f));
        const has = key => members.some(x => x.doc.monthly && Array.isArray(x.doc.monthly[key]));
        const capOf = p => (isNum(Number(p.entry.capacity_mw)) ? Number(p.entry.capacity_mw) : null);
        const inCF = p => opts.batteriesInCF || !isBattery(p.entry);

        const out = {
            generation_mwh: [], revenue_aud: [], capacity_factor: [], capacity_basis_mw: [],
            curtailment_pct: [], curtailment_actual_mwh: [], curtailment_potential_mwh: [],
            econ_curtailment_pct: [], captured_price: [], avg_rrp: [], price_capture_ratio: [],
            revenue_loss_adjusted_aud: [], revenue_mlf_status: [],
        };
        for (const month of months) {
            const present = prepared.filter(p => p.rows.has(month));
            const row = p => p.rows.get(month);
            const sum = (list, f) => list.reduce((a, p) => a + (isNum(f(p)) ? f(p) : 0), 0);

            const totalGen = sum(present, p => row(p).gen);
            out.generation_mwh.push(round(totalGen, 0));
            out.revenue_aud.push(round(sum(present, p => row(p).revenue), 0));

            // CF over the capacity of the members that reported this month.
            const cfMembers = present.filter(inCF);
            const caps = cfMembers.map(capOf).filter(isNum);
            const basis = caps.length ? caps.reduce((a, b) => a + b, 0) : null;
            out.capacity_basis_mw.push(basis ? round(basis, 3) : null);
            const cfGen = sum(cfMembers, p => row(p).gen);
            out.capacity_factor.push(basis > 0 ? round(cfGen / (basis * daysInMonth(month) * 24), 4) : null);

            // Curtailment: max(0, 1 − Σ actual / Σ potential), clipped after summing.
            const withEnergy = present.filter(p => isNum(row(p).pot) && row(p).pot > 0 && isNum(row(p).curtPct));
            const potSum = withEnergy.reduce((a, p) => a + row(p).pot, 0);
            const actSum = withEnergy.reduce((a, p) => a + (isNum(row(p).act) ? row(p).act : 0), 0);
            const pct = potSum > 0 ? round(Math.max(0, 1 - actSum / potSum), 4) : null;
            out.curtailment_pct.push(pct);
            out.curtailment_actual_mwh.push(potSum > 0 ? round(actSum, 1) : null);
            out.curtailment_potential_mwh.push(potSum > 0 ? round(potSum, 1) : null);

            // Economic curtailment, energy-weighted: Σ (economic % × potential) ÷ Σ potential,
            // never above the total shortfall.
            const withEcon = withEnergy.filter(p => isNum(row(p).econ));
            const econPot = withEcon.reduce((a, p) => a + row(p).pot, 0);
            let econ = econPot > 0 ? withEcon.reduce((a, p) => a + row(p).econ * row(p).pot, 0) / econPot : null;
            if (econ == null) {
                // No member has the energy: nameplate-weighted, as the pipeline's fallback.
                const anyEcon = present.filter(p => isNum(row(p).econ));
                const weighted = anyEcon.filter(p => isNum(capOf(p)));
                const w = weighted.reduce((a, p) => a + capOf(p), 0);
                if (weighted.length && w > 0) econ = weighted.reduce((a, p) => a + row(p).econ * capOf(p), 0) / w;
                else if (anyEcon.length) econ = anyEcon.reduce((a, p) => a + row(p).econ, 0) / anyEcon.length;
            }
            if (econ != null && pct != null) econ = Math.min(econ, pct);
            out.econ_curtailment_pct.push(round(econ, 4));

            // Price capture: generation-weighted over members that carry a captured price.
            const priced = present.filter(p => isNum(row(p).cp));
            const pricedGen = sum(priced, p => row(p).gen);
            const cp = totalGen > 0 && priced.length && pricedGen !== 0
                ? round(priced.reduce((a, p) => a + row(p).cp * (isNum(row(p).gen) ? row(p).gen : 0), 0) / pricedGen, 2)
                : null;
            out.captured_price.push(cp);
            const rrps = present.map(p => row(p).rrp).filter(isNum);
            const rrp = rrps.length ? round(rrps.reduce((a, b) => a + b, 0) / rrps.length, 2) : null;
            out.avg_rrp.push(rrp);
            out.price_capture_ratio.push(cp && rrp ? round(cp / rrp, 4) : null);

            // Revenue after MLF × DLF: a sum only when every present member carries it.
            const losses = present.map(p => row(p).revenueLoss);
            out.revenue_loss_adjusted_aud.push(losses.every(isNum) ? round(losses.reduce((a, b) => a + b, 0), 0) : null);
            // Most conservative member MLF status for the month.
            const statuses = present.map(p => row(p).mlfStatus).filter(s => s != null);
            out.revenue_mlf_status.push(statuses.length
                ? statuses.reduce((a, s) => ((STATUS_RANK[s] || 0) > (STATUS_RANK[a] || 0) ? s : a), 'exact')
                : null);
        }

        const monthly = {
            months,
            generation_mwh: out.generation_mwh,
            revenue_aud: out.revenue_aud,
            capacity_factor: out.capacity_factor,
            capacity_basis_mw: out.capacity_basis_mw,
        };
        if (has('revenue_loss_adjusted_aud')) monthly.revenue_loss_adjusted_aud = out.revenue_loss_adjusted_aud;
        if (curtailable && has('curtailment_pct')) {
            monthly.curtailment_pct = out.curtailment_pct;
            const version = members.map(x => x.doc.monthly && x.doc.monthly.curtailment_metric_version).find(Boolean);
            if (version) monthly.curtailment_metric_version = version;
            monthly.curtailment_actual_mwh = out.curtailment_actual_mwh;
            monthly.curtailment_potential_mwh = out.curtailment_potential_mwh;
        }
        if (curtailable && has('econ_curtailment_pct')) {
            monthly.econ_curtailment_pct = out.econ_curtailment_pct;
            monthly.econ_curtailment_weighting = 'energy';
        }
        if (has('captured_price')) {
            monthly.captured_price = out.captured_price;
            monthly.avg_rrp = out.avg_rrp;
            monthly.price_capture_ratio = out.price_capture_ratio;
        }
        if (has('revenue_mlf_status')) monthly.revenue_mlf_status = out.revenue_mlf_status;

        const regions = [...new Set(members.map(x => x.entry.region).filter(Boolean))].sort();
        const capacity = members.map(x => Number(x.entry.capacity_mw)).filter(isNum).reduce((a, b) => a + b, 0);
        const fuelMW = {};
        members.forEach(x => { fuelMW[x.entry.fuel_category] = (fuelMW[x.entry.fuel_category] || 0) + (Number(x.entry.capacity_mw) || 0); });
        const fuelRanked = Object.entries(fuelMW).sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));

        const doc = {
            type: 'portfolio',
            station_name: opts.name || 'Portfolio',
            duids: members.map(x => x.entry.duid),
            region: regions.length === 1 ? regions[0] : null,
            regions,
            fuel_category: fuelRanked.length === 1 ? fuelRanked[0][0] : 'Mixed',
            fuel_mix: Object.fromEntries(fuelRanked),
            capacity_mw: capacity ? round(capacity, 1) : null,
            technology: [...new Set(members.map(x => x.entry.technology).filter(Boolean))].join(', '),
            connection_points: members.map(x => x.entry.connection_point).filter(Boolean),
            lgc_eligible: members.length > 0 && members.every(x => x.doc.lgc_eligible),
            monthly,
        };

        const mlf = {};
        members.forEach(x => { if (x.doc.mlf && x.doc.mlf.values) mlf[x.entry.duid] = x.doc.mlf; });
        if (Object.keys(mlf).length) doc.mlf_by_duid = mlf;

        // Regional FCAS prices are the same for every unit in a region: shown for a
        // one-region portfolio only.
        if (regions.length === 1) {
            const withFcas = members.find(x => x.doc.fcas && x.doc.fcas.services);
            if (withFcas) doc.fcas = withFcas.doc.fcas;
        }

        const daily = combineDaily(members, opts, inCF, capOf);
        if (daily) doc.daily = daily;
        const dist = combinePriceDistribution(members);
        if (dist) doc.price_distribution = dist;

        doc.assets = prepared.map(p => ({
            duid: p.entry.duid,
            name: p.entry.station_name,
            fuel_category: p.entry.fuel_category,
            region: p.entry.region,
            capacity_mw: capOf(p),
            battery: isBattery(p.entry),
            in_cf: inCF(p),
            full_commercial_use: p.fcu.date,
            commissioning: p.fcu.commissioning,
            generation_mwh: months.map(mo => (p.rows.has(mo) ? p.rows.get(mo).gen : null)),
            capacity_factor: months.map(mo => (p.rows.has(mo) ? p.rows.get(mo).cf : null)),
            curtailment_pct: months.map(mo => (p.rows.has(mo) ? p.rows.get(mo).curtPct : null)),
        }));
        return doc;
    }

    function combineDaily(members, opts, inCF, capOf) {
        const byDate = new Map();
        for (const x of members) {
            const d = x.doc.daily;
            if (!d || !d.dates) continue;
            const fcuMonth = opts.excludeCommissioning ? (fullCommercialUse(x.doc).date || '').slice(0, 7) : '';
            d.dates.forEach((date, i) => {
                if (fcuMonth && date.slice(0, 7) < fcuMonth) return;
                if (!byDate.has(date)) byDate.set(date, []);
                byDate.get(date).push({ x, gen: d.generation_mwh[i],
                    obs: d.intervals_observed ? d.intervals_observed[i] : undefined,
                    exp: d.intervals_expected ? d.intervals_expected[i] : undefined });
            });
        }
        if (!byDate.size) return null;
        const dates = [...byDate.keys()].sort();
        const out = { dates, capacity_factor: [], generation_mwh: [] };
        const hasCov = members.some(x => x.doc.daily && x.doc.daily.intervals_observed);
        if (hasCov) { out.intervals_observed = []; out.intervals_expected = []; }
        for (const date of dates) {
            const rows = byDate.get(date);
            const total = rows.reduce((a, r) => a + (isNum(r.gen) ? r.gen : 0), 0);
            out.generation_mwh.push(round(total, 1));
            const cfRows = rows.filter(r => inCF(r.x));
            const caps = cfRows.map(r => capOf(r.x)).filter(isNum);
            const basis = caps.length ? caps.reduce((a, b) => a + b, 0) : 0;
            const cfGen = cfRows.reduce((a, r) => a + (isNum(r.gen) ? r.gen : 0), 0);
            out.capacity_factor.push(basis > 0 ? round(cfGen / (basis * 24), 4) : null);
            if (hasCov) {
                out.intervals_observed.push(rows.reduce((a, r) => a + (isNum(r.obs) ? r.obs : 0), 0));
                out.intervals_expected.push(rows.reduce((a, r) => a + (isNum(r.exp) ? r.exp : 0), 0));
            }
        }
        return out;
    }

    // Generation-weighted spot-price exposure: sum each bin's MWh across members.
    function combinePriceDistribution(members) {
        const withDist = members.map(x => x.doc.price_distribution).filter(d => d && d.bins && d.mwh);
        if (!withDist.length) return null;
        const bins = withDist[0].bins;
        if (!withDist.every(d => d.bins.length === bins.length && d.bins.every((b, i) => b === bins[i]))) return null;
        const mwh = bins.map((_, i) => withDist.reduce((a, d) => a + (isNum(d.mwh[i]) ? d.mwh[i] : 0), 0));
        const total = mwh.reduce((a, b) => a + b, 0);
        if (!(total > 0)) return null;
        return { bins, mwh: mwh.map(v => round(v, 1)), generation_share: mwh.map(v => round(v / total, 4)) };
    }

    const api = { parseHash, buildHash, combine, fullCommercialUse, HASH_KEY };
    if (typeof module === 'object' && module.exports) module.exports = api;
    else root.Portfolio = api;
})(typeof window !== 'undefined' ? window : globalThis);
