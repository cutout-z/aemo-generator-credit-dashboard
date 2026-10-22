---
project: AEMO Generator Credit dashboard
agent: claude
branch: fix/credit-low-findings
merged: false
status: PR #17 open with every 2026-10-20 code-pass low finding for credit fixed, except FCAS 1SEC (dropped); waiting on Zalen's merge yes
outstanding:
  - "to-do: Zalen's yes to merge PR #17 (includes an AGENTS.md edit); not while the 28-month rerun or the 05:15 run is going"
  - "to-do: after merge, check the next daily lane log: suite passes under pandas 3, commit label is the AEST date, processed-cache drops the 6 intermittent_quality files"
---

# Code pass 2026-10-20: deferred lows for the credit dashboard

Branch `fix/credit-low-findings`, PR #17. There is one commit per finding, and each has a test that fails when that commit's source is reverted.

| Finding | Outcome |
|---|---|
| Low-memory tests depend on library versions; deps unpinned | Fixed `7ddaf6cd9`: the frame takes nemosis's timestamp dtype for the installed pandas (a no-op on the lane). `requirements.txt` is pinned to the lane venv |
| VERSIONNO never reaches the BIDPEROFFER_D parquet | Fixed `caadc53e5`: VERSIONNO is no longer requested, and the false rebuild advice is gone. Not needed: Sep 2026 has 19,607,712 rows and the same number of distinct keys |
| Constraints lane retries 404s for FILE02 and pre-2015 months | Fixed `4fe362bff`: those 404s are final on the first try |
| Missing SECONDARY_TLF falls back silently | Fixed `327d554b8`: no import-factor fallback; revenue uses the tracker's export MLF; the lane is degraded and names the DUIDs. Has never fired |
| Dead code (fy_opening_tlf, intermittent import, snapshot glob) | Fixed `fd56e6313`. Six frozen intermittent_quality snapshots leave docs/data on the next publish |
| FCAS_BID_TYPES lacks 1SEC | Dropped: missing scope (prices, offers and page copy all lack R1/L1, and nemosis's defaults have no 1SEC price). Reopen if very fast FCAS should be shown |
| Commit label uses the UTC date | Fixed `489f3f791`: the label is the NEM date (TZ=AEST-10) |
| AGENTS.md baseline and pitfall | `314ddbfb6`: baseline 577/12/3/10; corrected the nemosis-columns pitfall. Needs Zalen's yes |

## Evidence

- Full suite, `/opt/anaconda3/bin/python3 -m pytest -q -p no:cacheprovider`: main 568/14/3/10, branch 577/12/3/10. All 22 non-passes are test_outputs lane-only files.
- Not run: the pipeline, and the suite under the lane's pandas 3.
- No published figure moves.

## Rulings

- Ruling: one PR, not several, because no item moves a published figure; if wrong: a reviewer reads seven commits in one PR.
- Ruling: pinned the exact lane versions, not ranges, because a lane reinstall must reproduce the tested set; if wrong: a Mac `pip install -r` pulls pandas 3 (the Mac uses anaconda, so it is unaffected).
- Ruling: a battery with no SECONDARY_TLF gets no dated factor (falls to the tracker's export MLF) rather than only being reported, because the import factor is never right for export; if wrong: nothing today, the case has never occurred.
- Ruling: dropped FCAS 1SEC as a feature, not a defect; if wrong: batteries' very fast offers stay unshown until a feature PR.
- Ruling: handback on `handback/fix/credit-low-findings`, not the PR branch, because this repo's main is public Pages and its convention keeps handbacks off main; if wrong: one cherry-pick.

## Seen in passing (not in scope)

- In the NAS cache, GENCONDATA 2026 holds only 01, 02, 05 and 08; 03, 04, 06 and 07 are missing, while SPDCP has 01 through 08. All files are dated Oct 7. This fits R3's [M] finding that `_stale_reason` only checks the newest month. The next constraints refresh after the 25-day cache age may refill them.
