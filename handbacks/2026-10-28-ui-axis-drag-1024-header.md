---
project: aemo-generator-credit-dashboard
agent: claude
branch: ui/axis-drag-and-1024-header
pr: cutout-z/aemo-generator-credit-dashboard#22
status: merged as bc3f048 (squash); both UI bugs fixed, verified in Chromium
noticed:
  - "stats bar and Portfolio label stay hidden at 1024-1279 though the wrapped bar has room -> decision: left as is; reviewer called it a lost opportunity, not a bug"
outstanding: []
---
## What changed
- `initAxisRangeDrag()` writes the drag range to the `matches` target (chartDailyCF x2 follows x); verify-interactions 14/15 -> 15/15.
- App bar one-row layout, sticky, group-header offset (`top-14`) and search dropdown alignment now start at 1280px; 1024-1279 wraps and scrolls away. scrollWidth == clientWidth at 390/1024/1100/1279/1280/1440/1600.

## Evidence
pytest 596 passed, 3 skipped, 22 known non-passes in tests/test_outputs.py (same on main per `land`); verify-design ADPBA1 passed; verify-interactions 15/15; screenshots `design/screens/header-{390,1024,1440}-after-fix.png`.

Review: tier 3 adversary blocked the first push (group header hidden under the wrapped sticky bar at 1024-1279); fixed in 14d524862, re-review APPROVE.

Ruling: sticky bar dropped below 1280 rather than kept at 147px tall, because a 147px sticky bar eats the viewport; if wrong: header scrolls away at 1024-1279.
