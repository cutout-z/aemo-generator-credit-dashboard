"""Peak-RSS profile of steps 3e/3e2/3e3 (_run_optional_factor_lanes) on cached months.

Usage: python profile_factor_lanes.py <repo> <bpdata dir> <out dir>
Runs in a fresh process; copies nothing: data_dir is a fresh temp dir whose
nemosis_cache is a symlink to the cached parquets.
"""
import logging, os, pickle, resource, sys, tempfile, threading, time
from pathlib import Path

repo, bpdata, out = map(Path, sys.argv[1:4])
sys.path.insert(0, str(repo))
os.chdir(repo)
import psutil
sys.path.insert(0, str(Path(__file__).parent)); from footprint import footprint

proc = psutil.Process()
samples = []
marks = []
stop = False

def sampler():
    while not stop:
        samples.append((time.time(), footprint()['footprint']))
        time.sleep(0.05)

class MarkHandler(logging.Handler):
    def emit(self, record):
        marks.append((time.time(), footprint()['footprint'], record.getMessage()[:110]))

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logging.getLogger().addHandler(MarkHandler())

from src import main as m

data_dir = Path(tempfile.mkdtemp(prefix="factor-lanes-"))
(data_dir / "nemosis_cache").symlink_to(bpdata / "nemosis_cache")
base = proc.memory_info().rss
t = threading.Thread(target=sampler, daemon=True); t.start()
t0 = time.time()
res = m._run_optional_factor_lanes(
    data_dir, [(2026, 7), (2026, 8), (2026, 9)],
    skip_fcas_factors=False, skip_offer_factors=False, full_refresh=False,
)
elapsed = time.time() - t0
stop = True; t.join()
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # bytes on macOS
out.mkdir(parents=True, exist_ok=True)
fcas, offer, curves, daily = res[:4]
with open(out / "outputs.pkl", "wb") as f:
    pickle.dump({"fcas": fcas, "offer": offer, "curves": curves, "daily": daily,
                 "lanes": [(l.source, l.status, l.attempted_months) for l in res[4:]]}, f)
# Peak sample and the log line just before it
pk_t, pk_rss = max(samples, key=lambda s: s[1])
before = [mk for mk in marks if mk[0] <= pk_t]
fp = footprint()
print(f"RESULT peak_footprint_GiB={fp['lifetime_max_footprint']/2**30:.2f} peak_maxrss_GiB={peak/2**30:.2f} sampled_peak_footprint_GiB={pk_rss/2**30:.2f} "
      f"import_baseline_GiB={base/2**30:.2f} elapsed_s={elapsed:.0f}")
print("peak after log line:", before[-1][2] if before else None)
for ts, rss, msg in marks:
    print(f"  {ts-t0:7.1f}s fp={rss/2**30:5.2f}GiB  {msg}")
# per-window max between log lines
edges = [t0] + [mk[0] for mk in marks] + [time.time()]
labels = ["start"] + [mk[2] for mk in marks]
print("window peaks (max RSS until next log line):")
for i in range(len(edges) - 1):
    w = [s[1] for s in samples if edges[i] <= s[0] < edges[i + 1]]
    if w:
        print(f"  {max(w)/2**30:5.2f}GiB  after: {labels[i][:90]}")
