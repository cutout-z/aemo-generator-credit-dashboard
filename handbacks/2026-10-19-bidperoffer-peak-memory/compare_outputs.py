import pickle, sys
import pandas as pd
a = pickle.load(open(sys.argv[1], "rb")); b = pickle.load(open(sys.argv[2], "rb"))
for k in ("fcas", "offer", "curves", "daily"):
    pd.testing.assert_frame_equal(a[k], b[k], check_exact=True)
    print(f"{k}: identical {a[k].shape}")
assert a["lanes"] == b["lanes"], (a["lanes"], b["lanes"]); print("lanes:", a["lanes"])
