import os

# Arrow's default memory pool is mimalloc (on the NAS too). By default it keeps
# freed pages resident until a delayed purge, and the BIDPEROFFER_D read frees
# each row group's string columns as soon as they are typed: one month's read
# peaked ~2.4 GiB above its live data (5.8 vs 3.3 GiB measured, 2026-10-19).
# A zero delay returns them at once. mimalloc reads this when pyarrow loads,
# so it must be set before anything imports pandas or pyarrow, which this
# package __init__ is (`python -m src.main` runs it first). An explicit
# setting in the environment wins.
os.environ.setdefault("MIMALLOC_PURGE_DELAY", "0")
