"""Independent re-computation of the 250 ms downsampling (numpy; shares no code with the Rust tool).

usage: verify_l2_slice.py <csv.gz> <RIC> <parquet> [freq_ms] [gap_s] [window_start_iso window_end_iso]

Expected emission is recomputed in a vectorised way: bucket state S_k = last row of grid point k;
a point is emitted iff it is the first of the file (or after a >= gap silence) or S_k != S_{k-1}
(the previously emitted state always equals the previous bucket's state).
With a window only the buckets after the first bucket of the window are compared.
"""
import sys, numpy as np, pyarrow as pa, pyarrow.compute, pyarrow.csv as pc, pyarrow.parquet as pq

csv_path, ric, pq_path = sys.argv[1:4]
F = int(sys.argv[4]) * 1_000_000 if len(sys.argv) > 4 else 250_000_000
GAP = int(sys.argv[5]) * 1_000_000_000 if len(sys.argv) > 5 else 1_800_000_000_000
win = sys.argv[6:8] if len(sys.argv) >= 8 else None

# ---- read the source rows of one RIC (streaming, all columns as text first)
names = pc.open_csv(csv_path).schema.names
conv = pc.ConvertOptions(column_types={n: pa.string() for n in names}, strings_can_be_null=True)
reader = pc.open_csv(csv_path, read_options=pc.ReadOptions(block_size=64 << 20), convert_options=conv)
ts_parts, mats = [], []
bookcols = names[5:65]
for batch in reader:
    ric_col = batch.column(0)
    mask = pa.compute.equal(ric_col, ric)
    if not pa.compute.any(mask).as_py():
        continue
    b = batch.filter(mask)
    ts = b.column(2).to_pylist()
    if win:
        keep = [i for i, t in enumerate(ts) if win[0] <= t <= win[1]]
        if not keep:
            continue
        b = b.take(pa.array(keep)); ts = [ts[i] for i in keep]
    ts_parts.append(np.array([t[:-1] for t in ts], dtype="datetime64[ns]").astype("int64"))
    cols = []
    for n in bookcols:
        c = b.column(names.index(n)).cast(pa.float64())   # empty -> null -> NaN
        cols.append(np.asarray(c.to_numpy(zero_copy_only=False), dtype="float64"))
    mats.append(np.stack(cols, axis=1))
t = np.concatenate(ts_parts); M = np.concatenate(mats)
n = len(t)
assert n > 0, "no source rows"
assert (np.diff(t) >= 0).all(), "source not time ordered"

# ---- grid assignment and bucket-final states
g = -((-t) // F)                                   # ceil(t / F)
last_of_bucket = np.r_[g[1:] != g[:-1], True]
first_of_bucket = np.r_[True, g[1:] != g[:-1]]
S = M[last_of_bucket]; T = g[last_of_bucket] * F; LU = t[last_of_bucket]
first_idx = np.flatnonzero(first_of_bucket)
gap_before = np.r_[False, (t[1:] - t[:-1]) >= GAP]
anchor_b = gap_before[first_idx]; anchor_b[0] = True
eq_prev = np.r_[False, ((S[1:] == S[:-1]) | (np.isnan(S[1:]) & np.isnan(S[:-1]))).all(axis=1)]
emit = anchor_b | ~eq_prev
if win:                                            # first bucket's predecessor is unknown
    emit[0] = False
    sel = np.arange(len(T)) > 0
    exp_T, exp_LU, exp_A, exp_S = T[emit & sel], LU[emit & sel], anchor_b[emit & sel], S[emit & sel]
else:
    exp_T, exp_LU, exp_A, exp_S = T[emit], LU[emit], anchor_b[emit], S[emit]

# ---- the Rust tool's output
tab = pq.read_table(pq_path)
assert set(tab.column("ric").to_pylist()) == {ric}
pT = tab.column("ts_utc_ns").to_numpy().astype("int64")
pLU = tab.column("last_update_ts_ns").to_numpy().astype("int64")
pA = tab.column("anchor").to_numpy(zero_copy_only=False).astype(bool)
pS = np.stack([np.asarray(tab.column(i).cast(pa.float64()).to_numpy(zero_copy_only=False), dtype="float64")
               for i in range(4, 64)], axis=1)
if win:
    lo = T[0]; hi = T[-1]
    m = (pT > lo) & (pT <= hi)
    pT, pLU, pA, pS = pT[m], pLU[m], pA[m], pS[m]

res = {"ric": ric, "source_rows": n, "buckets": int(len(T)), "expected_rows": int(len(exp_T)), "parquet_rows": int(len(pT))}
ok = len(exp_T) == len(pT)
bad = {}
if ok:
    bad["ts"] = int((exp_T != pT).sum()); bad["last_update"] = int((exp_LU != pLU).sum()); bad["anchor"] = int((exp_A != pA).sum())
    neq = ~((exp_S == pS) | (np.isnan(exp_S) & np.isnan(pS)))
    bad["book_cells"] = int(neq.sum())
    ok = all(v == 0 for v in bad.values())
else:
    # report which grid points differ
    se, sp = set(exp_T.tolist()), set(pT.tolist())
    bad["only_expected"] = len(se - sp); bad["only_parquet"] = len(sp - se)
res.update(mismatch=bad, ok=bool(ok))
# structural checks on the parquet itself
res["ts_on_grid"] = bool((pT % F == 0).all())
res["ts_strictly_increasing_and_step_ge_freq"] = bool(len(pT) < 2 or (np.diff(pT) >= F).all())
res["last_update_in_interval"] = bool(((pLU <= pT) & (pLU > pT - F)).all())
if not win:
    res["first_row_is_anchor"] = bool(pA[0]) if len(pA) else None
    res["first_bucket_T"] = int(T[0]); res["parquet_first_T"] = int(pT[0]) if len(pT) else None
    res["last_bucket_T"] = int(T[-1]); res["parquet_last_T"] = int(pT[-1]) if len(pT) else None
print(res)
sys.exit(0 if ok else 1)
