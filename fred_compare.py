"""Compare every panel variable that FRED also publishes against FRED, state by state.

Reads data/processed/state_month_long.parquet (run pull_data.py first), fetches the matching
FRED series, and writes to data/processed/:

    fred_comparison.csv         one row per state x variable: FRED ID, months compared,
                                mismatches, max abs diff, unit scale used, status
    fred_mismatches.csv         every mismatching month (ours vs FRED)
    fred_comparison_report.md   summary by variable, plus variables FRED doesn't carry

For each variable there is a list of candidate FRED IDs (FRED hosts some series under the
BLS ID and others under a short code, e.g. CANA for California total nonfarm). The first one
that exists is used. An ID pattern that misses for the first 5 states is dropped for the rest.

The unit scale (e.g. dollars vs millions of dollars) is the power of ten closest to the
median FRED/ours ratio. A month is a mismatch when |FRED - ours*scale| is more than half a
unit in FRED's last reported decimal place (FRED's own rounding). Months where only one side
has a value inside the common date range are counted separately as `only_ours` / `only_fred`.

FRED responses are cached under data/raw/fred/ like the other sources (misses too), so a rerun
makes no requests unless you pass --refresh. FRED allows 120 requests a minute; the script
waits about 0.55 s between requests. A full uncached run takes roughly 20-30 minutes.

Usage:
    python fred_compare.py                       # all states, all variables
    python fred_compare.py --states IN CA        # some states
    python fred_compare.py --vars unemp_rate_sa bfs_ba_sa
    python fred_compare.py --refresh             # re-download FRED series
    python fred_compare.py --env-file PATH       # keys file (default: $ENV_FILE, then ./.env)
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import sys
import time
from collections import defaultdict

import numpy as np
import pandas as pd

import pull_data as pdm  # first: it sets the project directories and makes config importable
import config  # noqa: E402

log = logging.getLogger("fred_compare")

FRED_OBS = config.FRED_API  # .../fred/series/observations
MIN_SECONDS_BETWEEN_REQUESTS = 0.55
DROP_PATTERN_AFTER_MISSES = 5

# Short FRED codes for CES industries that have one: {st}{code} (SA), {st}{code}N (NSA)
CES_MNEMONIC = {"00000000": "NA", "20000000": "CONS", "30000000": "MFG"}
LAUS_MNEMONIC = {"03": "UR", "06": "LF"}


def candidates(var: str, s: dict, series_id: str | None) -> list[str]:
    """FRED ID patterns to try for one state x variable, most likely first. Patterns use
    {st} (postal code), {fips}, {sid} (our series ID). Returning [] = FRED has no equivalent."""
    out: list[str] = []
    for m, (name, _u, _d) in config.LAUS_MEASURES.items():
        if var == name:
            if m in LAUS_MNEMONIC:
                out.append("{st}" + LAUS_MNEMONIC[m])
            out.append("LASST{fips}00000000000" + m)
    for code, (short, _d) in config.CES_INDUSTRIES.items():
        for sa in ("sa", "nsa"):
            if var == f"emp_{short}_{sa}":
                if code in CES_MNEMONIC:
                    out.append("{st}" + CES_MNEMONIC[code] + ("" if sa == "sa" else "N"))
                p = "SMS" if sa == "sa" else "SMU"
                out.append(p + "{fips}00000" + code + "01")
    if var.startswith("jolts_") or var.startswith("cpi_div_"):
        out.append("{sid}")  # FRED doesn't appear to carry these; kept so the report says so
    if var.startswith("cpi_region_"):
        out.append("{sid}")
    if var == "cpi_us_all_nsa":
        out.append("CPIAUCNS")
    if var == "cpi_us_all_sa":
        out.append("CPIAUCSL")
    if var.startswith("bfs_"):
        for code, (short, _u, _d) in config.BFS_SERIES.items():
            for sa in ("sa", "nsa"):
                if var == f"bfs_{short}_{sa}":
                    out.append(code.replace("_", "") + "TOTAL" + sa.upper() + "{st}")
    if var == "permits_units_total":
        out.append("{st}BPPRIV")
    if var == "permits_units_1unit":
        out.append("{st}BP1FH")
    if var == "imports_total_gen":
        # FRED IMPTOT{st} is general imports (matches GEN_VAL_MO exactly); FRED has no
        # imports-for-consumption state series, so imports_total_con isn't compared.
        out.append("IMPTOT{st}")
    return out


def fmt(pat: str, s: dict, sid: str | None) -> str | None:
    if "{sid}" in pat and not sid:
        return None
    return pat.format(st=s["state_abbr"], fips=s["state_fips"], sid=sid or "")


# ----------------------------------------------------------------------------- FRED fetch
class Fred:
    def __init__(self, key: str, refresh: bool):
        self.key = key
        self.cache = pdm.Cache("fred", refresh=refresh)
        self.mem: dict[str, pd.DataFrame | None] = {}
        self.requests = 0
        self._last = 0.0

    def _fetch(self, fid: str) -> bytes | None:
        wait = MIN_SECONDS_BETWEEN_REQUESTS - (time.time() - self._last)
        if wait > 0:
            time.sleep(wait)
        self._last = time.time()
        self.requests += 1
        r = pdm.SESSION.get(FRED_OBS, timeout=60, headers={"User-Agent": pdm.user_agent(False)},
                            params={"series_id": fid, "api_key": self.key, "file_type": "json",
                                    "observation_start": config.START_DATE})
        if r.status_code == 400 and "does not exist" in r.text:
            return None  # cached as a miss
        if r.status_code != 200:
            raise RuntimeError(f"FRED HTTP {r.status_code} for {fid}: {pdm.redact(r.text[:300])}")
        return r.content

    def get(self, fid: str) -> pd.DataFrame | None:
        """DataFrame [date, fred, fred_str] or None if FRED has no such series."""
        if fid in self.mem:
            return self.mem[fid]
        content, _ = self.cache.get(f"obs_{fid}", "json", lambda: self._fetch(fid))
        df = None
        if content:
            obs = json.loads(content).get("observations", [])
            df = pd.DataFrame(obs, columns=["date", "value"])
            df["date"] = pd.to_datetime(df["date"])
            df["fred_str"] = df["value"].astype(str)
            df["fred"] = pd.to_numeric(df["value"], errors="coerce")  # "." = missing
            df = df[["date", "fred", "fred_str"]]
        self.mem[fid] = df
        if self.requests and self.requests % 100 == 0:
            log.info("  ... %d FRED requests so far", self.requests)
        return df


# ----------------------------------------------------------------------------- comparison
def _decimals(s: str) -> int:
    return len(s.split(".", 1)[1]) if "." in s else 0


def compare(ours: pd.Series, fred: pd.DataFrame) -> tuple[dict, pd.DataFrame]:
    """ours: Series indexed by date. Returns (summary, mismatch rows)."""
    f = fred.set_index("date")
    o = ours.dropna()
    fv = f["fred"].dropna()
    lo, hi = max(o.index.min(), fv.index.min()), min(o.index.max(), fv.index.max())
    j = f.join(ours.rename("ours"), how="outer")
    j = j[(j.index >= lo) & (j.index <= hi)]
    both = j.dropna(subset=["fred", "ours"])
    only_ours = int((j["ours"].notna() & j["fred"].isna()).sum())
    only_fred = int((j["fred"].notna() & j["ours"].isna()).sum())
    res = dict(first=f"{lo:%Y-%m}", last=f"{hi:%Y-%m}", months_compared=len(both),
               only_ours=only_ours, only_fred=only_fred, scale=np.nan, mismatches=0,
               max_abs_diff=np.nan, max_rel_diff=np.nan)
    if both.empty:
        return res, pd.DataFrame()
    nz = both[(both["ours"] != 0) & (both["fred"] != 0)]
    ratio = float((nz["fred"] / nz["ours"]).median()) if len(nz) else 1.0
    scale = 10.0 ** round(math.log10(ratio)) if ratio > 0 else 1.0
    res["scale"] = scale
    b = both.copy()
    b["ours_scaled"] = b["ours"] * scale
    b["diff"] = b["fred"] - b["ours_scaled"]
    b["tol"] = b["fred_str"].map(lambda x: 0.5 * 10.0 ** -_decimals(x)) * 1.001 + 1e-9
    mism = b[b["diff"].abs() > b["tol"]]
    res["mismatches"] = len(mism)
    res["max_abs_diff"] = float(b["diff"].abs().max())
    denom = b["fred"].abs().replace(0, np.nan)
    res["max_rel_diff"] = float((b["diff"].abs() / denom).max()) if denom.notna().any() else np.nan
    return res, mism.reset_index()[["date", "ours", "ours_scaled", "fred", "diff"]]


def yoy(s: pd.Series) -> pd.Series:
    s = s.sort_index()
    prev = s.shift(12, freq="MS").reindex(s.index)
    return 100.0 * (s / prev - 1.0)


# ----------------------------------------------------------------------------- main
def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--states", nargs="*", help="postal codes (default: all 51)")
    ap.add_argument("--vars", nargs="*", help="variables (default: all in the panel)")
    ap.add_argument("--refresh", action="store_true", help="re-download FRED series")
    ap.add_argument("--env-file", default=None)
    a = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")
    pdm.load_dotenv(a.env_file)
    pdm.require_env(["FRED_API_KEY"])

    long_path = config.PROCESSED_DIR / "state_month_long.parquet"
    if not long_path.exists():
        raise SystemExit(f"{long_path} not found; run pull_data.py first.")
    long = pd.read_parquet(long_path)
    long["date"] = pd.to_datetime(long["date"])
    long["state_fips"] = long["state_fips"].astype(str).str.zfill(2)

    states = config.STATES
    if a.states:
        want = {x.upper() for x in a.states}
        states = [s for s in states if s["state_abbr"] in want]
    variables = [v for v in config.VARIABLES if v in set(long["variable"])]
    if a.vars:
        variables = [v for v in variables if v in set(a.vars)]
    numeric_vars = [v for v in variables if not v.endswith("_source")]

    fred = Fred(pdm.env("FRED_API_KEY"), a.refresh)
    pattern_hits: dict[str, int] = defaultdict(int)
    pattern_tries: dict[str, int] = defaultdict(int)
    dropped: set[str] = set()

    # series ID per state x variable (the first non-null one)
    sid_map = (long.dropna(subset=["series_id"]).groupby(["state_fips", "variable"])["series_id"]
               .first().to_dict())
    by_sv = {k: g.set_index("date")["value"].astype(float)
             for k, g in long[long["variable"].isin(numeric_vars)].groupby(["state_fips", "variable"])}

    rows, mism_rows = [], []
    for s in states:
        log.info("== %s", s["state_abbr"])
        for var in numeric_vars:
            key = (s["state_fips"], var)
            ours = by_sv.get(key)
            row = dict(state_abbr=s["state_abbr"], variable=var, fred_id="", status="")
            if var.startswith("cpi_state_proxy"):
                # derived: compare region-sourced months against 12-month change of FRED's region index
                # series_id in the long file says which index each month came from
                rid = f"CUUR{s['cpi_region_area']}" + ("SA0L1E" if "_core" in var else "SA0")
                src = long[(long["state_fips"] == s["state_fips"]) &
                           (long["variable"] == var)].set_index("date")["series_id"]
                src = src.where(src != rid, "region")
                f = fred.get(rid)
                row["fred_id"] = f"yoy({rid})"
                if ours is None or f is None:
                    row["status"] = "no data" if ours is None else "not on FRED"
                    rows.append(row)
                    continue
                reg_months = src[src.astype(str) == "region"].index
                fy = yoy(f.set_index("date")["fred"]).dropna()
                fdf = pd.DataFrame({"date": fy.index, "fred": fy.round(6).values})
                fdf["fred_str"] = "0.000"  # derived, tolerance 0.0005 pp
                o = ours[ours.index.isin(reg_months)]
                if o.dropna().empty:
                    row["status"] = "no region-based months (division used throughout)"
                    rows.append(row)
                    continue
                res, mm = compare(o, fdf)
                res["only_ours"] = res["only_fred"] = 0  # division months are excluded by design
                row.update(res)
                row["status"] = "ok" if res["mismatches"] == 0 else "MISMATCH"
                rows.append(row)
                for _, r in mm.iterrows():
                    mism_rows.append(dict(state_abbr=s["state_abbr"], variable=var,
                                          fred_id=row["fred_id"], **r.to_dict()))
                continue

            pats = candidates(var, s, sid_map.get(key))
            if not pats:
                row["status"] = "no FRED equivalent"
                rows.append(row)
                continue
            if ours is None or ours.dropna().empty:
                row["status"] = "no data in panel"
                rows.append(row)
                continue
            f = fid = None
            tried = []
            for pat0 in pats:
                pat = f"{var}|{pat0}"  # misses are tracked per variable x pattern
                if pat in dropped:
                    continue
                fid = fmt(pat0, s, sid_map.get(key))
                if not fid:
                    continue
                tried.append(fid)
                pattern_tries[pat] += 1
                f = fred.get(fid)
                if f is not None and f["fred"].notna().any():
                    pattern_hits[pat] += 1
                    break
                f = None
                if pattern_hits[pat] == 0 and pattern_tries[pat] >= DROP_PATTERN_AFTER_MISSES:
                    dropped.add(pat)
                    log.info("  %s: pattern %s missed for %d states; not trying it again", var, pat0,
                             pattern_tries[pat])
            if f is None:
                row["fred_id"] = " / ".join(tried) if tried else " / ".join(pats)
                row["status"] = "not on FRED"
                rows.append(row)
                continue
            row["fred_id"] = fid
            res, mm = compare(ours, f)
            row.update(res)
            row["status"] = "ok" if res["mismatches"] == 0 else "MISMATCH"
            rows.append(row)
            for _, r in mm.iterrows():
                mism_rows.append(dict(state_abbr=s["state_abbr"], variable=var, fred_id=fid,
                                      **r.to_dict()))

    out = pd.DataFrame(rows)
    mism = pd.DataFrame(mism_rows)
    config.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out.to_csv(config.PROCESSED_DIR / "fred_comparison.csv", index=False)
    if not mism.empty:
        mism["date"] = pd.to_datetime(mism["date"]).dt.strftime("%Y-%m")
    mism.to_csv(config.PROCESSED_DIR / "fred_mismatches.csv", index=False)
    write_report(out, mism, fred.requests)
    log.info("done: %d FRED requests; wrote fred_comparison.csv, fred_mismatches.csv, "
             "fred_comparison_report.md", fred.requests)


def write_report(out: pd.DataFrame, mism: pd.DataFrame, n_requests: int) -> None:
    L = ["# FRED comparison", "",
         f"Run {pdm.TODAY.isoformat()}. {n_requests} FRED requests (the rest came from the cache in data/raw/fred/).",
         "A mismatch is a month where |FRED - ours x scale| is more than half a unit in FRED's last "
         "decimal place. `only_ours` / `only_fred` count months inside the common date range where "
         "only one side has a value.", ""]
    cmp_ = out[out["status"].isin(["ok", "MISMATCH"])].copy()
    if not cmp_.empty:
        g = cmp_.groupby("variable", sort=False).agg(
            states=("state_abbr", "nunique"),
            fred_id_example=("fred_id", "first"),
            months=("months_compared", "sum"),
            mismatches=("mismatches", "sum"),
            states_with_mismatch=("mismatches", lambda x: int((x > 0).sum())),
            only_ours=("only_ours", "sum"), only_fred=("only_fred", "sum"),
            max_abs_diff=("max_abs_diff", "max"),
            scale=("scale", lambda x: ",".join(sorted({f"{v:g}" for v in x.dropna()}))),
        ).reset_index()
        tot_m, tot_x = int(g["months"].sum()), int(g["mismatches"].sum())
        L += ["## Summary", "",
              f"{len(g)} variables compared, {len(cmp_)} state x variable series, "
              f"{tot_m:,} state-months; {tot_x:,} mismatching months "
              f"({(tot_x / tot_m * 100 if tot_m else 0):.3f}%).", "",
              "## By variable", "", "```", g.to_string(index=False), "```", ""]
    if not mism.empty:
        top = (mism.assign(absdiff=mism["diff"].abs()).sort_values("absdiff", ascending=False)
               .drop(columns="absdiff").head(40))
        L += ["## Largest mismatches (all are in fred_mismatches.csv)", "", "```",
              top.to_string(index=False), "```", ""]
    other = out[~out["status"].isin(["ok", "MISMATCH"])]
    if not other.empty:
        g2 = (other.groupby(["variable", "status"], sort=False)
              .agg(states=("state_abbr", "nunique"), tried=("fred_id", "first")).reset_index())
        L += ["## Not compared", "", "```", g2.to_string(index=False), "```", ""]
    (config.PROCESSED_DIR / "fred_comparison_report.md").write_text("\n".join(L), encoding="utf-8")


if __name__ == "__main__":
    sys.exit(main())
