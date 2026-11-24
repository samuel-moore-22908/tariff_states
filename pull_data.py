"""Pull monthly state-level economic data (50 states + DC, 2015-01 onward) into a tidy
state x month panel. One function per dataset:

    pull_laus      BLS LAUS: unemployment rate, unemployed, employed, labor force (SA)
    pull_ces       BLS CES State and Area: payroll jobs by industry (SA, NSA fallback)
    pull_jolts     BLS state JOLTS: job openings, layoffs & discharges (SA)
    pull_cpi       BLS CPI-U: division/region indexes and a state proxy 12-month change
    pull_bfs       Census Business Formation Statistics (SA and NSA)
    pull_permits   Census Building Permits Survey: units and valuation by structure (NSA)
    pull_imports   Census International Trade: imports by state of destination (NSA)

Series lists and the state crosswalk live in config.py.

Usage:
    python pull_data.py --pilot            # Indiana only, every source, prints sample rows
    python pull_data.py                    # full run (reuses cached raw responses)
    python pull_data.py --refresh          # re-download everything (use this to update)
    python pull_data.py --sources laus cpi # subset of sources
    python pull_data.py --env-file PATH    # keys file (default: $ENV_FILE, then ./.env)
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import io
import json
import logging
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =============================================================================
# DIRECTORIES: edit these when moving the code to another machine
# =============================================================================
# PROJECT_DIR  folder holding this script, config.py and (by default) the .env file
# DATA_DIR     where downloads and outputs go (can be outside the repo, e.g. a shared drive)
# RAW_DIR      cached raw API/file responses, one subfolder per source
# PROCESSED_DIR  final panel, data dictionary, reports
# ENV_FILE     KEY=VALUE file with API keys and CONTACT_EMAIL. --env-file and $ENV_FILE
#              still take precedence over this setting.
PROJECT_DIR = Path(r"C:\Users\smoor\GitHub\tariff_states")
DATA_DIR = Path(r"C:\Users\smoor\GitHub\tariff_states\data")
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
ENV_FILE = PROJECT_DIR / ".env"

# Make config.py importable even when Python's working directory isn't the project folder
# (IDE consoles, notebooks, running from another directory).
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))
import config  # noqa: E402

# Every module reads paths from config, so set them there (fred_compare.py uses these too).
config.ROOT, config.RAW_DIR, config.PROCESSED_DIR = PROJECT_DIR, RAW_DIR, PROCESSED_DIR
# =============================================================================


# =============================================================================
# Series added on top of config.py
# =============================================================================
# CES "Mining, logging, and construction" (15000000): BLS publishes it for every state, while
# construction alone (20000000) isn't published for DE, DC and HI (checked in sm.series,
# 2026-11-24). Adds emp_mlc_sa (and emp_mlc_nsa, used only as a fallback), placed after total
# private. If config.CES_INDUSTRIES already has the code, this does nothing.
if "15000000" not in config.CES_INDUSTRIES:
    _ces = list(config.CES_INDUSTRIES.items())
    _at = [k for k, _ in _ces].index("05000000") + 1
    _ces.insert(_at, ("15000000", ("mlc", "Mining, logging, and construction")))
    config.CES_INDUSTRIES = dict(_ces)
    config.VARIABLES = config._build_variables()  # data dictionary / missing report pick it up
# =============================================================================

log = logging.getLogger("tariff_states")

TODAY = dt.date.today()
TODAY_STR = TODAY.strftime("%Y%m%d")
SHUTDOWN_MONTH = pd.Timestamp("2025-10-01")


# =============================================================================
# Environment and keys
# =============================================================================
def load_dotenv(path: Path | str | None = None) -> None:
    """Load KEY=VALUE lines from an env file without overriding variables already set
    in the environment. Names are case-insensitive; values are never logged. File used,
    in order: the `path` argument (--env-file), $ENV_FILE, then ENV_FILE set at the top of
    this script."""
    path = Path(path or os.environ.get("ENV_FILE") or ENV_FILE).expanduser()
    if not path.exists():
        if path != ENV_FILE:
            raise SystemExit(f"env file not found: {path}")
        return
    log.info("reading keys from %s", path)
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if line.lower().startswith("export "):
            line = line[7:].strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip().upper()
        v = v.strip().strip('"').strip("'")
        if k and v and not os.environ.get(k):
            os.environ[k] = v


def require_env(names: list[str]) -> None:
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        msg = ("Missing required environment variable(s): " + ", ".join(missing) +
               ". Set them in your environment or in a .env file in the repo root "
               "(see README.md). Stopping.")
        log.error(msg)
        raise SystemExit(msg)


def env(name: str) -> str | None:
    return os.environ.get(name) or None


def user_agent(contact: bool = False) -> str:
    """download.bls.gov and www2.census.gov require a contact email (CONTACT_EMAIL)."""
    if contact:
        email = env("CONTACT_EMAIL")
        if not email:
            raise SystemExit("CONTACT_EMAIL is not set; download.bls.gov and www2.census.gov "
                             "require a User-Agent with a contact email. Stopping.")
        return f"tariff-states-pipeline/1.0 (contact: {email})"
    return "tariff-states-pipeline/1.0 (python-requests)"


def redact(text: str) -> str:
    """Remove API keys from text before it is logged."""
    for k in ("BLS_API_KEY", "CENSUS_API_KEY", "FRED_API_KEY"):
        v = os.environ.get(k)
        if v:
            text = text.replace(v, "***")
    return re.sub(r"(key|registrationkey|api_key)=[^&\s]+", r"\1=***", text)


# =============================================================================
# HTTP session with retries, and the raw-response cache
# =============================================================================
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=6, connect=6, read=6, status=6, backoff_factor=1.5,  # 0,3,6,12,24,48 s
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=frozenset(["GET", "POST"]),
                  respect_retry_after_header=True, raise_on_status=False)
    adapter = HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


SESSION = make_session()


def http_get(url: str, *, params: dict | None = None, contact_ua: bool = False,
             timeout: int = 120) -> bytes | None:
    """GET with retries. Returns None for HTTP 204 (Census 'no data')."""
    r = SESSION.get(url, params=params, headers={"User-Agent": user_agent(contact_ua)}, timeout=timeout)
    if r.status_code == 204:
        return None
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} for {redact(r.url)}: {redact(r.text[:300])}")
    return r.content


class Cache:
    """Raw responses under data/raw/{source}/{key}__{YYYYMMDD}.{ext}. The newest file for a
    key is reused unless refresh=True; the date in the name is the pull date."""

    def __init__(self, source: str, refresh: bool = False):
        self.refresh = refresh
        self.dir = config.RAW_DIR / source
        self.dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _safe(key: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]+", "_", key)

    def get(self, key: str, ext: str, fetch) -> tuple[bytes | None, str]:
        """Return (content, pull_date). fetch() -> bytes or None ('no data', cached as empty)."""
        if not self.refresh:
            hits = sorted(self.dir.glob(f"{self._safe(key)}__*.{ext}"))
            if hits:
                d = hits[-1].stem.rsplit("__", 1)[1]
                b = hits[-1].read_bytes()
                return (b or None), f"{d[:4]}-{d[4:6]}-{d[6:8]}"
        content = fetch()
        (self.dir / f"{self._safe(key)}__{TODAY_STR}.{ext}").write_bytes(content or b"")
        return content, TODAY.isoformat()


def to_long(rows: list[dict]) -> pd.DataFrame:
    """Rows -> long DataFrame with the standard columns, from 2015-01 on."""
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=config.LONG_COLUMNS)
    for c in config.LONG_COLUMNS:
        if c not in df:
            df[c] = pd.NA
    df = df[config.LONG_COLUMNS].copy()
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["preliminary"] = df["preliminary"].fillna(False).astype(bool)
    df["footnotes"] = df["footnotes"].fillna("").astype(str)
    return df[df["date"] >= config.START_DATE].reset_index(drop=True)


# =============================================================================
# BLS API v2 (batching, daily quota, footnotes) and BLS series files
# =============================================================================
_BLS_QUOTA_FILE = config.RAW_DIR / "bls" / "_query_log.json"
_bls_last_request = 0.0


def _bls_queries_today() -> int:
    try:
        return json.loads(_BLS_QUOTA_FILE.read_text()).get(TODAY.isoformat(), 0)
    except (OSError, ValueError):
        return 0


def _bls_record_query() -> None:
    _BLS_QUOTA_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        d = json.loads(_BLS_QUOTA_FILE.read_text())
    except (OSError, ValueError):
        d = {}
    d[TODAY.isoformat()] = d.get(TODAY.isoformat(), 0) + 1
    _BLS_QUOTA_FILE.write_text(json.dumps(d, indent=1))


def _bls_post(series: list[str], y0: int, y1: int) -> dict:
    global _bls_last_request
    limit = config.BLS_DAILY_QUERY_LIMIT - config.BLS_DAILY_QUERY_SAFETY_MARGIN
    if _bls_queries_today() >= limit:
        raise SystemExit(f"BLS daily query budget reached ({limit} queries logged today in "
                         f"{_BLS_QUOTA_FILE}). Re-run tomorrow; cached batches will be reused.")
    wait = config.BLS_MIN_SECONDS_BETWEEN_REQUESTS - (time.monotonic() - _bls_last_request)
    if wait > 0:
        time.sleep(wait)
    _bls_last_request = time.monotonic()
    payload = {"seriesid": series, "startyear": str(y0), "endyear": str(y1),
               "registrationkey": env("BLS_API_KEY"), "catalog": False}
    r = SESSION.post(config.BLS_API_URL, json=payload, timeout=120,
                     headers={"User-Agent": user_agent(False), "Content-Type": "application/json"})
    _bls_record_query()
    if r.status_code != 200:
        raise RuntimeError(f"BLS API HTTP {r.status_code}: {redact(r.text[:300])}")
    d = r.json()
    if d.get("status") != "REQUEST_SUCCEEDED":
        raise RuntimeError(f"BLS API status {d.get('status')}: {redact(str(d.get('message')))}")
    return d


def bls_fetch(series_ids: list[str], start_year: int, end_year: int, tag: str,
              refresh: bool = False) -> tuple[dict[str, list[dict]], dict[str, str], list[str]]:
    """Fetch BLS series in batches of <=50 series and <=20 years, each batch cached.
    Returns ({series_id: [observations]}, {series_id: pull_date}, API messages)."""
    cache = Cache("bls", refresh)
    out: dict[str, list[dict]] = {s: [] for s in series_ids}
    pulls: dict[str, str] = {}
    messages: list[str] = []
    ids = list(dict.fromkeys(series_ids))
    n, step = config.BLS_MAX_SERIES_PER_QUERY, config.BLS_MAX_YEARS_PER_QUERY
    for i in range(0, len(ids), n):
        batch = ids[i:i + n]
        h = hashlib.sha1(",".join(batch).encode()).hexdigest()[:10]
        for y0 in range(start_year, end_year + 1, step):
            y1 = min(y0 + step - 1, end_year)
            content, pull = cache.get(f"{tag}_b{i // n:03d}_{y0}_{y1}_{h}", "json",
                                      lambda: json.dumps(_bls_post(batch, y0, y1)).encode())
            d = json.loads(content)
            messages += [m for m in d.get("message", []) if m]
            for s in d["Results"]["series"]:
                out.setdefault(s["seriesID"], []).extend(s.get("data", []))
                pulls[s["seriesID"]] = pull
    return out, pulls, messages


def bls_parse_obs(obs: dict) -> dict | None:
    """BLS observation -> {date, value, preliminary, footnotes}; None for M13 (annual average).
    Values like '-' (e.g. Oct 2025, footnote X) become NaN."""
    p = obs.get("period", "")
    if not p.startswith("M") or p == "M13":
        return None
    try:
        val = float(str(obs.get("value", "")).strip().replace(",", ""))
    except ValueError:
        val = float("nan")
    fns = [f for f in obs.get("footnotes", []) if f and f.get("code")]
    return {"date": dt.date(int(obs["year"]), int(p[1:]), 1), "value": val,
            "preliminary": "P" in {f["code"] for f in fns},
            "footnotes": "; ".join(f"{f['code']}: {f.get('text', '').strip()}" for f in fns)}


def bls_records(series_data: dict, pulls: dict, meta: dict[str, dict]) -> list[dict]:
    """Fetched series -> long records; meta[series_id] gives state and labels."""
    rows = []
    for sid, obs_list in series_data.items():
        if sid not in meta:
            continue
        seen = set()
        for o in obs_list:
            p = bls_parse_obs(o)
            if p is None or p["date"] in seen:
                continue
            seen.add(p["date"])
            rows.append({**meta[sid], **p, "series_id": sid, "pull_date": pulls.get(sid)})
    return rows


def bls_file(survey: str, name: str, refresh: bool = False) -> pd.DataFrame:
    """A BLS flat file, e.g. bls_file('sm', 'sm.industry'), as strings."""
    url = f"{config.BLS_FILES_BASE}/{survey}/{name}"
    content, _ = Cache("bls_files", refresh).get(name, "txt", lambda: http_get(url, contact_ua=True))
    df = pd.read_csv(io.BytesIO(content), sep="\t", dtype=str, keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]
    return df.apply(lambda s: s.str.strip())


def verify_bls_patterns(refresh: bool = False) -> dict:
    """Check CES industry codes against sm.industry and show jt.series for Indiana."""
    ind = bls_file("sm", "sm.industry", refresh)
    known = dict(zip(ind["industry_code"], ind["industry_name"]))
    problems = []
    for code, (_, desc) in config.CES_INDUSTRIES.items():
        if code in known:
            log.info("  sm.industry %s = %s", code, known[code])
        else:
            problems.append(f"CES industry {code} ({desc}) not in sm.industry")
            log.warning(problems[-1])
    js = bls_file("jt", "jt.series", refresh)
    jt = js[(js["state_code"] == "18") & (js["industry_code"] == "000000")
            & js["dataelement_code"].isin(["JO", "LD"]) & (js["seasonal"] == "S")
            & (js["area_code"] == "00000") & (js["sizeclass_code"] == "00")]
    jt = jt[["series_id", "dataelement_code", "ratelevel_code", "begin_year", "begin_period",
             "end_year", "end_period"]]
    log.info("JOLTS pattern check: jt.series rows for Indiana, total nonfarm, SA:\n%s",
             jt.to_string(index=False))
    return {"ces_industry_problems": problems, "jolts_series_indiana": jt.to_dict("records")}


def bls_lookup_failures(name: str, survey: str, ids: list[str], refresh: bool = False) -> None:
    """For series IDs that returned no data, look them up in {survey}.series."""
    if not ids:
        return
    if not env("CONTACT_EMAIL"):
        log.warning("%s: %d series had no data; set CONTACT_EMAIL to look them up in %s.series",
                    name, len(ids), survey)
        return
    try:
        found = bls_file(survey, f"{survey}.series", refresh)
        found = found[found["series_id"].isin(ids)]
    except Exception as e:  # never stop the run for a lookup
        log.warning("%s: could not load %s.series: %s", name, survey, e)
        return
    for sid in ids:
        row = found[found["series_id"] == sid]
        if row.empty:
            log.warning("%s: %s is NOT in %s.series (series does not exist)", name, sid, survey)
        else:
            r = row.iloc[0]
            log.warning("%s: %s exists in %s.series (%s %s - %s %s) but API returned no data",
                        name, sid, survey, r["begin_year"], r["begin_period"], r["end_year"], r["end_period"])


# =============================================================================
# 1. BLS LAUS
# =============================================================================
def laus_series_id(fips: str, measure: str) -> str:
    return f"LASST{fips}{'0' * 11}{measure}"


def pull_laus(states: list[dict], refresh: bool = False) -> tuple[pd.DataFrame, dict]:
    """Seasonally adjusted unemployment rate, unemployed, employed and labor force."""
    meta = {}
    for s in states:
        for m, (var, units, _) in config.LAUS_MEASURES.items():
            meta[laus_series_id(s["state_fips"], m)] = dict(
                state_fips=s["state_fips"], state_abbr=s["state_abbr"], variable=var,
                units=units, seasonal_adj="SA", geo_level="state", source="BLS LAUS")
    data, pulls, _ = bls_fetch(list(meta), config.START_YEAR, TODAY.year, "laus", refresh)
    empty = [sid for sid in meta if not data.get(sid)]
    if empty:
        log.warning("LAUS: %d series returned no data: %s", len(empty), empty[:10])
    bls_lookup_failures("LAUS", "la", empty, refresh)
    return to_long(bls_records(data, pulls, meta)), {"no_data": empty}


# =============================================================================
# 2. BLS CES State and Area
# =============================================================================
def ces_series_id(fips: str, industry: str, sa: bool = True) -> str:
    return f"{'SMS' if sa else 'SMU'}{fips}00000{industry}01"


def pull_ces(states: list[dict], refresh: bool = False) -> tuple[pd.DataFrame, dict]:
    """All employees (thousands), statewide, by industry. SA (SMS...) first; where BLS
    returns nothing, the NSA series (SMU...) goes in a separate emp_<industry>_nsa column."""
    def meta(s, short, sa):
        return dict(state_fips=s["state_fips"], state_abbr=s["state_abbr"],
                    variable=f"emp_{short}_{'sa' if sa else 'nsa'}", units="thousands of jobs",
                    seasonal_adj="SA" if sa else "NSA", geo_level="state",
                    source="BLS CES State and Area")

    by_fips = {s["state_fips"]: s for s in states}
    sa_meta = {ces_series_id(s["state_fips"], code): meta(s, short, True)
               for s in states for code, (short, _) in config.CES_INDUSTRIES.items()}
    data, pulls, _ = bls_fetch(list(sa_meta), config.START_YEAR, TODAY.year, "ces_sa", refresh)
    nsa_meta = {ces_series_id(sid[3:5], sid[10:18], False):
                meta(by_fips[sid[3:5]], config.CES_INDUSTRIES[sid[10:18]][0], False)
                for sid in sa_meta if not data.get(sid)}
    nsa_data, nsa_pulls = {}, {}
    if nsa_meta:
        nsa_data, nsa_pulls, _ = bls_fetch(list(nsa_meta), config.START_YEAR, TODAY.year,
                                           "ces_nsa", refresh)
    used_nsa = [sid for sid in nsa_meta if nsa_data.get(sid)]
    neither = [sid for sid in nsa_meta if not nsa_data.get(sid)]
    for sid in used_nsa:
        log.info("CES: SA not published, using NSA %s", sid)
    for sid in neither:
        log.warning("CES: neither SA nor NSA series exists for SMS%s / %s", sid[3:], sid)
    if neither and env("CONTACT_EMAIL"):
        try:
            sm = bls_file("sm", "sm.series", refresh)
            for sid in neither:
                alt = sm[(sm["state_code"] == sid[3:5]) & (sm["area_code"] == "00000")
                         & (sm["industry_code"] == sid[10:18]) & (sm["data_type_code"] == "01")]
                log.warning("CES %s: sm.series has %d statewide all-employee series for this "
                            "state x industry: %s", sid, len(alt), alt["series_id"].tolist())
        except Exception as e:
            log.warning("CES lookup in sm.series failed: %s", e)
    rows = bls_records(data, pulls, sa_meta) + bls_records(nsa_data, nsa_pulls, nsa_meta)
    return to_long(rows), {"nsa_fallback": used_nsa, "not_found": neither}


def ces_national_total_nonfarm(refresh: bool = False) -> pd.DataFrame:
    """National total nonfarm (CES0000000001), for the sum-of-states check."""
    sid = config.CES_NATIONAL_TOTAL_NONFARM
    data, _, _ = bls_fetch([sid], config.START_YEAR, TODAY.year, "ces_national", refresh)
    df = pd.DataFrame([r for r in (bls_parse_obs(o) for o in data.get(sid, [])) if r])
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df[["date", "value", "preliminary"]].drop_duplicates("date").sort_values("date")


# =============================================================================
# 3. BLS JOLTS (state)
# =============================================================================
def jolts_series_id(fips: str, element: str, lr: str) -> str:
    return f"JTS000000{fips}0000000{element}{lr}"


def pull_jolts(states: list[dict], refresh: bool = False) -> tuple[pd.DataFrame, dict]:
    """Job openings and layoffs & discharges, total nonfarm, SA, levels and rates."""
    meta = {}
    for s in states:
        for (el, lr), (var, units, _) in config.JOLTS_ELEMENTS.items():
            meta[jolts_series_id(s["state_fips"], el, lr)] = dict(
                state_fips=s["state_fips"], state_abbr=s["state_abbr"], variable=var,
                units=units, seasonal_adj="SA", geo_level="state", source="BLS JOLTS")
    data, pulls, _ = bls_fetch(list(meta), config.START_YEAR, TODAY.year, "jolts", refresh)
    empty = [sid for sid in meta if not data.get(sid)]
    if empty:
        log.warning("JOLTS: %d series returned no data: %s", len(empty), empty[:10])
    bls_lookup_failures("JOLTS", "jt", empty, refresh)
    return to_long(bls_records(data, pulls, meta)), {"no_data": empty}


# =============================================================================
# 4. BLS CPI-U state proxy
# =============================================================================
def _yoy(s: pd.Series) -> pd.Series:
    """12-month % change on a complete monthly index (no filling; NaN propagates)."""
    full = s.reindex(pd.date_range(s.index.min(), s.index.max(), freq="MS"))
    return (full / full.shift(12) - 1.0) * 100.0


def pull_cpi(states: list[dict], refresh: bool = False) -> tuple[pd.DataFrame, dict]:
    """BLS has no state CPIs. Per state: the raw Census division index (Dec 2017=100, NaN
    before), the raw region index, the U.S. city average, and cpi_state_proxy[_core]_yoy:
    the division's 12-month % change where it exists, otherwise the region's. Index levels
    with different bases are never spliced."""
    def sid_(area, item, sa=False):
        return f"CU{'S' if sa else 'U'}R{area}{item}"

    areas = {s["cpi_division_area"] for s in states} | {s["cpi_region_area"] for s in states}
    ids = [sid_(a, item) for a in sorted(areas) for item in config.CPI_ITEMS] + list(config.CPI_US_SERIES)
    # one extra year so the 2015 12-month changes can be computed
    data, pulls, _ = bls_fetch(ids, config.START_YEAR - 1, TODAY.year, "cpi", refresh)
    empty = [sid for sid in ids if not data.get(sid)]
    if empty:
        log.warning("CPI: series with no data: %s", empty)
    bls_lookup_failures("CPI", "cu", empty, refresh)

    series = {}
    for sid in ids:
        recs = [r for r in (bls_parse_obs(o) for o in data.get(sid, [])) if r]
        if recs:
            df = pd.DataFrame(recs).drop_duplicates("date")
            df["date"] = pd.to_datetime(df["date"])
            series[sid] = df.set_index("date").sort_index()

    def obs_rows(base, sid, var, units, sa, geo):
        return [{**base, "date": d, "variable": var, "value": r["value"], "units": units,
                 "seasonal_adj": sa, "geo_level": geo, "series_id": sid,
                 "preliminary": r["preliminary"], "footnotes": r["footnotes"],
                 "pull_date": pulls.get(sid)} for d, r in series[sid].iterrows()]

    rows = []
    idx = pd.date_range(config.START_DATE, TODAY.replace(day=1), freq="MS")
    for s in states:
        base = dict(state_fips=s["state_fips"], state_abbr=s["state_abbr"], source="BLS CPI")
        yoy = {}
        for item, lab in config.CPI_ITEMS.items():
            for kind, area, geo, units in (
                    ("div", s["cpi_division_area"], "division", "index, Dec 2017=100"),
                    ("region", s["cpi_region_area"], "region", "index, 1982-84=100")):
                sid = sid_(area, item)
                if sid in series:
                    rows += obs_rows(base, sid, f"cpi_{kind}_{lab}_nsa", units, "NSA", geo)
                    yoy[(lab, kind)] = (sid, _yoy(series[sid]["value"]).reindex(idx), pulls.get(sid))
        for sid, var in config.CPI_US_SERIES.items():
            if sid in series:
                rows += obs_rows(base, sid, var, "index, 1982-84=100",
                                 "SA" if sid.startswith("CUS") else "NSA", "national")
        for lab, suffix in (("all", ""), ("core", "_core")):
            div, reg = yoy.get((lab, "div")), yoy.get((lab, "region"))
            for d in idx:
                if div and pd.notna(div[1][d]):
                    sid, val, geo, pull = div[0], div[1][d], "division", div[2]
                elif reg and pd.notna(reg[1][d]):
                    sid, val, geo, pull = reg[0], reg[1][d], "region", reg[2]
                else:
                    continue  # neither change exists (e.g. Oct 2025, Oct 2026): leave missing
                rows.append({**base, "date": d, "variable": f"cpi_state_proxy{suffix}_yoy",
                             "value": round(float(val), 4),
                             "units": "percent change from 12 months earlier",
                             "seasonal_adj": "NSA", "geo_level": geo, "series_id": sid,
                             "preliminary": False, "footnotes": "", "pull_date": pull})
    return to_long(rows), {"no_data": empty}


# =============================================================================
# 5. Census Business Formation Statistics
# =============================================================================
BFS_MONTHS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
BFS_API_GET = "cell_value,data_type_code,time_slot_id,error_data,category_code,seasonally_adj,geo_level_code"


def _bfs_api(params: dict, cache: Cache, key: str):
    content, pull = cache.get(key, "json", lambda: http_get(
        config.BFS_API, params={**params, "get": BFS_API_GET, "for": "us:*", "key": env("CENSUS_API_KEY")}))
    if not content:
        return None, pull
    d = json.loads(content)
    df = pd.DataFrame(d[1:], columns=d[0])
    return df.loc[:, ~df.columns.duplicated()], pull


def bfs_load_csv(cache: Cache) -> tuple[pd.DataFrame, str]:
    content, pull = cache.get("bfs_monthly", "csv", lambda: http_get(config.BFS_CSV_URL))
    return pd.read_csv(io.BytesIO(content), dtype=str), pull


def bfs_us_api(refresh: bool = False) -> pd.DataFrame:
    """National totals from the API, to cross-check the CSV."""
    df, _ = _bfs_api({"time": f"from {config.START_YEAR}-01", "category_code": "TOTAL"},
                     Cache("bfs", refresh), "api_US_TOTAL")
    return df if df is not None else pd.DataFrame()


def bfs_us_csv(refresh: bool = False) -> pd.DataFrame:
    raw, _ = bfs_load_csv(Cache("bfs", refresh))
    us = raw[(raw["geo"] == "US") & (raw["naics_sector"] == "TOTAL") & (raw["series"] == "BA_BA") & (raw["sa"] == "A")]
    us = us.melt(id_vars=["year"], value_vars=BFS_MONTHS, var_name="m", value_name="value")
    us["date"] = pd.to_datetime(us["year"] + "-" + us["m"].map(lambda m: f"{BFS_MONTHS.index(m) + 1:02d}") + "-01")
    us["value"] = pd.to_numeric(us["value"], errors="coerce")
    return us


def pull_bfs(states: list[dict], refresh: bool = False) -> tuple[pd.DataFrame, dict]:
    """Business applications and formations, all industries, SA and NSA. Uses the API
    (timeseries/eits/bfs) if it serves states; as of 2026-11 it serves only the U.S.
    (HTTP 204 for states), so the monthly CSV linked from census.gov/econ/bfs is used."""
    cache = Cache("bfs", refresh)
    by_abbr = {s["state_abbr"]: s for s in states}
    probe, _ = _bfs_api({"time": "2024-01", "geo_level_code": "IN", "category_code": "TOTAL"},
                        cache, "api_probe_IN_2024-01")
    rows, info = [], {}

    def add(s, date, code, sa, value, sid, pull, src, footnote=""):
        short, units, _ = config.BFS_SERIES[code]
        rows.append(dict(state_fips=s["state_fips"], state_abbr=s["state_abbr"], date=date,
                         variable=f"bfs_{short}_{'sa' if sa else 'nsa'}", value=value, units=units,
                         seasonal_adj="SA" if sa else "NSA", geo_level="state", source=src,
                         series_id=sid, pull_date=pull, footnotes=footnote))

    if probe is not None and len(probe):
        for s in states:
            df, pull = _bfs_api({"time": f"from {config.START_YEAR}-01", "geo_level_code": s["state_abbr"],
                                 "category_code": "TOTAL"}, cache, f"api_{s['state_abbr']}_TOTAL")
            for r in (df.itertuples(index=False) if df is not None else []):
                if r.data_type_code in config.BFS_SERIES:
                    add(s, f"{r.time}-01", r.data_type_code, r.seasonally_adj == "yes", r.cell_value,
                        f"eits/bfs?data_type_code={r.data_type_code}&category_code=TOTAL&"
                        f"seasonally_adj={r.seasonally_adj}&geo_level_code={s['state_abbr']}",
                        pull, "Census BFS (API)")
        if len({r["state_abbr"] for r in rows}) == len(states):
            info["method"] = "api"
            return to_long(rows), info
        log.warning("BFS API is missing states; using the CSV for all states")
        rows = []
    else:
        log.warning("BFS API has no state-level data (HTTP 204 for geo_level_code=IN); "
                    "falling back to %s", config.BFS_CSV_URL)
    info["method"] = "csv_fallback (API serves US only)"
    raw, pull = bfs_load_csv(cache)
    raw = raw[(raw["naics_sector"] == "TOTAL") & (pd.to_numeric(raw["year"]) >= config.START_YEAR)]
    long = raw.melt(id_vars=["sa", "naics_sector", "series", "geo", "year"], value_vars=BFS_MONTHS,
                    var_name="month", value_name="raw_value")
    long["value"] = pd.to_numeric(long["raw_value"], errors="coerce")
    info["non_numeric_tokens"] = long[long["raw_value"].notna() & long["value"].isna()]["raw_value"].unique().tolist()
    for r in long[long["geo"].isin(by_abbr)].itertuples(index=False):
        if r.series not in config.BFS_SERIES or pd.isna(r.raw_value):
            continue  # blank cells = month not (yet) published
        fn = "" if pd.notna(r.value) else ("D: suppressed by Census (disclosure/quality)"
                                           if r.raw_value == "D" else f"non-numeric cell '{r.raw_value}'")
        add(by_abbr[r.geo], f"{r.year}-{BFS_MONTHS.index(r.month) + 1:02d}-01", r.series, r.sa == "A",
            r.value, f"bfs_monthly.csv: series={r.series}, sa={r.sa}, naics_sector=TOTAL, geo={r.geo}",
            pull, "Census BFS (monthly CSV)", fn)
    return to_long(rows), info


# =============================================================================
# 6. Census Building Permits Survey
# =============================================================================
BPS_TYPES = ["1u", "2u", "34u", "5u"]


def parse_bps_txt(content: bytes) -> pd.DataFrame:
    """Current-month text file st{YYMM}c.txt: date, FIPS, region, division, name, then
    Bldgs/Units/Value for 1, 2, 3-4 and 5+ units (estimates incl. imputation), then the
    reported-only block (not used). Total = sum of the four structure types."""
    rows = []
    for line in content.decode("latin-1").splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 17 or not re.fullmatch(r"\d{6}", parts[0]):
            continue
        rec = {"yyyymm": parts[0], "state_fips": parts[1].zfill(2), "region": parts[2],
               "division": parts[3], "name": parts[4]}
        for i, t in enumerate(BPS_TYPES):
            rec[f"units_{t}"] = float(parts[6 + 3 * i])
            rec[f"value_{t}"] = float(parts[7 + 3 * i])
        rows.append(rec)
    df = pd.DataFrame(rows)
    df["units_tot"] = df[[f"units_{t}" for t in BPS_TYPES]].sum(axis=1)
    df["value_tot"] = df[[f"value_{t}" for t in BPS_TYPES]].sum(axis=1)
    return df


def parse_bps_excel(content: bytes) -> pd.DataFrame:
    """Excel statemonthly_{YYYYMM}.xls: Units and Valuation sheets; current-month columns
    are Total, 1, 2, 3-4, 5+ (the year-to-date columns are ignored)."""
    xl = pd.read_excel(io.BytesIO(content), sheet_name=None, header=None, engine="xlrd")

    def sheet(word):
        raw = xl[next(k for k in xl if word in k.lower())]
        out = {}
        for _, r in raw.iterrows():
            key = re.sub(r"[^a-z ]", "", str(r.iloc[0]).lower()).strip()
            if key in config.NAME_TO_ABBR:
                out[config.NAME_TO_ABBR[key]] = pd.to_numeric(r.iloc[1:6], errors="coerce").tolist()
        return pd.DataFrame.from_dict(out, orient="index", columns=["tot", "1u", "2u", "34u", "5u"])

    df = sheet("unit").add_prefix("units_").join(sheet("val").add_prefix("value_"))
    df.index.name = "state_abbr"
    return df.reset_index()


def pull_permits(states: list[dict], refresh: bool = False) -> tuple[pd.DataFrame, dict]:
    """New privately owned housing units authorized and their valuation (thousands of $),
    by structure type, NSA. Text files are primary (they cover every month); Excel files
    (Nov 2019 on) are compared against them and used if a text file is missing."""
    cache = Cache("permits", refresh)

    def listing(key, url, pattern, contact):
        content, _ = cache.get(key, "html", lambda: http_get(url, contact_ua=contact))
        return set(re.findall(pattern, content.decode("utf-8", "replace")))

    txt_files = listing("state_dir_listing", config.BPS_STATE_DIR, r'href="(st\d{4}c\.txt)"', True)
    xls_files = listing("statemonthly_index", config.BPS_EXCEL_INDEX, r"statemonthly_(\d{6})\.xls", False)
    abbr = {s["state_fips"]: s["state_abbr"] for s in states}
    info = {"txt_months": [], "excel_months": [], "excel_only_months": [], "mismatches": [],
            "missing_months": []}
    frames, compare = [], []
    for d in pd.date_range(config.START_DATE, TODAY.replace(day=1), freq="MS"):
        yymm, yyyymm = d.strftime("%y%m"), d.strftime("%Y%m")
        txt = xls = None
        name = f"st{yymm}c.txt"
        if name in txt_files:
            content, pull = cache.get(name, "txt", lambda: http_get(config.BPS_STATE_DIR + name, contact_ua=True))
            txt = parse_bps_txt(content)
            txt["state_abbr"] = txt["state_fips"].map(abbr)
            txt = txt[txt["state_abbr"].notna()].assign(src=f"www2.census.gov/econ/bps/State/{name}", pull_date=pull)
            info["txt_months"].append(yyyymm)
        if yyyymm in xls_files:
            xname = f"statemonthly_{yyyymm}.xls"
            content, xpull = cache.get(xname, "xls", lambda: http_get(config.BPS_EXCEL_BASE + xname))
            try:
                xls = parse_bps_excel(content)
                xls = xls[xls["state_abbr"].isin(set(abbr.values()))].assign(
                    src=f"census.gov/construction/bps/xls/{xname}", pull_date=xpull)
                info["excel_months"].append(yyyymm)
            except Exception as e:  # the text file is primary; keep going
                log.warning("permits: could not parse %s: %s", xname, e)
        if txt is not None:
            frames.append(txt.assign(date=d))
            if xls is not None:
                m = txt.merge(xls, on="state_abbr", suffixes=("", "_x"))
                for c in [f"{k}_{t}" for k in ("units", "value") for t in BPS_TYPES + ["tot"]]:
                    for _, r in m[(m[c] - m[c + "_x"]).abs() > 0.5].iterrows():
                        info["mismatches"].append((yyyymm, r["state_abbr"], c, r[c], r[c + "_x"]))
                compare.append(m.assign(date=d))
        elif xls is not None:
            frames.append(xls.assign(date=d, state_fips=xls["state_abbr"].map(config.ABBR_TO_FIPS)))
            info["excel_only_months"].append(yyyymm)
        else:
            info["missing_months"].append(yyyymm)
    # months after the latest published file are "not yet released", not missing
    published = info["txt_months"] + info["excel_only_months"]
    if published:
        info["missing_months"] = [m for m in info["missing_months"] if m < max(published)]
    if not frames:
        return to_long([]), info
    df = pd.concat(frames, ignore_index=True)
    info["excel_compare"] = pd.concat(compare, ignore_index=True) if compare else pd.DataFrame()
    info["txt_region_division"] = df[["state_abbr", "region", "division"]].dropna().drop_duplicates()
    rows = []
    for kind, units in (("units", "housing units"), ("value", "thousands of dollars")):
        for t, lab in config.BPS_TYPES.items():
            col = f"{kind}_{t}"
            for r in df[["state_fips", "state_abbr", "date", col, "src", "pull_date"]].itertuples(index=False):
                rows.append(dict(state_fips=r.state_fips, state_abbr=r.state_abbr, date=r.date,
                                 variable=f"permits_{kind}_{lab}", value=getattr(r, col), units=units,
                                 seasonal_adj="NSA", geo_level="state",
                                 source="Census Building Permits Survey",
                                 series_id=r.src + (" (sum of structure types)"
                                                    if t == "tot" and "xls" not in r.src else ""),
                                 pull_date=r.pull_date))
    return to_long(rows), info


# =============================================================================
# 7. Census International Trade: imports by state of destination
# =============================================================================
# Dataset timeseries/intltrade/imports/statehs (the BETA 'statehsimport' path returns 404).
# Codes from variables.json and real responses: all commodities I_COMMODITY='-' with
# COMM_LVL='-'; all countries CTY_CODE='-'; HS2 chapters COMM_LVL='HS2'; SUMMARY_LVL 'DET'
# = individual country or total, 'CGP' = country grouping (dropped). The state dataset has
# no calculated-duty field (CAL_DUT_MO exists only in the national imports/hs dataset).
TRADE_GET_TOTAL_COUNTRY = "CTY_CODE,CTY_NAME,SUMMARY_LVL,SUMMARY_LVL2,CON_VAL_MO,GEN_VAL_MO"
TRADE_GET_HS2 = "I_COMMODITY,I_COMMODITY_SDESC,CON_VAL_MO,GEN_VAL_MO"


def _trade_query(url: str, params: dict, cache: Cache, key: str) -> tuple[pd.DataFrame, str, str]:
    content, pull = cache.get(key, "json", lambda: http_get(
        url, params={**params, "key": env("CENSUS_API_KEY")}, timeout=180))
    q = url.replace(config.CENSUS_API + "/", "") + "?" + "&".join(f"{k}={v}" for k, v in params.items())
    if not content:
        return pd.DataFrame(), pull, q
    d = json.loads(content)
    df = pd.DataFrame(d[1:], columns=d[0])
    return df.loc[:, ~df.columns.duplicated()], pull, q


def _trade_years():
    return [(y, f"from {y}-01 to {y}-12") for y in range(config.START_YEAR, TODAY.year + 1)]


def _imports_state_year(s: dict, year: int, time_: str, cache: Cache):
    """Two requests per state-year: (A) total + every origin country, (C) HS2 chapters."""
    st = s["state_abbr"]
    out = []
    for params, key in (
            ({"get": TRADE_GET_TOTAL_COUNTRY, "STATE": st, "COMM_LVL": "-", "time": time_},
             f"statehs_{st}_{year}_A_total_country"),
            ({"get": TRADE_GET_HS2, "STATE": st, "COMM_LVL": "HS2", "CTY_CODE": "-", "time": time_},
             f"statehs_{st}_{year}_C_hs2")):
        df, pull, q = _trade_query(config.TRADE_STATE_HS, params, cache, key)
        if not df.empty:
            df = df.assign(pull_date=pull, query=q, state_fips=s["state_fips"], state_abbr=st)
        out.append(df)
    return out


def pull_imports(states: list[dict], refresh: bool = False, workers: int = 6):
    """Imports for consumption and general imports, current $, NSA. Returns (long rows for
    the panel, country detail, HS2 detail, info)."""
    cache = Cache("imports", refresh)
    jobs = [(s, y, t) for s in states for y, t in _trade_years()]
    with ThreadPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(lambda j: _imports_state_year(*j, cache), jobs))
    A = pd.concat([r[0] for r in results if not r[0].empty], ignore_index=True)
    C = pd.concat([r[1] for r in results if not r[1].empty], ignore_index=True)
    for df in (A, C):
        df["date"] = pd.to_datetime(df["time"] + "-01")
        for v in ("CON_VAL_MO", "GEN_VAL_MO"):
            df[v] = pd.to_numeric(df[v], errors="coerce")
    det = A[A["SUMMARY_LVL"] == "DET"]
    total, country = det[det["CTY_CODE"] == "-"], det[det["CTY_CODE"] != "-"]

    def row(r, var, col, cty):
        return dict(state_fips=r.state_fips, state_abbr=r.state_abbr, date=r.date, variable=var,
                    value=getattr(r, col), units="current US dollars", seasonal_adj="NSA",
                    geo_level="state", source="Census International Trade (statehs)",
                    series_id=f"{r.query} -> {col}, CTY_CODE={cty}", pull_date=r.pull_date)

    rows = []
    for r in total.itertuples(index=False):
        rows += [row(r, "imports_total_con", "CON_VAL_MO", "-"), row(r, "imports_total_gen", "GEN_VAL_MO", "-")]
    for r in country[country["CTY_CODE"].isin(config.TRADE_COUNTRIES)].itertuples(index=False):
        rows.append(row(r, f"imports_{config.TRADE_COUNTRIES[r.CTY_CODE]}_con", "CON_VAL_MO", r.CTY_CODE))

    common = {"CON_VAL_MO": "imports_con", "GEN_VAL_MO": "imports_gen", "query": "api_query"}
    country_long = country.rename(columns={"CTY_CODE": "cty_code", "CTY_NAME": "cty_name", **common})[
        ["state_fips", "state_abbr", "date", "cty_code", "cty_name", "imports_con", "imports_gen",
         "api_query", "pull_date"]].sort_values(["state_fips", "date", "cty_code"]).reset_index(drop=True)
    hs2_long = C.rename(columns={"I_COMMODITY": "hs2", "I_COMMODITY_SDESC": "hs2_desc", **common})[
        ["state_fips", "state_abbr", "date", "hs2", "hs2_desc", "imports_con", "imports_gen",
         "api_query", "pull_date"]].sort_values(["state_fips", "date", "hs2"]).reset_index(drop=True)
    for df in (country_long, hs2_long):
        df["units"], df["seasonal_adj"], df["source"] = ("current US dollars", "NSA",
                                                         "Census International Trade (statehs)")
    info = {"duty_field": False, "totals": total[["state_abbr", "date", "CON_VAL_MO", "GEN_VAL_MO"]]}
    return to_long(rows), country_long, hs2_long, info


def imports_us_totals(refresh: bool = False) -> pd.DataFrame:
    """Per month: sums over all STATE codes (51 states, XX unidentified, PR, VI) and the
    national total from imports/hs, for the sum-of-states check."""
    cache = Cache("imports", refresh)
    out = []
    for y, t in _trade_years():
        st, _, _ = _trade_query(config.TRADE_STATE_HS, {"get": "STATE,CON_VAL_MO,GEN_VAL_MO", "COMM_LVL": "-",
                                                        "CTY_CODE": "-", "time": t}, cache, f"statehs_ALL_{y}_total")
        nat, _, _ = _trade_query(config.TRADE_NATIONAL_HS, {"get": "CON_VAL_MO,GEN_VAL_MO,CAL_DUT_MO",
                                                            "COMM_LVL": "-", "CTY_CODE": "-", "time": t},
                                 cache, f"hs_US_{y}_total")
        if st.empty or nat.empty:
            continue
        for v in ("CON_VAL_MO", "GEN_VAL_MO"):
            st[v], nat[v] = pd.to_numeric(st[v]), pd.to_numeric(nat[v])
        st["group"] = st["STATE"].map(lambda x: "states51" if x in config.ABBR_TO_FIPS
                                      else "statehs_us_total" if x == "-"  # national row in statehs
                                      else "unidentified_XX" if x == "XX" else "other_" + x)
        g = st.pivot_table(index="time", columns="group", values="CON_VAL_MO", aggfunc="sum")
        g["us_total_con"] = nat.set_index("time")["CON_VAL_MO"]
        g["us_total_gen"] = nat.set_index("time")["GEN_VAL_MO"]
        out.append(g.reset_index())
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


# =============================================================================
# Outputs: wide panel, data dictionary, missing report
# =============================================================================
def build_wide(long: pd.DataFrame, states: list[dict]) -> pd.DataFrame:
    """Full state x month grid (gaps stay NaN), one column per variable."""
    grid = pd.MultiIndex.from_product(
        [[s["state_fips"] for s in states], pd.date_range(config.START_DATE, long["date"].max(), freq="MS")],
        names=["state_fips", "date"])
    wide = long.pivot_table(index=["state_fips", "date"], columns="variable", values="value",
                            aggfunc="first", dropna=False).reindex(grid)
    for var in ("cpi_state_proxy_yoy", "cpi_state_proxy_core_yoy"):
        src = long[long["variable"] == var].set_index(["state_fips", "date"])["geo_level"]
        if not src.empty:
            wide[f"{var}_source"] = src.reindex(grid)
    wide = wide[[v for v in config.VARIABLES if v in wide.columns]].reset_index()
    wide.insert(1, "state_abbr", wide["state_fips"].map(config.FIPS_TO_ABBR))
    wide.columns.name = None
    return wide


def data_dictionary(wide: pd.DataFrame) -> pd.DataFrame:
    rows = [dict(column="state_fips", description="2-digit state FIPS code", units="text"),
            dict(column="state_abbr", description="USPS postal abbreviation", units="text"),
            dict(column="date", description="First day of the reference month (YYYY-MM-01)", units="date")]
    for col in wide.columns[3:]:
        m = config.VARIABLES.get(col, {})
        has = wide.loc[wide[col].notna(), "date"]
        rows.append(dict(column=col, description=m.get("description", ""), units=m.get("units", ""),
                         source=m.get("source", ""), series_id_pattern_or_query=m.get("pattern", ""),
                         seasonal_adj=m.get("seasonal_adj", ""), geo_level=m.get("geo_level", ""),
                         first_available_date=has.min().strftime("%Y-%m-%d") if len(has) else "",
                         last_available_date=has.max().strftime("%Y-%m-%d") if len(has) else "",
                         n_states_with_data=int(wide.loc[wide[col].notna(), "state_fips"].nunique())))
    out = pd.DataFrame(rows)
    out["n_states_with_data"] = out["n_states_with_data"].astype("Int64")
    return out


def missing_report(wide: pd.DataFrame, ces_info: dict | None = None,
                   long: pd.DataFrame | None = None) -> pd.DataFrame:
    """Per variable x state: missing months between the window start (2015-01, or the
    variable's design start) and the source's latest month for that variable."""
    ces_info = ces_info or {}
    flagged = {}  # (variable, fips) -> {footnote: n} for NaNs the source itself flagged
    if long is not None:
        f = long[long["value"].isna() & (long["footnotes"] != "")]
        for (var, fips, fn), g in f.groupby(["variable", "state_fips", "footnotes"]):
            flagged.setdefault((var, fips), {})[fn] = len(g)

    def by_var(ids):
        out = {}
        for sid in ids:
            out.setdefault(f"emp_{config.CES_INDUSTRIES[sid[10:18]][0]}_sa", set()).add(sid[3:5])
        return out
    fallback, not_found = by_var(ces_info.get("nsa_fallback", [])), by_var(ces_info.get("not_found", []))

    rows = []
    for col in [c for c in wide.columns[3:] if not c.endswith("_source")]:
        has = wide.loc[wide[col].notna(), "date"]
        if has.empty:
            continue
        start = pd.Timestamp(config.VARIABLES.get(col, {}).get("start", config.START_DATE))
        end = has.max()
        win = wide[(wide["date"] >= start) & (wide["date"] <= end)]
        for fips, g in win.groupby("state_fips"):
            if col.startswith("emp_") and col.endswith("_nsa") and g[col].notna().sum() == 0:
                continue  # NSA column only applies to states without an SA series
            miss = g.loc[g[col].isna(), "date"]
            note = []
            if fips in fallback.get(col, set()):
                note.append(f"SA series not published; NSA values are in {col[:-3]}_nsa")
            if fips in not_found.get(col, set()):
                note.append("neither SA nor NSA series published by BLS")
            for fn, n in flagged.get((col, fips), {}).items():
                note.append(f"{n} month(s) flagged by source [{fn}]")
            if SHUTDOWN_MONTH in set(miss) and len(miss) < len(g):
                note.append("Oct 2025 missing (2025 federal funding lapse)")
            if col.startswith("cpi_state_proxy") and len(miss):
                note.append("12-month change needs both months; shutdown gap also blanks Oct 2026")
            if col.startswith("imports_") and col not in ("imports_total_con", "imports_total_gen") and len(miss):
                note.append("country absent from Census detail that month (likely zero imports)")
            if config.VARIABLES.get(col, {}).get("start"):
                note.append(f"window starts {start:%Y-%m} by design")
            rows.append(dict(variable=col, state_fips=fips, state_abbr=config.FIPS_TO_ABBR[fips],
                             window_start=start.strftime("%Y-%m"), source_latest_month=end.strftime("%Y-%m"),
                             n_months_in_window=len(g), n_missing=len(miss),
                             missing_months=";".join(miss.dt.strftime("%Y-%m")), note="; ".join(note)))
    return pd.DataFrame(rows)


def write_outputs(long, wide, country, hs2, dictionary, missing) -> None:
    out = config.PROCESSED_DIR
    out.mkdir(parents=True, exist_ok=True)

    def save(df, name, fmt):
        d = df.copy()
        if "date" in d:
            d["date"] = d["date"].dt.date if fmt == "parquet" else d["date"].dt.strftime("%Y-%m-%d")
        d.to_parquet(out / name, index=False) if fmt == "parquet" else d.to_csv(out / name, index=False)

    long = long.sort_values(["state_fips", "variable", "date"]).reset_index(drop=True)
    for df, stem in ((long, "state_month_long"), (wide, "state_month_wide")):
        save(df, f"{stem}.parquet", "parquet")
        save(df, f"{stem}.csv", "csv")
    if country is not None:
        save(country, "state_month_imports_country.parquet", "parquet")
    if hs2 is not None:
        save(hs2, "state_month_imports_hs2.parquet", "parquet")
    dictionary.to_csv(out / "data_dictionary.csv", index=False)
    missing.to_csv(out / "missing_report.csv", index=False)
    log.info("wrote outputs to %s", out)


# =============================================================================
# Checks (reported in data/processed/checks_report.md; nothing fails the run)
# =============================================================================
RANGES = {  # variable prefix -> (min, max); longest matching prefix wins
    "unemp_rate_sa": (0, 30), "unemployed_sa": (0, 5e6), "employed_sa": (1e4, 3e7),
    "labor_force_sa": (1e4, 3e7), "emp_": (0.0001, 2e4),
    "jolts_openings_rate": (0, 30), "jolts_layoffs_rate": (0, 30),
    "jolts_openings_level": (0.1, 5e3), "jolts_layoffs_level": (0.01, 5e3),
    "cpi_div": (80, 250), "cpi_region": (150, 450), "cpi_us": (150, 450), "cpi_state_proxy": (-15, 25),
    "bfs_dur": (0, 8), "bfs_": (0, 5e5), "permits_units": (0, 5e4), "permits_value": (0, 2e7),
    "imports_": (0, 1e12),
}
FRED_CANDIDATES = [("unemp_rate_sa", "{st}UR"), ("emp_total_nonfarm_sa", "{st}NA"),
                   ("emp_mfg_sa", "{st}MFG"), ("labor_force_sa", "{st}LF")]


class Report:
    def __init__(self):
        self.lines: list[str] = []

    def h(self, t):
        self.lines += ["", f"## {t}", ""]
        log.info("CHECK: %s", t)

    def p(self, t=""):
        self.lines.append(t)
        if t:
            log.info("  %s", t)

    def table(self, df, max_rows=40):
        if df is None or df.empty:
            self.p("(none)")
            return
        self.lines += ["```", df.head(max_rows).to_string(index=False), "```"]
        if len(df) > max_rows:
            self.lines.append(f"... {len(df) - max_rows} more rows")

    def write(self, path):
        path.write_text("# Checks report\n" + "\n".join(self.lines) + "\n", encoding="utf-8")


def _ym(df, col="date"):
    df = df.copy()
    df[col] = df[col].dt.strftime("%Y-%m")
    return df


def check_coverage(rep, wide, missing):
    rep.h("Coverage: first/last month per variable")
    rows = []
    for col in [c for c in wide.columns[3:] if not c.endswith("_source")]:
        has = wide.loc[wide[col].notna()]
        if has.empty:
            rows.append(dict(variable=col, first="", latest="", states=0))
            continue
        firsts = has.groupby("state_fips")["date"].min()
        start = pd.Timestamp(config.VARIABLES.get(col, {}).get("start", config.START_DATE))
        rows.append(dict(variable=col, first=has["date"].min().strftime("%Y-%m"),
                         latest=has["date"].max().strftime("%Y-%m"), states=has["state_fips"].nunique(),
                         states_starting_late=",".join(config.FIPS_TO_ABBR[f] for f in firsts[firsts > start].index)))
    rep.table(pd.DataFrame(rows), 200)
    rep.p(f"State x variable cells with gaps inside their window: {int((missing['n_missing'] > 0).sum())} "
          "(details in missing_report.csv)")


def check_duplicates(rep, long):
    rep.h("Duplicates (state x month x variable)")
    d = long.duplicated(["state_fips", "date", "variable"], keep=False)
    rep.p(f"duplicate rows: {int(d.sum())}")
    if d.any():
        rep.table(long[d].head(20))


def check_ranges(rep, long):
    rep.h("Plausible ranges")
    bad = []
    for var, g in long.dropna(subset=["value"]).groupby("variable"):
        key = next((k for k in sorted(RANGES, key=len, reverse=True) if var.startswith(k)), None)
        if key is None:
            continue
        lo, hi = RANGES[key]
        out = g[(g["value"] < lo) | (g["value"] > hi)]
        if len(out):
            bad.append(dict(variable=var, range=f"[{lo}, {hi}]", n_out=len(out),
                            examples="; ".join(f"{r.state_abbr} {r.date:%Y-%m}={r.value:g}"
                                               for r in out.head(5).itertuples())))
    rep.p(f"variables with out-of-range values: {len(bad)}")
    rep.table(pd.DataFrame(bad))


def check_ces_vs_national(rep, wide, national):
    rep.h("Sum of state total nonfarm (SA) vs national CES0000000001")
    if national is None or national.empty or "emp_total_nonfarm_sa" not in wide:
        rep.p("skipped (no data)")
        return
    st = wide.groupby("date").agg(states_sum=("emp_total_nonfarm_sa", "sum"),
                                  n_states=("emp_total_nonfarm_sa", "count"))
    m = st.join(national.set_index("date")["value"].rename("national"), how="inner")
    m = m[m["n_states"] == 51]
    m["gap"] = m["states_sum"] - m["national"]
    m["gap_pct"] = 100 * m["gap"] / m["national"]
    rep.p(f"months compared: {len(m)}; mean gap {m['gap'].mean():.1f}k ({m['gap_pct'].mean():.2f}%), "
          f"min {m['gap_pct'].min():.2f}%, max {m['gap_pct'].max():.2f}%")
    rep.p("(State and national CES are estimated/benchmarked separately; small gaps are expected.)")
    rep.table(_ym(m.reset_index().tail(12)).round(2))


def check_imports(rep, info, country, hs2, us):
    rep.h("Imports: sum of 50 states + DC vs U.S. total (imports for consumption)")
    if us is not None and not us.empty:
        u = us.copy()
        other = [c for c in u.columns if c.startswith(("unidentified", "other_"))]
        u["gap_pct_51"] = 100 * (u["us_total_con"] - u["states51"]) / u["us_total_con"]
        u["gap_pct_all_codes"] = 100 * (u["us_total_con"] - u[["states51"] + other].sum(axis=1)) / u["us_total_con"]
        rep.p(f"non-state STATE codes in the dataset: {', '.join(c.replace('other_', '') for c in other)}")
        rep.p(f"share of U.S. imports not assigned to the 51: mean {u['gap_pct_51'].mean():.2f}%, "
              f"range {u['gap_pct_51'].min():.2f}% to {u['gap_pct_51'].max():.2f}%")
        rep.p(f"after adding XX/PR/VI etc.: mean gap {u['gap_pct_all_codes'].mean():.3f}%")
        if "statehs_us_total" in u:
            rep.p(f"statehs national row (STATE='-') vs imports/hs total: max abs diff "
                  f"${(u['statehs_us_total'] - u['us_total_con']).abs().max():,.0f}")
        rep.table(u[["time", "us_total_con", "states51", "gap_pct_51", "gap_pct_all_codes"]].tail(12).round(3))
    tot = info["totals"].set_index(["state_abbr", "date"])
    for name, df in (("HS2 chapters", hs2), ("origin countries", country)):
        rep.h(f"Imports: {name} add up to each state's total")
        m = tot.join(df.groupby(["state_abbr", "date"])[["imports_con", "imports_gen"]].sum(), how="outer")
        m["gap_imports_con"] = m["CON_VAL_MO"] - m["imports_con"]
        m["gap_imports_gen"] = m["GEN_VAL_MO"] - m["imports_gen"]
        bad = m[(m["gap_imports_con"].abs() > 1) | (m["gap_imports_gen"].abs() > 1) |
                m["CON_VAL_MO"].isna() | m["imports_con"].isna()]
        rep.p(f"state-months compared: {len(m)}; with |gap| > $1 or a missing side: {len(bad)}; "
              f"max abs gap (con) ${m['gap_imports_con'].abs().max():,.0f}")
        if len(bad):
            rep.table(_ym(bad.reset_index().head(20)))


def check_permits(rep, info, state="IN"):
    rep.h("Building permits: text vs Excel formats")
    t, x = info["txt_months"], info["excel_months"]
    rep.p(f"text months: {len(t)} ({min(t, default='')}-{max(t, default='')}); "
          f"Excel months: {len(x)} ({min(x, default='')}-{max(x, default='')})")
    rep.p(f"months only in Excel (used Excel): {info['excel_only_months'] or 'none'}")
    rep.p(f"months in neither format: {info['missing_months'] or 'none'}")
    rep.p(f"cells where text and Excel disagree (overlap months): {len(info['mismatches'])}")
    for m in info["mismatches"][:15]:
        rep.p(f"  {m}")
    cmp_ = info.get("excel_compare")
    if cmp_ is not None and not cmp_.empty:
        first = cmp_["date"].min()
        win = cmp_[(cmp_["state_abbr"] == state) & (cmp_["date"] <= first + pd.DateOffset(months=2))]
        rep.p(f"{state} around the first Excel month (text vs _x = Excel):")
        rep.table(_ym(win[["date", "units_tot", "units_tot_x", "units_1u", "units_1u_x", "units_5u", "units_5u_x"]]))
    rep.h("Crosswalk: region/division in config vs Census BPS files")
    rd = info.get("txt_region_division")
    if rd is None or rd.empty:
        rep.p("skipped")
        return
    cfg = {s["state_abbr"]: (str(s["region"]), str(s["division"])) for s in config.STATES}
    bad = [r for r in rd.itertuples() if cfg[r.state_abbr] != (str(r.region), str(r.division))]
    rep.p(f"states whose BPS region/division differ from config: {len(bad)}")
    for r in bad[:10]:
        rep.p(f"  {r.state_abbr}: BPS {r.region}/{r.division} vs config {cfg[r.state_abbr]}")


def check_bfs(rep, csv_us, api_us):
    rep.h("BFS: CSV vs API (U.S. BA_BA, SA)")
    if api_us is None or api_us.empty:
        rep.p("skipped")
        return
    a = api_us[(api_us["data_type_code"] == "BA_BA") & (api_us["seasonally_adj"] == "yes")]
    a = a.assign(date=pd.to_datetime(a["time"] + "-01"), api=pd.to_numeric(a["cell_value"]))
    m = a.set_index("date")[["api"]].join(csv_us.set_index("date")["value"].rename("csv"), how="inner")
    rep.p(f"months compared: {len(m)}; differing: {int(((m['api'] - m['csv']).abs() > 0.5).sum())}")


def check_fred(rep, wide, seed=None):
    rep.h("FRED cross-check (3 random state series)")
    key = env("FRED_API_KEY")
    if not key:
        rep.p("skipped: FRED_API_KEY not set")
        return
    rng = random.Random(seed)
    for s, var, pat in [(rng.choice(config.STATES), *rng.choice(FRED_CANDIDATES)) for _ in range(3)]:
        fid = pat.format(st=s["state_abbr"])
        try:
            r = SESSION.get(config.FRED_API, timeout=60, headers={"User-Agent": user_agent(False)},
                            params={"series_id": fid, "api_key": key, "file_type": "json",
                                    "observation_start": config.START_DATE})
            obs = r.json().get("observations", [])
        except Exception as e:
            rep.p(f"{fid}: request failed ({redact(str(e))})")
            continue
        if not obs:
            rep.p(f"{fid}: not found on FRED (HTTP {r.status_code})")
            continue
        f = pd.DataFrame(obs)
        f["date"] = pd.to_datetime(f["date"])
        f["fred"] = pd.to_numeric(f["value"], errors="coerce")
        ours = wide[wide["state_fips"] == s["state_fips"]].set_index("date")[var]
        # FRED {st}LF is in persons, like ours (checked 2026-11-24); no rescaling needed.
        # fred_compare.py runs the full comparison over every series FRED carries.
        m =f.set_index("date")[["fred"]].join(ours.rename("ours"), how="inner").dropna()
        mism = m[(m["fred"] - m["ours"]).abs() > (0.051 if var == "unemp_rate_sa" else 0.6)]
        rep.p(f"{fid} vs {var} ({s['state_abbr']}): {len(m)} months compared, {len(mism)} mismatches")
        for d, r_ in mism.head(5).iterrows():
            rep.p(f"    {d:%Y-%m}: FRED {r_['fred']} vs ours {r_['ours']}")


# =============================================================================
# Main
# =============================================================================
PULLERS = {"laus": pull_laus, "ces": pull_ces, "jolts": pull_jolts, "cpi": pull_cpi,
           "bfs": pull_bfs, "permits": pull_permits, "imports": pull_imports}
BLS_SOURCES = ["laus", "ces", "jolts", "cpi"]


def required_env(sources: list[str], verify: bool = True) -> list[str]:
    req = []
    if any(s in BLS_SOURCES for s in sources):
        req.append("BLS_API_KEY")
        if verify:
            req.append("CONTACT_EMAIL")  # download.bls.gov series files
    if any(s not in BLS_SOURCES for s in sources):
        req.append("CENSUS_API_KEY")
    if "permits" in sources:
        req.append("CONTACT_EMAIL")  # www2.census.gov
    return list(dict.fromkeys(req))


def pull_all(sources: list[str], states: list[dict], refresh: bool = False):
    """Run each selected pull_* function. Returns (long frames, info, country, hs2)."""
    results, info, country, hs2 = {}, {}, None, None
    for name in sources:
        log.info("== %s", name)
        if name == "imports":
            results[name], country, hs2, info[name] = pull_imports(states, refresh)
        else:
            results[name], info[name] = PULLERS[name](states, refresh)
    return results, info, country, hs2


def print_pilot(results, info, country, hs2):
    pd.set_option("display.width", 200, "display.max_columns", 20, "display.max_colwidth", 60)
    for name, df in results.items():
        print(f"\n===== {name}: {len(df)} rows, variables: {sorted(df['variable'].unique())}")
        if df.empty:
            print("  NO DATA")
            continue
        last = df.sort_values("date").groupby("variable").tail(1)
        print(last[["state_abbr", "date", "variable", "value", "units", "seasonal_adj",
                    "series_id", "preliminary"]].to_string(index=False))
    for label, df, cols in (("imports by country", country, ["cty_code", "cty_name"]),
                            ("imports by HS2", hs2, ["hs2", "hs2_desc"])):
        if df is not None:
            print(f"\n===== {label} (latest month, top 5):")
            top = df[df["date"] == df["date"].max()].nlargest(5, "imports_con")
            print(top[["state_abbr", "date", *cols, "imports_con", "imports_gen"]].to_string(index=False))
    for k, v in info.items():
        print(f"\n[{k}] info:", {kk: (f"<{len(vv)} rows>" if isinstance(vv, pd.DataFrame) else vv)
                                 for kk, vv in v.items() if kk != "totals"})


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--refresh", action="store_true", help="ignore cached raw responses and re-download")
    ap.add_argument("--pilot", nargs="?", const="IN", metavar="ST",
                    help="pull one state (default IN) from every source and print sample rows")
    ap.add_argument("--sources", nargs="+", choices=list(PULLERS), default=list(PULLERS))
    ap.add_argument("--env-file", default=None, help="KEY=VALUE file (default: $ENV_FILE, then ./.env)")
    ap.add_argument("--skip-verify", action="store_true", help="skip BLS series-file verification")
    ap.add_argument("--fred-seed", type=int, default=None)
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    load_dotenv(a.env_file)
    require_env(required_env(a.sources, verify=not a.skip_verify))

    verify = {}
    if not a.skip_verify and any(s in BLS_SOURCES for s in a.sources):
        log.info("Verifying CES industry codes against sm.industry ...")
        verify = verify_bls_patterns(a.refresh)

    if a.pilot:
        states = [s for s in config.STATES if s["state_abbr"] == a.pilot.upper()]
        if not states:
            raise SystemExit(f"unknown state {a.pilot}")
        print_pilot(*pull_all(a.sources, states, a.refresh))
        return

    states = config.STATES
    results, info, country, hs2 = pull_all(a.sources, states, a.refresh)
    long = pd.concat([df for df in results.values() if not df.empty], ignore_index=True)
    wide = build_wide(long, states)
    dictionary = data_dictionary(wide)
    missing = missing_report(wide, info.get("ces"), long)
    write_outputs(long, wide, country, hs2, dictionary, missing)

    rep = Report()
    check_coverage(rep, wide, missing)
    check_duplicates(rep, long)
    check_ranges(rep, long)
    if "ces" in results:
        check_ces_vs_national(rep, wide, ces_national_total_nonfarm(a.refresh))
    if "imports" in results:
        check_imports(rep, info["imports"], country, hs2, imports_us_totals(a.refresh))
    if "permits" in results:
        check_permits(rep, info["permits"])
    if "bfs" in results:
        check_bfs(rep, bfs_us_csv(), bfs_us_api(a.refresh))
    check_fred(rep, wide, a.fred_seed)
    rep.write(config.PROCESSED_DIR / "checks_report.md")

    g = long.dropna(subset=["value"]).groupby("source")["date"].max()
    summary = {
        "latest_month_by_source": {k: v.strftime("%Y-%m") for k, v in g.items()},
        "latest_month_by_variable": dictionary.set_index("column")["last_available_date"].iloc[3:].to_dict(),
        "ces_nsa_fallback": info.get("ces", {}).get("nsa_fallback", []),
        "ces_not_found": info.get("ces", {}).get("not_found", []),
        "laus_no_data": info.get("laus", {}).get("no_data", []),
        "jolts_no_data": info.get("jolts", {}).get("no_data", []),
        "cpi_no_data": info.get("cpi", {}).get("no_data", []),
        "bfs_method": info.get("bfs", {}).get("method"),
        "bfs_non_numeric_tokens": info.get("bfs", {}).get("non_numeric_tokens"),
        "permits_excel_only_months": info.get("permits", {}).get("excel_only_months"),
        "permits_missing_months": info.get("permits", {}).get("missing_months"),
        "permits_txt_excel_mismatches": len(info.get("permits", {}).get("mismatches", [])),
        "imports_duty_field_in_state_dataset": info.get("imports", {}).get("duty_field"),
        "verification": verify,
        "rows_long": len(long), "rows_wide": len(wide),
    }
    (config.PROCESSED_DIR / "run_summary.json").write_text(json.dumps(summary, indent=1, default=str))
    log.info("done. latest month by source: %s", summary["latest_month_by_source"])


if __name__ == "__main__":
    sys.exit(main())
