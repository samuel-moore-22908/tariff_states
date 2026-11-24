"""Pipeline configuration: period, state crosswalk, series lists and variable metadata.

Edit this file to add/remove series. Every wide-panel column must have an entry in
VARIABLES (it drives the data dictionary and the missing-data report).
"""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parent
RAW_DIR = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"

START_YEAR = 2015
START_DATE = "2015-01-01"

# ---------------------------------------------------------------------------
# State crosswalk: FIPS, postal code, name, Census division (1-9), region (1-4)
# Standard Census Bureau regions/divisions.
#   Regions:   1 Northeast, 2 Midwest, 3 South, 4 West
#   Divisions: 1 New England, 2 Middle Atlantic, 3 East North Central,
#              4 West North Central, 5 South Atlantic, 6 East South Central,
#              7 West South Central, 8 Mountain, 9 Pacific
# ---------------------------------------------------------------------------
_STATES = """
01 AL Alabama 6
02 AK Alaska 9
04 AZ Arizona 8
05 AR Arkansas 7
06 CA California 9
08 CO Colorado 8
09 CT Connecticut 1
10 DE Delaware 5
11 DC District_of_Columbia 5
12 FL Florida 5
13 GA Georgia 5
15 HI Hawaii 9
16 ID Idaho 8
17 IL Illinois 3
18 IN Indiana 3
19 IA Iowa 4
20 KS Kansas 4
21 KY Kentucky 6
22 LA Louisiana 7
23 ME Maine 1
24 MD Maryland 5
25 MA Massachusetts 1
26 MI Michigan 3
27 MN Minnesota 4
28 MS Mississippi 6
29 MO Missouri 4
30 MT Montana 8
31 NE Nebraska 4
32 NV Nevada 8
33 NH New_Hampshire 1
34 NJ New_Jersey 2
35 NM New_Mexico 8
36 NY New_York 2
37 NC North_Carolina 5
38 ND North_Dakota 4
39 OH Ohio 3
40 OK Oklahoma 7
41 OR Oregon 9
42 PA Pennsylvania 2
44 RI Rhode_Island 1
45 SC South_Carolina 5
46 SD South_Dakota 4
47 TN Tennessee 6
48 TX Texas 7
49 UT Utah 8
50 VT Vermont 1
51 VA Virginia 5
53 WA Washington 9
54 WV West_Virginia 5
55 WI Wisconsin 3
56 WY Wyoming 8
"""

DIVISION_TO_REGION = {1: 1, 2: 1, 3: 2, 4: 2, 5: 3, 6: 3, 7: 3, 8: 4, 9: 4}
DIVISION_NAMES = {
    1: "New England", 2: "Middle Atlantic", 3: "East North Central",
    4: "West North Central", 5: "South Atlantic", 6: "East South Central",
    7: "West South Central", 8: "Mountain", 9: "Pacific",
}
REGION_NAMES = {1: "Northeast", 2: "Midwest", 3: "South", 4: "West"}

STATES = []
for _line in _STATES.strip().splitlines():
    _fips, _abbr, _name, _div = _line.split()
    _div = int(_div)
    _reg = DIVISION_TO_REGION[_div]
    STATES.append({
        "state_fips": _fips,
        "state_abbr": _abbr,
        "state_name": _name.replace("_", " "),
        "division": _div,
        "division_name": DIVISION_NAMES[_div],
        "region": _reg,
        "region_name": REGION_NAMES[_reg],
        # BLS CPI area codes: division = 0{region}{division}0, region = 0{region}00
        "cpi_division_area": f"0{_reg}{_div}0",
        "cpi_region_area": f"0{_reg}00",
    })
assert len(STATES) == 51

FIPS_TO_ABBR = {s["state_fips"]: s["state_abbr"] for s in STATES}
ABBR_TO_FIPS = {s["state_abbr"]: s["state_fips"] for s in STATES}
NAME_TO_ABBR = {s["state_name"].lower(): s["state_abbr"] for s in STATES}

# ---------------------------------------------------------------------------
# BLS API
# ---------------------------------------------------------------------------
BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_MAX_SERIES_PER_QUERY = 50
BLS_MAX_YEARS_PER_QUERY = 20
# Registered v2 users: 500 queries/day, 50 series & 20 years per query,
# 50 requests per 10 seconds (https://www.bls.gov/developers/api_faqs.htm,
# checked 2026-11-23). We stop a little below the daily limit.
BLS_DAILY_QUERY_LIMIT = 500
BLS_DAILY_QUERY_SAFETY_MARGIN = 20
BLS_MIN_SECONDS_BETWEEN_REQUESTS = 0.25
BLS_FILES_BASE = "https://download.bls.gov/pub/time.series"

# LAUS: LASST{FIPS}00000000000{measure}
LAUS_MEASURES = {
    "03": ("unemp_rate_sa", "percent", "Unemployment rate"),
    "04": ("unemployed_sa", "persons", "Number unemployed"),
    "05": ("employed_sa", "persons", "Employment (household survey)"),
    "06": ("labor_force_sa", "persons", "Civilian labor force"),
}

# CES State and Area: SMS{FIPS}00000{industry}01 (SA), SMU... (NSA)
CES_INDUSTRIES = {
    "00000000": ("total_nonfarm", "Total nonfarm"),
    "05000000": ("total_private", "Total private"),
    "20000000": ("construction", "Construction"),
    "30000000": ("mfg", "Manufacturing"),
    "31000000": ("mfg_durable", "Durable goods manufacturing"),
    "32000000": ("mfg_nondurable", "Nondurable goods manufacturing"),
    "41000000": ("wholesale", "Wholesale trade"),
    "42000000": ("retail", "Retail trade"),
    # sm.industry: 43000000 = "Transportation, Warehousing, and Utilities";
    # transportation and warehousing alone is 43400089 (checked 2026-11-23).
    "43000000": ("twu", "Transportation, warehousing, and utilities"),
    "43400089": ("transport_wh", "Transportation and warehousing"),
}
CES_NATIONAL_TOTAL_NONFARM = "CES0000000001"

# JOLTS state: JTS000000{FIPS}0000000{element}{L|R}
JOLTS_ELEMENTS = {
    ("JO", "L"): ("jolts_openings_level_sa", "thousands", "Job openings, level"),
    ("JO", "R"): ("jolts_openings_rate_sa", "percent", "Job openings rate"),
    ("LD", "L"): ("jolts_layoffs_level_sa", "thousands", "Layoffs and discharges, level"),
    ("LD", "R"): ("jolts_layoffs_rate_sa", "percent", "Layoffs and discharges rate"),
}

# CPI-U
CPI_ITEMS = {"SA0": "all", "SA0L1E": "core"}
CPI_DIVISION_AREAS = ["0110", "0120", "0230", "0240", "0350", "0360", "0370", "0480", "0490"]
CPI_REGION_AREAS = ["0100", "0200", "0300", "0400"]
CPI_US_SERIES = {"CUUR0000SA0": "cpi_us_all_nsa", "CUSR0000SA0": "cpi_us_all_sa"}

# ---------------------------------------------------------------------------
# Census
# ---------------------------------------------------------------------------
CENSUS_API = "https://api.census.gov/data/timeseries"
BFS_API = f"{CENSUS_API}/eits/bfs"
BFS_CSV_URL = "https://www.census.gov/econ/bfs/csv/bfs_monthly.csv"
BFS_SERIES = {
    # code: (short name, units, description, SA versions published)
    "BA_BA": ("ba", "count", "Business applications"),
    "BA_HBA": ("hba", "count", "High-propensity business applications"),
    "BA_WBA": ("wba", "count", "Business applications with planned wages"),
    "BA_CBA": ("cba", "count", "Business applications from corporations"),
    "BF_BF4Q": ("bf4q", "count", "Business formations within 4 quarters of application"),
    "BF_BF8Q": ("bf8q", "count", "Business formations within 8 quarters of application"),
    "BF_PBF4Q": ("pbf4q", "count", "Projected business formations within 4 quarters"),
    "BF_PBF8Q": ("pbf8q", "count", "Projected business formations within 8 quarters"),
    "BF_SBF4Q": ("sbf4q", "count", "Spliced business formations within 4 quarters"),
    "BF_SBF8Q": ("sbf8q", "count", "Spliced business formations within 8 quarters"),
    "BF_DUR4Q": ("dur4q", "quarters", "Average duration from application to formation, within 4 quarters"),
    "BF_DUR8Q": ("dur8q", "quarters", "Average duration from application to formation, within 8 quarters"),
}

BPS_STATE_DIR = "https://www2.census.gov/econ/bps/State/"
BPS_EXCEL_INDEX = "https://www.census.gov/construction/bps/statemonthly.html"
BPS_EXCEL_BASE = "https://www.census.gov/construction/bps/xls/"
BPS_TYPES = {"1u": "1unit", "2u": "2units", "34u": "3_4units", "5u": "5plus", "tot": "total"}

TRADE_STATE_HS = f"{CENSUS_API}/intltrade/imports/statehs"
TRADE_NATIONAL_HS = f"{CENSUS_API}/intltrade/imports/hs"
TRADE_COUNTRIES = {"5700": "china", "1220": "canada", "2010": "mexico"}

FRED_API = "https://api.stlouisfed.org/fred/series/observations"

LONG_COLUMNS = [
    "state_fips", "state_abbr", "date", "variable", "value", "units", "seasonal_adj",
    "geo_level", "source", "series_id", "preliminary", "pull_date", "footnotes",
]


# ---------------------------------------------------------------------------
# Variable metadata for the data dictionary / missing report.
# Built programmatically so it stays in sync with the series lists above.
# start: first month the variable can exist in the 2015+ window (by design).
# ---------------------------------------------------------------------------
def _build_variables():
    v = {}
    for m, (name, units, desc) in LAUS_MEASURES.items():
        v[name] = dict(description=f"{desc}, seasonally adjusted", units=units,
                       source="BLS LAUS (API v2)", pattern=f"LASST{{FIPS}}00000000000{m}",
                       seasonal_adj="SA", geo_level="state")
    for code, (short, desc) in CES_INDUSTRIES.items():
        v[f"emp_{short}_sa"] = dict(
            description=f"All employees, {desc}, seasonally adjusted (CES State and Area)",
            units="thousands of jobs", source="BLS CES State and Area (API v2)",
            pattern=f"SMS{{FIPS}}00000{code}01", seasonal_adj="SA", geo_level="state")
        v[f"emp_{short}_nsa"] = dict(
            description=(f"All employees, {desc}, NOT seasonally adjusted. Populated only for "
                         "states where BLS does not publish the SA series (NSA fallback)."),
            units="thousands of jobs", source="BLS CES State and Area (API v2)",
            pattern=f"SMU{{FIPS}}00000{code}01", seasonal_adj="NSA", geo_level="state")
    for (el, lr), (name, units, desc) in JOLTS_ELEMENTS.items():
        v[name] = dict(description=f"{desc}, total nonfarm, seasonally adjusted (state JOLTS)",
                       units=units, source="BLS JOLTS state estimates (API v2)",
                       pattern=f"JTS000000{{FIPS}}0000000{el}{lr}", seasonal_adj="SA",
                       geo_level="state")
    for item, lab in CPI_ITEMS.items():
        what = "all items" if lab == "all" else "all items less food and energy"
        v[f"cpi_div_{lab}_nsa"] = dict(
            description=(f"CPI-U {what}, the state's Census division index (Dec 2017=100). "
                         "Division indexes start Dec 2017; NaN before."),
            units="index, Dec 2017=100", source="BLS CPI (API v2)",
            pattern=f"CUUR{{division area}}{item}", seasonal_adj="NSA", geo_level="division",
            start="2018-01-01")
        v[f"cpi_region_{lab}_nsa"] = dict(
            description=f"CPI-U {what}, the state's Census region index (1982-84=100)",
            units="index, 1982-84=100", source="BLS CPI (API v2)",
            pattern=f"CUUR{{region area}}{item}", seasonal_adj="NSA", geo_level="region")
    v["cpi_us_all_nsa"] = dict(description="CPI-U all items, U.S. city average (reference)",
                               units="index, 1982-84=100", source="BLS CPI (API v2)",
                               pattern="CUUR0000SA0", seasonal_adj="NSA", geo_level="national")
    v["cpi_us_all_sa"] = dict(description="CPI-U all items, U.S. city average, SA (reference)",
                              units="index, 1982-84=100", source="BLS CPI (API v2)",
                              pattern="CUSR0000SA0", seasonal_adj="SA", geo_level="national")
    for lab, what in (("", "all items"), ("_core", "all items less food and energy")):
        v[f"cpi_state_proxy{lab}_yoy"] = dict(
            description=(f"State CPI proxy, {what}: 12-month % change of the state's Census "
                         "division index where that change exists (from Dec 2018/Jan 2019 on), "
                         "otherwise of its Census region index. Index levels are never spliced. "
                         f"Which one was used is in cpi_state_proxy{lab}_yoy_source."),
            units="percent change from 12 months earlier", source="BLS CPI (API v2), derived",
            pattern="CUUR{division area}" + ("SA0L1E" if lab else "SA0") + " else CUUR{region area}"
                    + ("SA0L1E" if lab else "SA0"),
            seasonal_adj="NSA", geo_level="division or region")
        v[f"cpi_state_proxy{lab}_yoy_source"] = dict(
            description=f"'division' or 'region': which index cpi_state_proxy{lab}_yoy came from",
            units="text", source="derived", pattern="", seasonal_adj="NSA",
            geo_level="division or region")
    for code, (short, units, desc) in BFS_SERIES.items():
        for sa in ("sa", "nsa"):
            if code.startswith("BF_DUR") and sa == "sa":
                continue  # durations are published NSA only
            v[f"bfs_{short}_{sa}"] = dict(
                description=f"{desc}, {'seasonally adjusted' if sa == 'sa' else 'not seasonally adjusted'}, all industries",
                units=units, source="Census Business Formation Statistics",
                pattern=(f"bfs_monthly.csv / eits/bfs: data_type_code={code}, category_code=TOTAL, "
                         f"seasonally_adj={'yes' if sa == 'sa' else 'no'}, geo_level_code={{ST}}"),
                seasonal_adj=sa.upper(), geo_level="state")
    for t, lab in BPS_TYPES.items():
        struct = {"1unit": "1-unit", "2units": "2-unit", "3_4units": "3-4 unit",
                  "5plus": "5+ unit", "total": "all"}[lab]
        v[f"permits_units_{lab}"] = dict(
            description=f"New privately owned housing units authorized, {struct} structures (current month, not YTD)",
            units="housing units", source="Census Building Permits Survey",
            pattern="www2.census.gov/econ/bps/State/st{YYMM}c.txt (Excel statemonthly_{YYYYMM}.xls as cross-check/fallback)",
            seasonal_adj="NSA", geo_level="state")
        v[f"permits_value_{lab}"] = dict(
            description=f"Valuation of new privately owned housing units authorized, {struct} structures",
            units="thousands of dollars", source="Census Building Permits Survey",
            pattern="www2.census.gov/econ/bps/State/st{YYMM}c.txt (Excel statemonthly_{YYYYMM}.xls as cross-check/fallback)",
            seasonal_adj="NSA", geo_level="state")
    note = (" 'State of destination' is the state where goods are first delivered (often a "
            "distribution center), not necessarily where they are used or consumed.")
    v["imports_total_con"] = dict(description="Imports for consumption, all commodities, all countries." + note,
                                  units="current US dollars", source="Census International Trade API",
                                  pattern="intltrade/imports/statehs: CON_VAL_MO, STATE={ST}, COMM_LVL=-, CTY_CODE=-",
                                  seasonal_adj="NSA", geo_level="state")
    v["imports_total_gen"] = dict(description="General imports, all commodities, all countries." + note,
                                  units="current US dollars", source="Census International Trade API",
                                  pattern="intltrade/imports/statehs: GEN_VAL_MO, STATE={ST}, COMM_LVL=-, CTY_CODE=-",
                                  seasonal_adj="NSA", geo_level="state")
    for code, name in TRADE_COUNTRIES.items():
        v[f"imports_{name}_con"] = dict(
            description=f"Imports for consumption from {name.title()} (country code {code}), all commodities." + note,
            units="current US dollars", source="Census International Trade API",
            pattern=f"intltrade/imports/statehs: CON_VAL_MO, STATE={{ST}}, COMM_LVL=-, CTY_CODE={code}",
            seasonal_adj="NSA", geo_level="state")
    return v


VARIABLES = _build_variables()
