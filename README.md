# tariff_states

A re-runnable pipeline that builds a tidy **state × month panel** (50 states + DC, January 2015 through each source's latest month) from free public data:

| Source | What | SA? |
|---|---|---|
| BLS LAUS | unemployment rate, unemployed, employed, labor force | SA |
| BLS CES State & Area | payroll jobs: total nonfarm, private, construction, manufacturing (durable, nondurable), wholesale, retail, transportation/warehousing/utilities (43000000), transportation & warehousing (43400089) | SA (NSA fallback where SA isn't published; 43400089 is NSA-only) |
| BLS JOLTS (state) | job openings, layoffs & discharges: levels and rates | SA |
| BLS CPI-U | Census division and region indexes, U.S. city average, and a state proxy 12-month inflation rate | NSA (US SA for reference) |
| Census BFS | business applications (BA, HBA, WBA, CBA) and formations (BF) | SA and NSA |
| Census BPS | housing units authorized and their valuation, by structure type | NSA |
| Census International Trade | imports by state of destination: total, by origin country, by HS2 chapter | NSA |

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate            # Windows  (macOS/Linux: source .venv/bin/activate)
pip install -r requirements.txt
```

### Keys and contact email

The pipeline reads these from environment variables, or from an env file of `KEY=VALUE` lines. It uses `--env-file PATH` if given, otherwise the file named in `$ENV_FILE`, otherwise `.env` in the repo root (gitignored). Variables already set in the environment take precedence over the file:

```
BLS_API_KEY=...        # required: https://data.bls.gov/registrationEngine/
CENSUS_API_KEY=...     # required: https://api.census.gov/data/key_signup.html
CONTACT_EMAIL=...      # required: sent in the User-Agent to download.bls.gov and www2.census.gov
FRED_API_KEY=...       # optional: enables the FRED cross-check
```

Keys are never printed or written to the cache or the outputs. If a required variable is missing, the run stops and names it.

## Run

```bash
python pull_data.py --pilot          # Indiana only, every source; prints sample rows (do this first)
python pull_data.py                  # full run; reuses cached raw responses
python pull_data.py --refresh        # re-download everything (use this to update)
python pull_data.py --sources laus cpi   # only some sources
```

### FRED comparison

```bash
python fred_compare.py                   # every variable FRED also publishes, all 51 states
python fred_compare.py --states IN CA --vars unemp_rate_sa bfs_ba_sa
python fred_compare.py --refresh         # re-download the FRED series
```

Needs `FRED_API_KEY` and an existing `data/processed/state_month_long.parquet`. It writes `fred_comparison.csv` (one row per state × variable), `fred_mismatches.csv` (every month that differs) and `fred_comparison_report.md` to `data/processed/`. A month counts as a mismatch when the two values differ by more than FRED's own rounding. FRED has LAUS, CES, CPI regions and U.S., BFS, total and 1-unit permits, and general imports (`IMPTOT{st}`). It does not have state JOLTS, CPI divisions, permit valuations or other structure types, imports for consumption, or imports by country, and the report lists these as not compared. FRED responses are cached under `data/raw/fred/`. An uncached run makes about 2,000 requests and takes 20–30 minutes because of FRED's 120-requests-a-minute limit.

### Updating

Run `python pull_data.py --refresh`. Every raw response is cached under `data/raw/{source}/{name}__{YYYYMMDD}.{ext}`, and the date in the filename is the pull date carried into the outputs. A plain run reuses the newest cached file for each request. `--refresh` downloads fresh copies (including the directory listings that tell the permits module which months exist) and keeps the old files as history.

BLS allows 500 queries a day for registered keys, with up to 50 series and 20 years per query. A full run uses about 30. Queries are counted in `data/raw/bls/_query_log.json`, and the run stops before reaching the limit.

## Outputs (`data/processed/`)

| File | Contents |
|---|---|
| `state_month_long.parquet` / `.csv` | one row per state × month × variable: `state_fips, state_abbr, date, variable, value, units, seasonal_adj, geo_level, source, series_id, preliminary, pull_date, footnotes` |
| `state_month_wide.parquet` / `.csv` | one row per state × month (full grid; gaps are NaN), one column per variable |
| `state_month_imports_country.parquet` | state × month × origin country: imports for consumption and general imports |
| `state_month_imports_hs2.parquet` | state × month × HS2 chapter: imports for consumption and general imports |
| `data_dictionary.csv` | one row per wide column: description, units, source, series ID pattern or API query, SA/NSA, geography level, first and last available month |
| `missing_report.csv` | per variable × state: months missing inside the variable's window (2015-01, or its design start, through the source's latest month) |
| `checks_report.md` | coverage, duplicates, ranges, state sums vs national totals, permit format comparison, FRED cross-check |
| `run_summary.json` | latest month by source and variable, NSA fallbacks, series not found |

Dates are the first of the month. Missing months are never interpolated or filled.

## Notes on the data

* **CES NSA fallback.** BLS doesn't publish every state × industry series seasonally adjusted. In that case the NSA series (`SMU…`) goes in a separate `emp_<industry>_nsa` column, and the SA column stays NaN for that state. `missing_report.csv` notes which states this applies to.
* **CPI proxy.** BLS has no state CPIs. `cpi_state_proxy_yoy` is the 12-month % change of the state's Census division index where that change exists (division indexes start Dec 2017 = 100, so from Dec 2018), otherwise the change of its Census region index. `cpi_state_proxy_yoy_source` says which. Index levels with different bases are never spliced.
* **2025 federal funding lapse.** Several BLS series have no October 2025 value (footnote `X`). Those months stay NaN, and the CPI 12-month change for October 2026 is also NaN because its base month is missing.
* **BFS.** The Census BFS API (`timeseries/eits/bfs`) only serves national data, so state data come from the monthly CSV `https://www.census.gov/econ/bfs/csv/bfs_monthly.csv`. Cells marked `D` (suppressed) are NaN. Actual formations (`BF_BF4Q`, `BF_BF8Q`) are only final with a lag, so they end earlier than the applications series.
* **Building permits.** Current-month text files (`st{YYMM}c.txt`) exist for every month in the period and are the primary source. The Excel files (`statemonthly_{YYYYMM}.xls`, from Nov 2019) are parsed and compared against the text files in the overlap, and are used if a text file is missing. Text files have no total column, so the total is the sum of the four structure types; this is checked against the Excel total. Valuation is in thousands of dollars.
* **Imports.** These come from `timeseries/intltrade/imports/statehs`. "State of destination" is where goods are first delivered, often a distribution center, not necessarily where they're used. The state dataset has no calculated-duty field (`CAL_DUT_MO` is only in the national `imports/hs` dataset). Some imports aren't assigned to a state (`STATE=XX`), so the 51 don't add up to the U.S. total. The checks report shows the gap.
