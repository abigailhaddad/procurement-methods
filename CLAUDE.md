# procurement-methods

An open-source analysis of how the federal government buys IT services —
specifically whether agencies use LPTA (Lowest Price Technically Acceptable)
or best-value tradeoff evaluation when hiring developers and data scientists,
and what that means for which kinds of vendors win.

Standalone demo/research project. NOT related to sole-source contracting,
corruption, or political donations.

## Data pipeline (run in order)

### Step 1 — `fetch.py`
Pulls all NAICS 541511 + 541512 contracts from FY2020–present via Tango API.
- Batches by **month** with checkpoints at `data/checkpoints/{naics}_{yyyymm}.csv`
- Resume-safe: re-running skips completed months
- ~1,200 API calls total; free tier (100/day) ≈ 12 days; Micro ($25/mo, 250/day) ≈ 5 days
- Output: `data/contracts_raw.csv`
- **Run daily until complete.** Rate limit hits fast; just re-run each day.

### Step 2 — `enrich_sam.py`
Downloads SAM.gov monthly bulk extract, filters to UEIs in contracts_raw.csv.
- Requires `SAM_API_KEY` in `.env` (free key: sam.gov → Account Details → API Keys)
- SAM key is already in `.env` (copied from makegov repo)
- Downloads ~1–3GB ZIP once, cached at `data/sam_extract_cache.zip`
- Uses `csv.DictReader` with named columns (robust to SAM layout changes)
- Prints discovered column names for NUMBER_OF_EMPLOYEES and SBA fields on first run
- Output: `data/sam_lookup.csv`

Fields captured from SAM extract:
- `uei`, `legal_business_name`
- `sam_registration_date` — when entity first registered in SAM
- `entity_start_date` — incorporation / entity start date
- `city`, `state`
- `number_of_employees` — self-reported integer (may be blank)
- `sba_business_types` — tilde-delimited SBA certification names (non-empty = certified small)
- `business_types` — general entity business type codes

### Step 3 — `analyze.py`
Joins contracts + SAM data, computes derived fields, outputs dashboard JSONs to `web/data/`.
- Can run on partial data (before fetch.py finishes all batches)
- Outputs: `summary.json`, `by_fy.json`, `by_agency.json`, `by_contract_type.json`,
  `by_set_aside.json`, `by_winner_type.json`, `by_vendor_age.json`, `top_vendors.json`, `filters.json`
- **Commit `web/data/` after running** — these are what Vercel serves

Derived fields computed:
- `fiscal_year` — Oct–Sep (award month ≥ 10 → year+1)
- `engagement_type` — Deliverable (FFP/J) vs. Staff Aug (T&M/Y or Labor Hours/Z)
- `is_small_biz_setaside` — set_aside code in small-biz set {SBA, 8A, WOSB, HZC, …}
- `vendor_age_years` — (award_date − entity_start_date) in years (SAM required)
- `vendor_age_bin` — <2 / 2–5 / 5–10 / 10–20 / 20+ yrs (SAM required)
- `is_new_entrant` — award within 365 days of sam_registration_date (SAM required)
- `is_sba_small` — non-empty sba_business_types in SAM (SAM required)
- `num_employees` — parsed integer from SAM number_of_employees (SAM required)

## NAICS scope

- **541511** — Custom Computer Programming Services
- **541512** — Computer Systems Design Services

Excluded: 541513 (IT ops), 541519 (too noisy).

## Key contract fields

- `tradeoff_code`: LPTA / TO (best-value tradeoff) / O (other, incl. Brooks Act) / null (not reported)
- `contract_type`: J=FFP / Y=T&M / Z=Labor Hours (from `competition.contract_type.code`)
- `set_aside`: SBA small biz category or NONE
- `obligated`, `award_date`, `naics_code`
- `recipient_uei`, `recipient_name`
- `department`, `agency` (from `awarding_office`)

## Tango API notes

- `shape` parameter (not `fields`) for field selection
- `award_date_gte` / `award_date_lte` for date filtering (not `fiscal_year_start`)
- Cursor-based pagination via `resp.cursor` + `resp.next`
- `awarding_office(*)` works; sub-field selection like `awarding_office(name)` does NOT
- `competition(*)` works; `competition(contract_type)` does NOT
- `tradeoff_process(*)` returns `{code, description}` or null
- `contract_type` is at `competition.contract_type.code` in the response dict
- `awarding_office.agency` = department name; `awarding_office.name` = office name

## Dashboard (`web/index.html`)

Static site deployable to Vercel (root → `web/`). `vercel.json` routes `/ → web/`.
Uses Chart.js v4 (CDN) + custom `FilterManager` class in `web/shared/filters.js`.

Charts:
- LPTA vs. Best-Value Tradeoff by Fiscal Year (stacked bar)
- Deliverable (FFP) vs. Staff Aug by eval method (stacked horizontal bar)
- Small biz set-aside vs. unrestricted LPTA rate
- Who Wins section: small biz share, market concentration (top-10 share), median contract size, new entrant %, median vendor age (SAM-gated), vendor age distribution (SAM-gated)
- LPTA rate by agency (horizontal bar, top 25)
- Top 20 vendors table (LPTA vs. tradeoff split)
- Methodology section with data pipeline flow, variable definitions, caveats

SAM-enriched charts (vendor age, age distribution) are hidden until `by_winner_type.json`
has non-null `median_vendor_age_yrs` — degrades gracefully without SAM data.

## Files

```
fetch.py              — Tango API pull (run daily until complete)
enrich_sam.py         — SAM bulk extract enrichment (run after fetch.py)
analyze.py            — Build dashboard JSONs (run after enrich_sam.py)
web/index.html        — Dashboard
web/shared/filters.js — FilterManager (+ Add Filter → chips UX)
web/shared/shared.css — Design tokens + component styles
web/data/*.json       — Dashboard data (committed for Vercel)
data/                 — Raw data (gitignored)
  contracts_raw.csv
  sam_lookup.csv
  sam_extract_cache.zip
  checkpoints/
.env                  — TANGO_API_KEY + SAM_API_KEY (gitignored)
vercel.json           — Routes / → web/
```
