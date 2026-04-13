# procurement-methods

An open-source analysis of how the federal government buys IT services —
specifically whether agencies use LPTA (Lowest Price Technically Acceptable)
or best-value tradeoff evaluation when hiring developers and data scientists,
and what that means for which kinds of vendors win.

Standalone demo/research project. NOT related to sole-source contracting,
corruption, or political donations.

## Data pipeline (run in order)

### Step 1 — `fetch_bulk.py`
Downloads transaction-level contract records from USASpending bulk award archives.
- Downloads one ZIP per agency per fiscal year from files.usaspending.gov
- Filters to NAICS prefix `5415` (IT services), keeps ~75 columns
- Checkpoints per agency/FY at `data/bulk_checkpoints/`
- Resume-safe: re-running skips completed agencies
- May get IP-blocked after ~50 agencies; re-run to continue from a new IP
- Output: `data/contracts_bulk.csv` (transaction-level, all agencies merged)

```bash
python3 fetch_bulk.py                    # all agencies, FY2022-FY2026
python3 fetch_bulk.py --fy 2026          # one year
python3 fetch_bulk.py --agencies 097 036 # specific agencies
```

GitHub Actions workflow (`fetch.yml`) runs this with R2 checkpoint persistence
and auto-chains new runs when IP-blocked.

### Step 2 — `fetch_tradeoff.py`
Pulls LPTA/tradeoff evaluation codes from the Tango API (FPDS data).
- USASpending does NOT publish `source_selection_process` (LPTA vs tradeoff)
- This script fetches only `key` + `tradeoff_process` to minimize API calls
- Batches by NAICS + month with checkpoints at `data/tradeoff_checkpoints/`
- Rate-limited: free tier ~100 calls/day. Run daily until complete.
- Output: `data/tradeoff_lookup.csv`

### Step 3 — `build_contracts.py`
Joins USASpending bulk data with Tango tradeoff codes.
- Filters bulk data to NAICS 541511 + 541512
- Aggregates transaction-level data to one row per contract
- Joins tradeoff codes on `contract_award_unique_key`
- Classifies each contract into an `eval_method` (see below)
- Output: `data/contracts_raw.csv`

### Step 4 — `enrich_sam.py`
Downloads SAM.gov monthly bulk extract, filters to UEIs in contracts_raw.csv.
- Requires `SAM_API_KEY` in `.env` (free key: sam.gov → Account Details → API Keys)
- Downloads ~1–3GB ZIP once, cached at `data/sam_extract_cache.zip`
- Output: `data/sam_lookup.csv`

Fields captured from SAM extract:
- `uei`, `legal_business_name`
- `sam_registration_date` — when entity first registered in SAM
- `entity_start_date` — incorporation / entity start date
- `city`, `state`
- `number_of_employees` — self-reported integer (may be blank)
- `sba_business_types` — tilde-delimited SBA certification names (non-empty = certified small)
- `business_types` — general entity business type codes

### Step 5 — `analyze.py`
Joins contracts + SAM data, computes derived fields, outputs dashboard JSONs to `web/data/`.
- Can run on partial data (before all steps finish)
- Outputs: `summary.json`, `by_eval_method.json`, `by_eval_method_fy.json`,
  `by_fy.json`, `by_agency.json`, `by_contract_type.json`,
  `by_set_aside.json`, `by_winner_type.json`, `by_vendor_age.json`,
  `top_vendors.json`, `filters.json`
- **Commit `web/data/` after running** — these are what Vercel serves

## Evaluation method classification (`eval_method`)

Each contract is classified using two data sources. Tango tradeoff codes take
priority when available; USASpending competition fields classify the rest.
See `build_contracts.py: classify_eval_method()` for the implementation.

| Category | Rule | Data source |
|---|---|---|
| **LPTA** | `tradeoff_process.code` = "LPTA" | Tango API (FPDS) |
| **Best-Value Tradeoff** | `tradeoff_process.code` = "TO" | Tango API (FPDS) |
| **Fair Opportunity** | `solicitation_procedures_code` = "MAFO" | USASpending |
| **Negotiated Proposal** | `extent_competed` in (A, D) AND `solicitation_procedures` = "NP" | USASpending |
| **Simplified Acquisition** | `extent_competed` in (F, G) | USASpending |
| **Sole Source** | `solicitation_procedures` = "SSS" | USASpending |
| **Not Competed** | `extent_competed` in (B, C), not sole source | USASpending |

Key design decisions:
- **Priority order matters.** A contract with both `tradeoff_code=TO` and
  `solicitation_procedures=MAFO` is "Best-Value Tradeoff" because Tango is
  more specific than USASpending competition fields.
- **"Negotiated Proposal" is broad.** It captures full-and-open competitions
  that aren't LPTA, tradeoff, or fair opportunity. Likely includes multiple
  evaluation approaches we can't distinguish from available data.
- **LPTA/tradeoff coverage is partial.** FPDS `tradeoff_process` is
  contractor-reported and blank for ~40–60% of awards. Only ~10% of contracts
  currently have Tango tradeoff codes matched.

## Other derived fields

- `fiscal_year` — Oct–Sep (award month ≥ 10 → year+1)
- `engagement_type` — Deliverable (FFP/J) vs. Staff Aug (T&M/Y or Labor Hours/Z)
- `is_small_biz_setaside` — set_aside code in {SBA, 8A, 8AN, SDVOSBC, WOSB, HZC, …}
- `vendor_age_years` — (award_date − entity_start_date) in years (SAM required)
- `vendor_age_bin` — <2 / 2–5 / 5–10 / 10–20 / 20+ yrs (SAM required)
- `is_new_entrant` — award within 365 days of sam_registration_date (SAM required)
- `is_sba_small` — non-empty sba_business_types in SAM (SAM required)
- `num_employees` — parsed integer from SAM number_of_employees (SAM required)

## NAICS scope

- **541511** — Custom Computer Programming Services
- **541512** — Computer Systems Design Services

Excluded: 541513 (IT ops), 541519 (too noisy).

## Key contract fields (in contracts_raw.csv)

- `key` — `contract_award_unique_key` (USASpending unique ID)
- `tradeoff_code` — LPTA / TO / O / null (from Tango, often null)
- `eval_method` — full classification (see table above, always populated)
- `contract_type` — J=FFP / Y=T&M / Z=Labor Hours (`type_of_contract_pricing_code`)
- `set_aside` — SBA small biz category or NONE (`type_of_set_aside_code`)
- `extent_competed` — A/B/C/D/F/G (`extent_competed_code`)
- `solicitation_procedures` — MAFO/NP/SSS/SP1 etc. (`solicitation_procedures_code`)
- `obligated` — total dollars obligated (cumulative from USASpending)
- `award_date`, `naics_code`
- `recipient_uei`, `recipient_name`
- `department`, `agency`

## Aggregation logic

USASpending bulk data is transaction-level (one row per contract modification).
`build_contracts.py` aggregates to one row per contract:
- `total_dollars_obligated` → take latest (it's cumulative in USASpending)
- `federal_action_obligation` → sum (used as fallback when cumulative is missing)
- Categorical fields (set-aside, pricing, competition) → take latest modification
- `award_date` → max across all transactions

This means if a contract's competition method changed across modifications, only
the latest value is reflected.

## Tango API notes

- `shape` parameter (not `fields`) for field selection
- `award_date_gte` / `award_date_lte` for date filtering
- Cursor-based pagination via `resp.cursor` + `resp.next`
- `tradeoff_process(*)` returns `{code, description}` or null
- Only used for `source_selection_process` — all other fields come from USASpending

## Dashboard (`web/index.html`)

Static site deployable to Vercel (root → `web/`). `vercel.json` routes `/ → web/`.
Uses Chart.js v4 (CDN) + custom `FilterManager` class in `web/shared/filters.js`.

Charts:
- **How Contracts Are Evaluated** — eval_method breakdown by count and by dollars
- **Eval Method Over Time** — stacked bar by fiscal year (all 7 categories)
- LPTA vs. Best-Value Tradeoff by FY (Tango-only subset)
- Deliverable (FFP) vs. Staff Aug by eval method
- Small biz set-aside vs. unrestricted LPTA rate
- Who Wins section: small biz share, market concentration, median contract size, new entrant %, vendor age
- LPTA rate by agency (top 25)
- Top 20 vendors table
- Methodology section with pipeline flow, eval_method classification table, variable definitions, caveats

SAM-enriched charts (vendor age, age distribution) are hidden until `by_winner_type.json`
has non-null `median_vendor_age_yrs` — degrades gracefully without SAM data.

## Files

```
fetch_bulk.py         — USASpending bulk archive download
fetch_tradeoff.py     — Tango API tradeoff code pull (run daily)
build_contracts.py    — Join bulk + tradeoff, classify eval_method
enrich_sam.py         — SAM bulk extract enrichment
analyze.py            — Build dashboard JSONs
fetch.py              — (legacy) Full Tango API pull, now superseded
r2_sync.py            — R2 checkpoint sync for GitHub Actions
.github/workflows/
  fetch.yml           — GitHub Actions: fetch_bulk.py with R2 persistence
web/index.html        — Dashboard
web/shared/filters.js — FilterManager (+ Add Filter → chips UX)
web/shared/shared.css — Design tokens + component styles
web/data/*.json       — Dashboard data (committed for Vercel)
data/                 — Raw data (gitignored)
  contracts_bulk.csv        — USASpending transactions (from fetch_bulk.py)
  tradeoff_lookup.csv       — Tango tradeoff codes (from fetch_tradeoff.py)
  contracts_raw.csv         — Joined + classified (from build_contracts.py)
  sam_lookup.csv             — SAM entity data (from enrich_sam.py)
  bulk_checkpoints/          — fetch_bulk.py per-agency checkpoints
  tradeoff_checkpoints/      — fetch_tradeoff.py per-month checkpoints
.env                  — TANGO_API_KEY + SAM_API_KEY (gitignored)
vercel.json           — Routes / → web/
```
