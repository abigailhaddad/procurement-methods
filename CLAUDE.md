# procurement-methods

An open-source analysis of how the federal government buys IT services —
specifically whether agencies use LPTA (Lowest Price Technically Acceptable)
or best-value tradeoff evaluation when hiring developers and data scientists,
and what that means for which kinds of vendors win.

Standalone demo/research project. NOT related to sole-source contracting,
corruption, or political donations.

## Sibling pipeline: `rfp_text_pipeline.py`

Independent daily pipeline that extracts text from RFP attachments for 541511/541512
opportunities on SAM.gov. Self-contained, doesn't feed the dashboard.

**Bulk-search mode.** One call to `opportunities/v2/search` returns up to 1,000
opportunities *with their resourceLinks attached*, so a full daily / weekly
catch typically needs just a handful of API calls — not one-per-opp like the
original design.

Flow:

1. Load state from R2: `processed.json` (noticeIds bundled) +
   `last_fetched_date.json` (cursor).
2. Set the posted-date window: from `last_fetched_date - 1 day` to today.
   First run falls back to `--start-date` or 180 days back.
3. Paginate `opportunities/v2/search?postedFrom=&postedTo=&limit=1000&offset=`
   until a short page. Each page = 1 API call.
4. For every opp in the window:
   - Skip if NAICS prefix doesn't match (default: `541`).
   - Skip if notice type isn't in the RFP-adjacent set.
   - Skip if noticeId already in `processed`.
   - Download each `resourceLink` (free S3), extract text via pypdf /
     python-docx / openpyxl, run the regex label classifier, write
     `bundles/{noticeId}.json`.
5. On clean drain, advance `last_fetched_date = posted_to` and clear
   `scan_cursor`. On 429 or page cap, save `scan_cursor.json =
   {posted_from, posted_to, offset}` so the *next* run resumes mid-drain
   (pinned window, offset advances) instead of re-scanning the top of the
   sort and only seeing already-processed opps.
6. Push state + bundles to R2 (prefix `it_rfps/`).

**Labels (regex-only for now):**
Every bundle carries `labels.{mentions_rtm, shall_count, has_agile_vocab,
has_user_vocab}` computed from `attachments[].text + metadata.description`.

**Notice types kept** (Award Notice deliberately skipped — metadata is enough):
Solicitation, Combined Synopsis/Solicitation, Sources Sought, Presolicitation,
Special Notice, Justification, Fair Opportunity / Limited Sources Justification.

**State on R2:** prefix `it_rfps/` — `state/processed.json`,
`state/last_fetched_date.json`, `state/scan_cursor.json` (present only
mid-drain — pins the window and saved offset), `state/quota.json`
(last run's stats), `bundles/{noticeId}.json`.

**Daily cron:** `.github/workflows/rfp_text.yml` at 09:00 UTC.

**Deps:** `pypdf`, `python-docx`, `openpyxl`, `boto3`, `requests`.

Run locally:
```bash
python3 rfp_text_pipeline.py --dry-run                   # probe first page only
python3 rfp_text_pipeline.py --start-date 2025-10-01     # bootstrap a 6mo window
python3 rfp_text_pipeline.py                              # daily incremental
python3 rfp_text_pipeline.py --max-api-calls 5            # bound a single run
```

Extraction coverage on sample bundles: ~76% of attachments via
pypdf/python-docx/openpyxl. XLSX preserves CLIN-level pricing. Misses are
image-only PDFs (no OCR yet).

## Data pipeline (run in order)

### Step 1 — `fetch_bulk.py`
Downloads transaction-level contract records from USASpending bulk award archives.
- Downloads one ZIP per agency per fiscal year from files.usaspending.gov
- Filters to NAICS 541511 + 541512 (custom programming + systems design), keeps ~75 columns
- Checkpoints per agency/FY at `data/bulk_checkpoints/`
- Resume-safe: re-running skips completed agencies
- May get IP-blocked after ~50 agencies; re-run to continue from a new IP
- Output: `data/contracts_bulk.csv` (transaction-level, all agencies merged)

```bash
python3 fetch_bulk.py                    # all agencies, FY2022–present
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
- Rate-limited: free tier ~100 calls/day. Runs daily via `.github/workflows/fetch_tradeoff.yml`.
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

## Evaluation method classification

Two separate fields — kept distinct because they come from different sources
and have different coverage.

### `eval_method` — USASpending competition fields (always populated)

| Category | Rule |
|---|---|
| **Fair Opportunity** | `solicitation_procedures_code` = "MAFO" |
| **Negotiated Proposal** | `extent_competed` in (A, D) AND `solicitation_procedures` = "NP" |
| **Simplified Acquisition** | `extent_competed` in (F, G) |
| **Sole Source** | `solicitation_procedures` = "SSS" |
| **Not Competed** | `extent_competed` in (B, C), not sole source |
| **Unknown** | None of the above matched |

### `tradeoff_code` — Tango API / FPDS (partial coverage)

| Value | Meaning |
|---|---|
| `LPTA` | Lowest Price Technically Acceptable |
| `TO` | Best-Value Tradeoff |
| `O` | Other |
| null | Not yet fetched or not reported |

FPDS `tradeoff_process` is contractor-reported and blank for ~40–60% of awards.
Coverage grows daily as `fetch_tradeoff.py` runs via GitHub Actions.

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
fetch_bulk.py              — USASpending bulk archive download
fetch_tradeoff.py          — Tango API tradeoff code pull (run daily)
build_contracts.py         — Join bulk + tradeoff, classify eval_method
enrich_sam.py              — SAM bulk extract enrichment
fetch_protests.py          — Tango API GAO protest pull
analyze.py                 — Build dashboard JSONs from contracts + SAM + protests
fetch_solicitations.py     — SAM.gov opportunity CSV download + NAICS filter
build_rfp_signals.py       — Pull RFP bundles from R2, build rfp_signals.json + rfp_bundles.json
build_combined_table.py    — Join RFP bundles with matched contracts → combined_table.json
rfp_text_pipeline.py       — Independent daily pipeline: SAM.gov RFP text extraction to R2
r2_sync.py                 — R2 checkpoint sync for GitHub Actions
.github/workflows/
  fetch.yml                — GitHub Actions: fetch_bulk.py with R2 persistence (monthly)
  fetch_tradeoff.yml       — GitHub Actions: fetch_tradeoff.py daily at 10:00 UTC
  rfp_text.yml             — GitHub Actions: rfp_text_pipeline.py daily at 09:00 UTC
  rebuild.yml              — GitHub Actions: rebuild web/data/ after tradeoff fetch; data tests gate the commit
web/index.html             — Dashboard + RFP browser
web/shared/filters.js      — FilterManager class (shared filter UX component)
web/shared/shared.css      — Design tokens + component styles
web/data/*.json            — Dashboard data (committed for Vercel)
data/                      — Raw data (gitignored)
  contracts_bulk.csv             — USASpending transactions (from fetch_bulk.py)
  tradeoff_lookup.csv            — Tango tradeoff codes (from fetch_tradeoff.py)
  contracts_raw.csv              — Joined + classified (from build_contracts.py)
  sam_lookup.csv                 — SAM entity data (from enrich_sam.py)
  solicitations/filtered.csv     — SAM.gov opportunities (from fetch_solicitations.py)
  bulk_checkpoints/              — fetch_bulk.py per-agency checkpoints
  tradeoff_checkpoints/          — fetch_tradeoff.py per-month checkpoints
.env                       — TANGO_API_KEY + SAM_API_KEY (gitignored)
vercel.json                — Routes / → web/
```
