# procurement-methods

## What this is

An open-source analysis of how the federal government buys IT services —
specifically whether agencies use LPTA (Lowest Price Technically Acceptable)
or best-value tradeoff evaluation when hiring developers and data scientists,
and what that means for which kinds of vendors win.

This is a standalone demo/research project. It is NOT related to sole-source
contracting, corruption, or political donations — those are separate client work.

## Data source

Tango API (govcon.dev) — free tier is sufficient for the full build.
API key is in `.env` as `TANGO_API_KEY`.

## NAICS scope

- **541511** — Custom Computer Programming Services ("hire a developer")
- **541512** — Computer Systems Design Services ("hire a systems architect")

Deliberately excluded:
- 541513 (facilities management — IT ops, not development)
- 541519 (too noisy — hardware reselling mixed in)

## Key fields

- `tradeoff_code`: LPTA / TO (trade-off) / O (other) / null
- `contract_type`: J (FFP/deliverable) / Y (T&M/staff aug) / Z (Labor Hours)
- `set_aside`: small business category or NONE
- `obligated`, `award_date`, `naics_code`, `recipient_uei`, `recipient_name`
- `department`, `agency`

## Research questions

1. What fraction of IT services contracts use LPTA vs. best-value tradeoff?
2. Does evaluation methodology differ by contract type (FFP deliverable vs T&M staff aug)?
3. Do small businesses win more or less under LPTA vs. tradeoff?
4. Which agencies use LPTA most heavily for IT services?
5. Has LPTA usage changed over time (FY2020–present)?
6. Does contract size predict methodology — is LPTA concentrated at the low end?

## Files

- `fetch.py` — pulls all 541511+541512 contracts FY2020–FY2025 → `data/contracts_raw.csv`
  - Batches by fiscal year; checkpoints each (naics, fy) to `data/checkpoints/`
  - Resume-safe: re-running skips completed batches
  - ~117k records total, ~1,200 API calls; free tier ~12 days, Micro ~5 days
- `enrich_sam.py` — downloads SAM.gov monthly bulk extract, filters to our UEIs
  - One API call (~500MB download), no per-entity rate limiting
  - Outputs `data/sam_lookup.csv` with registration date, entity start date, city, state
  - Requires `SAM_API_KEY` in `.env` (free key from sam.gov)
- `data/contracts_raw.csv` — output of fetch.py (gitignored)
- `data/sam_lookup.csv` — output of enrich_sam.py (gitignored)
- `data/checkpoints/` — per-(naics,fy) CSVs for resume (gitignored)
- `data/sam_extract_cache.zip` — cached SAM bulk extract (gitignored, ~500MB)
- `dashboard.html` — (TODO) single-file analysis dashboard

## fetch.py status

Fiscal year batching with checkpointing. First run hit daily rate limit at
NAICS 541511 FY2020 page 19 (~1,900 records). The new version saves each
completed (naics, fy) batch as a checkpoint — re-running resumes cleanly.
Run daily until complete; merges all checkpoints into contracts_raw.csv each time.

## Tango API notes

- `shape` parameter (not `fields`) for field selection
- `fiscal_year_gte` (not `fiscal_year_start`)
- Cursor-based pagination via `resp.cursor` + `resp.next`
- `awarding_office(*)` works; sub-field selection like `awarding_office(name)` does NOT
- `competition(*)` works; `competition(contract_type)` does NOT
- `tradeoff_process(*)` works correctly — returns `{code, description}` or null
- contract_type is inside competition dict: `competition.contract_type.code`
  codes: J=FFP, Y=T&M, Z=Labor Hours, U/V/S=cost-type variants

## SAM.gov enrichment plan

Use the SAM.gov **bulk monthly extract** (not per-entity API calls) to join
vendor registration data onto contracts_raw.csv.

Source: `pull_usaspending/pull_usaspending/fetch_sam_extract.py` — adapt by
changing the UEI collection step to read from `data/contracts_raw.csv` instead
of `data/history_agg.csv`. Everything else (download, parse, filter) is identical.

One API call downloads the full ~500MB SAM snapshot (pipe-delimited .dat file
inside a zip). Filter to UEIs present in contracts_raw.csv → `data/sam_lookup.csv`.

Fields we get from SAM bulk extract (field positions confirmed):
- col 0:  `uei`
- col 7:  `sam_registration_date` (YYYYMMDD) — when entity first registered in SAM
- col 11: `legal_business_name`
- col 17: `city`
- col 18: `state`
- col 24: `entity_start_date` (YYYYMMDD) — incorporation / entity start date

Key derived field: `days_from_sam_registration_to_first_contract` — how long
after registering in SAM did this vendor win their first IT services contract?
Short gap = potential new entrant. This is the incumbent-vs-newcomer signal.

SAM_API_KEY goes in `.env` (free key from sam.gov Account Details → API Keys).

## Planned dashboard cuts

- LPTA rate by agency (bar chart, top 20 agencies)
- LPTA rate by contract type (FFP vs T&M)
- LPTA rate by fiscal year (trend line)
- Obligated $ distribution: LPTA vs tradeoff (box plot or violin)
- Small business win rate: LPTA vs tradeoff vs other
- Top vendors under each methodology
