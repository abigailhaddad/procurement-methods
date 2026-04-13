# How the Federal Government Buys IT Services

An open analysis of federal IT procurement methods — how contracts for custom
software and systems design are competed, evaluated, and awarded.

**[View the dashboard](https://procurement-methods.vercel.app)** |
**[View the data pipeline](#how-it-works)**

## What this is

This project pulls federal contract data from two public sources and classifies
every IT services contract by how it was evaluated:

- **LPTA** (Lowest Price Technically Acceptable)
- **Best-Value Tradeoff**
- **Fair Opportunity** (IDIQ/GWAC task orders)
- **Negotiated Proposal** (full and open competition)
- **Simplified Acquisition**
- **Sole Source**

The current dataset covers **NAICS 541511** (Custom Computer Programming) and
**541512** (Computer Systems Design) as a sample — the pipeline can be extended
to other NAICS codes.

## What's in the dashboard

- How IT contracts are evaluated (by dollars and by count)
- How the mix has changed over time (FY2022–2026)
- Vendor age distribution (from SAM.gov entity data)
- Which agencies use which methods
- Top vendors by obligated dollars
- GAO bid protests matched to IT solicitations
- Full methodology with classification rules and data sources

## How it works

```
fetch_bulk.py           USASpending bulk archives → data/contracts_bulk.csv
                        (contract details: dollars, agencies, vendors, competition fields)

fetch_tradeoff.py       Tango API (FPDS) → data/tradeoff_lookup.csv
                        (LPTA vs. best-value tradeoff codes — not in USASpending)

build_contracts.py      Join bulk + tradeoff → data/contracts_raw.csv
                        (classifies each contract into an eval_method)

enrich_sam.py           SAM.gov monthly extract → data/sam_lookup.csv
                        (vendor age, registration date)

fetch_protests.py       Tango API → data/protests_matched.csv
                        (GAO bid protests matched to IT solicitations)

analyze.py              contracts_raw + SAM + protests → web/data/*.json
                        (dashboard data files)
```

## Quick start

```bash
# Install dependencies
pip install -r requirements.txt

# Set up API keys in .env
echo "TANGO_API_KEY=your_key" >> .env
echo "SAM_API_KEY=your_key" >> .env

# Step 1: Download contract data from USASpending (no API key needed)
python3 fetch_bulk.py --fy 2026          # start with one year

# Step 2: Pull LPTA/tradeoff codes from Tango API (rate-limited, run daily)
python3 fetch_tradeoff.py

# Step 3: Join and classify
python3 build_contracts.py

# Step 4: SAM vendor enrichment (optional — adds vendor age)
python3 enrich_sam.py

# Step 5: GAO protests (optional — rate-limited, same pool as tradeoff)
python3 fetch_protests.py

# Step 6: Build dashboard data
python3 analyze.py

# View locally
cd web && python3 -m http.server 8000
```

All scripts are checkpoint/resume safe. If they get rate-limited or interrupted,
just re-run and they'll pick up where they left off.

## Data sources

| Source | What it provides | Access |
|--------|-----------------|--------|
| [USASpending](https://www.usaspending.gov) bulk archives | Contract details (75 fields per transaction) | Free, no key needed |
| [Tango API](https://govcon.dev) (FPDS) | LPTA/tradeoff evaluation codes, GAO protests | Free tier: 100 calls/day |
| [SAM.gov](https://sam.gov) monthly extract | Vendor entity data (age, registration date) | Free API key |

## GitHub Actions

The `fetch_bulk.py` step runs automatically via GitHub Actions (`.github/workflows/fetch.yml`).
It uses Cloudflare R2 for checkpoint persistence and auto-chains new runs when
IP-blocked by USASpending.

## License

MIT
