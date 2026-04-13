"""
build_contracts.py — Join USASpending bulk data with Tango tradeoff codes.

Reads data/contracts_bulk.csv (from fetch_bulk.py) and data/tradeoff_lookup.csv
(from fetch_tradeoff.py), joins on contract_award_unique_key, and outputs
data/contracts_raw.csv in the format that analyze.py expects.

The bulk data has everything (dollars, agencies, recipients, competition info)
except the LPTA/tradeoff evaluation method — that comes from Tango.

Run:
    python3 build_contracts.py
"""

import pandas as pd
from pathlib import Path

BULK_CSV     = Path("data/contracts_bulk.csv")
TRADEOFF_CSV = Path("data/tradeoff_lookup.csv")
OUTPUT_CSV   = Path("data/contracts_raw.csv")

# Only keep the NAICS codes we care about
NAICS_KEEP = {"541511", "541512"}


def classify_eval_method(df: pd.DataFrame) -> pd.Series:
    """
    Classify each contract into a granular evaluation method.

    USASpending does not publish LPTA vs. best-value tradeoff — that field
    (tradeoff_process) is only in FPDS, accessible via the Tango API. But
    USASpending does publish how each contract was competed (extent_competed_code,
    solicitation_procedures_code), which lets us classify the ~90% of contracts
    that don't have a Tango tradeoff code.

    Two data sources, combined:
      1. Tango API tradeoff_process → LPTA or Best-Value Tradeoff (highest priority)
      2. USASpending competition fields → everything else

    Categories (applied in priority order — later rules override earlier):

      LPTA                   tradeoff_code = "LPTA"
                             Source: Tango API (FPDS tradeoff_process)

      Best-Value Tradeoff    tradeoff_code = "TO"
                             Source: Tango API (FPDS tradeoff_process)

      Fair Opportunity       solicitation_procedures_code = "MAFO"
                             Task orders off IDIQ/GWAC multi-award vehicles.
                             These are competed among pre-qualified vendors but
                             don't use formal LPTA/tradeoff evaluation.

      Negotiated Proposal    extent_competed in (A, D) AND solicitation = "NP"
                             Full and open competition using negotiated proposals.
                             A = full and open; D = full and open after exclusion
                             of sources.

      Simplified Acquisition extent_competed in (F, G)
                             F = competed under SAP (simplified acquisition);
                             G = not competed under SAP (below threshold).

      Sole Source            solicitation_procedures_code = "SSS"
                             Only one source — various FAR 6.302 justifications.

      Not Competed           extent_competed in (B, C) AND solicitation != "SSS"
                             B = not available for competition;
                             C = not competed (other reasons).

      Unknown                None of the above matched.

    The priority order matters: a contract with BOTH tradeoff_code="TO" and
    solicitation="MAFO" is classified as "Best-Value Tradeoff" because Tango
    is the more specific source.

    Reference:
      extent_competed_code: FAR Part 6 / FPDS data dictionary
      solicitation_procedures_code: FPDS data dictionary
      tradeoff_process: FPDS source_selection_process via Tango API
    """
    method = pd.Series("Unknown", index=df.index)

    tc = df["tradeoff_code"].fillna("")
    ext = df["extent_competed"].fillna("")
    sol = df["solicitation_procedures"].fillna("")

    # Apply in reverse priority order (later overwrites earlier)
    # Not competed (B = not available, C = not competed)
    method[ext.isin(["B", "C"])] = "Not Competed"
    # Sole source
    method[sol == "SSS"] = "Sole Source"
    # Simplified acquisition (F = competed under SAP, G = not competed under SAP)
    method[ext.isin(["F", "G"])] = "Simplified Acquisition"
    # Negotiated proposal — full and open competition
    method[(ext.isin(["A", "D"])) & (sol == "NP")] = "Negotiated Proposal"
    # Fair opportunity — task orders off multi-award vehicles
    method[sol == "MAFO"] = "Fair Opportunity"
    # Tango tradeoff codes override everything (most specific)
    method[tc == "TO"] = "Best-Value Tradeoff"
    method[tc == "LPTA"] = "LPTA"

    return method


def main():
    if not BULK_CSV.exists():
        print(f"{BULK_CSV} not found — run fetch_bulk.py first.")
        return

    # ---- Load bulk data ----
    print("Loading bulk data...")
    bulk = pd.read_csv(BULK_CSV, low_memory=False, dtype={"naics_code": str})
    print(f"  {len(bulk):,} transaction rows")

    # Filter to our NAICS codes
    bulk = bulk[bulk["naics_code"].isin(NAICS_KEEP)].copy()
    print(f"  {len(bulk):,} after NAICS filter (541511 + 541512)")

    # ---- Aggregate to contract level ----
    # Bulk data is transaction-level (mods, etc). We want one row per contract
    # with the latest action date and total obligations.
    print("Aggregating to contract level...")

    # Sort by action_date so last row per contract has the latest info
    bulk["action_date"] = pd.to_datetime(bulk["action_date"], errors="coerce")
    bulk = bulk.sort_values("action_date")

    # USASpending bulk data is transaction-level: one row per modification.
    # We aggregate to one row per contract_award_unique_key.
    #
    # total_dollars_obligated is CUMULATIVE in USASpending — the latest
    # transaction carries the running total, so we take "last" (after sorting
    # by action_date). federal_action_obligation is the per-transaction delta,
    # so we sum it as a fallback when total_dollars_obligated is missing.
    #
    # For categorical fields (set-aside, pricing, competition), we take the
    # latest value. Modifications can change these, but the most recent
    # reflects the contract's current state.
    agg = bulk.groupby("contract_award_unique_key").agg(
        award_date=("action_date", "max"),
        obligated=("total_dollars_obligated", "last"),  # cumulative — take latest
        federal_action_obligation=("federal_action_obligation", "sum"),
        naics_code=("naics_code", "last"),
        set_aside=("type_of_set_aside_code", "last"),
        contract_type=("type_of_contract_pricing_code", "last"),
        recipient_uei=("recipient_uei", "last"),
        recipient_name=("recipient_name", "last"),
        department=("awarding_agency_name", "last"),
        agency=("awarding_sub_agency_name", "last"),
        extent_competed=("extent_competed_code", "last"),
        solicitation_procedures=("solicitation_procedures_code", "last"),
        other_than_full_and_open=("other_than_full_and_open_competition_code", "last"),
        number_of_offers=("number_of_offers_received", "last"),
        business_size=("contracting_officers_determination_of_business_size_code", "last"),
    ).reset_index()

    # Use total_dollars_obligated where available, fall back to summed actions
    agg["obligated"] = pd.to_numeric(agg["obligated"], errors="coerce")
    mask = agg["obligated"].isna() | (agg["obligated"] == 0)
    agg.loc[mask, "obligated"] = pd.to_numeric(
        agg.loc[mask, "federal_action_obligation"], errors="coerce"
    )

    # Rename key column to match analyze.py expectations
    agg = agg.rename(columns={"contract_award_unique_key": "key"})

    print(f"  {len(agg):,} unique contracts")

    # ---- Join tradeoff codes ----
    if TRADEOFF_CSV.exists():
        print("Joining tradeoff codes from Tango...")
        tradeoff = pd.read_csv(TRADEOFF_CSV)
        # Clean up empty strings
        tradeoff["tradeoff_code"] = tradeoff["tradeoff_code"].replace("", pd.NA)
        tradeoff = tradeoff.dropna(subset=["tradeoff_code"])
        print(f"  {len(tradeoff):,} tradeoff records")

        agg = agg.merge(
            tradeoff[["contract_award_unique_key", "tradeoff_code"]],
            left_on="key",
            right_on="contract_award_unique_key",
            how="left",
        )
        agg = agg.drop(columns=["contract_award_unique_key"], errors="ignore")

        matched = agg["tradeoff_code"].notna().sum()
        print(f"  {matched:,} contracts matched with tradeoff code ({matched/len(agg)*100:.1f}%)")
    else:
        print("No tradeoff_lookup.csv — skipping tradeoff join")
        agg["tradeoff_code"] = pd.NA

    # ---- Clean up set-aside codes ----
    # Replace empty/blank with NONE to match analyze.py expectations
    agg["set_aside"] = agg["set_aside"].fillna("NONE").replace("", "NONE")

    # ---- Build eval_method ----
    # Combines Tango tradeoff_code with USASpending competition fields
    # for a full breakdown of how each contract was evaluated.
    print("Classifying eval_method...")
    agg["eval_method"] = classify_eval_method(agg)
    em = agg["eval_method"].value_counts()
    for method, n in em.items():
        print(f"  {method:35s}  {n:>6,}")

    # ---- Write output ----
    out_cols = [
        "key", "obligated", "award_date", "naics_code", "set_aside",
        "tradeoff_code", "eval_method", "contract_type",
        "extent_competed", "solicitation_procedures",
        "recipient_uei", "recipient_name",
        "department", "agency",
    ]
    out = agg[out_cols].copy()
    out.to_csv(OUTPUT_CSV, index=False)
    print(f"\nWrote {len(out):,} contracts to {OUTPUT_CSV}")

    # ---- Summary ----
    print(f"\n--- Summary ---")
    print(f"Total contracts:     {len(out):,}")
    print(f"With tradeoff code:  {out['tradeoff_code'].notna().sum():,}")
    tc = out["tradeoff_code"].value_counts()
    for code, n in tc.items():
        print(f"  {code:5s}  {n:>6,}")
    print(f"Total obligated:     ${out['obligated'].sum()/1e9:.1f}B")
    print(f"NAICS 541511:        {(out['naics_code']=='541511').sum():,}")
    print(f"NAICS 541512:        {(out['naics_code']=='541512').sum():,}")
    print(f"Unique vendors:      {out['recipient_uei'].nunique():,}")
    print(f"Unique agencies:     {out['department'].nunique():,}")


if __name__ == "__main__":
    main()
