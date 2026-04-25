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
    Classify each contract by competition method using USASpending fields only.

    Categories (applied in reverse priority order — later overwrites earlier):

      Fair Opportunity       solicitation_procedures_code = "MAFO"
                             Task orders off IDIQ/GWAC multi-award vehicles.

      Negotiated Proposal    extent_competed in (A, D) AND solicitation = "NP"
                             Full and open competition using negotiated proposals.

      Simplified Acquisition extent_competed in (F, G)
                             F = competed under SAP; G = not competed under SAP.

      Sole Source            solicitation_procedures_code = "SSS"

      Not Competed           extent_competed in (B, C) AND solicitation != "SSS"

      Unknown                None of the above matched.

    LPTA / Best-Value Tradeoff are stored separately in the tradeoff_code field
    (from Tango API / FPDS) and are not mixed into this classification.

    Reference:
      extent_competed_code: FAR Part 6 / FPDS data dictionary
      solicitation_procedures_code: FPDS data dictionary
    """
    method = pd.Series("Unknown", index=df.index)

    ext = df["extent_competed"].fillna("")
    sol = df["solicitation_procedures"].fillna("")

    method[ext.isin(["B", "C"])] = "Not Competed"
    method[sol == "SSS"] = "Sole Source"
    method[ext.isin(["F", "G"])] = "Simplified Acquisition"
    method[(ext.isin(["A", "D"])) & (sol == "NP")] = "Negotiated Proposal"
    method[sol == "MAFO"] = "Fair Opportunity"

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

    # ---- Flag termination modifications ----
    # FPDS reason-for-modification codes:
    #   E = Terminate for Default, F = Terminate for Convenience, X = Terminate for Cause
    # A contract may have a termination mod among its transactions. We capture
    # the most severe termination type per contract before aggregating.
    TERMINATION_CODES = {"E": "Terminate for Default", "F": "Terminate for Convenience", "X": "Terminate for Cause"}
    if "action_type_code" in bulk.columns:
        bulk["_is_termination"] = bulk["action_type_code"].isin(TERMINATION_CODES)
        # Severity: cause/default > convenience (for "worst" termination per contract)
        severity = {"X": 2, "E": 2, "F": 1}
        bulk["_term_severity"] = bulk["action_type_code"].map(severity).fillna(0).astype(int)
    else:
        bulk["_is_termination"] = False
        bulk["_term_severity"] = 0

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
        potential_value=("potential_total_value_of_award", "last"),
        pop_start=("period_of_performance_start_date", "first"),
        pop_end=("period_of_performance_current_end_date", "last"),
        pop_potential_end=("period_of_performance_potential_end_date", "last"),
        parent_award_agency=("parent_award_agency_name", "last"),
        parent_award_type=("parent_award_type_code", "last"),
        idc_type=("type_of_idc_code", "last"),
        place_state=("primary_place_of_performance_state_code", "last"),
        place_country=("primary_place_of_performance_country_code", "last"),
        number_of_actions=("number_of_actions", "last"),
        award_description=("award_description", "last"),
        solicitation_id=("solicitation_identifier", "last"),
        was_terminated=("_is_termination", "any"),
        _max_term_severity=("_term_severity", "max"),
    ).reset_index()

    # Map severity back to termination type
    term_map = {0: None, 1: "Convenience", 2: "Default/Cause"}
    agg["termination_type"] = agg["_max_term_severity"].map(term_map)
    agg = agg.drop(columns=["_max_term_severity"])

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
        "key", "obligated", "potential_value", "award_date", "naics_code",
        "set_aside", "tradeoff_code", "eval_method", "contract_type",
        "extent_competed", "solicitation_procedures",
        "recipient_uei", "recipient_name",
        "department", "agency",
        "pop_start", "pop_end", "pop_potential_end",
        "parent_award_agency", "parent_award_type", "idc_type",
        "place_state", "place_country",
        "number_of_actions", "award_description",
        "was_terminated", "termination_type",
        "solicitation_id",
    ]
    out = agg[out_cols].copy()
    out.to_csv(OUTPUT_CSV, index=False)
    print(f"\nWrote {len(out):,} contracts to {OUTPUT_CSV}")

    # ---- Summary ----
    print(f"\n--- Summary ---")
    print(f"Total contracts:     {len(out):,}")
    terminated = out["was_terminated"].sum()
    print(f"Terminated:          {terminated:,} ({terminated/len(out)*100:.1f}%)")
    if terminated > 0:
        for tt, n in out["termination_type"].value_counts().items():
            if tt:
                print(f"  {tt:25s}  {n:>6,}")
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
