"""
analyze.py — Build summary JSON files for the dashboard from contracts_raw.csv.

Run after fetch.py (can run on partial data too).
Outputs go to web/data/ and are committed to the repo for Vercel/Netlify.

Run:
    python analyze.py
"""

import json
from pathlib import Path

import pandas as pd

CONTRACTS_CSV = Path("data/contracts_raw.csv")
SAM_CSV       = Path("data/sam_lookup.csv")
WEB_DATA_DIR  = Path("web/data")

CONTRACT_TYPE_LABELS = {
    "J": "Firm Fixed Price",
    "Y": "Time & Materials",
    "Z": "Labor Hours",
    "U": "Cost Plus Fixed Fee",
    "V": "Cost Plus Award Fee",
    "S": "Cost No Fee",
    "A": "BPA Call",
}

TRADEOFF_LABELS = {
    "LPTA": "LPTA",
    "TO":   "Best-Value Tradeoff",
    "O":    "Other",
}

EVAL_METHOD_LABELS = {
    "LPTA":                   "LPTA",
    "Best-Value Tradeoff":    "Best-Value Tradeoff",
    "Fair Opportunity":       "Fair Opportunity (IDIQ/GWAC)",
    "Negotiated Proposal":    "Negotiated Proposal",
    "Simplified Acquisition": "Simplified Acquisition",
    "Sole Source":            "Sole Source",
    "Not Competed":           "Not Competed",
    "Unknown":                "Unknown",
}

# Order for charts (competed → non-competed)
EVAL_METHOD_ORDER = [
    "LPTA", "Best-Value Tradeoff", "Fair Opportunity",
    "Negotiated Proposal", "Simplified Acquisition",
    "Sole Source", "Not Competed", "Unknown",
]

SET_ASIDE_LABELS = {
    "SBA":     "Small Business",
    "8A":      "8(a)",
    "8AN":     "8(a) Sole Source",
    "SDVOSBC": "Service-Disabled Veteran",
    "SDVOSBS": "Service-Disabled Veteran (SS)",
    "WOSB":    "Women-Owned",
    "HZC":     "HUBZone",
    "HZS":     "HUBZone Sole Source",
    "NONE":    "No Set-Aside",
    "SBP":     "Small Business Set-Aside (Partial)",
    "ESB":     "Emerging Small Business",
    "ISBEE":   "Indian Economic Enterprise",
}

NAICS_LABELS = {
    541511: "Custom Programming (541511)",
    541512: "Systems Design (541512)",
}

SMALL_BIZ_SET_ASIDES = {"SBA","8A","8AN","SDVOSBC","SDVOSBS","WOSB","HZC","HZS","SBP","ESB","ISBEE"}


def load_data() -> pd.DataFrame:
    df = pd.read_csv(CONTRACTS_CSV, low_memory=False)
    df["award_date"] = pd.to_datetime(df["award_date"], errors="coerce")
    df["fiscal_year"] = df["award_date"].apply(
        lambda d: d.year + 1 if d.month >= 10 else d.year if pd.notna(d) else None
    )
    df["obligated"] = pd.to_numeric(df["obligated"], errors="coerce")
    df["tradeoff_label"]     = df["tradeoff_code"].map(TRADEOFF_LABELS)
    df["contract_type_label"] = df["contract_type"].map(CONTRACT_TYPE_LABELS)
    df["set_aside_label"]    = df["set_aside"].map(SET_ASIDE_LABELS).fillna(df["set_aside"])

    df["engagement_type"] = df["contract_type"].map({
        "J": "Deliverable (FFP)",
        "Y": "Staff Aug (T&M)",
        "Z": "Staff Aug (Labor Hours)",
    })

    # eval_method: granular classification from build_contracts.py
    if "eval_method" not in df.columns:
        df["eval_method"] = df["tradeoff_code"].map(TRADEOFF_LABELS)
    df["eval_method_label"] = df["eval_method"].map(EVAL_METHOD_LABELS).fillna(df["eval_method"])

    # Small biz from set-aside codes (always available)
    df["is_small_biz_setaside"] = df["set_aside"].isin(SMALL_BIZ_SET_ASIDES)

    if SAM_CSV.exists():
        sam = pd.read_csv(SAM_CSV)
        sam["sam_registration_date"] = pd.to_datetime(sam["sam_registration_date"], errors="coerce")
        sam["entity_start_date"]     = pd.to_datetime(sam["entity_start_date"], errors="coerce")

        # Employee count and SBA types are optional — not all SAM extracts have them
        if "number_of_employees" in sam.columns:
            sam["num_employees"] = pd.to_numeric(
                sam["number_of_employees"].astype(str).str.strip().str.replace(",", ""),
                errors="coerce"
            )
        else:
            sam["num_employees"] = None

        if "sba_business_types" in sam.columns:
            sam["is_sba_small"] = sam["sba_business_types"].fillna("").str.len() > 0
        else:
            sam["is_sba_small"] = None

        merge_cols = ["uei", "sam_registration_date", "entity_start_date"]
        for col in ["city", "state", "num_employees", "is_sba_small", "sba_business_types"]:
            if col in sam.columns:
                merge_cols.append(col)

        df = df.merge(sam[merge_cols], left_on="recipient_uei", right_on="uei", how="left")

        # Vendor age at time of award (years)
        df["vendor_age_years"] = (df["award_date"] - df["entity_start_date"]).dt.days / 365.25
        df["vendor_age_years"] = df["vendor_age_years"].where(df["vendor_age_years"] > 0)

        # New-entrant: award within 12 months of SAM registration
        df["days_since_sam_reg"] = (df["award_date"] - df["sam_registration_date"]).dt.days
        df["is_new_entrant"] = (
            df["days_since_sam_reg"].notna() &
            (df["days_since_sam_reg"] >= 0) &
            (df["days_since_sam_reg"] < 365)
        )

        # Vendor age bins
        bins   = [0, 2, 5, 10, 20, float("inf")]
        labels = ["<2 yrs","2–5 yrs","5–10 yrs","10–20 yrs","20+ yrs"]
        df["vendor_age_bin"] = pd.cut(df["vendor_age_years"], bins=bins, labels=labels, right=False)
    else:
        df["vendor_age_years"] = None
        df["is_new_entrant"]   = None
        df["vendor_age_bin"]   = None
        df["is_sba_small"]     = None
        df["num_employees"]    = None

    return df


def summary_stats(df: pd.DataFrame) -> dict:
    total   = len(df)
    has_tp  = df["tradeoff_code"].notna().sum()
    lpta    = (df["tradeoff_code"] == "LPTA").sum()
    to_     = (df["tradeoff_code"] == "TO").sum()
    has_ct  = df["contract_type"].notna().sum()
    staff   = df["contract_type"].isin(["Y","Z"]).sum()
    ffp     = (df["contract_type"] == "J").sum()
    total_obligated = df["obligated"].sum()
    return {
        "total_contracts":       int(total),
        "total_obligated_b":     round(total_obligated / 1e9, 1),
        "has_tradeoff_pct":      round(has_tp / total * 100, 1),
        "lpta_pct":              round(lpta / has_tp * 100, 1) if has_tp else None,
        "tradeoff_pct":          round(to_  / has_tp * 100, 1) if has_tp else None,
        "has_contract_type_pct": round(has_ct / total * 100, 1),
        "staff_aug_pct":         round(staff / has_ct * 100, 1) if has_ct else None,
        "ffp_pct":               round(ffp   / has_ct * 100, 1) if has_ct else None,
        "fy_min":                int(df["fiscal_year"].min()) if df["fiscal_year"].notna().any() else None,
        "fy_max":                int(df["fiscal_year"].max()) if df["fiscal_year"].notna().any() else None,
        "unique_vendors":        int(df["recipient_uei"].nunique()) if "recipient_uei" in df.columns else None,
        "unique_agencies":       int(df["department"].nunique()) if "department" in df.columns else None,
    }


def by_eval_method(df: pd.DataFrame) -> list:
    """Full breakdown of how contracts were evaluated/competed."""
    sub = df[df["eval_method"].notna() & (df["eval_method"] != "Unknown")].copy()
    total = len(sub)
    if total == 0:
        return []

    result = []
    for method in EVAL_METHOD_ORDER:
        g = sub[sub["eval_method"] == method]
        if len(g) == 0:
            continue
        obl = g["obligated"].sum()
        result.append({
            "method":       method,
            "label":        EVAL_METHOD_LABELS.get(method, method),
            "count":        int(len(g)),
            "pct":          round(len(g) / total * 100, 1),
            "obligated_b":  round(obl / 1e9, 2),
            "obligated_pct": round(obl / sub["obligated"].sum() * 100, 1) if sub["obligated"].sum() else 0,
            "median_k":     round(g["obligated"].median() / 1000) if g["obligated"].notna().any() else None,
        })
    return result


def by_eval_method_fy(df: pd.DataFrame) -> dict:
    """Eval method breakdown by fiscal year for trend chart."""
    sub = df[df["eval_method"].notna() & (df["eval_method"] != "Unknown")].copy()
    if sub.empty:
        return {}

    result = {}
    for fy, g in sub.groupby("fiscal_year"):
        if pd.isna(fy):
            continue
        fy_total = len(g)
        methods = {}
        for method in EVAL_METHOD_ORDER:
            mg = g[g["eval_method"] == method]
            if len(mg) == 0:
                continue
            methods[method] = {
                "count": int(len(mg)),
                "pct":   round(len(mg) / fy_total * 100, 1),
            }
        result[int(fy)] = methods
    return result


def by_fiscal_year(df: pd.DataFrame) -> dict:
    sub = df[df["tradeoff_code"].isin(["LPTA","TO","O"])].copy()
    grp = sub.groupby(["fiscal_year","tradeoff_code"]).size().reset_index(name="count")
    totals = sub.groupby("fiscal_year").size().reset_index(name="total")
    grp = grp.merge(totals, on="fiscal_year")
    grp["pct"] = (grp["count"] / grp["total"] * 100).round(1)
    result = {}
    for fy, g in grp.groupby("fiscal_year"):
        if pd.isna(fy):
            continue
        result[int(fy)] = {
            row["tradeoff_code"]: {"count": int(row["count"]), "pct": row["pct"]}
            for _, row in g.iterrows()
        }
    return result


def by_agency(df: pd.DataFrame, top_n: int = 25) -> list:
    sub = df[df["tradeoff_code"].isin(["LPTA","TO","O"]) & df["department"].notna()].copy()
    totals = sub.groupby("department").size().reset_index(name="total")
    lpta   = sub[sub["tradeoff_code"]=="LPTA"].groupby("department").size().reset_index(name="lpta")
    merged = totals.merge(lpta, on="department", how="left").fillna(0)
    merged["lpta_pct"] = (merged["lpta"] / merged["total"] * 100).round(1)
    merged = merged[merged["total"] >= 20].sort_values("lpta_pct", ascending=False).head(top_n)
    return merged.rename(columns={"department":"agency"})[["agency","total","lpta","lpta_pct"]].to_dict("records")


def by_contract_type(df: pd.DataFrame) -> dict:
    sub = df[df["tradeoff_code"].isin(["LPTA","TO","O"]) & df["engagement_type"].notna()].copy()
    grp = sub.groupby(["engagement_type","tradeoff_code"]).size().reset_index(name="count")
    totals = sub.groupby("engagement_type").size().reset_index(name="total")
    grp = grp.merge(totals, on="engagement_type")
    grp["pct"] = (grp["count"] / grp["total"] * 100).round(1)
    result = {}
    for et, g in grp.groupby("engagement_type"):
        result[et] = {
            row["tradeoff_code"]: {"count": int(row["count"]), "pct": row["pct"]}
            for _, row in g.iterrows()
        }
    return result


def by_set_aside(df: pd.DataFrame) -> list:
    sub = df[df["tradeoff_code"].isin(["LPTA","TO","O"])].copy()
    rows = []
    for label, mask in [
        ("Small Business Set-Aside", sub["set_aside"].isin(SMALL_BIZ_SET_ASIDES)),
        ("Unrestricted",             sub["set_aside"] == "NONE"),
    ]:
        g = sub[mask]
        if len(g) < 10:
            continue
        lpta = (g["tradeoff_code"] == "LPTA").sum()
        rows.append({
            "category": label,
            "total":    len(g),
            "lpta":     int(lpta),
            "lpta_pct": round(lpta / len(g) * 100, 1),
        })
    return rows


def by_winner_type(df: pd.DataFrame) -> dict:
    """
    For each eval method: small biz share, market concentration, median contract
    size, new entrant share, median vendor age, employee size distribution.
    """
    methods = ["LPTA", "TO", "O"]
    result  = {}

    for method in methods:
        sub = df[df["tradeoff_code"] == method]
        if len(sub) < 10:
            continue

        # Small biz: set-aside codes (always available) OR SBA cert in SAM (if enriched)
        if df["is_sba_small"].notna().any():
            # Prefer SAM-certified small biz flag when we have it
            is_small = sub["is_sba_small"].fillna(sub["is_small_biz_setaside"])
        else:
            is_small = sub["is_small_biz_setaside"]
        small_biz_pct = is_small.mean() * 100

        # Market concentration: top-10 vendors' share of contracts
        top10_share = None
        if "recipient_uei" in sub.columns and sub["recipient_uei"].notna().any():
            vc = sub["recipient_uei"].value_counts()
            top10_share = vc.head(10).sum() / len(sub) * 100

        # Median contract size
        median_k = sub["obligated"].median() / 1000 if sub["obligated"].notna().any() else None

        # New entrant %
        new_entrant_pct = None
        if sub["is_new_entrant"].notna().any():
            new_entrant_pct = sub["is_new_entrant"].mean() * 100

        # Median vendor age at award
        median_age_yrs = None
        if sub["vendor_age_years"].notna().any():
            median_age_yrs = sub["vendor_age_years"].median()

        # Median employee count (where available)
        median_employees = None
        if "num_employees" in sub.columns and sub["num_employees"].notna().any():
            median_employees = sub["num_employees"].median()

        result[method] = {
            "small_biz_pct":        round(small_biz_pct, 1),
            "large_biz_pct":        round(100 - small_biz_pct, 1),
            "median_obligated_k":   round(median_k) if median_k is not None else None,
            "unique_vendors":       int(sub["recipient_uei"].nunique()) if "recipient_uei" in sub.columns else None,
            "top10_share_pct":      round(top10_share, 1) if top10_share is not None else None,
            "new_entrant_pct":      round(new_entrant_pct, 1) if new_entrant_pct is not None else None,
            "median_vendor_age_yrs": round(median_age_yrs, 1) if median_age_yrs is not None else None,
            "median_employees":     round(median_employees) if median_employees is not None else None,
        }

    return result


def by_vendor_age(df: pd.DataFrame) -> list:
    """LPTA vs. tradeoff split by vendor age bucket at time of award."""
    if df["vendor_age_bin"].isna().all():
        return []

    sub = df[df["tradeoff_code"].isin(["LPTA","TO","O"]) & df["vendor_age_bin"].notna()].copy()
    if sub.empty:
        return []

    grp = sub.groupby(["vendor_age_bin","tradeoff_code"], observed=True).size().reset_index(name="count")
    totals = sub.groupby("vendor_age_bin", observed=True).size().reset_index(name="total")
    grp = grp.merge(totals, on="vendor_age_bin")
    grp["pct"] = (grp["count"] / grp["total"] * 100).round(1)

    result = []
    for age_bin, g in grp.groupby("vendor_age_bin", observed=True):
        row = {"age_bin": str(age_bin), "total": int(g["total"].iloc[0])}
        for _, r in g.iterrows():
            row[r["tradeoff_code"] + "_pct"] = r["pct"]
            row[r["tradeoff_code"] + "_count"] = int(r["count"])
        result.append(row)
    return result


def top_vendors(df: pd.DataFrame, n: int = 20) -> list:
    sub = df[df["tradeoff_code"].isin(["LPTA","TO"]) & df["recipient_name"].notna()].copy()
    grp = (sub.groupby(["recipient_name","tradeoff_code"])
             .agg(contracts=("key","count"), obligated=("obligated","sum"))
             .reset_index())
    totals = (sub.groupby("recipient_name")
                .agg(total_contracts=("key","count"), total_obligated=("obligated","sum"))
                .reset_index())
    pivot = grp.pivot(index="recipient_name", columns="tradeoff_code",
                      values=["contracts","obligated"]).fillna(0)
    pivot.columns = ["_".join(c) for c in pivot.columns]
    pivot = pivot.reset_index().merge(totals, on="recipient_name")
    pivot["lpta_share"] = (pivot.get("contracts_LPTA", 0) / pivot["total_contracts"] * 100).round(1)
    pivot = pivot.sort_values("total_obligated", ascending=False).head(n)
    return [
        {
            "name":               row["recipient_name"],
            "total_contracts":    int(row["total_contracts"]),
            "total_obligated_m":  round(row["total_obligated"] / 1e6, 1),
            "lpta_contracts":     int(row.get("contracts_LPTA", 0)),
            "to_contracts":       int(row.get("contracts_TO", 0)),
            "lpta_share_pct":     row["lpta_share"],
        }
        for _, row in pivot.iterrows()
    ]


PROTESTS_CSV = Path("data/protests_matched.csv")


def protest_summary() -> dict | None:
    """Load protest data and build summary for dashboard."""
    if not PROTESTS_CSV.exists():
        return None
    protests = pd.read_csv(PROTESTS_CSV)
    if protests.empty:
        return None
    return {
        "solicitations_protested": int(len(protests)),
        "total_protests":          int(protests["protest_count"].sum()),
        "sustained":               int(protests["sustained_count"].sum()),
        "denied":                  int(protests["denied_count"].sum()),
        "dismissed":               int(protests["dismissed_count"].sum()),
        "withdrawn":               int(protests["withdrawn_count"].sum()),
        "pending":                 int(protests.get("pending_count", pd.Series([0])).sum()),
        "protests": protests.sort_values("protest_count", ascending=False).head(20).to_dict("records"),
    }


def filter_options(df: pd.DataFrame) -> dict:
    def clean(series, label_map=None):
        vals = sorted(series.dropna().unique().tolist())
        if label_map:
            return [{"value": v, "label": label_map.get(str(v), str(v))} for v in vals]
        return [{"value": v, "label": str(v)} for v in vals]

    fys = sorted([int(x) for x in df["fiscal_year"].dropna().unique()])
    return {
        "fiscal_years":   [{"value": y, "label": f"FY{y}"} for y in fys],
        "naics_codes":    clean(df["naics_code"], {str(k): v for k, v in NAICS_LABELS.items()}),
        "departments":    clean(df["department"]),
        "tradeoff_codes": clean(df["tradeoff_code"], TRADEOFF_LABELS),
        "contract_types": clean(df["contract_type"], CONTRACT_TYPE_LABELS),
    }


def main():
    if not CONTRACTS_CSV.exists():
        print(f"{CONTRACTS_CSV} not found — run fetch.py first.")
        return

    WEB_DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading data...")
    df = load_data()
    print(f"  {len(df):,} contracts loaded")

    has_sam = SAM_CSV.exists()
    if has_sam:
        enriched = df["vendor_age_years"].notna().sum()
        new_ent  = df["is_new_entrant"].sum() if df["is_new_entrant"].notna().any() else 0
        print(f"  SAM enrichment: {enriched:,} contracts with vendor age, {int(new_ent):,} new entrants")
    else:
        print("  No SAM data — run enrich_sam.py for age/size enrichment")

    outputs = {
        "summary.json":            summary_stats(df),
        "by_eval_method.json":     by_eval_method(df),
        "by_eval_method_fy.json":  by_eval_method_fy(df),
        "by_fy.json":              by_fiscal_year(df),
        "by_agency.json":          by_agency(df),
        "by_contract_type.json":   by_contract_type(df),
        "by_set_aside.json":       by_set_aside(df),
        "by_winner_type.json":     by_winner_type(df),
        "by_vendor_age.json":      by_vendor_age(df),
        "top_vendors.json":        top_vendors(df),
        "filters.json":            filter_options(df),
    }

    # Protest data (optional — only if fetch_protests.py has been run)
    protest_data = protest_summary()
    if protest_data:
        outputs["protests.json"] = protest_data
        print(f"  Protests: {protest_data['total_protests']} on {protest_data['solicitations_protested']} solicitations "
              f"({protest_data['sustained']} sustained)")
    else:
        print("  No protest data — run fetch_protests.py to add GAO protest analysis")

    for fname, data in outputs.items():
        path = WEB_DATA_DIR / fname
        path.write_text(json.dumps(data, indent=2, default=str))
        print(f"  Wrote {path}")

    print("\nDone. Commit web/data/ to deploy to Vercel/Netlify.")


if __name__ == "__main__":
    main()
