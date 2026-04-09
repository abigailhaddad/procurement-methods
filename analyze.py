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


def load_data():
    df = pd.read_csv(CONTRACTS_CSV, low_memory=False)
    df["award_date"] = pd.to_datetime(df["award_date"], errors="coerce")
    df["fiscal_year"] = df["award_date"].apply(
        lambda d: d.year + 1 if d.month >= 10 else d.year if pd.notna(d) else None
    )
    df["obligated"] = pd.to_numeric(df["obligated"], errors="coerce")
    df["tradeoff_label"] = df["tradeoff_code"].map(TRADEOFF_LABELS)
    df["contract_type_label"] = df["contract_type"].map(CONTRACT_TYPE_LABELS)
    df["set_aside_label"] = df["set_aside"].map(SET_ASIDE_LABELS).fillna(df["set_aside"])
    df["naics_label"] = df["naics_code"].map(NAICS_LABELS)

    # Staff aug = T&M or Labor Hours; Deliverable = FFP
    df["engagement_type"] = df["contract_type"].map({
        "J": "Deliverable (FFP)",
        "Y": "Staff Aug (T&M)",
        "Z": "Staff Aug (Labor Hours)",
    })

    if SAM_CSV.exists():
        sam = pd.read_csv(SAM_CSV)
        sam["sam_registration_date"] = pd.to_datetime(sam["sam_registration_date"], errors="coerce")
        sam["entity_start_date"]     = pd.to_datetime(sam["entity_start_date"], errors="coerce")
        df = df.merge(sam[["uei","sam_registration_date","entity_start_date","city","state"]],
                      left_on="recipient_uei", right_on="uei", how="left")

    return df


def summary_stats(df):
    total = len(df)
    has_tp = df["tradeoff_code"].notna().sum()
    lpta   = (df["tradeoff_code"] == "LPTA").sum()
    to_    = (df["tradeoff_code"] == "TO").sum()
    has_ct = df["contract_type"].notna().sum()
    staff  = df["contract_type"].isin(["Y","Z"]).sum()
    ffp    = (df["contract_type"] == "J").sum()
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
    }


def by_fiscal_year(df):
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


def by_agency(df, top_n=25):
    sub = df[df["tradeoff_code"].isin(["LPTA","TO","O"]) & df["department"].notna()].copy()
    totals = sub.groupby("department").size().reset_index(name="total")
    lpta   = sub[sub["tradeoff_code"]=="LPTA"].groupby("department").size().reset_index(name="lpta")
    merged = totals.merge(lpta, on="department", how="left").fillna(0)
    merged["lpta_pct"] = (merged["lpta"] / merged["total"] * 100).round(1)
    merged = merged[merged["total"] >= 20].sort_values("lpta_pct", ascending=False).head(top_n)
    return merged.rename(columns={"department":"agency"})[["agency","total","lpta","lpta_pct"]].to_dict("records")


def by_contract_type(df):
    """LPTA/TO breakdown within FFP vs T&M/Labor Hours."""
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


def by_set_aside(df):
    """LPTA rate for small-business set-asides vs unrestricted."""
    sub = df[df["tradeoff_code"].isin(["LPTA","TO","O"])].copy()
    sub["is_small_biz"] = sub["set_aside"].isin(["SBA","8A","8AN","SDVOSBC","SDVOSBS","WOSB","HZC","HZS","SBP","ESB"])
    sub["is_unrestricted"] = sub["set_aside"] == "NONE"
    rows = []
    for label, mask in [("Small Business Set-Aside", sub["is_small_biz"]),
                         ("Unrestricted", sub["is_unrestricted"])]:
        g = sub[mask]
        if len(g) < 10:
            continue
        lpta = (g["tradeoff_code"] == "LPTA").sum()
        rows.append({
            "category": label,
            "total": len(g),
            "lpta": int(lpta),
            "lpta_pct": round(lpta / len(g) * 100, 1),
        })
    return rows


def top_vendors(df, n=20):
    """Top vendors by obligated amount, split by tradeoff methodology."""
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
            "name": row["recipient_name"],
            "total_contracts": int(row["total_contracts"]),
            "total_obligated_m": round(row["total_obligated"] / 1e6, 1),
            "lpta_contracts": int(row.get("contracts_LPTA", 0)),
            "to_contracts": int(row.get("contracts_TO", 0)),
            "lpta_share_pct": row["lpta_share"],
        }
        for _, row in pivot.iterrows()
    ]


def filter_options(df):
    """Unique values for dashboard filter dropdowns."""
    def clean(series, label_map=None):
        vals = sorted(series.dropna().unique().tolist())
        if label_map:
            return [{"value": v, "label": label_map.get(str(v), str(v))} for v in vals]
        return [{"value": v, "label": str(v)} for v in vals]

    fys = sorted([int(x) for x in df["fiscal_year"].dropna().unique()])
    return {
        "fiscal_years":     [{"value": y, "label": f"FY{y}"} for y in fys],
        "naics_codes":      clean(df["naics_code"], {str(k): v for k, v in NAICS_LABELS.items()}),
        "departments":      clean(df["department"]),
        "tradeoff_codes":   clean(df["tradeoff_code"], TRADEOFF_LABELS),
        "contract_types":   clean(df["contract_type"], CONTRACT_TYPE_LABELS),
    }


def main():
    if not CONTRACTS_CSV.exists():
        print(f"{CONTRACTS_CSV} not found — run fetch.py first.")
        return

    WEB_DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading data...")
    df = load_data()
    print(f"  {len(df):,} contracts loaded")

    outputs = {
        "summary.json":       summary_stats(df),
        "by_fy.json":         by_fiscal_year(df),
        "by_agency.json":     by_agency(df),
        "by_contract_type.json": by_contract_type(df),
        "by_set_aside.json":  by_set_aside(df),
        "top_vendors.json":   top_vendors(df),
        "filters.json":       filter_options(df),
    }

    for fname, data in outputs.items():
        path = WEB_DATA_DIR / fname
        path.write_text(json.dumps(data, indent=2, default=str))
        print(f"  Wrote {path}")

    print("\nDone. Commit web/data/ to deploy to Vercel/Netlify.")


if __name__ == "__main__":
    main()
