"""
build_rfp_signals.py — Aggregate RFP bundle labels for the dashboard.

Pulls every bundle from R2 under it_rfps/bundles/, tallies the regex labels
that rfp_text_pipeline.py attaches to each bundle, and writes two JSONs:

  - web/data/rfp_signals.json  — overall label share (for the bar panel)
  - web/data/rfp_bundles.json  — per-bundle metadata + snippet list
                                 (for the "browse RFPs" viewer)

Labels currently tracked (see rfp_text_pipeline.classify_bundle_text):
  - mentions_rtm     — "requirements traceability matrix" / "RTM"
  - shall_count      — number of "shall" occurrences (normalized to bool here)
  - has_agile_vocab  — sprint / agile / scrum / kanban / backlog / user story / ...
  - has_user_vocab   — end user / stakeholder / user research / UX / ...

Run:
    python3 build_rfp_signals.py                     # from R2
    python3 build_rfp_signals.py --local data/rfp_text/bundles  # local dir
"""

from __future__ import annotations

import argparse
import json
import os
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

OUT_SIGNALS = Path("web/data/rfp_signals.json")
OUT_BUNDLES = Path("web/data/rfp_bundles.json")
R2_PREFIX   = "it_rfps/bundles/"

# Regexes kept in sync with rfp_text_pipeline.classify_bundle_text. If the
# pipeline's patterns change we update here too — they're intentionally
# the same strings.
_RE = {
    "shall_count":     re.compile(r"\bshall\b", re.IGNORECASE),
    "has_user_vocab":  re.compile(
        r"\b(end[- ]?users?|stakeholders?|user\s+research|user\s+needs?|user\s+experience|ux)\b",
        re.IGNORECASE),
    "has_agile_vocab": re.compile(
        r"\b(sprint|agile|scrum|kanban|iteration|backlog|user\s+stor(y|ies)|mvp|working\s+software|ceremon(y|ies)|stand[- ]?up|retrospective)\b",
        re.IGNORECASE),
    "mentions_rtm":    re.compile(r"\brequirements?\s+traceability\s+matrix\b", re.IGNORECASE),
}

SNIPPET_RADIUS    = 120   # chars of context on each side of a match
MAX_SNIPPETS_PER_LABEL_PER_BUNDLE = 5

LABELS = [
    ("shall_count",     "Contains 'shall' clauses",        "FAR-style requirement language ('the contractor shall...')"),
    ("has_user_vocab",  "Mentions users / stakeholders",   "end users, stakeholders, user research, UX"),
    ("has_agile_vocab", "Uses agile vocabulary",           "sprints, scrum, kanban, backlog, user stories"),
    ("mentions_rtm",    "Mentions an RTM",                 "requirements traceability matrix"),
]


def iter_bundles_r2():
    import boto3
    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['CF_R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["CF_R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["CF_R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )
    bucket = os.environ["CF_R2_BUCKET"]
    p = s3.get_paginator("list_objects_v2")
    for page in p.paginate(Bucket=bucket, Prefix=R2_PREFIX):
        for o in page.get("Contents", []):
            body = s3.get_object(Bucket=bucket, Key=o["Key"])["Body"].read()
            yield json.loads(body)


def iter_bundles_local(path: Path):
    for f in sorted(path.glob("*.json")):
        yield json.loads(f.read_text())


def _snippet(text: str, start: int, end: int) -> dict:
    """Return a small snippet-around-match record."""
    s = max(0, start - SNIPPET_RADIUS)
    e = min(len(text), end + SNIPPET_RADIUS)
    before = text[s:start]
    match  = text[start:end]
    after  = text[end:e]
    # collapse whitespace / newlines so the UI can render on one line
    def clean(x: str) -> str:
        return re.sub(r"\s+", " ", x).strip()
    return {
        "before": ("… " if s > 0 else "") + clean(before),
        "match":  clean(match),
        "after":  clean(after) + (" …" if e < len(text) else ""),
    }


def extract_snippets(attachments: list[dict], description: str) -> dict:
    """For each label, return up to N snippets across all attachment text + the
    opportunity description. Keeps the filename so the UI can show provenance."""
    out = {k: [] for k in _RE}
    # Scan each source separately so we know which attachment / source a
    # snippet came from.
    sources = [(a.get("filename") or f"attachment_{i}", a.get("text") or "")
               for i, a in enumerate(attachments)]
    if description:
        sources.append(("(notice description)", description))

    for src, text in sources:
        if not text:
            continue
        for label, pat in _RE.items():
            if len(out[label]) >= MAX_SNIPPETS_PER_LABEL_PER_BUNDLE:
                continue
            for m in pat.finditer(text):
                out[label].append({"source": src, **_snippet(text, m.start(), m.end())})
                if len(out[label]) >= MAX_SNIPPETS_PER_LABEL_PER_BUNDLE:
                    break
    # drop empty keys for leaner JSON
    return {k: v for k, v in out.items() if v}


def aggregate(bundles):
    total = 0
    with_att = 0
    label_bool_hits = Counter()
    dept = Counter()
    ntype = Counter()
    posted = []
    examples = {k: [] for k, _, _ in LABELS}
    bundle_rows: list[dict] = []

    # by_dept: dept -> {total, label_key -> hit_count}
    dept_stats: dict[str, dict] = {}
    # by_month: "YYYY-MM" -> {total, label_key -> hit_count}
    month_stats: dict[str, dict] = {}

    for b in bundles:
        total += 1
        atts = b.get("attachments") or []
        if atts:
            with_att += 1
        m = b.get("metadata") or {}
        d = m.get("department") or "(none)"
        t = m.get("type") or "(none)"
        dept[d] += 1
        ntype[t] += 1
        posted_date = (m.get("posted_date") or "")[:10]
        if posted_date:
            posted.append(posted_date)
        month_key = posted_date[:7] if posted_date else None  # "YYYY-MM"

        # Recompute labels from stored text rather than trusting the
        # pipeline-time labels[] field — so regex tweaks here take effect on
        # the next rebuild without a pipeline re-run.
        full_text = "\n\n".join(
            (a.get("text") or "") for a in atts if a.get("text")
        ) + "\n\n" + (m.get("description") or "")
        labels = {
            "shall_count":     len(_RE["shall_count"].findall(full_text)),
            "has_user_vocab":  bool(_RE["has_user_vocab"].search(full_text)),
            "has_agile_vocab": bool(_RE["has_agile_vocab"].search(full_text)),
            "mentions_rtm":    bool(_RE["mentions_rtm"].search(full_text)),
        }
        snippets = extract_snippets(atts, m.get("description") or "")
        label_hits = {}
        for key, _, _ in LABELS:
            v = labels.get(key)
            hit = bool(v) if not isinstance(v, int) else v > 0
            if hit:
                label_bool_hits[key] += 1
                label_hits[key] = v
                if len(examples[key]) < 3:
                    examples[key].append({
                        "title":       m.get("title"),
                        "type":        m.get("type"),
                        "department":  d,
                        "posted_date": posted_date,
                        "ui_link":     m.get("ui_link"),
                    })

            # by_dept accumulation
            ds = dept_stats.setdefault(d, {"total": 0, **{k: 0 for k, _, _ in LABELS}})
            if key not in ds:
                ds[key] = 0
            if hit:
                ds[key] += 1

            # by_month accumulation
            if month_key:
                ms = month_stats.setdefault(month_key, {"total": 0, **{k: 0 for k, _, _ in LABELS}})
                if key not in ms:
                    ms[key] = 0
                if hit:
                    ms[key] += 1

        # increment totals outside the label loop
        dept_stats.setdefault(d, {"total": 0, **{k: 0 for k, _, _ in LABELS}})["total"] += 1
        if month_key:
            month_stats.setdefault(month_key, {"total": 0, **{k: 0 for k, _, _ in LABELS}})["total"] += 1

        bundle_rows.append({
            "notice_id":   b.get("notice_id"),
            "title":       m.get("title"),
            "type":        m.get("type"),
            "department":  d,
            "posted_date": posted_date,
            "naics":       m.get("naics_code"),
            "set_aside":   m.get("set_aside_desc") or m.get("set_aside"),
            "ui_link":     m.get("ui_link"),
            "label_hits":  label_hits,
            "attachment_count": len(atts),
            "snippets":    snippets,
        })

    bundle_rows.sort(key=lambda r: (r.get("posted_date") or "", r.get("title") or ""), reverse=True)

    label_keys = [k for k, _, _ in LABELS]

    # Build by_dept: sorted by total desc, at least 5 bundles
    by_dept = []
    for d_name, stats in sorted(dept_stats.items(), key=lambda x: -x[1]["total"]):
        n = stats["total"]
        if n < 5:
            continue
        by_dept.append({
            "dept":  d_name,
            "total": n,
            "pcts":  {k: round(stats[k] / n * 100, 1) for k in label_keys},
        })

    # Build by_month: sorted chronologically
    by_month = []
    for mo in sorted(month_stats.keys()):
        stats = month_stats[mo]
        n = stats["total"]
        by_month.append({
            "month": mo,
            "total": n,
            "pcts":  {k: round(stats[k] / n * 100, 1) for k in label_keys},
        })

    signals = {
        "generated_at":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
        "total_bundles":  total,
        "with_attachments": with_att,
        "date_range":     {"from": min(posted) if posted else None,
                           "to":   max(posted) if posted else None},
        "labels": [
            {
                "key":         key,
                "label":       label,
                "description": desc,
                "count":       label_bool_hits[key],
                "percent":     round(label_bool_hits[key] / total * 100, 1) if total else 0,
                "examples":    examples[key],
            }
            for key, label, desc in LABELS
        ],
        "top_departments": [{"name": n, "count": c} for n, c in dept.most_common(10)],
        "top_notice_types": [{"name": n, "count": c} for n, c in ntype.most_common()],
        "by_dept":  by_dept,
        "by_month": by_month,
    }
    return signals, bundle_rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--local", type=Path, default=None,
                    help="Read bundles from a local directory instead of R2")
    args = ap.parse_args()

    bundles = iter_bundles_local(args.local) if args.local else iter_bundles_r2()
    signals, bundle_rows = aggregate(bundles)

    OUT_SIGNALS.parent.mkdir(parents=True, exist_ok=True)
    OUT_SIGNALS.write_text(json.dumps(signals, indent=2))
    # The bundles file is large — dump compact (no indent) to keep it small.
    OUT_BUNDLES.write_text(json.dumps(bundle_rows, separators=(",", ":")))

    print(f"wrote {OUT_SIGNALS}  ({signals['total_bundles']} bundles, "
          f"{signals['date_range']['from']} → {signals['date_range']['to']})")
    for lb in signals["labels"]:
        print(f"  {lb['percent']:>5.1f}%  {lb['label']}  ({lb['count']})")
    size_kb = OUT_BUNDLES.stat().st_size / 1024
    print(f"wrote {OUT_BUNDLES}  ({len(bundle_rows)} rows, {size_kb:.0f} KB)")


if __name__ == "__main__":
    main()
