"""CLI for mandata_kr.

Usage examples (no install required — just `python -m mandata_kr.cli` from the
parent folder, or `python -m mandata_kr` for short):

    python -m mandata_kr lookup "samsungelec"
    python -m mandata_kr lookup "삼성전자"
    python -m mandata_kr lookup "005935"               # preferred share
    python -m mandata_kr lookup "00126380"             # DART corp code
    python -m mandata_kr lookup "005930 KS Equity"     # Bloomberg ticker
    python -m mandata_kr search "한미"
    python -m mandata_kr members KOSPI200
    python -m mandata_kr members KOSDAQ150
    python -m mandata_kr validate KR7005930003
    python -m mandata_kr bulk input.csv -o output.csv  # CSV column "query"

Output is human-readable by default; pass `--json` for JSON.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import asdict
from pathlib import Path

from .identifier import (
    Identifier, validate_isin, fix_isin, sync_status,
)


def _print_record(rec, *, json_out: bool = False):
    if not rec:
        print("(no match)", file=sys.stderr)
        sys.exit(2 if not json_out else 0)
    if json_out:
        print(json.dumps(asdict(rec), ensure_ascii=False, indent=2))
        return
    print(f"{'─' * 64}")
    print(f"  {rec.name_kr or '(no Korean name)':<30}  {rec.name_en or ''}")
    print(f"{'─' * 64}")
    rows = [
        ("ISIN",            rec.isin),
        ("Local code",      rec.local_code),
        ("Bloomberg",       rec.bloomberg_ticker),
        ("RIC",             rec.ric),
        ("Market",          rec.market),
        ("Share class",     rec.share_class),
        ("Sector (GICS)",   f"{rec.sector_code_gics}  {rec.sector_name_en}" if rec.sector_code_gics else ""),
        ("Industry (KR)",   rec.industry_name_kr),
        ("Listing date",    rec.listing_date),
        ("DART corp code",  rec.dart_corp_code),
        ("DART filings",    rec.dart_url),
        ("KOSPI 200",       "✓" if rec.kospi200 else ""),
        ("KOSDAQ 150",      "✓" if rec.kosdaq150 else ""),
        ("KRX 300",         "✓" if rec.krx300 else ""),
        ("Match method",    f"{rec.match_method_human or rec.match_method}  (confidence {rec.confidence:.2f})"),
    ]
    for k, v in rows:
        if v:
            print(f"  {k:<18} {v}")
    if rec.aliases:
        print()
        print(f"  Also searchable as ─────────────────────────────────")
        for a in rec.aliases:
            note = f"  ({a['note']})" if a.get('note') else ""
            print(f"    · {a['kind']:<28} {a['value']}{note}")
    if rec.related:
        print()
        print(f"  Related instruments ────────────────────────────────")
        for rel in rec.related:
            print(f"    └ {rel['relation']:<14} {rel['local_code']}  {rel['name_kr']}  {rel.get('pref_class','')}")
    print()


def cmd_lookup(args, idr: Identifier):
    rec = idr.lookup(args.query)
    _print_record(rec, json_out=args.json)


def cmd_search(args, idr: Identifier):
    hits = idr.search(args.query, limit=args.limit)
    if args.json:
        print(json.dumps([asdict(r) for r in hits], ensure_ascii=False, indent=2))
        return
    if not hits:
        print("(no matches)", file=sys.stderr)
        sys.exit(2)
    print(f"{len(hits)} match(es) for {args.query!r}:")
    print()
    for r in hits:
        flag = " 200" if r.kospi200 else " 150" if r.kosdaq150 else "    "
        print(f"  {r.local_code}  {r.isin}  {flag}  {r.name_kr:<20}  {r.name_en}")
    print()


def cmd_members(args, idr: Identifier):
    rows = idr.members(args.index)
    if args.json:
        print(json.dumps([asdict(r) for r in rows], ensure_ascii=False, indent=2))
        return
    if not rows:
        print(f"(no members for index {args.index!r} — try KOSPI200 / KOSDAQ150 / KRX300)",
              file=sys.stderr)
        sys.exit(2)
    print(f"{args.index} — {len(rows)} member(s):")
    print()
    for r in rows:
        print(f"  {r.local_code}  {r.isin}  {r.name_kr:<24}  {r.market}")
    print()


def cmd_validate(args, idr: Identifier):
    isin = args.isin.strip().upper()
    valid = validate_isin(isin)
    if args.json:
        print(json.dumps({
            "isin": isin,
            "valid": valid,
            "corrected": fix_isin(isin),
        }, ensure_ascii=False))
        return
    if valid:
        print(f"✓  {isin}  is a valid ISIN (check digit passes)")
    else:
        corrected = fix_isin(isin)
        print(f"✗  {isin}  fails ISIN check digit")
        if corrected:
            print(f"   suggested correction: {corrected}")


def cmd_bulk(args, idr: Identifier):
    in_path = Path(args.input)
    if not in_path.exists():
        print(f"error: {in_path} not found", file=sys.stderr)
        sys.exit(1)
    out_path = Path(args.out) if args.out else in_path.with_name(
        in_path.stem + "_mandata_resolved.csv"
    )
    with in_path.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        print("error: input has no rows", file=sys.stderr)
        sys.exit(1)
    # Column to use for the query
    qcol = args.column
    if qcol not in rows[0]:
        # Heuristic — first column
        qcol = next(iter(rows[0].keys()))
    out_fields = list(rows[0].keys()) + [
        "matched_isin", "matched_local_code", "matched_name_kr", "matched_name_en",
        "matched_market", "matched_share_class", "matched_method", "matched_confidence",
    ]
    matched = 0
    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=out_fields)
        w.writeheader()
        for r in rows:
            res = idr.lookup(r.get(qcol, ""))
            if res:
                matched += 1
                r["matched_isin"]        = res.isin
                r["matched_local_code"]  = res.local_code
                r["matched_name_kr"]     = res.name_kr
                r["matched_name_en"]     = res.name_en
                r["matched_market"]      = res.market
                r["matched_share_class"] = res.share_class
                r["matched_method"]      = res.match_method
                r["matched_confidence"]  = f"{res.confidence:.3f}"
            else:
                for k in ("matched_isin","matched_local_code","matched_name_kr",
                          "matched_name_en","matched_market","matched_share_class",
                          "matched_method","matched_confidence"):
                    r[k] = ""
            w.writerow(r)
    print(f"{matched}/{len(rows)} rows resolved → {out_path}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="mandata-id",
        description="Mandata Korea security identifier — multi-modal lookup."
    )
    sp = p.add_subparsers(dest="cmd", required=True)

    p_l = sp.add_parser("lookup", help="Resolve a single identifier to a security record.")
    p_l.add_argument("query")
    p_l.add_argument("--json", action="store_true", help="JSON output")
    p_l.set_defaults(func=cmd_lookup)

    p_s = sp.add_parser("search", help="Substring search across name fields.")
    p_s.add_argument("query")
    p_s.add_argument("--limit", type=int, default=10)
    p_s.add_argument("--json", action="store_true")
    p_s.set_defaults(func=cmd_search)

    p_m = sp.add_parser("members", help="List index members (KOSPI200, KOSDAQ150, KRX300).")
    p_m.add_argument("index")
    p_m.add_argument("--json", action="store_true")
    p_m.set_defaults(func=cmd_members)

    p_v = sp.add_parser("validate", help="Validate an ISIN's check digit.")
    p_v.add_argument("isin")
    p_v.add_argument("--json", action="store_true")
    p_v.set_defaults(func=cmd_validate)

    p_b = sp.add_parser("bulk", help="Resolve a whole CSV of queries.")
    p_b.add_argument("input")
    p_b.add_argument("-o", "--out", help="Output CSV path")
    p_b.add_argument("--column", default="query",
                     help="Column name in input CSV that holds the query (default: 'query')")
    p_b.set_defaults(func=cmd_bulk)

    p_st = sp.add_parser("status", help="Show dataset freshness (last KRX sync).")
    p_st.add_argument("--json", action="store_true")
    p_st.set_defaults(func=cmd_status)

    args = p.parse_args(argv)
    idr = Identifier()
    args.func(args, idr)
    return 0


def cmd_status(args, idr: Identifier):
    meta = sync_status()
    total = len(idr.equities) + len(idr.non_equity) + len(idr.pref_pairs)
    if args.json:
        print(json.dumps({
            "equity_master_rows": len(idr.equities),
            "preferred_pairs":    len(idr.pref_pairs),
            "non_equity_rows":    len(idr.non_equity),
            "total_records":      total,
            "sync_meta":          meta,
        }, ensure_ascii=False, indent=2))
        return
    print(f"  equity_master.csv          {len(idr.equities)} rows")
    print(f"  preferred_pairs.csv        {len(idr.pref_pairs)} pairs")
    print(f"  non_equity_underlyings.csv {len(idr.non_equity)} rows")
    print(f"  TOTAL RECORDS              {total}")
    print()
    if meta:
        print(f"  Last KRX sync: {meta.get('last_synced_utc')}")
        print(f"  Source:        {meta.get('source')}")
        print(f"  Row count:     {meta.get('row_count')}")
        new = meta.get('new_rows')
        if new is not None:
            print(f"  New rows added in last sync: {new}")
    else:
        print("  Last KRX sync: NEVER — currently running on the bundled hand-curated demo set.")
        print("  To pull the full ~2,500-stock KRX universe:")
        print("    python -m mandata_kr.sync")


if __name__ == "__main__":
    raise SystemExit(main())
