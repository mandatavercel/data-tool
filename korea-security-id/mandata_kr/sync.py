"""Live sync from KRX free JSON API → refreshes the bundled CSVs and the web app.

Usage from the parent directory of the package:

    python -m mandata_kr.sync                # equity master sync + rebuild web
    python -m mandata_kr.sync --dry-run      # show what would change
    python -m mandata_kr.sync --no-web       # update CSVs only
    python -m mandata_kr.sync --no-merge     # full replace (don't preserve our enrichment)

What it does
------------
1. POSTs to `data.krx.co.kr/comm/bldAttendant/getJsonData.cmd` with the official
   "all listed equities" report (bld = MDCSTAT01901) for KOSPI, KOSDAQ, KONEX.
2. Maps KRX field names → our `equity_master.csv` schema.
3. **Merges**, preserving the hand-curated enrichment columns
   (sector_code_gics, sector_name_en, industry_name_kr, listing_date,
   dart_corp_code, kospi200, kosdaq150, krx300) for the rows we already
   have. New rows from KRX get those columns blank — fill them in over time.
4. Writes `mandata_kr/data/equity_master.csv` and `mandata_kr/data/sync_meta.json`.
5. (Unless `--no-web`) rebuilds `web/index.html` so the browser tool picks
   up the new universe immediately.

Requires only the standard library + `urllib`. No `requests` dependency.

Notes
-----
- KRX serves the response over HTTP, not HTTPS. Don't be surprised.
- KRX field-name keys differ slightly across endpoint revisions. The mapper
  below tries multiple key names per field; if KRX changes its schema, the
  fix is one dictionary update.
- The bld codes used here are unofficial and verified against the public
  page generators. KRX has rate limits — don't loop this every minute.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# --------------------------------------------------------------------------- #
# KRX endpoint config
# --------------------------------------------------------------------------- #

KRX_BASE   = "http://data.krx.co.kr"
KRX_JSON   = f"{KRX_BASE}/comm/bldAttendant/getJsonData.cmd"
KRX_REF    = f"{KRX_BASE}/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Unofficial bld codes (KRX may change them; isolated here for easy patching).
BLD_EQUITY_MASTER = "dbms/MDC/STAT/standard/MDCSTAT01901"   # 전종목 기본정보

DATA_DIR = Path(__file__).parent / "data"
EQUITY_CSV = DATA_DIR / "equity_master.csv"
META_JSON  = DATA_DIR / "sync_meta.json"
WEB_HTML   = Path(__file__).parent.parent / "web" / "index.html"

# Columns whose values we DON'T want KRX to overwrite (because we enrich them
# manually). The KRX feed leaves these blank for new rows; preserve any existing
# value on rows we already know.
PRESERVE_COLS = (
    "sector_code_gics", "sector_name_en", "industry_name_kr",
    "listing_date", "dart_corp_code",
    "kospi200", "kosdaq150", "krx300",
)
ALL_COLS = (
    "local_code", "isin", "company_name_en", "company_name_kr",
    "krx_short_en", "market",
    "sector_code_gics", "sector_name_en", "industry_name_kr",
    "listing_date", "dart_corp_code",
    "kospi200", "kosdaq150", "krx300",
)


# --------------------------------------------------------------------------- #
# HTTP layer
# --------------------------------------------------------------------------- #

def krx_post(bld: str, **params) -> list[dict]:
    """POST to KRX JSON endpoint, return the `OutBlock_1` rows."""
    body = {
        "bld": bld,
        "mktId": "ALL",
        "share": "1",
        "money": "1",
        "csvxls_isNo": "false",
    }
    body.update(params)
    data = urllib.parse.urlencode(body).encode("utf-8")
    req = urllib.request.Request(
        KRX_JSON, data=data, method="POST",
        headers={
            "User-Agent": USER_AGENT,
            "Referer":    KRX_REF,
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        },
    )
    with urllib.request.urlopen(req, timeout=45) as r:
        payload = json.loads(r.read().decode("utf-8"))
    rows = payload.get("OutBlock_1") or payload.get("output") or []
    return rows


def pick(row: dict, *names: str, default: str = "") -> str:
    """Return the first non-empty value from `row` matching any of `names`."""
    for n in names:
        v = row.get(n)
        if v is not None and str(v).strip():
            return str(v).strip()
    return default


# --------------------------------------------------------------------------- #
# Mapping KRX response → our schema
# --------------------------------------------------------------------------- #

def map_equity_row(r: dict) -> dict | None:
    local  = pick(r, "ISU_SRT_CD", "SHRT_ISU_CD", "ISU_NM_SRT_CD")
    isin   = pick(r, "ISU_CD", "STD_ISU_CD")
    kr     = pick(r, "ISU_KOR_NM", "ISU_NM", "ISU_ABBRV_KR")
    en     = pick(r, "ISU_ABBRV", "ISU_ENG_NM")
    market = pick(r, "MKT_NM", "MKT_TP_NM")
    listdt = pick(r, "LIST_DT")  # YYYYMMDD format
    if not (local and isin):
        return None
    # Normalize listing date YYYYMMDD → YYYY-MM-DD
    if listdt and len(listdt) == 8 and listdt.isdigit():
        listdt = f"{listdt[:4]}-{listdt[4:6]}-{listdt[6:]}"
    return {
        "local_code":       local.zfill(6),
        "isin":             isin,
        "company_name_en":  en or kr,
        "company_name_kr":  kr,
        "krx_short_en":     en,
        "market":           market or "KOSPI",
        # enrichment columns left blank — will be filled in by merge with existing
        "sector_code_gics": "",
        "sector_name_en":   "",
        "industry_name_kr": "",
        "listing_date":     listdt,
        "dart_corp_code":   "",
        "kospi200":         "",
        "kosdaq150":        "",
        "krx300":           "",
    }


# --------------------------------------------------------------------------- #
# CSV merge (preserve our enrichment)
# --------------------------------------------------------------------------- #

def load_existing(path: Path) -> dict[str, dict]:
    """Read the existing equity_master.csv as {local_code: row}."""
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as f:
        return {r["local_code"]: r for r in csv.DictReader(f) if r.get("local_code")}


def merge_rows(fresh: list[dict], existing: dict[str, dict]) -> list[dict]:
    """For each fresh KRX row, preserve any PRESERVE_COLS we already had."""
    out = []
    for new in fresh:
        old = existing.get(new["local_code"])
        if old:
            for col in PRESERVE_COLS:
                if not new.get(col) and old.get(col):
                    new[col] = old[col]
        out.append(new)
    return out


def write_csv(rows: list[dict], path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=ALL_COLS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in ALL_COLS})


# --------------------------------------------------------------------------- #
# Web HTML rebuild
# --------------------------------------------------------------------------- #

def rebuild_web(html_path: Path) -> int:
    """Re-embed the latest CSVs into web/index.html. Returns row count."""
    if not html_path.exists():
        return 0
    # Build the same payload shape the existing HTML expects
    eq = list(csv.DictReader(EQUITY_CSV.open(encoding="utf-8"))) if EQUITY_CSV.exists() else []
    pp_path = DATA_DIR / "preferred_pairs.csv"
    ne_path = DATA_DIR / "non_equity_underlyings.csv"
    prefs = []
    if pp_path.exists():
        prefs = [p for p in csv.DictReader(pp_path.open(encoding="utf-8"))
                 if p.get("pref_local_code") and p.get("pref_isin")]
    non_eq = []
    if ne_path.exists():
        non_eq = list(csv.DictReader(ne_path.open(encoding="utf-8")))

    pref_virtuals = [{
        "local_code": p["pref_local_code"],
        "isin": p["pref_isin"],
        "company_name_en": p.get("pref_name_en") or "",
        "company_name_kr": p["pref_name_kr"],
        "krx_short_en": "",
        "market": "PREFERRED",
        "share_class": "PREFERRED_" + p.get("pref_class","1우").replace("우","U"),
        "sector_code_gics":"", "sector_name_en":"", "industry_name_kr":"",
        "listing_date":"", "dart_corp_code":"",
        "kospi200":"", "kosdaq150":"", "krx300":"",
    } for p in prefs]
    ne_virtuals = [{
        "local_code":"", "isin": r["reference_code"],
        "company_name_en": r["name_en"], "company_name_kr": r["name_kr"],
        "krx_short_en": r["krx_short_en"], "market": r["underlying_type"],
        "share_class": r["underlying_type"].upper(),
        "sector_code_gics":"", "sector_name_en":"", "industry_name_kr":"",
        "listing_date":"", "dart_corp_code":"",
        "kospi200":"", "kosdaq150":"", "krx300":"",
    } for r in non_eq]
    all_rows = eq + pref_virtuals + ne_virtuals
    pref_pairs_min = [{
        "common_local_code": p["common_local_code"],
        "common_isin":       p["common_isin"],
        "common_name_kr":    p["common_name_kr"],
        "pref_local_code":   p["pref_local_code"],
        "pref_isin":         p["pref_isin"],
        "pref_name_kr":      p["pref_name_kr"],
        "pref_class":        p.get("pref_class",""),
    } for p in prefs]

    payload = {"rows": all_rows, "pref_pairs": pref_pairs_min}
    js = json.dumps(payload, ensure_ascii=False, separators=(",",":"))

    html = html_path.read_text(encoding="utf-8")
    new_html, n = re.subn(
        r"const PAYLOAD = \{.*?\};",
        f"const PAYLOAD = {js};",
        html, count=1, flags=re.S,
    )
    if n != 1:
        raise RuntimeError("Could not find PAYLOAD assignment in web/index.html")
    html_path.write_text(new_html, encoding="utf-8")
    return len(all_rows)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="mandata_kr.sync",
        description="Refresh equity master from KRX free JSON API.",
    )
    p.add_argument("--dry-run", action="store_true",
                   help="Show what would change without writing files.")
    p.add_argument("--no-web", action="store_true",
                   help="Skip rebuilding web/index.html.")
    p.add_argument("--no-merge", action="store_true",
                   help="Full replace — do NOT preserve existing enrichment columns.")
    args = p.parse_args(argv)

    print("=" * 64)
    print("  Mandata Korea Security ID — KRX live sync")
    print("=" * 64)
    print(f"  Fetching equity master from {KRX_BASE} …")
    t0 = time.time()
    try:
        raw = krx_post(BLD_EQUITY_MASTER)
    except urllib.error.URLError as e:
        print(f"  ✗ KRX request failed: {e}")
        print("    Are you on a network with access to data.krx.co.kr?")
        return 1
    except Exception as e:
        print(f"  ✗ Unexpected error: {e}")
        return 1
    elapsed = time.time() - t0
    print(f"  ✓ Received {len(raw)} raw rows in {elapsed:.1f}s")

    # Map → schema
    fresh = []
    skipped = 0
    for r in raw:
        m = map_equity_row(r)
        if m: fresh.append(m)
        else: skipped += 1
    print(f"  Mapped {len(fresh)} rows ({skipped} skipped — missing code or ISIN)")

    if len(fresh) < 100:
        print(f"  ✗ Only {len(fresh)} rows — KRX endpoint format may have changed.")
        print("    Check the field-name mapping in `map_equity_row()`.")
        return 2

    # Merge with existing enrichment
    existing = {} if args.no_merge else load_existing(EQUITY_CSV)
    merged = merge_rows(fresh, existing) if existing else fresh

    # Stats
    preserved = sum(
        1 for r in merged
        if existing.get(r["local_code"]) and any(r.get(c) for c in PRESERVE_COLS)
    )
    new_rows = sum(1 for r in merged if r["local_code"] not in existing)
    print(f"  Merge: {new_rows} new rows · {preserved} rows kept their enrichment")

    if args.dry_run:
        print()
        print("  [dry-run] Would write:")
        print(f"    {EQUITY_CSV}  ({len(merged)} rows)")
        print(f"    {META_JSON}")
        if not args.no_web:
            print(f"    {WEB_HTML}  (rebuild embedded data)")
        return 0

    # Write CSV
    EQUITY_CSV.parent.mkdir(parents=True, exist_ok=True)
    write_csv(merged, EQUITY_CSV)
    print(f"  ✓ Wrote {EQUITY_CSV} ({len(merged)} rows)")

    # Write sync meta
    meta = {
        "last_synced_utc":  datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source":           "KRX data.krx.co.kr (MDCSTAT01901)",
        "row_count":        len(merged),
        "new_rows":         new_rows,
        "enrichment_preserved": preserved,
    }
    META_JSON.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  ✓ Wrote {META_JSON}")

    if not args.no_web:
        try:
            web_n = rebuild_web(WEB_HTML)
            print(f"  ✓ Rebuilt {WEB_HTML} (embedded {web_n} records)")
        except Exception as e:
            print(f"  ⚠ Web rebuild failed: {e}")

    print()
    print(f"  Done in {time.time()-t0:.1f}s. Run `python -m mandata_kr members KOSPI200` to verify.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
