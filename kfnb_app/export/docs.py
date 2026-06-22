"""
kfnb_app/export/docs.py — 데이터 상품 문서 생성.

투자기관이 한국어 지식 없이 바로 쓸 수 있도록 README / data dictionary /
version metadata 를 만든다. (data_spec)
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

__version__ = "0.2.0"

# 컬럼 사전 (data_dictionary)
DICTIONARY = [
    ("daily_sales_en.csv", "transaction_date / date", "거래 일자 (YYYYMMDD)"),
    ("daily_sales_en.csv", "isin", "상장사 ISIN (안정 회사 ID)"),
    ("daily_sales_en.csv", "company_name_en", "공식 영문 회사명"),
    ("daily_sales_en.csv", "brand_id", "표준 브랜드 ID (언어무관)"),
    ("daily_sales_en.csv", "brand_name_en", "표준 영문 브랜드명"),
    ("daily_sales_en.csv", "sku_id", "표준 SKU ID (언어무관)"),
    ("daily_sales_en.csv", "sku_name_en", "표준 영문 SKU명"),
    ("daily_sales_en.csv", "sales_amt", "매출액 (KRW, VAT 포함 여부는 원천 정의)"),
    ("daily_sales_en.csv", "sales_qty", "판매수량 (바코드별 소매 판매단위)"),
    ("company_master.csv", "company_id", "ISIN (없으면 slug)"),
    ("brand_master.csv", "brand_aliases", "매칭용 별칭 (Bloomberg/FactSet 등)"),
    ("sku_master.csv", "mapping_confidence", "high / medium / low"),
    ("sku_master.csv", "mapping_status", "verified / auto_mapped / needs_review"),
]

LIMITATIONS = """## Known Limitations
- Channel coverage: single convenience-store chain (CU) unless otherwise noted;
  not a full national panel.
- No promotion/regular-price/store-count fields in source (B-type) — demand vs.
  promotion and same-store vs. distribution effects cannot be separated yet.
- Long-tail SKUs are machine-standardized (romanized) with confidence flags;
  top revenue-contributing brands/SKUs are human-verified.
- Domestic POS does not capture export momentum (e.g., overseas-driven growth).
"""


def write_docs(out_dir: str | Path, *, source_name: str, sector_label: str,
               profile: dict, coverage: dict, qc_result: dict,
               products: list[str]) -> dict:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    files: dict[str, str] = {}

    # README.md
    s = profile.get("summary", {})
    readme = f"""# {sector_label} POS Alpha Dataset

Raw local Korean retail/POS data, standardized into a global investment-grade
dataset by the Mandata K-F&B Product Agent.

- Source: {source_name}
- Period: {s.get('period', '?')}
- Coverage: companies {s.get('companies','?')} · brands {s.get('brands','?')} · SKUs {s.get('skus','?')}
- Customer products: {', '.join(products)}

## Mapping coverage (by sales)
- Company mapped: {coverage.get('company_coverage_pct','?')}%
- SKU verified (curated): {coverage.get('sku_verified_pct','?')}%
- SKU named incl. romanized (ref only): {coverage.get('sku_named_pct','?')}%
- High-confidence: {coverage.get('high_confidence_pct','?')}%
- Listed-company sales: {coverage.get('listed_coverage_pct','?')}%

> We provide verified English mapping for top revenue-contributing brands and
> SKUs, while long-tail SKUs are machine-standardized with confidence flags.

## QC
- Warnings: {qc_result.get('n_warnings',0)} · Critical: {qc_result.get('n_critical',0)}

## Files
- data/: daily_sales_en.csv (optional), company_master.csv, brand_master.csv,
  sku_master.csv, category_master.csv
- qc/: qc_summary.csv, unmapped_items.csv, outlier_report.csv, mapping_coverage.csv
- docs/: README.md, data_dictionary.csv
- metadata/: version.json

## Design principles
1. Raw Korean names are never discarded (kept in *_ko columns).
2. IDs over names — company_id / brand_id / sku_id keep time series stable.
3. Confidence flags over perfect mapping.
4. Coverage reported by sales, not SKU count.

{LIMITATIONS}
"""
    (out / "README.md").write_text(readme, encoding="utf-8")
    files["docs/README.md"] = str(out / "README.md")

    # data_dictionary.csv
    dd = pd.DataFrame(DICTIONARY, columns=["file", "column", "description"])
    dd.to_csv(out / "data_dictionary.csv", index=False, encoding="utf-8-sig")
    files["docs/data_dictionary.csv"] = str(out / "data_dictionary.csv")

    return files


def write_version(out_dir: str | Path, *, source_name: str, sector_label: str,
                  rows: int, products: list[str]) -> dict:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    meta = {
        "product": f"{sector_label}_POS_ALPHA_DATASET",
        "version": __version__,
        "release_date": date.today().isoformat(),
        "source": source_name,
        "rows": rows,
        "customer_products": products,
        "generator": "kfnb_app (Mandata K-F&B Product Agent)",
    }
    p = out / "version.json"
    p.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"metadata/version.json": str(p)}
