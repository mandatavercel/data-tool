"""
kfnb_app/profile.py — ① 프로파일링 스테이지.

원천 데이터의 스키마/기간/커버리지/품질을 스캔해 '데이터 헬스' 구조를 만든다.
streamlit 비의존.
"""
from __future__ import annotations

from kfnb_app import config
from kfnb_app.ingest.dataio import Source


def _fmt_date(yyyymmdd) -> str:
    if yyyymmdd is None:
        return "?"
    s = str(int(yyyymmdd))
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}" if len(s) == 8 else s


def build_profile(src: Source) -> dict:
    """Source → 프로파일 dict.

    반환:
        {
          stats: {...원시 통계...},
          summary: {기간, 행수, 회사, 브랜드, SKU, 지역, 일수, ...},
          sectors: [cat_l1 …],
          quality: {null_co, null_sku, nonpos_amt, nonpos_pct, barcode_ok_pct},
        }
    """
    s = src.profile_stats()
    n_rows = int(s.get("n_rows", 0) or 0)
    n_sku = int(s.get("n_sku", 0) or 0)
    nonpos = int(s.get("nonpos_amt", 0) or 0)
    bc_ok = int(s.get("barcode_len_ok", 0) or 0)

    summary = {
        "period": f"{_fmt_date(s.get('min_d'))} ~ {_fmt_date(s.get('max_d'))}",
        "rows": n_rows,
        "companies": int(s.get("n_co", 0) or 0),
        "brands": int(s.get("n_brand", 0) or 0),
        "skus": n_sku,
        "regions": int(s.get("n_region", 0) or 0),
        "days": int(s.get("n_days", 0) or 0),
        "total_sales": float(s.get("tot_amt", 0.0) or 0.0),
    }
    quality = {
        "null_company": int(s.get("null_co", 0) or 0),
        "null_sku": int(s.get("null_sku", 0) or 0),
        "nonpos_amt": nonpos,
        "nonpos_pct": (nonpos / n_rows * 100) if n_rows else 0.0,
        "barcode_ok": bc_ok,
        "barcode_ok_pct": (bc_ok / n_sku * 100) if n_sku else 0.0,
    }
    return {
        "stats": s,
        "summary": summary,
        "sectors": list(s.get("cat_l1", []) or []),
        "quality": quality,
        "canonical_cols": getattr(src, "canonical_cols", []),
    }
