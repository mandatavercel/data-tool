"""
kfnb_app/mapping.py — ④ 회사 → 상장 식별자 매핑 스테이지.

config.COMPANY_MAP 을 사용해 SKU/패널 테이블에 KRX 종목코드·Bloomberg 티커·
ISIN·영문사명·상장여부를 부착한다. 매핑 사전에 없는 회사는 'unmapped' 로
플래그해 사람 검수 큐로 넘긴다 (human-in-the-loop 게이트).
streamlit 비의존.
"""
from __future__ import annotations

import pandas as pd

from kfnb_app import config


def dart_overlay(resolved: dict) -> dict:
    """DART resolve() 결과 → {회사명: CompanyRef}.

    ⚠️ 큐레이션 마스터(company_master.csv)가 **우선**한다. DART 이름매칭은 동명
    회사(예: 동원→동원시스템즈, CJ제일제당→CJ지주)를 잘못 잡을 수 있으므로,
    마스터에 종목코드/영문명이 있으면 그것을 신뢰하고 DART 는 *빈 칸만* 채운다.
    마스터에 없는 회사만 DART 결과로 신규 매핑한다(사용자가 ④단계에서 검수/수정)."""
    overlay = {}
    for name, r in (resolved or {}).items():
        base = config.COMPANY_MAP.get(str(name))
        # 큐레이션 우선 — 있으면 마스터 값, 없으면 DART 값
        krx = (base.krx_code if (base and base.krx_code) else r.get("krx_code", ""))
        eng = (base.company_en_official if (base and base.company_en_official)
               else r.get("company_en_official", ""))
        note = "큐레이션 마스터(DART 확인)" if base else "DART 자동해석(검수 필요)"
        overlay[str(name)] = config.CompanyRef(
            company_en=(base.company_en if base else (eng or str(name))),
            krx_code=krx, listed=bool(krx),
            slug=(base.slug if base else _slug(name)),
            company_en_official=(eng or (base.company_en_official if base else "")),
            gics_sub_code=(base.gics_sub_code if base else ""),
            gics_sub_name=(base.gics_sub_name if base else ""),
            gics_sector=(base.gics_sector if base else ""),
            note=note)
    return overlay


def _slug(name: str) -> str:
    import re
    return re.sub(r"[^A-Za-z0-9]+", "_", str(name)).upper().strip("_") or "CO"


def map_companies(df: pd.DataFrame, company_col: str = "company_kr",
                  extra_map: dict | None = None) -> pd.DataFrame:
    """company_col 기준으로 식별자 컬럼 추가. extra_map(DART 등)이 마스터를 덮어씀."""
    out = df.copy()
    cmap = {**config.COMPANY_MAP, **(extra_map or {})}

    def _f(name, attr):
        ref = cmap.get(str(name))
        if ref is None:
            return ""
        return getattr(ref, attr)

    out["company_en"] = out[company_col].map(lambda n: _f(n, "company_en"))
    out["company_en_official"] = out[company_col].map(lambda n: _f(n, "company_en_official"))
    out["company_slug"] = out[company_col].map(lambda n: _f(n, "slug"))
    out["krx_code"]   = out[company_col].map(lambda n: _f(n, "krx_code"))
    out["bbg_ticker"] = out[company_col].map(lambda n: _f(n, "bbg_ticker"))
    out["bloomberg_code"] = out[company_col].map(lambda n: _f(n, "bloomberg_code"))
    out["isin"]       = out[company_col].map(lambda n: _f(n, "isin"))
    out["gics_sub_code"] = out[company_col].map(lambda n: _f(n, "gics_sub_code"))
    out["gics_sub_name"] = out[company_col].map(lambda n: _f(n, "gics_sub_name"))
    out["gics_sector"]   = out[company_col].map(lambda n: _f(n, "gics_sector"))
    out["listed"]     = out[company_col].map(
        lambda n: cmap[str(n)].listed if str(n) in cmap else False)
    out["map_status"] = out[company_col].map(
        lambda n: ("listed" if (str(n) in cmap and cmap[str(n)].listed)
                   else "private" if str(n) in cmap
                   else "unmapped"))
    return out


def mapping_report(df: pd.DataFrame, company_col: str = "company_kr",
                   amount_col: str = "sales_amt", extra_map: dict | None = None) -> dict:
    """매핑 커버리지 리포트 — 검증 게이트 입력."""
    cmap = {**config.COMPANY_MAP, **(extra_map or {})}
    companies = sorted(set(str(c) for c in df[company_col].dropna()))
    unmapped = [c for c in companies if c not in cmap]
    private = [c for c in companies if c in cmap and not cmap[c].listed]
    listed = [c for c in companies if c in cmap and cmap[c].listed]

    total_amt = pd.to_numeric(df[amount_col], errors="coerce").sum()
    listed_amt = pd.to_numeric(
        df.loc[df[company_col].isin(listed), amount_col], errors="coerce").sum()
    unmapped_amt = pd.to_numeric(
        df.loc[df[company_col].isin(unmapped), amount_col], errors="coerce").sum()

    return {
        "companies": companies,
        "listed": listed,
        "private": private,
        "unmapped": unmapped,
        "listed_amt_pct": (listed_amt / total_amt * 100) if total_amt else 0.0,
        "unmapped_amt_pct": (unmapped_amt / total_amt * 100) if total_amt else 0.0,
    }
