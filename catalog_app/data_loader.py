"""
카탈로그 로더 — catalog/*.parquet 자동 스캔 + 최신 사용.

데이터 소스 우선순위:
    1. 사용자 직접 업로드 (st.file_uploader) — runtime
    2. catalog/ 폴더의 최신 .parquet — repo 동봉
    3. 데모 데이터 (fallback) — 카탈로그 미존재 시 풍부한 샘플 제공

스키마 — 필수 (legacy):
    company:        str   회사명
    ticker:         str   KRX 6자리 종목코드 (없으면 빈 문자열)
    sector:         str   섹터/카테고리
    signal_score:   float 0.0~1.0 — 매출-주가 시그널 강도
    mom_growth:     float % — 최근 매출 MoM
    coverage_months int   데이터 커버 기간 (월)
    has_dart:       bool  DART 공시 연동 여부
    has_stock:      bool  주가 데이터 매칭 여부

스키마 — 확장 (기관투자자용 필터 지원):
    🌍 universe       region, country, exchange, currency, index_member, is_adr, isin
    💰 size/liquidity market_cap_usd, adv_usd, free_float_pct, is_shortable, is_tradeable
    🏭 sector/theme   gics_sector, gics_industry, themes
    📦 data quality   update_frequency, data_latency_days, completeness_pct, panel_size, n_sources
    📡 data source    data_sources
    🎯 signal         ic, ic_tstat, hit_ratio_pct, backtest_sharpe, lead_time_days, decay_half_life
    📈 growth         yoy_growth, growth_3m, growth_6m, acceleration
    💼 fundamentals   revenue_ltm_usd_m, revenue_growth_yoy, ebitda_margin_pct, roe_pct,
                      net_debt_ebitda, forward_pe
    ⚖️ risk           beta, vol_ann_pct, max_dd_pct, earnings_vol_pct
    🌱 esg            esg_score, carbon_intensity, controversies
    🔗 coverage       has_consensus, has_news, has_filings

모든 확장 컬럼은 누락 시 자동으로 합리적 기본값. 필터 UI 는 컬럼이 없으면 알아서 숨김.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional
import pandas as pd
import streamlit as st


_CATALOG_DIR = Path(__file__).parent.parent / "catalog"


def list_catalogs() -> list[Path]:
    """catalog/ 폴더의 .parquet 파일 목록 — 최신 순."""
    if not _CATALOG_DIR.exists():
        return []
    files = sorted(_CATALOG_DIR.glob("*.parquet"),
                   key=lambda p: p.stat().st_mtime, reverse=True)
    return files


def load_latest_catalog() -> Optional[pd.DataFrame]:
    """가장 최근 catalog.parquet 로드. 없으면 None."""
    files = list_catalogs()
    if not files:
        return None
    try:
        return pd.read_parquet(files[0])
    except Exception:
        return None


def load_from_upload(uploaded) -> Optional[pd.DataFrame]:
    """업로드된 parquet/xlsx 파일을 DataFrame으로."""
    try:
        if uploaded.name.endswith(".parquet"):
            return pd.read_parquet(uploaded)
        if uploaded.name.endswith(".xlsx"):
            return pd.read_excel(uploaded)
        if uploaded.name.endswith(".csv"):
            return pd.read_csv(uploaded)
    except Exception as e:
        st.error(f"파일 읽기 실패: {type(e).__name__}: {e}")
    return None


# ── 데모 카탈로그 — 글로벌 기관투자자가 보고 싶어할 필드를 풍부히 ────────
_REGIONS = {
    "KR": ("South Korea", "KRW", ["KOSPI", "KOSDAQ"], ["KOSPI200", "MSCI_EM"]),
    "US": ("United States", "USD", ["NYSE", "NASDAQ"], ["SP500", "RUSSELL_1000", "NASDAQ100"]),
    "JP": ("Japan", "JPY", ["TSE"], ["NIKKEI225", "TOPIX"]),
    "CN": ("China", "CNY", ["SSE", "SZSE"], ["CSI300", "MSCI_EM"]),
    "HK": ("Hong Kong", "HKD", ["HKEX"], ["HSI", "MSCI_EM"]),
    "EU": ("Europe", "EUR", ["LSE", "Euronext", "XETRA"], ["STOXX600", "MSCI_EU"]),
}

_GICS_SECTORS = {
    "Consumer Staples":        ["음식료·식품", "가정용품·생활용품"],
    "Consumer Discretionary":  ["패션·의류", "뷰티·화장품", "여행·레저", "자동차"],
    "Communication Services":  ["미디어", "통신"],
    "Information Technology":  ["반도체", "소프트웨어", "하드웨어"],
    "Health Care":             ["제약·바이오", "의료기기"],
    "Industrials":             ["기계·장비", "운송·물류"],
    "Financials":              ["은행", "보험", "증권"],
    "Energy":                  ["석유·가스", "신재생"],
    "Utilities":               ["전력·가스"],
    "Real Estate":             ["리츠·부동산"],
    "Materials":               ["화학", "철강·금속"],
}

_THEMES = [
    "AI", "Semiconductor", "EV", "Renewable", "Cloud", "5G",
    "Cybersecurity", "Robotics", "BioTech", "Fintech", "Metaverse", "Korean-Wave",
]

_UPDATE_FREQ = ["daily", "weekly", "monthly", "quarterly"]


def demo_catalog() -> pd.DataFrame:
    """기관투자자 필터 데모를 위한 확장 데모 카탈로그 (~120 회사, 12+ 지표)."""
    import numpy as np
    rng = np.random.default_rng(42)

    n = 120
    rows: list[dict] = []
    for i in range(n):
        # 지역 분포: KR 40%, US 30%, JP/HK/CN/EU 7~8%씩
        region = rng.choice(
            list(_REGIONS.keys()),
            p=[0.40, 0.30, 0.08, 0.08, 0.07, 0.07],
        )
        country, currency, exchanges, idx_pool = _REGIONS[region]
        exchange = rng.choice(exchanges)
        # 인덱스 멤버십: 1~3개 랜덤
        n_idx = int(rng.integers(0, 3))
        idx_member = ",".join(rng.choice(idx_pool, size=n_idx, replace=False)) if n_idx else ""

        gics = rng.choice(list(_GICS_SECTORS.keys()))
        local_sector = rng.choice(_GICS_SECTORS[gics])
        industry = f"{gics} — Industry"
        themes = ",".join(rng.choice(_THEMES, size=int(rng.integers(0, 3)), replace=False).tolist())

        # 데이터 소스 — 회사별로 각 canonical 소스에 대해 가용성 확률 → 커버리지%
        from catalog_app.sources import CANONICAL_SOURCES as _CS, coverage_col as _cc
        src_coverage: dict[str, float] = {}
        src_has: dict[str, bool] = {}
        for s_key, _lbl, _en, _desc, prob in _CS:
            has = bool(rng.random() < prob)
            if has:
                # 커버리지 — 베타 분포로 0~30%
                cov = float(rng.beta(2, 6)) * 30.0  # 평균 ~7.5%, max ~25%
                cov = round(cov, 1)
            else:
                cov = 0.0
            src_coverage[s_key] = cov
            src_has[s_key] = (cov > 0.0)

        # legacy data_sources 문자열 (호환성)
        data_sources = ",".join(k for k, h in src_has.items() if h)
        n_sources_owned = sum(src_has.values())

        # 시가총액 — 로그스케일 (10M ~ 1T USD)
        mc = float(10 ** rng.uniform(1.0, 6.0))  # USD millions
        adv = float(mc * rng.uniform(0.0005, 0.02) * 1_000_000)  # USD

        # ID
        if region == "KR":
            ticker = f"{rng.integers(1000, 999999):06d}"
            company = f"한국기업_{i:03d}"
            isin = f"KR7{ticker}007"
        elif region == "US":
            ticker = "".join(rng.choice(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), size=4))
            company = f"USCorp_{i:03d}"
            isin = f"US{rng.integers(1e8, 1e9-1):09d}1"
        else:
            ticker = f"{rng.integers(1000, 9999)}"
            company = f"{region}Co_{i:03d}"
            isin = f"{region}{rng.integers(1e8, 1e9-1):010d}"

        rows.append({
            # ── legacy 필수 ──
            "company":          company,
            "ticker":           ticker,
            "sector":           local_sector,
            "signal_score":     round(rng.uniform(0.1, 0.95), 2),
            "mom_growth":       round(rng.uniform(-30, 50), 1),
            "coverage_months":  int(rng.integers(6, 60)),
            "has_dart":         (region == "KR") and (rng.random() > 0.2),
            "has_stock":        rng.random() > 0.15,
            # ── 🌍 universe ──
            "region":           region,
            "country":          country,
            "exchange":         exchange,
            "currency":         currency,
            "index_member":     idx_member,
            "is_adr":           bool((region != "US") and (rng.random() > 0.85)),
            "isin":             isin,
            # ── 💰 size & liquidity ──
            "market_cap_usd":   round(mc, 1),
            "adv_usd":          round(adv, 0),
            "free_float_pct":   round(float(rng.uniform(15, 100)), 1),
            "is_shortable":     bool(rng.random() > 0.25),
            "is_tradeable":     bool(rng.random() > 0.05),
            # ── 🏭 sector & theme ──
            "gics_sector":      gics,
            "gics_industry":    industry,
            "themes":           themes,
            # ── 📦 data quality ──
            "update_frequency":  rng.choice(_UPDATE_FREQ, p=[0.55, 0.30, 0.10, 0.05]),
            "data_latency_days": int(rng.integers(0, 30)),
            "completeness_pct":  round(float(rng.uniform(55, 99.5)), 1),
            "panel_size":        int(10 ** rng.uniform(3, 7)),
            "n_sources":         n_sources_owned,
            # ── 📡 data sources ──
            "data_sources":      data_sources,
            # 회사별 소스 커버리지 + 보유 플래그 (확장)
            **{f"src_{k}_coverage": v for k, v in src_coverage.items()},
            **{f"src_{k}":          v for k, v in src_has.items()},
            # ── 🎯 signal performance ──
            "ic":                round(float(rng.normal(0.04, 0.06)), 3),
            "ic_tstat":          round(float(rng.normal(1.5, 1.2)), 2),
            "hit_ratio_pct":     round(float(rng.uniform(40, 70)), 1),
            "backtest_sharpe":   round(float(rng.normal(0.8, 0.7)), 2),
            "lead_time_days":    int(rng.integers(1, 90)),
            "decay_half_life":   int(rng.integers(5, 180)),
            # ── 📈 growth ──
            "yoy_growth":        round(float(rng.uniform(-25, 80)), 1),
            "growth_3m":         round(float(rng.uniform(-20, 40)), 1),
            "growth_6m":         round(float(rng.uniform(-25, 60)), 1),
            "acceleration":      round(float(rng.normal(0, 8)), 2),
            # ── 💼 fundamentals ──
            "revenue_ltm_usd_m":   round(float(mc * rng.uniform(0.3, 2.0)), 1),
            "revenue_growth_yoy":  round(float(rng.uniform(-15, 60)), 1),
            "ebitda_margin_pct":   round(float(rng.uniform(-5, 45)), 1),
            "roe_pct":             round(float(rng.uniform(-10, 40)), 1),
            "net_debt_ebitda":     round(float(rng.uniform(-2, 6)), 2),
            "forward_pe":          round(float(rng.uniform(5, 60)), 1),
            # ── ⚖️ risk ──
            "beta":              round(float(rng.normal(1.0, 0.45)), 2),
            "vol_ann_pct":       round(float(rng.uniform(15, 65)), 1),
            "max_dd_pct":        round(float(-rng.uniform(10, 70)), 1),
            "earnings_vol_pct":  round(float(rng.uniform(5, 50)), 1),
            # ── 🌱 ESG ──
            "esg_score":         round(float(rng.uniform(20, 95)), 1),
            "carbon_intensity":  round(float(rng.uniform(5, 500)), 1),
            "controversies":     bool(rng.random() > 0.85),
            # ── 🔗 coverage flags ──
            "has_consensus":     bool(rng.random() > 0.35),
            "has_news":          bool(rng.random() > 0.20),
            "has_filings":       bool(rng.random() > 0.30),
        })

    return pd.DataFrame(rows)


# ── 정규화 ────────────────────────────────────────────────────────────────
# 모든 확장 컬럼에 대해 기본값과 dtype 을 지정. 외부 카탈로그에서 누락된
# 컬럼은 자동으로 채워지고, 필터 UI 는 카탈로그에 존재하는 컬럼만 노출한다.

_DEFAULTS: dict[str, object] = {
    # legacy
    "company":          "",
    "ticker":           "",
    "sector":           "기타",
    "signal_score":     0.0,
    "mom_growth":       0.0,
    "coverage_months":  0,
    "has_dart":         False,
    "has_stock":        False,
}

_NUMERIC_COLS = [
    "signal_score", "mom_growth", "coverage_months",
    "market_cap_usd", "adv_usd", "free_float_pct",
    "data_latency_days", "completeness_pct", "panel_size", "n_sources",
    "ic", "ic_tstat", "hit_ratio_pct", "backtest_sharpe",
    "lead_time_days", "decay_half_life",
    "yoy_growth", "growth_3m", "growth_6m", "acceleration",
    "revenue_ltm_usd_m", "revenue_growth_yoy", "ebitda_margin_pct",
    "roe_pct", "net_debt_ebitda", "forward_pe",
    "beta", "vol_ann_pct", "max_dd_pct", "earnings_vol_pct",
    "esg_score", "carbon_intensity",
]

_BOOL_COLS = [
    "has_dart", "has_stock",
    "is_adr", "is_shortable", "is_tradeable",
    "controversies",
    "has_consensus", "has_news", "has_filings",
]

_STR_COLS = [
    "company", "ticker", "sector",
    "region", "country", "exchange", "currency", "index_member", "isin",
    "gics_sector", "gics_industry", "themes",
    "update_frequency", "data_sources",
]


def _normalize_ticker(t: str) -> str:
    """KR 티커는 6자리 zero-pad, 미국·일본·기타 알파벳 티커는 원형 유지."""
    s = (t or "").strip()
    if not s or s.lower() == "nan":
        return ""
    if s.isdigit() and len(s) <= 6:
        s = s.zfill(6)
        return "" if s == "000000" else s
    return s


def normalize_catalog(df: pd.DataFrame) -> pd.DataFrame:
    """필수 컬럼 보장 + 누락 컬럼은 자동 채움. 확장 컬럼은 dtype 정규화만."""
    from catalog_app.sources import SOURCE_KEYS, coverage_col, has_col
    out = df.copy()

    # 데이터 소스 컬럼 — 없으면 0/False 로 채움 (legacy 카탈로그 호환)
    for k in SOURCE_KEYS:
        cc, hc = coverage_col(k), has_col(k)
        if cc not in out.columns:
            out[cc] = 0.0
        if hc not in out.columns:
            # has 플래그가 없으면 coverage > 0 에서 파생
            out[hc] = pd.to_numeric(out[cc], errors="coerce").fillna(0.0) > 0
        # 타입 보정
        out[cc] = pd.to_numeric(out[cc], errors="coerce").fillna(0.0).clip(lower=0.0)
        out[hc] = out[hc].fillna(False).astype(bool)

    # legacy 필수 — 무조건 채움
    for c, default in _DEFAULTS.items():
        if c not in out.columns:
            out[c] = default

    # legacy 타입 보정
    out["company"]         = out["company"].astype(str)
    out["ticker"]          = out["ticker"].astype(str).map(_normalize_ticker)
    out["sector"]          = out["sector"].astype(str)
    out["signal_score"]    = pd.to_numeric(out["signal_score"], errors="coerce").fillna(0.0)
    out["mom_growth"]      = pd.to_numeric(out["mom_growth"], errors="coerce").fillna(0.0)
    out["coverage_months"] = pd.to_numeric(out["coverage_months"], errors="coerce").fillna(0).astype(int)
    out["has_dart"]        = out["has_dart"].astype(bool)
    out["has_stock"]       = out["has_stock"].astype(bool)

    # 확장 — 있을 때만 dtype 정규화
    for c in _NUMERIC_COLS:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    for c in _BOOL_COLS:
        if c in out.columns:
            out[c] = out[c].fillna(False).astype(bool)
    for c in _STR_COLS:
        if c in out.columns:
            out[c] = out[c].fillna("").astype(str)

    return out.reset_index(drop=True)
