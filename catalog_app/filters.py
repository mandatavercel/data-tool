"""
Mandata Data Catalog — 기관투자자용 고도화 필터 엔진.

설계 원칙:
    • 카탈로그 스키마 변동에 강건 — 컬럼이 없는 필터는 자동으로 비활성화
    • UI(catalog_app.py) 와 로직(여기) 분리 — UI 변경이 필터 로직에 영향 X
    • 모든 필터는 dict 기반 spec — 직렬화/공유 가능 (URL share, 저장 등)

필터 카테고리 (12):
    1. 🌍 universe       — 지역/거래소/통화/인덱스/ADR
    2. 💰 size_liquidity — 시가총액/ADV/Free float/Shortable
    3. 🏭 sector_theme   — GICS 섹터/산업/테마 태그
    4. 📦 data_quality   — 커버리지/업데이트빈도/지연/완전성/패널사이즈
    5. 📡 data_source    — 카드/웹/앱/방문객/위성/리뷰/채용공고
    6. 🎯 signal         — Score/IC/t-stat/Hit ratio/Sharpe/Lead time
    7. 📈 growth         — MoM/YoY/3M/6M 모멘텀
    8. 💼 fundamentals   — 매출/마진/ROE/P/E
    9. ⚖️ risk           — Beta/Vol/MaxDD
    10. 🌱 esg           — ESG Score/Carbon/Controversies
    11. 🔗 coverage      — DART/Stock/Consensus/News/Filings 플래그
    12. 🔎 search        — 자유 텍스트 검색 (회사/티커/ISIN)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional
import pandas as pd


# ── 필터 컬럼 메타 ────────────────────────────────────────────────────────
# 각 카테고리에 어떤 컬럼이 매핑되는지 정의. 컬럼이 카탈로그에 없으면
# UI에서 자동으로 해당 위젯이 숨겨지거나 비활성화된다.

UNIVERSE_COLS = {
    "region":          ("지역",          "categorical"),
    "country":         ("국가",          "categorical"),
    "exchange":        ("거래소",        "categorical"),
    "currency":        ("통화",          "categorical"),
    "index_member":    ("인덱스 편입",   "categorical_multi"),  # 'KOSPI200,MSCI_EM' 형태
    "is_adr":          ("ADR/이중상장",  "bool"),
}

SIZE_LIQ_COLS = {
    "market_cap_usd":  ("시가총액 (USD M)",   "numeric_log"),
    "adv_usd":         ("ADV (USD)",          "numeric_log"),
    "free_float_pct":  ("Free Float (%)",     "numeric"),
    "is_shortable":    ("공매도 가능",        "bool"),
    "is_tradeable":    ("거래 가능",          "bool"),
}

SECTOR_THEME_COLS = {
    "sector":          ("섹터",              "categorical"),
    "gics_sector":     ("GICS Sector",       "categorical"),
    "gics_industry":   ("GICS Industry",     "categorical"),
    "themes":          ("테마",              "tag_list"),  # 'AI,EV,Semiconductor' 형태
}

DATA_QUALITY_COLS = {
    "coverage_months":   ("커버리지 (개월)",        "numeric"),
    "update_frequency":  ("업데이트 빈도",          "categorical"),  # daily/weekly/monthly
    "data_latency_days": ("데이터 지연 (일)",       "numeric"),
    "completeness_pct":  ("데이터 완전성 (%)",      "numeric"),
    "panel_size":        ("패널 사이즈 (N)",        "numeric_log"),
    "n_sources":         ("데이터 소스 수",         "numeric"),
}

# 데이터 소스(원천) 필터는 메인 영역 picker 로 처리.
# 사이드바에서는 합산 커버리지·매칭 카운트만 노출.
DATA_SOURCE_COLS = {
    "combined_coverage_pct": ("선택 소스 합산 커버리지 (%)", "numeric"),
    "matched_sources_n":     ("선택 소스 매칭 개수",          "numeric"),
}

SIGNAL_COLS = {
    "signal_score":      ("시그널 점수",         "numeric_0_1"),
    "ic":                ("IC (정보계수)",       "numeric_pm"),
    "ic_tstat":          ("IC t-stat",            "numeric_pm"),
    "hit_ratio_pct":     ("Hit Ratio (%)",        "numeric"),
    "backtest_sharpe":   ("Backtest Sharpe",      "numeric_pm"),
    "lead_time_days":    ("Lead Time (일)",       "numeric"),
    "decay_half_life":   ("Decay 반감기 (일)",    "numeric"),
}

GROWTH_COLS = {
    "mom_growth":   ("MoM 성장률 (%)",     "numeric_pm"),
    "yoy_growth":   ("YoY 성장률 (%)",     "numeric_pm"),
    "growth_3m":    ("3M 모멘텀 (%)",      "numeric_pm"),
    "growth_6m":    ("6M 모멘텀 (%)",      "numeric_pm"),
    "acceleration": ("Acceleration (%p)",  "numeric_pm"),
}

FUNDAMENTALS_COLS = {
    "revenue_ltm_usd_m":    ("LTM 매출 (USD M)",     "numeric_log"),
    "revenue_growth_yoy":   ("매출 YoY (%)",          "numeric_pm"),
    "ebitda_margin_pct":    ("EBITDA 마진 (%)",       "numeric_pm"),
    "roe_pct":              ("ROE (%)",               "numeric_pm"),
    "net_debt_ebitda":      ("Net Debt / EBITDA",     "numeric_pm"),
    "forward_pe":           ("Forward P/E",            "numeric"),
}

RISK_COLS = {
    "beta":              ("Beta",                "numeric_pm"),
    "vol_ann_pct":       ("연환산 변동성 (%)",   "numeric"),
    "max_dd_pct":        ("최대낙폭 (%)",        "numeric"),  # 음수
    "earnings_vol_pct":  ("실적 변동성 (%)",     "numeric"),
}

ESG_COLS = {
    "esg_score":         ("ESG 점수 (0~100)",      "numeric"),
    "carbon_intensity":  ("탄소집약도 (tCO2/M$)",  "numeric"),
    "controversies":     ("논란 이슈 여부",        "bool"),
}

COVERAGE_FLAG_COLS = {
    "has_dart":       ("DART 매칭",          "bool"),
    "has_stock":      ("주가 데이터",        "bool"),
    "has_consensus":  ("애널리스트 컨센서스","bool"),
    "has_news":       ("뉴스 데이터",        "bool"),
    "has_filings":    ("공시 데이터",        "bool"),
}


CATEGORY_REGISTRY: dict[str, dict] = {
    "universe":        {"label": "🌍 시장·유니버스",       "cols": UNIVERSE_COLS},
    "size_liquidity":  {"label": "💰 시가총액·유동성",     "cols": SIZE_LIQ_COLS},
    "sector_theme":    {"label": "🏭 섹터·테마",            "cols": SECTOR_THEME_COLS},
    "data_quality":    {"label": "📦 데이터 품질",          "cols": DATA_QUALITY_COLS},
    "data_source":     {"label": "📡 데이터 소스 유형",     "cols": DATA_SOURCE_COLS},
    "signal":          {"label": "🎯 시그널 성과",          "cols": SIGNAL_COLS},
    "growth":          {"label": "📈 성장·모멘텀",          "cols": GROWTH_COLS},
    "fundamentals":    {"label": "💼 펀더멘털",             "cols": FUNDAMENTALS_COLS},
    "risk":            {"label": "⚖️ 리스크",               "cols": RISK_COLS},
    "esg":             {"label": "🌱 ESG",                  "cols": ESG_COLS},
    "coverage":        {"label": "🔗 커버리지 플래그",      "cols": COVERAGE_FLAG_COLS},
}


# ── 필터 상태 (dict 기반) ─────────────────────────────────────────────────
# UI는 이 형태로 selection을 만들어서 apply_filters() 에 넘긴다.
#
# 예시:
# {
#   "search": "농심",
#   "categorical": {"sector": ["음식료"], "region": ["KR", "US"]},
#   "categorical_multi": {"index_member": ["KOSPI200", "MSCI_EM"]},
#   "tag_list": {"themes": ["AI"], "data_sources": ["card","web"]},
#   "numeric": {"market_cap_usd": (1000, 100000), "signal_score": (0.3, 1.0)},
#   "bool_required": ["has_dart", "is_shortable"],     # True 만
#   "bool_excluded": ["controversies"],                  # False 만
# }


def empty_selection() -> dict[str, Any]:
    return {
        "search":            "",
        "categorical":       {},
        "categorical_multi": {},
        "tag_list":          {},
        "numeric":           {},
        "bool_required":     [],
        "bool_excluded":     [],
    }


# ── 필터 적용 ─────────────────────────────────────────────────────────────
def apply_filters(df: pd.DataFrame, sel: dict[str, Any]) -> pd.DataFrame:
    """selection 을 카탈로그에 적용. 없는 컬럼은 안전하게 무시."""
    out = df

    # 1) Free-text search (회사·티커·ISIN)
    q = (sel.get("search") or "").strip().lower()
    if q:
        mask = pd.Series(False, index=out.index)
        for c in ("company", "ticker", "isin"):
            if c in out.columns:
                mask = mask | out[c].astype(str).str.lower().str.contains(q, na=False)
        out = out[mask]

    # 2) Categorical (단일 컬럼, 여러 값 OR)
    for col, values in (sel.get("categorical") or {}).items():
        if not values or col not in out.columns:
            continue
        out = out[out[col].astype(str).isin([str(v) for v in values])]

    # 3) Categorical multi (한 행에 'A,B,C' 형태로 여러 멤버십 — 하나라도 매칭 OR)
    for col, values in (sel.get("categorical_multi") or {}).items():
        if not values or col not in out.columns:
            continue
        out = out[out[col].apply(lambda x: _any_token_match(x, values))]

    # 4) Tag list (멤버십 검사 — AND/OR 선택 가능, 기본 OR)
    for col, values in (sel.get("tag_list") or {}).items():
        if not values or col not in out.columns:
            continue
        out = out[out[col].apply(lambda x: _any_token_match(x, values))]

    # 5) Numeric range
    for col, rng in (sel.get("numeric") or {}).items():
        if col not in out.columns or rng is None:
            continue
        lo, hi = rng
        s = pd.to_numeric(out[col], errors="coerce")
        # NaN 은 일단 통과 (제외하려면 dropna 옵션 추가 가능)
        mask = s.between(lo, hi) | s.isna()
        # 사용자가 명시적으로 범위를 좁혔을 때만 NaN 제외
        if lo is not None and hi is not None:
            full_lo, full_hi = _col_range(df, col)
            if lo > full_lo or hi < full_hi:
                mask = s.between(lo, hi)
        out = out[mask]

    # 6) Bool required (True 만)
    for col in sel.get("bool_required") or []:
        if col in out.columns:
            out = out[out[col].astype(bool)]

    # 7) Bool excluded (False 만)
    for col in sel.get("bool_excluded") or []:
        if col in out.columns:
            out = out[~out[col].astype(bool)]

    return out


def _any_token_match(cell: Any, needles: list[str]) -> bool:
    """셀이 'A,B,C' 형태이거나 list 일 때 needles 중 하나라도 매칭하면 True."""
    if cell is None or (isinstance(cell, float) and pd.isna(cell)):
        return False
    if isinstance(cell, (list, tuple, set)):
        toks = {str(t).strip().lower() for t in cell}
    else:
        toks = {t.strip().lower() for t in str(cell).split(",") if t.strip()}
    needles_l = {str(n).strip().lower() for n in needles}
    return bool(toks & needles_l)


def _col_range(df: pd.DataFrame, col: str) -> tuple[float, float]:
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    if s.empty:
        return (0.0, 1.0)
    return (float(s.min()), float(s.max()))


# ── 필터 요약 (export·노트용) ─────────────────────────────────────────────
def summarize_selection(sel: dict[str, Any]) -> str:
    """선택된 필터를 사람-읽기 가능한 문자열로."""
    parts: list[str] = []
    if sel.get("search"):
        parts.append(f"검색='{sel['search']}'")
    for col, vals in (sel.get("categorical") or {}).items():
        if vals:
            parts.append(f"{col}={','.join(map(str, vals))}")
    for col, vals in (sel.get("categorical_multi") or {}).items():
        if vals:
            parts.append(f"{col}∋{{{','.join(map(str, vals))}}}")
    for col, vals in (sel.get("tag_list") or {}).items():
        if vals:
            parts.append(f"{col}∋{{{','.join(map(str, vals))}}}")
    for col, (lo, hi) in (sel.get("numeric") or {}).items():
        parts.append(f"{col}∈[{_fmt_num(lo)},{_fmt_num(hi)}]")
    for col in sel.get("bool_required") or []:
        parts.append(f"{col}=True")
    for col in sel.get("bool_excluded") or []:
        parts.append(f"{col}=False")
    return " · ".join(parts) if parts else "(필터 미적용)"


def _fmt_num(x: Any) -> str:
    if x is None:
        return "—"
    try:
        if abs(x) >= 1000:
            return f"{x:,.0f}"
        if abs(x) >= 10:
            return f"{x:.1f}"
        return f"{x:.2f}"
    except Exception:
        return str(x)


# ── 활성 필터 칩(chip) — UI 표시용 ───────────────────────────────────────
def active_chips(sel: dict[str, Any], df_full: pd.DataFrame) -> list[tuple[str, str]]:
    """현재 적용된 필터를 (라벨, 값) 튜플 리스트로 반환.

    UI 가 칩으로 그릴 때 사용. 빈 필터는 제외.
    """
    chips: list[tuple[str, str]] = []
    label_map = _label_map()

    if sel.get("search"):
        chips.append(("🔎 검색", sel["search"]))

    for col, vals in (sel.get("categorical") or {}).items():
        if vals:
            chips.append((label_map.get(col, col), ", ".join(map(str, vals))))
    for col, vals in (sel.get("categorical_multi") or {}).items():
        if vals:
            chips.append((label_map.get(col, col), " ∪ ".join(map(str, vals))))
    for col, vals in (sel.get("tag_list") or {}).items():
        if vals:
            chips.append((label_map.get(col, col), " ∪ ".join(map(str, vals))))

    for col, (lo, hi) in (sel.get("numeric") or {}).items():
        if col not in df_full.columns:
            continue
        full_lo, full_hi = _col_range(df_full, col)
        # 범위가 풀(full)과 같으면 활성 필터로 안 침
        if lo <= full_lo + 1e-9 and hi >= full_hi - 1e-9:
            continue
        chips.append((label_map.get(col, col), f"{_fmt_num(lo)} ~ {_fmt_num(hi)}"))

    for col in sel.get("bool_required") or []:
        chips.append((label_map.get(col, col), "✓ 필수"))
    for col in sel.get("bool_excluded") or []:
        chips.append((label_map.get(col, col), "✗ 제외"))

    return chips


def _label_map() -> dict[str, str]:
    m: dict[str, str] = {}
    for cat in CATEGORY_REGISTRY.values():
        for col, (lbl, _kind) in cat["cols"].items():
            m[col] = lbl
    return m


# ── 헬퍼: 어떤 카테고리가 카탈로그에 존재하는지 ──────────────────────────
def available_categories(df: pd.DataFrame) -> list[str]:
    """카탈로그에 매핑 컬럼이 1개 이상 존재하는 카테고리만 반환."""
    out: list[str] = []
    for cat_key, meta in CATEGORY_REGISTRY.items():
        if any(c in df.columns for c in meta["cols"]):
            out.append(cat_key)
    return out


def category_columns_present(df: pd.DataFrame, cat_key: str) -> list[tuple[str, str, str]]:
    """카테고리에서 카탈로그에 실제 존재하는 (column, label, kind) 만 반환."""
    meta = CATEGORY_REGISTRY.get(cat_key)
    if not meta:
        return []
    out: list[tuple[str, str, str]] = []
    for col, (lbl, kind) in meta["cols"].items():
        if col in df.columns:
            out.append((col, lbl, kind))
    return out


# ── 프리셋 ─────────────────────────────────────────────────────────────────
PRESETS: dict[str, dict[str, Any]] = {
    "🌟 High-conviction Long": {
        "numeric": {
            "signal_score":    (0.6, 1.0),
            "ic":              (0.05, 0.5),
            "hit_ratio_pct":   (55.0, 100.0),
            "backtest_sharpe": (1.0, 5.0),
        },
        "bool_required": ["has_stock", "is_tradeable"],
    },
    "🚀 Momentum + Quality": {
        "numeric": {
            "mom_growth":     (5.0, 100.0),
            "yoy_growth":     (10.0, 200.0),
            "roe_pct":        (10.0, 100.0),
        },
        "bool_required": ["has_stock"],
    },
    "💎 Large-cap Liquid (US$1B+)": {
        "numeric": {
            "market_cap_usd": (1000.0, 1e7),
            "adv_usd":        (1e6, 1e10),
        },
        "bool_required": ["is_tradeable", "is_shortable"],
    },
    "📊 Fresh Alt-Data (≤7d latency)": {
        "numeric": {
            "data_latency_days": (0.0, 7.0),
            "coverage_months":   (12.0, 240.0),
            "completeness_pct":  (80.0, 100.0),
        },
    },
    "🌱 ESG-compliant": {
        "numeric": {"esg_score": (60.0, 100.0)},
        "bool_excluded": ["controversies"],
    },
    "🇰🇷 Korea KOSPI200": {
        "categorical": {"region": ["KR"]},
        "categorical_multi": {"index_member": ["KOSPI200"]},
    },
}
