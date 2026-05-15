"""
Mandata Data Catalog — 가격 책정 엔진.

회사별 단가(USD) 산정 + 묶음 할인 + 세금 → 최종 결제액.

설계:
    개당 가격 = base
              × (signal_mult: 시그널 강도)
              × (size_tier: 시가총액 티어 보너스)
              + completeness_bonus
              + source_count_bonus
              + coverage_bonus
              + freshness_bonus

    묶음 할인 = qty 기반 tier (10/50/100/200/500)
    세금 = VAT 10%

가격은 회사별로 결정적(deterministic) — 같은 입력엔 항상 같은 출력.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional
import math
import pandas as pd


# ── 가격 파라미터 ─────────────────────────────────────────────────────────
BASE_PRICE_USD          = 2_000.0   # 기본 단가
SIGNAL_MULT_RANGE       = (0.5, 2.5)  # signal 0~1 → 0.5x~2.5x
COMPLETENESS_BONUS_MAX  = 400.0     # 완전성 100% 시 최대 +$400
SOURCE_BONUS_PER        = 200.0     # 소스당 +$200
COVERAGE_BONUS_PER_YEAR = 150.0     # 1년당 +$150 (캡 36개월)
FRESHNESS_BONUS_FRESH   = 250.0     # latency ≤ 7d 시 +$250
FRESHNESS_PENALTY_STALE = -200.0    # latency > 30d 시 -$200

# 시가총액 티어 (USD millions)
SIZE_TIERS = [
    (100_000.0, 1.50, "Mega-cap"),     # >= $100B
    (10_000.0,  1.30, "Large-cap"),    # >= $10B
    (2_000.0,   1.15, "Mid-cap"),      # >= $2B
    (300.0,     1.00, "Small-cap"),    # >= $300M
    (0.0,       0.85, "Micro-cap"),    # < $300M
]

# 묶음 할인 — qty 임계값 기준
VOLUME_TIERS = [
    (500, 0.35, "Top 500+ Tier"),
    (200, 0.30, "Top 200+ Tier"),
    (100, 0.25, "Top 100+ Tier"),
    (50,  0.20, "Top 50+ Tier"),
    (10,  0.10, "Bulk 10+ Tier"),
    (1,   0.00, "Standard"),
]

VAT_RATE = 0.10


# ── 단가 계산 ─────────────────────────────────────────────────────────────
@dataclass
class UnitPrice:
    """회사 한 곳의 단가 + 구성요소 breakdown."""
    company:      str
    base:         float
    signal_mult:  float          # 1.0 기준 multiplier
    size_mult:    float          # 1.0 기준 multiplier
    size_tier:    str
    completeness_bonus: float
    source_bonus:       float
    coverage_bonus:     float
    freshness_bonus:    float
    unit_price:   float

    def components_summary(self) -> list[tuple[str, float]]:
        return [
            ("Base",              self.base),
            (f"Signal × {self.signal_mult:.2f}",  self.base * (self.signal_mult - 1.0)),
            (f"Size × {self.size_mult:.2f} ({self.size_tier})", self.base * self.signal_mult * (self.size_mult - 1.0)),
            ("Data Completeness", self.completeness_bonus),
            ("Source Diversity",  self.source_bonus),
            ("History Coverage",  self.coverage_bonus),
            ("Freshness",         self.freshness_bonus),
        ]


def _signal_mult(score: float) -> float:
    lo, hi = SIGNAL_MULT_RANGE
    s = max(0.0, min(1.0, float(score) if pd.notna(score) else 0.0))
    return lo + (hi - lo) * s


def _size_tier(mc_usd_m: Optional[float]) -> tuple[float, str]:
    """시가총액(M USD) → (multiplier, label)."""
    if mc_usd_m is None or pd.isna(mc_usd_m):
        return (1.0, "Unrated")
    for threshold, mult, label in SIZE_TIERS:
        if mc_usd_m >= threshold:
            return (mult, label)
    return (1.0, "Unrated")


def _row_value(row: pd.Series, col: str, default: Any = None) -> Any:
    if col in row.index:
        v = row[col]
        if pd.isna(v):
            return default
        return v
    return default


def calc_unit_price(row: pd.Series) -> UnitPrice:
    """카탈로그 한 행 → UnitPrice."""
    signal     = float(_row_value(row, "signal_score", 0.0) or 0.0)
    mc_usd_m   = _row_value(row, "market_cap_usd", None)
    complet    = float(_row_value(row, "completeness_pct", 0.0) or 0.0)
    n_sources  = float(_row_value(row, "n_sources", 0.0) or 0.0)
    coverage_m = float(_row_value(row, "coverage_months", 0.0) or 0.0)
    latency_d  = _row_value(row, "data_latency_days", None)

    sig_mult = _signal_mult(signal)
    size_mult, size_label = _size_tier(mc_usd_m if mc_usd_m is None else float(mc_usd_m))

    base = BASE_PRICE_USD
    completeness_bonus = COMPLETENESS_BONUS_MAX * (max(0.0, complet - 50.0) / 50.0)
    source_bonus       = SOURCE_BONUS_PER * max(0.0, n_sources - 1.0)
    coverage_bonus     = COVERAGE_BONUS_PER_YEAR * min(coverage_m / 12.0, 3.0)

    if latency_d is None:
        freshness_bonus = 0.0
    else:
        lat = float(latency_d)
        if lat <= 7:
            freshness_bonus = FRESHNESS_BONUS_FRESH
        elif lat > 30:
            freshness_bonus = FRESHNESS_PENALTY_STALE
        else:
            freshness_bonus = 0.0

    unit_price = (
        base * sig_mult * size_mult
        + completeness_bonus + source_bonus + coverage_bonus + freshness_bonus
    )
    # 가격은 음수 금지, $10 단위 반올림
    unit_price = max(500.0, round(unit_price / 10.0) * 10.0)

    return UnitPrice(
        company=str(_row_value(row, "company", "")),
        base=base,
        signal_mult=sig_mult,
        size_mult=size_mult,
        size_tier=size_label,
        completeness_bonus=completeness_bonus,
        source_bonus=source_bonus,
        coverage_bonus=coverage_bonus,
        freshness_bonus=freshness_bonus,
        unit_price=unit_price,
    )


def attach_unit_price(df: pd.DataFrame) -> pd.DataFrame:
    """카탈로그 전체에 unit_price 컬럼 추가."""
    if df.empty:
        return df.assign(unit_price=pd.Series(dtype="float64"))
    prices = df.apply(calc_unit_price, axis=1)
    out = df.copy()
    out["unit_price"] = [p.unit_price for p in prices]
    return out


# ── 묶음 할인 ─────────────────────────────────────────────────────────────
def volume_tier(qty: int) -> tuple[float, str]:
    """(할인율, 라벨)."""
    for threshold, rate, label in VOLUME_TIERS:
        if qty >= threshold:
            return (rate, label)
    return (0.0, "Standard")


# ── 정렬: Top N 담기용 ────────────────────────────────────────────────────
def quality_rank(df: pd.DataFrame) -> pd.Series:
    """필터 결과를 정렬할 합성 품질 점수 (높을수록 우선).

    가용한 컬럼만 사용 — signal_score 가 핵심, IC·Sharpe·Hit ratio 가 보조.
    """
    parts: list[pd.Series] = []
    weights: list[float] = []
    if "signal_score" in df.columns:
        parts.append(pd.to_numeric(df["signal_score"], errors="coerce").fillna(0.0))
        weights.append(0.50)
    if "ic" in df.columns:
        s = pd.to_numeric(df["ic"], errors="coerce").fillna(0.0)
        # IC 는 보통 -0.1~0.3 → 0~1 로 노멀라이즈
        s = ((s + 0.10) / 0.40).clip(0, 1)
        parts.append(s)
        weights.append(0.20)
    if "backtest_sharpe" in df.columns:
        s = pd.to_numeric(df["backtest_sharpe"], errors="coerce").fillna(0.0)
        s = (s / 3.0).clip(0, 1)   # Sharpe 0~3 → 0~1
        parts.append(s)
        weights.append(0.15)
    if "hit_ratio_pct" in df.columns:
        s = pd.to_numeric(df["hit_ratio_pct"], errors="coerce").fillna(50.0)
        s = ((s - 40.0) / 30.0).clip(0, 1)   # 40~70 → 0~1
        parts.append(s)
        weights.append(0.15)
    if not parts:
        return pd.Series([0.0] * len(df), index=df.index)
    total_w = sum(weights)
    weights = [w / total_w for w in weights]
    score = sum(p * w for p, w in zip(parts, weights))
    return score


def top_n_companies(df: pd.DataFrame, n: int) -> list[str]:
    """필터 결과 중 품질 상위 n개 회사명 리스트."""
    if df.empty or n <= 0:
        return []
    if "company" not in df.columns:
        return []
    rank = quality_rank(df)
    ordered = df.assign(_q=rank).sort_values("_q", ascending=False)
    return ordered["company"].head(n).tolist()


# ── 카트 합계 계산 ────────────────────────────────────────────────────────
@dataclass
class CheckoutLine:
    company:    str
    ticker:     str
    region:     str
    sector:     str
    unit_price: float
    breakdown:  list[tuple[str, float]]


@dataclass
class CheckoutTotals:
    qty:         int
    subtotal:    float
    volume_rate: float
    volume_tier_label: str
    volume_discount:   float
    after_discount:    float
    tax:               float
    grand_total:       float


def build_checkout_lines(catalog: pd.DataFrame, cart: set[str]) -> list[CheckoutLine]:
    """카트에 담긴 회사 → CheckoutLine 리스트."""
    if not cart or catalog.empty:
        return []
    sub = catalog[catalog["company"].isin(cart)].copy()
    lines: list[CheckoutLine] = []
    for _, row in sub.iterrows():
        up = calc_unit_price(row)
        lines.append(CheckoutLine(
            company=str(_row_value(row, "company", "")),
            ticker=str(_row_value(row, "ticker", "")),
            region=str(_row_value(row, "region", "")),
            sector=str(_row_value(row, "sector", "")),
            unit_price=up.unit_price,
            breakdown=up.components_summary(),
        ))
    # company 알파벳 정렬 (결제내역 안정성)
    lines.sort(key=lambda l: l.company)
    return lines


def calc_totals(lines: list[CheckoutLine]) -> CheckoutTotals:
    qty = len(lines)
    subtotal = sum(l.unit_price for l in lines)
    rate, label = volume_tier(qty)
    discount = subtotal * rate
    after = subtotal - discount
    tax = after * VAT_RATE
    grand = after + tax
    return CheckoutTotals(
        qty=qty,
        subtotal=subtotal,
        volume_rate=rate,
        volume_tier_label=label,
        volume_discount=discount,
        after_discount=after,
        tax=tax,
        grand_total=grand,
    )


# ── 디스플레이 헬퍼 ───────────────────────────────────────────────────────
def fmt_usd(x: float) -> str:
    if x is None or pd.isna(x):
        return "—"
    sign = "-" if x < 0 else ""
    a = abs(x)
    if a >= 1_000_000:
        return f"{sign}${a/1_000_000:.2f}M"
    if a >= 1_000:
        return f"{sign}${a:,.0f}"
    return f"{sign}${a:.2f}"
