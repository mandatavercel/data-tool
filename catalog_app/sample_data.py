"""
Mandata Data Catalog — 회사별 샘플 데이터 생성 (월별 시계열).

기관투자자가 카탈로그에서 회사를 카트에 담으면 받게 될 실제 데이터의
형태를 시뮬레이션. 카탈로그 메타(revenue_ltm_usd_m, yoy_growth, mom_growth,
signal_score, market_cap_usd)에서 파생.

설계:
    • 결정적 — 회사명 해시를 시드로 사용해 같은 회사면 항상 같은 시리즈
    • 합리적 — 매출은 revenue_ltm 으로 캘리브레이션, YoY/MoM 으로 trend
    • 다중 메트릭 — 매출 USD, 거래건수, 고유 이용자, 시그널 점수 history
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
import hashlib
import numpy as np
import pandas as pd


# 카탈로그 행 → 결정적 RNG 시드
def _seed(company: str, offset: int = 0) -> int:
    h = hashlib.md5(company.encode("utf-8")).hexdigest()
    return (int(h[:8], 16) + offset) & 0xFFFFFFFF


def _row_get(row: Any, col: str, default: float = 0.0) -> float:
    if hasattr(row, "get"):
        v = row.get(col, default)
    else:
        v = row[col] if col in getattr(row, "index", []) else default
    try:
        if pd.isna(v):
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _month_labels(n_months: int, end: datetime | None = None) -> list[str]:
    """최근 n_months 개월의 YYYY-MM 라벨 (오름차순)."""
    end = end or datetime.now()
    labels: list[str] = []
    # 이번 달부터 거꾸로
    y, m = end.year, end.month
    for _ in range(n_months):
        labels.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return list(reversed(labels))


# ── 월별 매출/거래/이용자 시계열 ──────────────────────────────────────────
def monthly_aggregates(row: Any, n_months: int = 24) -> pd.DataFrame:
    """한 회사의 월별 매출·거래·이용자·시그널 시계열.

    Columns: month, revenue_usd_m, transactions, unique_users, mom_pct,
             yoy_pct, signal_score
    """
    company = str(row.get("company", "")) if hasattr(row, "get") else str(row["company"])
    rng = np.random.default_rng(_seed(company))

    rev_ltm = _row_get(row, "revenue_ltm_usd_m", 100.0)  # USD millions
    yoy     = _row_get(row, "yoy_growth", 10.0) / 100.0  # decimal
    mom     = _row_get(row, "mom_growth", 1.0) / 100.0
    sig     = _row_get(row, "signal_score", 0.5)
    mc      = _row_get(row, "market_cap_usd", 1000.0)
    sector = str(row.get("sector", "")) if hasattr(row, "get") else ""

    # 평균 월 매출 (USD millions)
    base_monthly = max(rev_ltm / 12.0, 0.5)

    # 월 단위 성장률 — yoy 의 12승근으로 일관성
    monthly_trend = (1.0 + yoy) ** (1.0 / 12.0) if yoy > -0.95 else 0.95

    # 시리즈 생성 — 중심점(현재 - n/2)에서 base = base_monthly 가 되도록
    revenues: list[float] = []
    center = n_months / 2
    for i in range(n_months):
        offset = i - center
        trend_val = base_monthly * (monthly_trend ** offset)
        noise = rng.normal(0, 0.06)  # 6% 변동
        seasonal = 1.0 + 0.04 * np.sin(2 * np.pi * (i % 12) / 12)  # 약한 계절성
        val = trend_val * (1.0 + noise) * seasonal
        revenues.append(max(val, base_monthly * 0.1))

    # 마지막 달 (= 현재) 은 MoM 으로 미세조정
    if len(revenues) >= 2:
        revenues[-1] = revenues[-2] * (1.0 + mom + rng.normal(0, 0.02))

    revenues_arr = np.array(revenues)

    # 거래건수 — 매출 / 평균 객단가. 섹터에 따라 다름
    avg_ticket_map = {
        "음식료·식품": 8.0, "가정용품·생활용품": 25.0,
        "패션·의류": 70.0, "뷰티·화장품": 45.0, "여행·레저": 250.0,
        "자동차": 25000.0,
        "미디어": 15.0, "통신": 50.0,
        "반도체": 5000.0, "소프트웨어": 200.0, "하드웨어": 800.0,
        "제약·바이오": 80.0, "의료기기": 1500.0,
        "기계·장비": 12000.0, "운송·물류": 80.0,
        "은행": 200.0, "보험": 800.0, "증권": 60.0,
        "석유·가스": 60.0, "신재생": 400.0, "전력·가스": 100.0,
        "리츠·부동산": 2000.0, "화학": 600.0, "철강·금속": 1200.0,
    }
    avg_ticket = avg_ticket_map.get(str(sector), 100.0)
    transactions = (revenues_arr * 1_000_000 / avg_ticket).astype(int)

    # 고유 이용자 — 거래 대비 0.3~0.7배 (반복 구매 고려)
    user_ratio = 0.3 + 0.4 * rng.random()
    unique_users = (transactions * user_ratio).astype(int)

    # MoM·YoY 산출
    mom_pct = np.zeros(n_months)
    yoy_pct = np.zeros(n_months)
    for i in range(n_months):
        if i > 0:
            mom_pct[i] = (revenues_arr[i] / revenues_arr[i-1] - 1.0) * 100
        if i >= 12:
            yoy_pct[i] = (revenues_arr[i] / revenues_arr[i-12] - 1.0) * 100

    # Signal score history — 현재값 sig 기준으로 ±0.1 진동
    sig_hist = np.clip(
        sig + rng.normal(0, 0.04, n_months).cumsum() * 0.3,
        0.0, 1.0,
    )

    months = _month_labels(n_months)
    return pd.DataFrame({
        "month":         months,
        "revenue_usd_m": np.round(revenues_arr, 3),
        "transactions":  transactions,
        "unique_users":  unique_users,
        "mom_pct":       np.round(mom_pct, 2),
        "yoy_pct":       np.round(yoy_pct, 2),
        "signal_score":  np.round(sig_hist, 3),
    })


# ── 다중 회사 시계열 (long format) ────────────────────────────────────────
def monthly_aggregates_multi(catalog: pd.DataFrame, companies: list[str],
                             n_months: int = 24) -> pd.DataFrame:
    """여러 회사를 stacked long-format DataFrame 으로."""
    if not companies:
        return pd.DataFrame()
    sub = catalog[catalog["company"].isin(companies)]
    frames = []
    for _, row in sub.iterrows():
        df = monthly_aggregates(row, n_months=n_months)
        df.insert(0, "company", row["company"])
        df.insert(1, "ticker", str(row.get("ticker", "")) if hasattr(row, "get") else "")
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ── 소스별 월별 기여도 (다운로드용) ──────────────────────────────────────
def monthly_by_source(row: pd.Series, selected_sources: list[str],
                      n_months: int = 24) -> pd.DataFrame:
    """한 회사의 *선택된 소스*별 월별 매출 추정.

    Returns wide format: month, src_card, src_pos, ... (USD millions)
    각 소스의 기여도 = 회사 총매출 × (소스 coverage% / 합산 coverage%)
    """
    from catalog_app.sources import coverage_col, has_col

    base = monthly_aggregates(row, n_months=n_months)
    out = pd.DataFrame({"month": base["month"], "total_revenue_usd_m": base["revenue_usd_m"]})

    # 회사가 *실제로 보유* 한 소스 중 selected_sources 교집합
    matched: list[tuple[str, float]] = []
    for k in selected_sources:
        hc, cc = has_col(k), coverage_col(k)
        if hc not in row.index or not bool(row.get(hc, False)):
            continue
        cov = float(row.get(cc, 0.0) or 0.0)
        if cov > 0:
            matched.append((k, cov))

    if not matched:
        return out

    total_cov = sum(c for _, c in matched)
    # 매출 = total × source_cov / total_cov, 약간의 source-특이 변동 추가
    company = str(row.get("company", ""))
    seed_base = _seed(company)
    for k, cov in matched:
        share = cov / total_cov
        rng = np.random.default_rng(seed_base + hash(k) & 0xFFFFFFFF)
        noise = rng.normal(1.0, 0.08, n_months)  # 8% 소스별 변동
        out[f"src_{k}"] = (base["revenue_usd_m"].values * share * noise).round(3)
    return out


def monthly_by_source_multi(catalog: pd.DataFrame, companies: list[str],
                            selected_sources: list[str],
                            n_months: int = 24) -> pd.DataFrame:
    """여러 회사 × 선택 소스 — long format."""
    if not companies or not selected_sources:
        return pd.DataFrame()
    sub = catalog[catalog["company"].isin(companies)]
    frames = []
    for _, row in sub.iterrows():
        df = monthly_by_source(row, selected_sources, n_months=n_months)
        df.insert(0, "company", row["company"])
        df.insert(1, "ticker", str(row.get("ticker", "")) if hasattr(row, "get") else "")
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
