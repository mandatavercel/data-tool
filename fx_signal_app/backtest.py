"""
FX Signal — 백테스트 엔진.

지난 N년에 대해, 매월 가상 입금된 USD를 어떤 방식으로 환전했어야 가장 좋았는지 시뮬레이션.

시나리오:
  - "immediate": 입금 즉시 100% 환전 (no-timing baseline)
  - "signal":    신호 점수에 따라 환전 비중 조절 + 보유 기간 한도

질문: "신호 따랐을 때 그냥 즉시 환전한 것보다 KRW를 얼마나 더 받았나?"
답:   outperformance % = (signal_avg_rate / immediate_avg_rate - 1) × 100
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd

from .data import SeriesSnapshot, USDKRW_SIGN, LABELS, _snapshot_from_series
from .signals import compute_short_term, compute_mid_term


# ─────────────────────────────────────────────────────────────
# 데이터 구조
# ─────────────────────────────────────────────────────────────
@dataclass
class BacktestParams:
    monthly_deposit_usd: float = 10_000.0
    max_hold_days: int = 180                  # 6개월 이상 묵힌 USD는 강제 환전
    threshold_strong: float = -35.0           # 이 점수 이하면 100% 환전
    threshold_weak: float = -20.0             # 이 점수 이하면 50% 환전
    ratio_strong: float = 1.0
    ratio_weak: float = 0.5


@dataclass
class BacktestResult:
    trades: pd.DataFrame                      # date, usd, rate, krw, scenario, reason
    summary: pd.DataFrame                     # 시나리오별 avg_rate, total_krw, total_usd
    cumulative_rate: pd.DataFrame             # date × scenario → 누적 평균 환율
    score_series: pd.Series                   # 백테스트 기간의 일별 종합 점수
    outperformance_pct: float                 # signal vs immediate (KRW 기준 %)


# ─────────────────────────────────────────────────────────────
# 신호 점수 시계열 (백테스트 핵심)
# ─────────────────────────────────────────────────────────────
def _snap_from_partial(key: str, full_series: pd.Series, t: pd.Timestamp) -> Optional[SeriesSnapshot]:
    """full_series 의 t 이전(t 포함) 데이터로 snapshot."""
    sub = full_series.loc[:t].dropna()
    if sub.empty or len(sub) < 2:
        return None
    return _snapshot_from_series(key, sub)


def build_score_series(
    full_series_map: dict[str, pd.Series],
    rebal_dates: pd.DatetimeIndex,
    horizon: str = "combined",
) -> pd.Series:
    """
    각 rebalance 날짜 t 마다 단기+중기 점수 평균을 산출한 Series.

    horizon:
      'short'    : 단기 점수만
      'mid'      : 중기 점수만
      'combined' : (단기 + 중기) / 2  [기본]
    """
    scores: dict[pd.Timestamp, float] = {}
    for t in rebal_dates:
        snaps: dict[str, SeriesSnapshot] = {}
        for k, s in full_series_map.items():
            snap = _snap_from_partial(k, s, t)
            if snap is not None:
                snaps[k] = snap
        if "USDKRW" not in snaps:
            continue
        short = compute_short_term(snaps)
        mid = compute_mid_term(snaps)
        if horizon == "short":
            scores[t] = short.score
        elif horizon == "mid":
            scores[t] = mid.score
        else:
            scores[t] = (short.score + mid.score) / 2.0
    return pd.Series(scores, name="score").sort_index()


# ─────────────────────────────────────────────────────────────
# 시뮬레이션 엔진
# ─────────────────────────────────────────────────────────────
def _is_first_business_day_of_month(dates: pd.DatetimeIndex, i: int) -> bool:
    """dates[i]가 그 달의 첫 영업일인지."""
    if i == 0:
        return True
    return dates[i].month != dates[i - 1].month


def simulate_immediate(usdkrw: pd.Series, params: BacktestParams) -> pd.DataFrame:
    """입금 즉시 100% 환전. baseline."""
    dates = usdkrw.index
    rows = []
    for i, t in enumerate(dates):
        if _is_first_business_day_of_month(dates, i):
            rate = float(usdkrw.iloc[i])
            usd = params.monthly_deposit_usd
            rows.append({
                "date": t, "usd": usd, "rate": rate, "krw": usd * rate,
                "scenario": "즉시 환전", "reason": "입금일 즉시",
            })
    return pd.DataFrame(rows)


def simulate_signal(
    usdkrw: pd.Series,
    score_series: pd.Series,
    params: BacktestParams,
) -> pd.DataFrame:
    """
    신호 기반 환전.
      - 매월 첫 영업일 입금
      - 신호 점수가 임계값 이하일 때 풀의 비중만큼 환전 (FIFO)
      - max_hold_days 이상 묵힌 USD는 강제 환전 (운영자금 리스크)
      - 기간 종료 시 잔여 USD는 마지막 환율로 청산 (공정 비교)
    """
    dates = usdkrw.index
    # usd_pool: list of [deposit_date, usd_remaining]
    usd_pool: list[list] = []
    rows = []

    for i, t in enumerate(dates):
        # 1) 입금
        if _is_first_business_day_of_month(dates, i):
            usd_pool.append([t, float(params.monthly_deposit_usd)])

        rate = float(usdkrw.iloc[i])

        # 2) 6개월 이상 묵힌 USD 강제 환전
        new_pool = []
        forced = 0.0
        for dep_date, usd in usd_pool:
            if (t - dep_date).days >= params.max_hold_days:
                forced += usd
            else:
                new_pool.append([dep_date, usd])
        if forced > 0:
            rows.append({
                "date": t, "usd": forced, "rate": rate, "krw": forced * rate,
                "scenario": "신호 기반", "reason": f"강제 ({params.max_hold_days}일 보유 한도)",
            })
        usd_pool = new_pool

        # 3) 신호 점수 → 환전 비중
        if t not in score_series.index:
            continue
        score = float(score_series.loc[t])
        if score <= params.threshold_strong:
            ratio = params.ratio_strong
            reason = f"강한 환전 신호 ({score:+.0f})"
        elif score <= params.threshold_weak:
            ratio = params.ratio_weak
            reason = f"환전 신호 ({score:+.0f})"
        else:
            ratio = 0.0
            reason = None

        if ratio <= 0 or not usd_pool:
            continue

        total_pool = sum(u for _, u in usd_pool)
        to_convert = total_pool * ratio
        new_pool = []
        remaining = to_convert
        converted_total = 0.0
        for dep_date, usd in usd_pool:
            if remaining <= 1e-6:
                new_pool.append([dep_date, usd])
                continue
            if usd <= remaining:
                converted_total += usd
                remaining -= usd
            else:
                converted_total += remaining
                new_pool.append([dep_date, usd - remaining])
                remaining = 0.0
        usd_pool = new_pool
        if converted_total > 0:
            rows.append({
                "date": t, "usd": converted_total, "rate": rate,
                "krw": converted_total * rate,
                "scenario": "신호 기반", "reason": reason,
            })

    # 4) 기간 종료 잔여 청산
    final_rate = float(usdkrw.iloc[-1])
    final_date = dates[-1]
    leftover = sum(u for _, u in usd_pool)
    if leftover > 0:
        rows.append({
            "date": final_date, "usd": leftover, "rate": final_rate,
            "krw": leftover * final_rate,
            "scenario": "신호 기반", "reason": "기간 종료 청산",
        })

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────
# 최상위 — 전체 백테스트
# ─────────────────────────────────────────────────────────────
def run_backtest(
    full_series_map: dict[str, pd.Series],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    params: BacktestParams,
) -> BacktestResult:
    """
    full_series_map: USDKRW 외 매크로 지표들의 전체 시계열.
        - 'USDKRW' 키는 반드시 있어야 함.
        - 다른 지표는 있으면 사용, 없으면 신호가 단순화됨.

    start_date / end_date: 백테스트 기간.

    Returns: BacktestResult with trade log, summary, cumulative rates, score series.
    """
    if "USDKRW" not in full_series_map:
        raise ValueError("full_series_map에 USDKRW가 반드시 있어야 합니다.")

    usdkrw_full = full_series_map["USDKRW"].dropna()
    mask = (usdkrw_full.index >= start_date) & (usdkrw_full.index <= end_date)
    usdkrw = usdkrw_full.loc[mask]
    if len(usdkrw) < 30:
        raise ValueError(f"백테스트 기간 데이터가 너무 짧음 ({len(usdkrw)} days)")

    # 1) 각 영업일의 신호 점수 (combined)
    score_series = build_score_series(full_series_map, usdkrw.index, horizon="combined")

    # 2) 시나리오별 시뮬레이션
    trades_immediate = simulate_immediate(usdkrw, params)
    trades_signal = simulate_signal(usdkrw, score_series, params)

    all_trades = pd.concat([trades_immediate, trades_signal], ignore_index=True)
    all_trades["date"] = pd.to_datetime(all_trades["date"])

    # 3) 시나리오별 누적 평균 환율 = cumsum(krw) / cumsum(usd)
    def _cum_rate(df: pd.DataFrame) -> pd.Series:
        df = df.sort_values("date").reset_index(drop=True)
        cum_krw = df["krw"].cumsum()
        cum_usd = df["usd"].cumsum()
        s = pd.Series((cum_krw / cum_usd).values, index=df["date"].values, name="rate")
        # 같은 날짜 중복 시 마지막 값만 (강제+신호 환전이 같은 날 동시 일어날 수 있음)
        s = s[~s.index.duplicated(keep="last")]
        return s

    cum_imm = _cum_rate(trades_immediate)
    cum_sig = _cum_rate(trades_signal)

    cum_df = pd.DataFrame({"즉시 환전": cum_imm, "신호 기반": cum_sig})
    cum_df = cum_df.sort_index().ffill()

    # 4) 시나리오별 요약
    def _summarize(df: pd.DataFrame, name: str) -> dict:
        return {
            "시나리오": name,
            "환전 횟수": int(len(df)),
            "누적 USD": float(df["usd"].sum()),
            "누적 KRW": float(df["krw"].sum()),
            "평균 실효 환율": float(df["krw"].sum() / df["usd"].sum()) if df["usd"].sum() > 0 else float("nan"),
        }

    summary = pd.DataFrame([
        _summarize(trades_immediate, "즉시 환전"),
        _summarize(trades_signal, "신호 기반"),
    ])

    # 5) outperformance (KRW 받은 양 기준)
    sig_avg = summary.loc[summary["시나리오"] == "신호 기반", "평균 실효 환율"].iloc[0]
    imm_avg = summary.loc[summary["시나리오"] == "즉시 환전", "평균 실효 환율"].iloc[0]
    outperf = (sig_avg / imm_avg - 1.0) * 100.0 if imm_avg > 0 else 0.0

    return BacktestResult(
        trades=all_trades,
        summary=summary,
        cumulative_rate=cum_df,
        score_series=score_series,
        outperformance_pct=float(outperf),
    )
