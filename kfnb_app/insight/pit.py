"""
kfnb_app/insight/pit.py — Point-in-Time (PIT) walk-forward 시그널 패널.

감사 A1/A2 리메디에이션. 두 가지를 보장한다:
  1) available_date = 월말 + 릴리즈 지연(일) — "그 시점에 입수 가능했던 날짜".
     백테스터는 trade_date >= available_date 행만 사용하면 look-ahead 가 없다.
  2) 인과적(causal) 시계열 — 각 월 t 의 시그널은 t 까지의 데이터만 사용
     (YoY=t와 t-12, MoM=t와 t-1, share=t 시점 단면). 종점 스냅샷이 아니라
     '매월의 값'을 모두 보존 → walk-forward 백테스트 가능.

종점(use-case) 신호는 이 패널의 마지막 available 행을 읽은 것일 뿐이다.
순수 pandas (테스트 가능).
"""
from __future__ import annotations

import datetime as _dt

import pandas as pd

from kfnb_app import config

SIGNALS = ["sales_yoy", "sales_mom", "asp_yoy", "share", "share_mom"]


def month_end(ym: int) -> _dt.date:
    y, m = ym // 100, ym % 100
    if m == 12:
        return _dt.date(y, 12, 31)
    return _dt.date(y, m + 1, 1) - _dt.timedelta(days=1)


def available_date(ym: int, lag_days: int | None = None) -> _dt.date:
    """월(ym)의 데이터가 입수 가능해지는 날짜 = 월말 + 릴리즈 지연."""
    lag = config.THRESHOLDS.pos_release_lag_days if lag_days is None else lag_days
    return month_end(ym) + _dt.timedelta(days=int(lag))


def day_available_date(ymd: int, lag_days: int | None = None) -> _dt.date:
    lag = config.THRESHOLDS.pos_release_lag_days if lag_days is None else lag_days
    s = str(int(ymd))
    return _dt.date(int(s[:4]), int(s[4:6]), int(s[6:8])) + _dt.timedelta(days=int(lag))


def build_pit_panel(monthly_panel: pd.DataFrame,
                    lag_days: int | None = None) -> pd.DataFrame:
    """월별 패널 → PIT walk-forward 시그널 패널 (long).

    컬럼: krx_code, ym, available_date, signal, value
    각 value 는 ym 시점까지의 데이터만으로 계산되는 인과적 시계열.
    """
    cols = ["krx_code", "ym", "available_date", "signal", "value"]
    if monthly_panel is None or monthly_panel.empty:
        return pd.DataFrame(columns=cols)
    p = monthly_panel.copy()
    if "krx_code" not in p:
        return pd.DataFrame(columns=cols)
    p = p[p["krx_code"].astype(str) != ""]
    if p.empty:
        return pd.DataFrame(columns=cols)

    tm = (p.groupby(["krx_code", "ym"])
          .agg(sales=("sales_amt", "sum"), qty=("sales_qty", "sum"))
          .reset_index())
    # 월별 단면 점유율 (매핑 유니버스 내) — t 시점만 사용 → causal
    tot = tm.groupby("ym")["sales"].transform("sum")
    tm["share"] = tm["sales"] / tot.where(tot > 0) * 100
    tm["asp"] = tm["sales"] / tm["qty"].where(tm["qty"] > 0)
    tm = tm.sort_values(["krx_code", "ym"])
    g = tm.groupby("krx_code", group_keys=False)
    tm["sales_yoy"] = g["sales"].apply(lambda s: s.pct_change(12, fill_method=None) * 100)
    tm["sales_mom"] = g["sales"].apply(lambda s: s.pct_change(1, fill_method=None) * 100)
    tm["asp_yoy"] = g["asp"].apply(lambda s: s.pct_change(12, fill_method=None) * 100)
    tm["share_mom"] = g["share"].apply(lambda s: s.diff())

    long = tm.melt(id_vars=["krx_code", "ym"], value_vars=SIGNALS,
                   var_name="signal", value_name="value").dropna(subset=["value"])
    long["available_date"] = long["ym"].map(
        lambda y: available_date(int(y), lag_days).isoformat())
    return long[cols].sort_values(["krx_code", "signal", "ym"]).reset_index(drop=True)


def latest_available(pit_panel: pd.DataFrame, as_of: _dt.date | None = None) -> pd.DataFrame:
    """as_of(기본 오늘) 기준으로 입수 가능한 가장 최신 시그널 1행/(종목,시그널)."""
    if pit_panel is None or pit_panel.empty:
        return pit_panel
    as_of = as_of or _dt.date.today()
    av = pd.to_datetime(pit_panel["available_date"]).dt.date
    usable = pit_panel[av <= as_of]
    if usable.empty:
        return usable
    return (usable.sort_values("ym").groupby(["krx_code", "signal"]).tail(1)
            .reset_index(drop=True))
