"""
AR Management — 계약 기반 수금 스케줄 계산 (인보이스 저장 없음).

계약의 billing_frequency / 기간 / fee 로 '수금 예정 시점'을 그때그때 계산한다.
실제 수금 여부는 collections.json 의 3자 서명 상태로 판단한다.

  monthly    : 매월. 회당 = yearly_fee / 12
  quarterly  : 분기마다. 회당 = quarterly_fee (없으면 yearly_fee / 4)
  annually   : 매년. 회당 = yearly_fee
  one-time   : 1건. 회당 = yearly_fee
"""
from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

from .models import Contract, parse_iso, period_key


@dataclass
class Period:
    """계약에서 계산된 1회 수금 예정."""
    contract_id: str
    customer_id: str
    key: str            # period_key(contract_id, due_date)
    label: str          # 표시용 (예: "2024 Q1")
    due_date: str       # ISO yyyy-mm-dd (수금 예정일)
    amount: float
    currency: str


def _safe_day(year: int, month: int, day: int) -> date:
    last = calendar.monthrange(year, month)[1]
    return date(year, month, min(day, last))


def _add_months(d: date, n: int, target_day: int) -> date:
    y = d.year + (d.month - 1 + n) // 12
    m = (d.month - 1 + n) % 12 + 1
    return _safe_day(y, m, target_day)


def _quarter_label(d: date) -> str:
    return f"{d.year} Q{(d.month - 1) // 3 + 1}"


def expected_collections(contract: Contract, today: Optional[date] = None) -> list[Period]:
    """계약 정보로 수금 예정(Period) 목록을 계산.
    자동 갱신 계약은 구독 종료일이 지나도 계속 갱신되므로 horizon(약 1년 뒤)까지 생성한다."""
    if today is None:
        today = date.today()
    start = parse_iso(contract.effective_date) or parse_iso(contract.initial_delivery_date)
    end = parse_iso(contract.subscription_end_date) or parse_iso(contract.termination_date)
    freq = (contract.billing_frequency or "quarterly").lower()
    cur = contract.currency or "USD"
    yearly = float(contract.yearly_fee or 0.0)
    quarterly = float(contract.quarterly_fee or 0.0) or (yearly / 4.0)

    if not start:
        return []

    periods: list[Period] = []

    def _push(d: date, amount: float, label: str):
        ds = d.isoformat()
        periods.append(Period(
            contract_id=contract.id, customer_id=contract.customer_id,
            key=period_key(contract.id, ds), label=label,
            due_date=ds, amount=round(amount, 2), currency=cur,
        ))

    if freq == "one-time":
        _push(start, yearly, start.isoformat())
        return periods

    if not end:
        return []
    # 자동 갱신이면 계약기간이 끝났어도 계속 진행 → 현재+1년까지 연장 생성
    if contract.auto_renewal:
        horizon = today + timedelta(days=365)
        if end < horizon:
            end = horizon
    if end < start:
        return []

    anchor = start.day
    if freq == "monthly":
        step, amt = 1, yearly / 12.0
    elif freq == "annually":
        step, amt = 12, yearly
    else:  # quarterly (기본)
        step, amt = 3, quarterly

    cur_d = start
    guard = 0
    while cur_d <= end and guard < 600:
        if freq == "quarterly":
            lbl = _quarter_label(cur_d)
        elif freq == "annually":
            lbl = f"{cur_d.year}년"
        else:
            lbl = cur_d.strftime("%Y-%m")
        _push(cur_d, amt, lbl)
        cur_d = _add_months(cur_d, step, anchor)
        guard += 1

    return periods
