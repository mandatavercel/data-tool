"""
AR Management — 인보이스 일정 자동 생성.

계약의 billing_frequency 에 따라 Invoice 시리즈 생성:
  monthly    : 매월 billing_day 에 발행. 총액 / 개월 수 = 회당 금액
  quarterly  : 분기마다 (1/4/7/10월) billing_day 에 발행
  annually   : 매년 같은 월·일에 발행
  one-time   : 단 1건 (start_date 에 발행)
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from .models import Contract, Invoice, next_invoice_id, parse_iso


def _safe_day(year: int, month: int, day: int) -> date:
    """그 달에 day 가 없으면(예: 2월 30일) 그 달의 마지막 날로."""
    import calendar
    last = calendar.monthrange(year, month)[1]
    return date(year, month, min(day, last))


def _add_months(d: date, n: int, target_day: int) -> date:
    y = d.year + (d.month - 1 + n) // 12
    m = (d.month - 1 + n) % 12 + 1
    return _safe_day(y, m, target_day)


def _months_between(start: date, end: date) -> int:
    """start 부터 end 까지 포함된 월의 수 (start, end 포함)."""
    return (end.year - start.year) * 12 + (end.month - start.month) + 1


def generate_invoice_schedule(
    contract: Contract,
    existing_invoices: Optional[list[Invoice]] = None,
) -> list[Invoice]:
    """
    contract 정보로 Invoice 시리즈 생성. existing_invoices 는 ID 충돌 회피용.
    """
    if existing_invoices is None:
        existing_invoices = []

    start = parse_iso(contract.start_date)
    end = parse_iso(contract.end_date)
    if not start:
        return []

    freq = (contract.billing_frequency or "monthly").lower()
    bday = max(1, min(28, contract.billing_day_of_month or 1))
    pay_days = max(0, contract.payment_terms_days or 30)
    currency = contract.currency or "KRW"

    invoices: list[Invoice] = []
    issue_dates: list[date] = []

    if freq == "one-time":
        issue_dates = [start]
        amount_per = float(contract.total_amount)

    elif freq == "monthly":
        if not end:
            return []
        # 첫 발행: start 가 속한 월의 billing_day. start 보다 작으면 다음 달로.
        first_candidate = _safe_day(start.year, start.month, bday)
        if first_candidate < start:
            first_candidate = _add_months(first_candidate, 1, bday)

        n_months = _months_between(first_candidate, end)
        if n_months <= 0:
            return []
        amount_per = float(contract.total_amount) / float(n_months)

        cur = first_candidate
        while cur <= end:
            issue_dates.append(cur)
            cur = _add_months(cur, 1, bday)

    elif freq == "quarterly":
        if not end:
            return []
        # 첫 발행: start 의 같은 달부터, 분기 (3개월) 간격
        first_candidate = _safe_day(start.year, start.month, bday)
        if first_candidate < start:
            first_candidate = _add_months(first_candidate, 1, bday)
        n_periods = _months_between(first_candidate, end) // 3 + (
            1 if _months_between(first_candidate, end) % 3 > 0 else 0
        )
        if n_periods <= 0:
            return []
        amount_per = float(contract.total_amount) / float(n_periods)
        cur = first_candidate
        while cur <= end:
            issue_dates.append(cur)
            cur = _add_months(cur, 3, bday)

    elif freq == "annually":
        if not end:
            return []
        first_candidate = _safe_day(start.year, start.month, bday)
        if first_candidate < start:
            first_candidate = _add_months(first_candidate, 12, bday)
        n_years = (end.year - first_candidate.year) + 1
        if n_years <= 0:
            return []
        amount_per = float(contract.total_amount) / float(n_years)
        cur = first_candidate
        while cur <= end:
            issue_dates.append(cur)
            cur = _add_months(cur, 12, bday)

    else:
        # 알 수 없는 빈도 → 1회성으로 처리
        issue_dates = [start]
        amount_per = float(contract.total_amount)

    # Invoice 객체 생성
    running_existing = list(existing_invoices)  # ID 중복 방지 누적
    for idx, issue_d in enumerate(issue_dates, start=1):
        due_d = issue_d + timedelta(days=pay_days)
        inv = Invoice(
            id=next_invoice_id(contract.id, issue_d.isoformat(), running_existing),
            contract_id=contract.id,
            customer_id=contract.customer_id,
            issue_date=issue_d.isoformat(),
            due_date=due_d.isoformat(),
            amount=round(amount_per, 2),
            currency=currency,
            status="pending",
            auto_generated=True,
        )
        invoices.append(inv)
        running_existing.append(inv)

    return invoices


def regenerate_for_contract(
    contract: Contract,
    all_invoices: list[Invoice],
) -> list[Invoice]:
    """
    특정 계약의 인보이스를 재생성.
    - 이미 issued/paid 된 인보이스는 보존
    - pending 상태인 auto-generated 인보이스는 제거 후 재생성
    """
    keep = [
        i for i in all_invoices
        if i.contract_id != contract.id or i.status not in ("pending",) or not i.auto_generated
    ]
    new_for_contract = generate_invoice_schedule(contract, existing_invoices=keep)
    return keep + new_for_contract
