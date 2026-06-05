"""
AR Management — 데이터 모델 + JSON 로더/세이버.

3개의 핵심 엔티티:
  - Customer  : 고객사
  - Contract  : 계약 (고객사 1 : N)
  - Invoice   : 인보이스 (계약 1 : N, 자동 생성 + 수동 보정 가능)

부수 데이터:
  - RevenueShare : 계약별 데이터 오너 배분율

저장:
  ar_app/data/{customers,contracts,invoices}.json
  Streamlit Cloud는 ephemeral fs라 변경 시 Export → GitHub 커밋 권장.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Optional


HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"
DATA_DIR.mkdir(exist_ok=True)

CUSTOMERS_PATH = DATA_DIR / "customers.json"
CONTRACTS_PATH = DATA_DIR / "contracts.json"
INVOICES_PATH = DATA_DIR / "invoices.json"
STAFF_PATH = DATA_DIR / "staff.json"


# ─────────────────────────────────────────────────────────────
# Dataclasses
# ─────────────────────────────────────────────────────────────
@dataclass
class Staff:
    """담당자/회계자 풀. 한 번 등록해두고 고객사 매핑 시 재사용."""
    id: str
    name: str
    email: str
    role: str = ""           # "AR 담당" / "회계" / "고객 담당" 등
    notes: str = ""
    created_at: str = ""


@dataclass
class Customer:
    id: str
    name: str
    biz_no: str = ""                # 사업자등록번호
    contact_name: str = ""          # 고객사 담당자 이름
    contact_email: str = ""         # 고객사 담당자 이메일 (인보이스 발행 대상)
    ar_manager_id: str = ""         # 우리 쪽 AR 담당자 Staff.id 참조
    accounting_id: str = ""         # 우리 쪽 회계 담당자 Staff.id 참조
    created_at: str = ""
    notes: str = ""


@dataclass
class RevenueShare:
    """계약별 데이터 오너 배분 — 합계가 1.0 이하여야 함 (나머지는 회사 몫)."""
    owner: str          # 데이터 오너 이름
    ratio: float        # 0.0 ~ 1.0
    contact_email: str = ""   # 배분 송금 대상 이메일
    note: str = ""


@dataclass
class Contract:
    id: str
    customer_id: str
    title: str
    contract_type: str = "annual"       # annual / monthly / one-time / custom
    start_date: str = ""                # ISO yyyy-mm-dd
    end_date: str = ""                  # ISO yyyy-mm-dd
    total_amount: float = 0.0
    currency: str = "KRW"               # KRW / USD
    billing_frequency: str = "monthly"  # monthly / quarterly / annually / one-time
    billing_day_of_month: int = 1       # 매월 발행일 (1~28 권장)
    payment_terms_days: int = 30        # 발행 후 수금 기한 (일)
    revenue_shares: list[RevenueShare] = field(default_factory=list)
    status: str = "active"              # active / paused / ended
    order_form_url: str = ""
    notes: str = ""
    created_at: str = ""


@dataclass
class Invoice:
    id: str
    contract_id: str
    customer_id: str                # 편의 캐시
    issue_date: str                 # 발행 예정일 (또는 실제 발행일)
    due_date: str                   # 수금 기한
    amount: float
    currency: str = "KRW"
    status: str = "pending"         # pending / issued / paid / overdue / void
    issued_at: str = ""             # 실제 발행 처리한 일자
    paid_at: str = ""               # 실제 수금 확인 일자
    paid_amount: float = 0.0
    notes: str = ""
    auto_generated: bool = True     # schedule.py가 만든 건지


# ─────────────────────────────────────────────────────────────
# JSON 로드/저장
# ─────────────────────────────────────────────────────────────
def _ensure_file(path: Path) -> None:
    if not path.exists():
        path.write_text("[]", encoding="utf-8")


def load_customers() -> list[Customer]:
    _ensure_file(CUSTOMERS_PATH)
    try:
        raw = json.loads(CUSTOMERS_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    result: list[Customer] = []
    valid_fields = {f for f in Customer.__dataclass_fields__}
    for c in raw:
        # backward compat: 옛 필드(ar_manager, accounting_email) 제거
        c_clean = {k: v for k, v in c.items() if k in valid_fields}
        result.append(Customer(**c_clean))
    return result


def save_customers(customers: list[Customer]) -> None:
    data = [asdict(c) for c in customers]
    CUSTOMERS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_staff() -> list[Staff]:
    _ensure_file(STAFF_PATH)
    try:
        raw = json.loads(STAFF_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    valid_fields = {f for f in Staff.__dataclass_fields__}
    return [Staff(**{k: v for k, v in s.items() if k in valid_fields}) for s in raw]


def save_staff(staff: list[Staff]) -> None:
    data = [asdict(s) for s in staff]
    STAFF_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_contracts() -> list[Contract]:
    _ensure_file(CONTRACTS_PATH)
    try:
        raw = json.loads(CONTRACTS_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    result: list[Contract] = []
    for c in raw:
        shares_raw = c.get("revenue_shares", []) or []
        shares = [RevenueShare(**s) for s in shares_raw]
        c_clean = {k: v for k, v in c.items() if k != "revenue_shares"}
        result.append(Contract(revenue_shares=shares, **c_clean))
    return result


def save_contracts(contracts: list[Contract]) -> None:
    data = [asdict(c) for c in contracts]
    CONTRACTS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_invoices() -> list[Invoice]:
    _ensure_file(INVOICES_PATH)
    try:
        raw = json.loads(INVOICES_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    return [Invoice(**i) for i in raw]


def save_invoices(invoices: list[Invoice]) -> None:
    data = [asdict(i) for i in invoices]
    INVOICES_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ─────────────────────────────────────────────────────────────
# ID 자동 생성
# ─────────────────────────────────────────────────────────────
def _next_id(prefix: str, existing_ids: list[str]) -> str:
    nums = []
    for x in existing_ids:
        if x.startswith(prefix):
            tail = x[len(prefix):]
            if tail.isdigit():
                nums.append(int(tail))
    n = max(nums, default=0) + 1
    return f"{prefix}{n:03d}"


def next_customer_id(existing: list[Customer]) -> str:
    return _next_id("C", [c.id for c in existing])


def next_contract_id(existing: list[Contract]) -> str:
    return _next_id("CT", [c.id for c in existing])


def next_staff_id(existing: list[Staff]) -> str:
    return _next_id("S", [s.id for s in existing])


def next_invoice_id(contract_id: str, issue_date: str, existing: list[Invoice]) -> str:
    """INV-{YYYY-MM}-{contract_id}-{idx}"""
    try:
        d = datetime.strptime(issue_date, "%Y-%m-%d").date()
        ym = d.strftime("%Y-%m")
    except (ValueError, TypeError):
        ym = "XXXX-XX"
    base = f"INV-{ym}-{contract_id}"
    # idx = 같은 base 가 이미 있으면 -2, -3 ...
    same = [i for i in existing if i.id.startswith(base)]
    if not same:
        return base
    return f"{base}-{len(same) + 1}"


# ─────────────────────────────────────────────────────────────
# 상태 계산 헬퍼
# ─────────────────────────────────────────────────────────────
def update_invoice_status(inv: Invoice, today: Optional[date] = None) -> Invoice:
    """
    today 기준으로 status를 자동 갱신.
      paid → 그대로
      issued 인데 today > due_date 면 overdue
      나머지는 변경 없음
    원본 변경 후 반환.
    """
    if today is None:
        today = date.today()
    if inv.status == "paid":
        return inv
    if inv.status == "void":
        return inv
    try:
        due = datetime.strptime(inv.due_date, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return inv
    if inv.status == "issued" and today > due:
        inv.status = "overdue"
    return inv


def parse_iso(s: str) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None
