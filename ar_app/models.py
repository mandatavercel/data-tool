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
SETTINGS_PATH = DATA_DIR / "settings.json"
COLLECTIONS_PATH = DATA_DIR / "collections.json"

# 수금 확인 3자 서명 역할
COLLECTION_ROLES = ("manager", "accounting", "ar")
COLLECTION_ROLE_LABELS = {"manager": "책임자", "accounting": "회계담당자", "ar": "AR담당자"}

# 배분 확인 3단계 (파트너사별): 세금계산서 발행 → 당사 지출결의 → 입금
PAYOUT_STEPS = ("tax_invoice", "expense", "paid")
PAYOUT_STEP_LABELS = {
    "tax_invoice": "세금계산서 발행", "expense": "지출결의", "paid": "입금",
}

# 통화 기본값
BASE_CURRENCY = "KRW"
DEFAULT_USD_KRW = 1380.0
DEFAULT_SETTINGS = {"usd_krw": DEFAULT_USD_KRW}


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
    """
    계약별 데이터 오너 배분.
    입력 방식 2가지 (mode):
      - "ratio"  : 배분율(0.0~1.0). 매 수금분 × ratio 를 배분.
      - "amount" : 계약금액(연간, 계약 통화) 중 가져갈 고정 금액.
                   내부적으로 ratio = amount / yearly_fee 로 환산해 동일하게 적용.
    효과 비율(effective ratio) 합계는 1.0 이하여야 함 (나머지는 회사 몫).
    """
    owner: str          # 데이터 오너 이름
    ratio: float = 0.0  # 0.0 ~ 1.0 (mode="ratio"일 때 사용)
    amount: float = 0.0 # 고정 배분액 (mode="amount"일 때 사용, 계약 통화)
    mode: str = "ratio" # "ratio" | "amount"
    contact_email: str = ""   # 배분 송금 대상 이메일
    note: str = ""

    def effective_ratio(self, yearly_fee: float) -> float:
        """계약금액(연간) 대비 실제 적용 비율."""
        if self.mode == "amount":
            return (float(self.amount) / float(yearly_fee)) if yearly_fee else 0.0
        return float(self.ratio or 0.0)

    def is_active(self) -> bool:
        return (self.amount > 0) if self.mode == "amount" else (self.ratio > 0)

    def label(self, yearly_fee: float, currency: str = "USD") -> str:
        """표시용: '30%' 또는 '₩500,000 고정(≈20%)'."""
        eff = self.effective_ratio(yearly_fee)
        if self.mode == "amount":
            sym = "₩" if (currency or "USD").upper() == "KRW" else "$"
            return f"{sym}{self.amount:,.0f} 고정 (≈{eff*100:.0f}%)"
        return f"{eff*100:.0f}%"


@dataclass
class Contract:
    """
    계약(주문서). 계약금액 = ARR = 연간 구독료(yearly_fee).
    인보이스 자동 생성은 하지 않고, 수금 스케줄은 계약 정보로 그때그때 계산한다.
    """
    id: str
    customer_id: str
    order_form_name: str = ""           # 주문서명
    provided_data: str = ""             # 제공 데이터
    contract_type: str = "annual"       # 계약 형식: annual / monthly / one-time / custom
    am: str = ""                        # Account Manager (우리 쪽 담당)
    billing_frequency: str = "quarterly"  # monthly / quarterly / annually / one-time
    auto_renewal: bool = False          # 자동 갱신 여부
    effective_date: str = ""            # Effective Date (계약 발효일)
    termination_date: str = ""          # Termination Date (해지일)
    initial_delivery_date: str = ""     # Initial Delivery Date (최초 제공일)
    subscription_end_date: str = ""     # Subscription End Date (구독 종료일)
    yearly_fee: float = 0.0             # Yearly Fee = 계약금액 = ARR
    quarterly_fee: float = 0.0          # Quarterly Fee
    currency: str = "USD"               # USD / KRW
    payment_terms_days: int = 30        # 인보이스 발행 → 입금 기한(일)
    payout_terms_days: int = 30         # 입금 → 파트너 배분 기한(일)
    revenue_shares: list[RevenueShare] = field(default_factory=list)  # 파트너사 배분율
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


# ─────────────────────────────────────────────────────────────
# 저장 백엔드 인디렉션 — Google Sheets(공유) ↔ JSON 파일(폴백)
#   Sheets 활성(secrets 설정)이면 시트를 source of truth 로 사용하고,
#   미설정/실패 시 로컬 JSON 파일로 폴백한다. 어떤 경우에도 예외로 앱이
#   죽지 않도록 try/except 로 감싼다.
# ─────────────────────────────────────────────────────────────
_PATH_BY_NAME = {
    "customers": CUSTOMERS_PATH,
    "contracts": CONTRACTS_PATH,
    "staff": STAFF_PATH,
    "settings": SETTINGS_PATH,
    "collections": COLLECTIONS_PATH,
}


def _read_file(name: str, default):
    path = _PATH_BY_NAME[name]
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, ValueError):
        return default


def _write_file(name: str, data) -> None:
    _PATH_BY_NAME[name].write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_raw(name: str, default):
    """DB(설정 시) 우선, 실패/미설정 시 파일. DB가 비어있으면 파일 데이터로 최초 시드."""
    try:
        from . import db_store
        if db_store.enabled():
            data = db_store.read(name)
            if data is not None:
                return data
            seed = _read_file(name, default)  # 첫 배포 시 기존 파일 데이터로 시드
            try:
                db_store.write(name, seed)
            except Exception:
                pass
            return seed
    except Exception:
        pass  # 어떤 오류든 파일로 폴백
    return _read_file(name, default)


def _write_raw(name: str, data) -> None:
    """DB(설정 시) + 로컬 파일 둘 다 기록(파일은 백업/로컬개발용)."""
    try:
        from . import db_store
        if db_store.enabled():
            try:
                db_store.write(name, data)
            except Exception:
                pass
    except Exception:
        pass
    try:
        _write_file(name, data)
    except Exception:
        pass


def load_customers() -> list[Customer]:
    raw = _read_raw("customers", [])
    if not isinstance(raw, list):
        return []
    result: list[Customer] = []
    valid_fields = {f for f in Customer.__dataclass_fields__}
    for c in raw:
        # backward compat: 옛 필드(ar_manager, accounting_email) 제거
        c_clean = {k: v for k, v in c.items() if k in valid_fields}
        result.append(Customer(**c_clean))
    return result


def save_customers(customers: list[Customer]) -> None:
    _write_raw("customers", [asdict(c) for c in customers])


def load_staff() -> list[Staff]:
    raw = _read_raw("staff", [])
    if not isinstance(raw, list):
        return []
    valid_fields = {f for f in Staff.__dataclass_fields__}
    return [Staff(**{k: v for k, v in s.items() if k in valid_fields}) for s in raw]


def save_staff(staff: list[Staff]) -> None:
    _write_raw("staff", [asdict(s) for s in staff])


def _migrate_contract(c: dict) -> dict:
    """구버전 계약 필드(title/total_amount/start_date 등)를 신버전으로 변환."""
    c = dict(c)
    # title → order_form_name
    if "order_form_name" not in c and "title" in c:
        c["order_form_name"] = c.get("title", "")
    # total_amount → yearly_fee (계약금액 = ARR)
    if "yearly_fee" not in c and "total_amount" in c:
        c["yearly_fee"] = c.get("total_amount", 0.0)
    if "quarterly_fee" not in c:
        yf = c.get("yearly_fee", 0.0) or 0.0
        c["quarterly_fee"] = round(yf / 4.0, 2)
    # 날짜 매핑
    if "effective_date" not in c and "start_date" in c:
        c["effective_date"] = c.get("start_date", "")
    if "initial_delivery_date" not in c and "start_date" in c:
        c["initial_delivery_date"] = c.get("start_date", "")
    if "subscription_end_date" not in c and "end_date" in c:
        c["subscription_end_date"] = c.get("end_date", "")
    if "termination_date" not in c and "end_date" in c:
        c["termination_date"] = c.get("end_date", "")
    return c


def load_contracts() -> list[Contract]:
    raw = _read_raw("contracts", [])
    if not isinstance(raw, list):
        return []
    valid_fields = {f for f in Contract.__dataclass_fields__}
    result: list[Contract] = []
    for c in raw:
        c = _migrate_contract(c)
        shares_raw = c.get("revenue_shares", []) or []
        shares = [RevenueShare(**{k: v for k, v in s.items()
                                  if k in RevenueShare.__dataclass_fields__})
                  for s in shares_raw]
        c_clean = {k: v for k, v in c.items()
                   if k in valid_fields and k != "revenue_shares"}
        result.append(Contract(revenue_shares=shares, **c_clean))
    return result


def save_contracts(contracts: list[Contract]) -> None:
    _write_raw("contracts", [asdict(c) for c in contracts])


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
# 설정 (환율 등)
# ─────────────────────────────────────────────────────────────
def load_settings() -> dict:
    """앱 설정 로드. 키가 없으면 기본값으로 보정."""
    raw = _read_raw("settings", dict(DEFAULT_SETTINGS))
    merged = dict(DEFAULT_SETTINGS)
    if isinstance(raw, dict):
        merged.update({k: v for k, v in raw.items() if k in DEFAULT_SETTINGS})
    return merged


def save_settings(settings: dict) -> None:
    _write_raw("settings", settings)


def to_base(amount: float, currency: str, usd_krw: float = DEFAULT_USD_KRW) -> float:
    """금액을 기준통화(KRW)로 환산. USD만 환율 적용, 그 외는 그대로."""
    cur = (currency or BASE_CURRENCY).upper()
    if cur == "USD":
        return float(amount) * float(usd_krw)
    return float(amount)


# ─────────────────────────────────────────────────────────────
# 수금 확인(3자 서명) 상태 저장
#   { period_key: {"manager": bool, "accounting": bool, "ar": bool,
#                  "collected_at": "YYYY-MM-DD"} }
# period_key = "{contract_id}|{due_date}"
# ─────────────────────────────────────────────────────────────
def load_collections() -> dict:
    raw = _read_raw("collections", {})
    return raw if isinstance(raw, dict) else {}


def save_collections(coll: dict) -> None:
    _write_raw("collections", coll)


def period_key(contract_id: str, due_date: str) -> str:
    return f"{contract_id}|{due_date}"


def empty_signoff() -> dict:
    return {r: False for r in COLLECTION_ROLES}


def empty_record() -> dict:
    """수금 단계 전체 상태(발행 → 입금 3자 → 파트너별 배분 3단계)."""
    rec = {r: False for r in COLLECTION_ROLES}
    rec.update({
        "invoiced": False, "invoiced_at": "",
        "paid_at": "",
        # 파트너별 배분: { owner: {tax_invoice, expense, paid} }
        "payout": {},
    })
    return rec


# ── 배분(파트너별 3단계) 헬퍼 ──────────────────────────────
def get_payout_steps(record: Optional[dict], owner: str) -> dict:
    p = (record or {}).get("payout") or {}
    o = p.get(owner) or {}
    return {k: bool(o.get(k)) for k in PAYOUT_STEPS}


def is_owner_paid_out(record: Optional[dict], owner: str) -> bool:
    """한 파트너의 배분 3단계(세금계산서·지출결의·입금)가 모두 끝났는지."""
    o = get_payout_steps(record, owner)
    return all(o.values())


def is_payout_done(record: Optional[dict], owners) -> bool:
    """대상 파트너 전원의 배분이 끝났는지. 파트너 없으면 True."""
    owners = list(owners or [])
    if not owners:
        return True
    return all(is_owner_paid_out(record, o) for o in owners)


def set_payout_step(record: dict, owner: str, step: str, value: bool) -> None:
    record.setdefault("payout", {})
    record["payout"].setdefault(owner, {s: False for s in PAYOUT_STEPS})
    record["payout"][owner][step] = bool(value)


def is_paid(state: Optional[dict]) -> bool:
    """입금 확정 = 책임자·회계·AR 3자 모두 체크."""
    if not state:
        return False
    return all(bool(state.get(r)) for r in COLLECTION_ROLES)


# 하위호환 별칭
def is_collected(state: Optional[dict]) -> bool:
    return is_paid(state)


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
