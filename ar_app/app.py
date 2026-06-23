"""
AR Management — 메인 페이지 (v4).

구조:
  - 계약(주문서) 중심. 계약금액 = ARR = Yearly Fee(연간 구독료).
  - 인보이스 발행/생성 없음. 수금 예정은 계약 정보로 자동 계산.
  - 수금 확인은 3자 서명(책임자·회계담당자·AR담당자) 체크로. 셋 다 체크 → 수금 완료.
  - 금액은 USD 기준 표기 + 원화 보조.
"""
from __future__ import annotations

import calendar
import sys
from datetime import date, timedelta
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
for p in (str(_ROOT), str(_HERE.parent)):
    if p not in sys.path:
        sys.path.insert(0, p)

import pandas as pd
import streamlit as st

try:
    st.set_page_config(page_title="AR Management", page_icon="💰", layout="wide")
except Exception:
    pass

try:
    from ar_app import models as ar_models
    from ar_app import schedule as ar_schedule
    from ar_app import lock as ar_lock
except ImportError:
    import models as ar_models           # type: ignore
    import schedule as ar_schedule       # type: ignore
    import lock as ar_lock               # type: ignore

import time as _time
import uuid as _uuid


# 전체 콘텐츠 폭 제한 — 와이드 모니터에서 좌우로 늘어지는 것 방지
st.markdown(
    """
    <style>
      .block-container {
          max-width: 1150px !important;
          padding-top: 2.2rem !important;
          padding-left: 1.5rem !important;
          padding-right: 1.5rem !important;
      }
    </style>
    """,
    unsafe_allow_html=True,
)


# ─────────────────────────────────────────────────────────────
# 1인 편집 락 — 한 번에 한 명만 사용(동시 편집 충돌 방지)
# ─────────────────────────────────────────────────────────────
if "_ar_lock_id" not in st.session_state:
    st.session_state["_ar_lock_id"] = _uuid.uuid4().hex
_lock_id = st.session_state["_ar_lock_id"]

try:
    import auth as _auth  # type: ignore
    _my_email = _auth.get_current_email() or "사용자"
    _is_admin = bool(_auth.is_admin(_my_email))
except Exception:
    _my_email, _is_admin = "사용자", False

# 사용자가 '사용 종료'를 누른 상태 → 다른 사람에게 양보
if st.session_state.get("_ar_released"):
    st.markdown("### 💰 AR Management")
    st.info("AR Management 사용을 종료했습니다. (다른 분이 사용할 수 있어요)")
    if st.button("▶️ 다시 사용하기", type="primary"):
        st.session_state["_ar_released"] = False
        st.rerun()
    st.stop()

_ok, _holder = ar_lock.acquire(_lock_id, _my_email)
if not _ok:
    st.markdown("### 💰 AR Management")
    _who = (_holder or {}).get("email") or "다른 사용자"
    _left = ar_lock.remaining(_holder or {})
    st.warning(f"🔒 현재 **{_who}** 님이 사용 중입니다.\n\n"
               "동시 편집으로 데이터가 꼬이지 않도록, 한 번에 한 명만 사용할 수 있어요.")
    st.caption(f"상대가 활동을 멈추면 약 {ar_lock.TTL_SECONDS // 60}분 뒤 자동으로 풀립니다 "
               f"(현재 약 {_left}초 남음). 아래 버튼으로 다시 확인하세요.")
    _bc = st.columns(2)
    if _bc[0].button("🔄 다시 시도", use_container_width=True):
        st.rerun()
    if _is_admin:
        if _bc[1].button("🔓 강제로 넘겨받기 (관리자)", use_container_width=True):
            ar_lock.force_release()
            st.rerun()
    st.stop()


# ─────────────────────────────────────────────────────────────
# 디자인 토큰 + 공통 헬퍼
# ─────────────────────────────────────────────────────────────
C_TEXT = "#F1F5F9"
C_MUTED = "rgba(241,245,249,0.62)"
C_LABEL = "rgba(241,245,249,0.5)"
C_AMBER = "#F59E0B"
C_RED = "#EF4444"
C_GREEN = "#22C55E"
C_BLUE = "#3B82F6"
MONO = "'JetBrains Mono','SF Mono',monospace"

CT_STATUS = {
    "active": ("진행중", C_GREEN),
    "paused": ("일시중지", C_AMBER),
    "ended": ("종료", "#94A3B8"),
}
COLL_BADGE = {"완료": C_GREEN, "미수": C_AMBER, "연체": C_RED, "예정": C_BLUE}

# 수금 프로세스 단계: 발행 → 입금 → 배분
STAGE = {
    "예정":     ("미도래",   "#94A3B8"),
    "발행필요": ("발행 필요", C_AMBER),
    "입금대기": ("입금 대기", C_BLUE),
    "입금연체": ("입금 연체", C_RED),
    "입금완료": ("입금 완료", C_GREEN),
    "배분예정": ("배분 예정", C_BLUE),
    "배분진행": ("배분 진행", C_AMBER),
    "배분연체": ("배분 연체", C_RED),
    "배분완료": ("배분 완료", C_GREEN),
    "완료":     ("완료",     C_GREEN),
}


def stage_badge(stage: str) -> str:
    txt, col = STAGE.get(stage, (stage, "#94A3B8"))
    return badge(txt, col)


def label_block(label: str, value: str, *, color: str = C_TEXT,
                mono: bool = False, weight: int = 600, top: int = 0) -> str:
    fam = f"font-family:{MONO};" if mono else ""
    mt = f"margin-top:{top}px;" if top else ""
    return (
        f"<div style='font-size:0.74rem;color:{C_LABEL};{mt}'>{label}</div>"
        f"<div style='color:{color};{fam}font-weight:{weight};font-size:0.9rem;'>{value}</div>"
    )


def badge(text: str, color: str) -> str:
    return (
        f"<span style='display:inline-block;padding:3px 11px;border-radius:999px;"
        f"background:{color}22;color:{color};font-weight:600;font-size:0.78rem;"
        f"line-height:1.5;'>{text}</span>"
    )


def ct_badge(status: str) -> str:
    txt, col = CT_STATUS.get(status, (status, "#94A3B8"))
    return badge(txt, col)


# 통화 설정
settings = ar_models.load_settings()
USD_KRW = float(settings.get("usd_krw") or ar_models.DEFAULT_USD_KRW)


def to_krw(amount: float, currency: str) -> float:
    return ar_models.to_base(amount, currency, USD_KRW)


def to_usd(amount: float, currency: str) -> float:
    cur = (currency or "KRW").upper()
    if cur == "USD":
        return float(amount)
    return float(amount) / USD_KRW if USD_KRW else 0.0


def fmt_usd(v: float) -> str:
    return f"${v:,.0f}"


def fmt_krw(v: float) -> str:
    return f"₩{v:,.0f}"


def money_dual(amount: float, currency: str, *, align: str = "right",
               big: str = "1.0rem") -> str:
    usd = to_usd(amount, currency)
    krw = to_krw(amount, currency)
    return (
        f"<div style='text-align:{align};line-height:1.25;'>"
        f"<div style='font-family:{MONO};font-weight:700;color:{C_TEXT};font-size:{big};'>{fmt_usd(usd)}</div>"
        f"<div style='font-size:0.72rem;color:{C_MUTED};font-family:{MONO};'>{fmt_krw(krw)}</div>"
        f"</div>"
    )


def kpi_card(label: str, usd_val: float, krw_val: float, sub: str,
             accent: str = C_TEXT) -> str:
    return (
        f"<div style='border:1px solid rgba(255,255,255,0.08);border-radius:12px;"
        f"padding:14px 16px;background:rgba(255,255,255,0.02);'>"
        f"<div style='font-size:0.82rem;color:{C_MUTED};'>{label}</div>"
        f"<div style='font-size:1.7rem;font-weight:700;color:{accent};"
        f"font-family:{MONO};letter-spacing:-0.01em;margin-top:2px;'>{fmt_usd(usd_val)}</div>"
        f"<div style='font-size:0.76rem;color:{C_MUTED};font-family:{MONO};'>≈ {fmt_krw(krw_val)}</div>"
        f"<div style='font-size:0.76rem;color:{C_LABEL};margin-top:5px;'>{sub}</div>"
        f"</div>"
    )


def alert_card(icon: str, label: str, count: int, usd: float, krw: float, color: str) -> str:
    on = count > 0
    c = color if on else "#6B7280"
    bg = f"{color}14" if on else "rgba(255,255,255,0.02)"
    bd = f"{color}44" if on else "rgba(255,255,255,0.08)"
    return (
        f"<div style='border:1px solid {bd};border-radius:12px;padding:12px 14px;background:{bg};'>"
        f"<div style='font-size:0.8rem;color:{C_MUTED};'>{icon} {label}</div>"
        f"<div style='font-size:1.5rem;font-weight:800;color:{c};line-height:1.2;margin-top:2px;'>{count}건</div>"
        f"<div style='font-size:0.72rem;color:{C_MUTED};font-family:{MONO};'>{fmt_usd(usd)} · {fmt_krw(krw)}</div>"
        f"</div>"
    )


# ─────────────────────────────────────────────────────────────
# 헤더
# ─────────────────────────────────────────────────────────────
st.markdown(
    """
    <div style="margin:6px 0 14px 0;">
      <div style="font-size:1.55rem; font-weight:700; color:#F1F5F9; letter-spacing:-0.02em;">
        💰 AR Management
      </div>
      <div style="font-size:0.85rem; color:rgba(241,245,249,0.6); margin-top:3px;">
        계약(주문서) · 수금 확인 · 파트너 수익배분 통합 관리. 계약금액 = ARR(연간 구독료).
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ─────────────────────────────────────────────────────────────
# 데이터 로드
# ─────────────────────────────────────────────────────────────
customers = ar_models.load_customers()
contracts = ar_models.load_contracts()
staff = ar_models.load_staff()
collections = ar_models.load_collections()

# 구버전 수금 기록(3자 체크 = 완료) 호환: 입금됐으면 발행도 된 것으로 간주
for _rec in collections.values():
    if ar_models.is_paid(_rec) and not _rec.get("invoiced"):
        _rec["invoiced"] = True
        _rec["invoiced_at"] = _rec.get("invoiced_at") or _rec.get("collected_at") or ""
        if not _rec.get("paid_at"):
            _rec["paid_at"] = _rec.get("collected_at") or ""

today = date.today()

customer_by_id = {c.id: c for c in customers}
contract_by_id = {c.id: c for c in contracts}
staff_by_id = {s.id: s for s in staff}


def staff_options(role_filter: str = "") -> list[str]:
    out = [""]
    for s in staff:
        if role_filter and s.role != role_filter:
            continue
        out.append(s.id)
    return out


def staff_label(s_id: str) -> str:
    if not s_id:
        return "— 미지정 —"
    s = staff_by_id.get(s_id)
    if not s:
        return f"(삭제됨: {s_id})"
    role_part = f" · {s.role}" if s.role else ""
    return f"{s.name} ({s.email}){role_part}"


# ─────────────────────────────────────────────────────────────
# 계약 기반 수금 스케줄 계산 + 3자 서명 상태 결합
# 스케줄은 계약 내용에만 의존(수금 상태와 무관)하므로 캐시 → 체크박스 클릭마다 재생성 안 함
# ─────────────────────────────────────────────────────────────
def _contract_sig(ct) -> tuple:
    return (ct.id, ct.customer_id, ct.effective_date, ct.initial_delivery_date,
            ct.subscription_end_date, ct.termination_date, ct.billing_frequency,
            ct.currency, float(ct.yearly_fee or 0), float(ct.quarterly_fee or 0),
            bool(ct.auto_renewal))


@st.cache_data(show_spinner=False)
def _cached_schedule(sig: tuple, today_iso: str):
    from types import SimpleNamespace
    ns = SimpleNamespace(
        id=sig[0], customer_id=sig[1], effective_date=sig[2], initial_delivery_date=sig[3],
        subscription_end_date=sig[4], termination_date=sig[5], billing_frequency=sig[6],
        currency=sig[7], yearly_fee=sig[8], quarterly_fee=sig[9], auto_renewal=sig[10],
    )
    return ar_schedule.expected_collections(ns, date.fromisoformat(today_iso))


_today_iso = today.isoformat()
periods_by_contract = {ct.id: _cached_schedule(_contract_sig(ct), _today_iso) for ct in contracts}
periods_all = [p for ct in contracts if ct.status == "active"
               for p in periods_by_contract[ct.id]]


def period_state(p) -> dict:
    return collections.get(p.key) or ar_models.empty_record()


def period_paid(p) -> bool:
    return ar_models.is_paid(collections.get(p.key))


def _due(p) -> date:
    return ar_models.parse_iso(p.due_date) or today


def _terms(ct, attr, default=30) -> int:
    return int(getattr(ct, attr, default) or default) if ct else default


def quarter_index(d: date) -> int:
    return d.year * 4 + (d.month - 1) // 3


def quarter_range(qi: int):
    y, q = divmod(qi, 4)
    sm = q * 3 + 1
    last = calendar.monthrange(y, sm + 2)[1]
    return date(y, sm, 1), date(y, sm + 2, last), f"{y} Q{q + 1}"


def paid_date(p):
    s = collections.get(p.key) or {}
    return ar_models.parse_iso(s.get("paid_at", "")) or ar_models.parse_iso(s.get("collected_at", ""))


def payment_due_date(p) -> "date | None":
    """인보이스 발행일 + 입금 기한. 미발행이면 None."""
    s = collections.get(p.key) or {}
    if not s.get("invoiced"):
        return None
    inv_at = ar_models.parse_iso(s.get("invoiced_at", "")) or _due(p)
    ct = contract_by_id.get(p.contract_id)
    return inv_at + timedelta(days=_terms(ct, "payment_terms_days"))


def payout_due_date(p) -> "date | None":
    """배분 정산 기한 = 수금 완료 분기의 '다음 분기 첫 달' 말일.
    (예: 1~3월 수금 → 4월 한 달 → 4/30). 미입금이면 None."""
    pd = paid_date(p)
    if not pd:
        return None
    nstart, _, _ = quarter_range(quarter_index(pd) + 1)  # 다음 분기 첫 달 1일
    last = calendar.monthrange(nstart.year, nstart.month)[1]
    return date(nstart.year, nstart.month, last)


def payout_due_label(p) -> str:
    """정산기한을 'YYYY-MM월' 형태로."""
    due = payout_due_date(p)
    return f"{due.year}-{due.month:02d}월" if due else "—"


def share_owners(p) -> list:
    ct = contract_by_id.get(p.contract_id)
    return [rs.owner for rs in ct.revenue_shares if rs.ratio > 0] if ct else []


def has_shares(p) -> bool:
    return bool(share_owners(p))


def payout_done(p) -> bool:
    return ar_models.is_payout_done(collections.get(p.key), share_owners(p))


def period_stage(p) -> str:
    """발행 → 입금 → 배분 단계."""
    s = collections.get(p.key) or {}
    invoiced = bool(s.get("invoiced"))
    paid = ar_models.is_paid(s)
    if not invoiced:
        return "예정" if _due(p) > today else "발행필요"
    if not paid:
        pdd = payment_due_date(p)
        return "입금연체" if (pdd and today > pdd) else "입금대기"
    if has_shares(p) and not payout_done(p):
        return "배분진행"
    return "완료"


# 단계별 버킷
_by_stage: dict[str, list] = {k: [] for k in STAGE}
for p in periods_all:
    _by_stage[period_stage(p)].append(p)
for k in _by_stage:
    _by_stage[k].sort(key=_due)

need_invoice = _by_stage["발행필요"]      # 도래·미발행
pay_waiting = _by_stage["입금대기"]       # 발행·입금 대기(정상)
pay_overdue = _by_stage["입금연체"]       # 발행 후 기한 경과·미입금
payout_inprogress = _by_stage["배분진행"]  # 입금완료·배분 미완료

# 미수금(도래·미입금) = 발행필요 + 입금대기 + 입금연체
unpaid_due = need_invoice + pay_waiting + pay_overdue

# 배분 진행 대상 → 파트너별 금액
payouts = []  # (period, contract, revenue_share, payout_native)
for p in payout_inprogress:
    ct = contract_by_id.get(p.contract_id)
    for rs in ct.revenue_shares:
        if rs.ratio > 0:
            payouts.append((p, ct, rs, p.amount * rs.ratio))


# ─────────────────────────────────────────────────────────────
# KPI
# ─────────────────────────────────────────────────────────────
def _sum_usd(items, amt=lambda x: x.amount, cur=lambda x: x.currency):
    return sum(to_usd(amt(i), cur(i)) for i in items)


def _sum_krw(items, amt=lambda x: x.amount, cur=lambda x: x.currency):
    return sum(to_krw(amt(i), cur(i)) for i in items)


active_contracts = [c for c in contracts if c.status == "active"]


def _subscription_end(ct):
    return ar_models.parse_iso(ct.subscription_end_date) or ar_models.parse_iso(ct.termination_date)


def is_running(ct) -> bool:
    """진행중 = status active 이고, 자동 갱신이거나 구독 종료일이 아직 안 지난 계약.
    (자동 갱신이면 계약서상 기간이 끝났어도 계속 진행으로 간주)"""
    if ct.status != "active":
        return False
    if ct.auto_renewal:
        return True
    end = _subscription_end(ct)
    return end is None or end >= today


def is_expired(ct) -> bool:
    """만료 = active 인데 자동 갱신도 아니고 구독 종료일이 지난 계약."""
    return ct.status == "active" and not is_running(ct)


def contract_badge(ct) -> str:
    """계약 상태 배지(칩 분리). active 라도 만료면 '만료'로."""
    if ct.status == "active":
        if is_running(ct):
            renew = (badge("자동갱신", C_BLUE) if ct.auto_renewal
                     else badge("수동갱신", "#94A3B8"))
            return (f"<div style='display:flex;flex-wrap:wrap;gap:4px;justify-content:flex-end;'>"
                    f"{badge('진행중', C_GREEN)}{renew}</div>")
        return badge("만료", C_RED)
    return ct_badge(ct.status)


# 만료된(자동갱신 아님 + 기간 종료) active 계약은 ARR/프로세스에서 제외
running_contracts = [c for c in active_contracts if is_running(c)]
expired_active = [c for c in active_contracts if is_expired(c)]
arr_usd = sum(to_usd(c.yearly_fee, c.currency) for c in running_contracts)
arr_krw = sum(to_krw(c.yearly_fee, c.currency) for c in running_contracts)

ni_usd, ni_krw = _sum_usd(need_invoice), _sum_krw(need_invoice)
po_usd, po_krw = _sum_usd(pay_overdue), _sum_krw(pay_overdue)
pay_usd = sum(to_usd(amt, prd.currency) for prd, _, _, amt in payouts)
pay_krw = sum(to_krw(amt, prd.currency) for prd, _, _, amt in payouts)

with st.expander(f"⚙️ 환율 설정 · 현재 USD 1 = {USD_KRW:,.0f}원  (원화 환산 기준)"):
    ec = st.columns([2, 1, 3])
    with ec[0]:
        new_rate = st.number_input("USD → KRW 환율", min_value=1.0, max_value=100000.0,
                                    value=float(USD_KRW), step=10.0)
    with ec[1]:
        st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        if st.button("저장", type="primary", use_container_width=True):
            settings["usd_krw"] = float(new_rate)
            ar_models.save_settings(settings)
            st.rerun()
    with ec[2]:
        st.caption("모든 금액은 USD 기준으로 표기하고, 원화는 이 환율로 환산해 보조 표기합니다.")

st.markdown("")


# ═══════════════════════════════════════════════════════════
# 모달 (st.dialog)
# ═══════════════════════════════════════════════════════════
@st.dialog("👥 신규 담당자 추가")
def dialog_add_staff():
    with st.form("dlg_add_staff"):
        name = st.text_input("이름 *", placeholder="홍길동")
        email = st.text_input("이메일 *", placeholder="hong@mandata.kr")
        role = st.selectbox("역할", ["책임자", "회계담당자", "AR담당자", "기타"], index=0)
        notes = st.text_area("메모", height=68)
        if st.form_submit_button("저장", type="primary"):
            if not name.strip() or not email.strip():
                st.error("이름과 이메일은 필수")
                return
            staff.append(ar_models.Staff(
                id=ar_models.next_staff_id(staff),
                name=name.strip(), email=email.strip(),
                role=role, notes=notes.strip(), created_at=today.isoformat(),
            ))
            ar_models.save_staff(staff)
            st.rerun()


@st.dialog("👥 담당자 수정")
def dialog_edit_staff(staff_id: str):
    s = staff_by_id.get(staff_id)
    if not s:
        st.error("담당자 없음")
        return
    roles = ["책임자", "회계담당자", "AR담당자", "기타"]
    pf = f"edit_staff_{staff_id}"  # 담당자별 위젯 키 분리(값 섞임 방지)
    with st.form(f"dlg_edit_staff_{staff_id}"):
        name = st.text_input("이름 *", value=s.name, key=f"{pf}_name")
        email = st.text_input("이메일 *", value=s.email, key=f"{pf}_email")
        role = st.selectbox("역할", roles,
                            index=roles.index(s.role) if s.role in roles else 3, key=f"{pf}_role")
        notes = st.text_area("메모", value=s.notes, height=68, key=f"{pf}_notes")
        cols = st.columns([1, 1])
        with cols[0]:
            if st.form_submit_button("저장", type="primary", use_container_width=True):
                for x in staff:
                    if x.id == staff_id:
                        x.name, x.email, x.role, x.notes = name.strip(), email.strip(), role, notes.strip()
                ar_models.save_staff(staff)
                st.rerun()
        with cols[1]:
            if st.form_submit_button("🗑 삭제", use_container_width=True):
                in_use = any(c.ar_manager_id == staff_id or c.accounting_id == staff_id
                             for c in customers)
                if in_use:
                    st.error("이 담당자는 고객사에 배정돼 있어 삭제 불가.")
                else:
                    staff[:] = [x for x in staff if x.id != staff_id]
                    ar_models.save_staff(staff)
                    st.rerun()


@st.dialog("🏢 신규 고객사 추가")
def dialog_add_customer():
    with st.form("dlg_add_cust"):
        name = st.text_input("고객사명 *", placeholder="㈜OO")
        biz_no = st.text_input("사업자번호", placeholder="123-45-67890")
        contact_name = st.text_input("고객사 담당자(상대편) 이름")
        contact_email = st.text_input("고객사 담당자 이메일", placeholder="kim@customer.com")
        st.markdown("**우리 쪽 담당자**")
        ar_id = st.selectbox("AR 담당자", options=staff_options(),
                             format_func=staff_label, key="add_cust_ar")
        acc_id = st.selectbox("회계 담당자", options=staff_options(),
                              format_func=staff_label, key="add_cust_acc")
        notes = st.text_area("메모", height=68)
        if st.form_submit_button("저장", type="primary"):
            if not name.strip():
                st.error("고객사명은 필수")
                return
            customers.append(ar_models.Customer(
                id=ar_models.next_customer_id(customers),
                name=name.strip(), biz_no=biz_no.strip(),
                contact_name=contact_name.strip(), contact_email=contact_email.strip(),
                ar_manager_id=ar_id, accounting_id=acc_id,
                notes=notes.strip(), created_at=today.isoformat(),
            ))
            ar_models.save_customers(customers)
            st.rerun()


@st.dialog("🏢 고객사 수정")
def dialog_edit_customer(customer_id: str):
    c = customer_by_id.get(customer_id)
    if not c:
        st.error("고객사 없음")
        return
    pf = f"edit_cust_{customer_id}"  # 고객사별 위젯 키 분리(값 섞임 방지)
    with st.form(f"dlg_edit_cust_{customer_id}"):
        name = st.text_input("고객사명 *", value=c.name, key=f"{pf}_name")
        biz_no = st.text_input("사업자번호", value=c.biz_no, key=f"{pf}_biz")
        contact_name = st.text_input("고객사 담당자 이름", value=c.contact_name, key=f"{pf}_cn")
        contact_email = st.text_input("고객사 담당자 이메일", value=c.contact_email, key=f"{pf}_ce")
        st.markdown("**우리 쪽 담당자**")
        opts = staff_options()
        ar_idx = opts.index(c.ar_manager_id) if c.ar_manager_id in opts else 0
        acc_idx = opts.index(c.accounting_id) if c.accounting_id in opts else 0
        ar_id = st.selectbox("AR 담당자", options=opts, index=ar_idx,
                             format_func=staff_label, key=f"{pf}_ar")
        acc_id = st.selectbox("회계 담당자", options=opts, index=acc_idx,
                              format_func=staff_label, key=f"{pf}_acc")
        notes = st.text_area("메모", value=c.notes, height=68, key=f"{pf}_notes")
        cols = st.columns([1, 1])
        with cols[0]:
            if st.form_submit_button("저장", type="primary", use_container_width=True):
                for x in customers:
                    if x.id == customer_id:
                        x.name, x.biz_no = name.strip(), biz_no.strip()
                        x.contact_name, x.contact_email = contact_name.strip(), contact_email.strip()
                        x.ar_manager_id, x.accounting_id, x.notes = ar_id, acc_id, notes.strip()
                ar_models.save_customers(customers)
                st.rerun()
        with cols[1]:
            if st.form_submit_button("🗑 삭제", use_container_width=True):
                n_ct = sum(1 for ct in contracts if ct.customer_id == customer_id)
                if n_ct > 0:
                    st.error(f"이 고객사에 계약이 {n_ct}건 있어 삭제 불가.")
                else:
                    customers[:] = [x for x in customers if x.id != customer_id]
                    ar_models.save_customers(customers)
                    st.rerun()


# ── 계약 폼 공통 필드 ──────────────────────────────────────
_CT_TYPES = ["annual", "monthly", "one-time", "custom"]
_FREQS = ["monthly", "quarterly", "annually", "one-time"]


def _date_or_none(s: str):
    return ar_models.parse_iso(s) if s else None


def contract_form_fields(prefix: str, ct: "ar_models.Contract | None" = None) -> dict:
    cust_opts = [c.id for c in customers]
    cust_idx = cust_opts.index(ct.customer_id) if ct and ct.customer_id in cust_opts else 0
    cust_id = st.selectbox("고객사 *", options=cust_opts, index=cust_idx,
                           format_func=lambda x: customer_by_id[x].name, key=f"{prefix}_cust")
    order_form_name = st.text_input("주문서명 *", value=ct.order_form_name if ct else "",
                                    placeholder="Order Form No.1 (쿠팡)", key=f"{prefix}_ofn")
    provided_data = st.text_area("제공 데이터", value=ct.provided_data if ct else "",
                                 placeholder="예: K-F&B 표준화 데이터셋", height=68, key=f"{prefix}_pd")

    r1 = st.columns(2)
    with r1[0]:
        contract_type = st.selectbox("계약 형식", _CT_TYPES,
                                     index=_CT_TYPES.index(ct.contract_type) if ct and ct.contract_type in _CT_TYPES else 0,
                                     key=f"{prefix}_type")
    with r1[1]:
        am = st.text_input("AM (Account Manager)", value=ct.am if ct else "", key=f"{prefix}_am")

    billing_frequency = st.selectbox("Billing Frequency *", _FREQS,
                                     index=_FREQS.index(ct.billing_frequency) if ct and ct.billing_frequency in _FREQS else 1,
                                     key=f"{prefix}_freq")

    st.markdown("**계약 기간**")
    d1 = st.columns(2)
    with d1[0]:
        effective_date = st.date_input("Effective Date",
                                       value=_date_or_none(ct.effective_date) if ct else today,
                                       key=f"{prefix}_eff")
    with d1[1]:
        termination_date = st.date_input("Termination Date",
                                         value=_date_or_none(ct.termination_date) if ct else None,
                                         key=f"{prefix}_term")
    d2 = st.columns(2)
    with d2[0]:
        initial_delivery_date = st.date_input("Initial Delivery Date",
                                              value=_date_or_none(ct.initial_delivery_date) if ct else None,
                                              key=f"{prefix}_idd")
    with d2[1]:
        subscription_end_date = st.date_input("Subscription End Date",
                                              value=_date_or_none(ct.subscription_end_date) if ct else None,
                                              key=f"{prefix}_sed")
    auto_renewal = st.checkbox("🔁 자동 갱신  ·  계약기간이 끝나도 계속 진행 (ARR·수금·배분 계속 적용)",
                               value=ct.auto_renewal if ct else False, key=f"{prefix}_auto")

    st.markdown("**금액 · 통화**  ·  *계약금액 = ARR = Yearly Fee*")
    m = st.columns([2, 2, 1])
    with m[0]:
        yearly_fee = st.number_input("Yearly Fee *", min_value=0.0, step=1000.0,
                                     value=float(ct.yearly_fee) if ct else 0.0, key=f"{prefix}_yf")
    with m[1]:
        quarterly_fee = st.number_input("Quarterly Fee", min_value=0.0, step=1000.0,
                                        value=float(ct.quarterly_fee) if ct else 0.0, key=f"{prefix}_qf")
    with m[2]:
        currency = st.selectbox("통화", ["USD", "KRW"],
                                index=["USD", "KRW"].index(ct.currency) if ct and ct.currency in ("USD", "KRW") else 0,
                                key=f"{prefix}_cur")

    payment_terms_days = st.number_input("입금 기한 (발행→입금, 일)", min_value=0, max_value=365,
                                         value=int(ct.payment_terms_days) if ct else 30, key=f"{prefix}_pt",
                                         help="배분 정산 기한은 '수금 완료 분기의 다음 분기'로 자동 적용됩니다.")

    status = ct.status if ct else "active"
    if ct:
        st.markdown("**상태**")
        status = st.selectbox("상태", list(CT_STATUS.keys()),
                              index=list(CT_STATUS.keys()).index(ct.status) if ct.status in CT_STATUS else 0,
                              format_func=lambda x: CT_STATUS[x][0], key=f"{prefix}_status")

    url = st.text_input("계약서/오더폼 URL", value=ct.order_form_url if ct else "", key=f"{prefix}_url")
    notes = st.text_area("메모", value=ct.notes if ct else "", height=68, key=f"{prefix}_notes")

    st.markdown("**💼 파트너사 배분율 (옵션)**  ·  합계 1.0 이하, 최대 4명")
    existing = ct.revenue_shares if ct else []
    rs_rows = []
    for k in range(4):
        ex = existing[k] if k < len(existing) else None
        cc = st.columns([3, 1])
        with cc[0]:
            owner = st.text_input(f"파트너 #{k+1}", value=ex.owner if ex else "",
                                  label_visibility="collapsed", placeholder=f"파트너 #{k+1} 이름",
                                  key=f"{prefix}_rs_o_{k}")
        with cc[1]:
            ratio = st.number_input(f"비율 #{k+1}", min_value=0.0, max_value=1.0, step=0.05,
                                    value=float(ex.ratio) if ex else 0.0,
                                    label_visibility="collapsed", key=f"{prefix}_rs_r_{k}")
        if owner.strip() and ratio > 0:
            rs_rows.append(ar_models.RevenueShare(owner=owner.strip(), ratio=float(ratio),
                                                  contact_email=ex.contact_email if ex else ""))

    return dict(
        customer_id=cust_id, order_form_name=order_form_name.strip(),
        provided_data=provided_data.strip(), contract_type=contract_type, am=am.strip(),
        billing_frequency=billing_frequency, auto_renewal=bool(auto_renewal),
        effective_date=effective_date.isoformat() if effective_date else "",
        termination_date=termination_date.isoformat() if termination_date else "",
        initial_delivery_date=initial_delivery_date.isoformat() if initial_delivery_date else "",
        subscription_end_date=subscription_end_date.isoformat() if subscription_end_date else "",
        yearly_fee=float(yearly_fee), quarterly_fee=float(quarterly_fee), currency=currency,
        payment_terms_days=int(payment_terms_days),
        status=status, order_form_url=url.strip(), notes=notes.strip(), revenue_shares=rs_rows,
    )


def _validate_contract(v: dict) -> "str | None":
    if not v["order_form_name"]:
        return "주문서명은 필수"
    if v["yearly_fee"] <= 0:
        return "Yearly Fee(계약금액)는 0보다 커야 함"
    if v["billing_frequency"] != "one-time" and not v["subscription_end_date"]:
        return "Subscription End Date 가 필요합니다 (일회성 제외)"
    if sum(rs.ratio for rs in v["revenue_shares"]) > 1.0001:
        return "파트너 배분율 합계가 1.0을 초과"
    return None


@st.dialog("📋 신규 계약 추가", width="large")
def dialog_add_contract():
    if not customers:
        st.warning("먼저 고객사를 등록해주세요.")
        return
    with st.form("dlg_add_ct"):
        v = contract_form_fields("add_ct")
        if st.form_submit_button("저장", type="primary"):
            err = _validate_contract(v)
            if err:
                st.error(err)
                return
            v["status"] = "active"
            contracts.append(ar_models.Contract(
                id=ar_models.next_contract_id(contracts), created_at=today.isoformat(), **v,
            ))
            ar_models.save_contracts(contracts)
            st.rerun()


@st.dialog("📋 계약 수정", width="large")
def dialog_edit_contract(contract_id: str):
    ct = contract_by_id.get(contract_id)
    if not ct:
        st.error("계약 없음")
        return
    with st.form(f"dlg_edit_ct_{contract_id}"):
        # 위젯 키를 계약 ID별로 분리 → 다른 계약 수정값이 섞이지 않게(배분율 사라짐/생김 방지)
        v = contract_form_fields(f"edit_ct_{contract_id}", ct)
        cols = st.columns([1, 1])
        with cols[0]:
            if st.form_submit_button("저장", type="primary", use_container_width=True):
                err = _validate_contract(v)
                if err:
                    st.error(err)
                    return
                for x in contracts:
                    if x.id == contract_id:
                        for kk, vv in v.items():
                            setattr(x, kk, vv)
                ar_models.save_contracts(contracts)
                st.rerun()
        with cols[1]:
            if st.form_submit_button("🗑 계약 삭제", use_container_width=True):
                contracts[:] = [x for x in contracts if x.id != contract_id]
                # 관련 수금 서명 상태 정리
                for key in [k for k in collections if k.startswith(f"{contract_id}|")]:
                    collections.pop(key, None)
                ar_models.save_contracts(contracts)
                ar_models.save_collections(collections)
                st.rerun()


# ═══════════════════════════════════════════════════════════
# 탭
# ═══════════════════════════════════════════════════════════
tab_dash, tab_cust, tab_ct, tab_collect, tab_payout, tab_settle, tab_staff = st.tabs([
    "🏠 대시보드", "🏢 고객사", "📋 계약", "📊 수금 확인", "💸 배분 확인", "📑 정산 현황표", "👥 담당자",
])


def render_proc_row(p, *, date_label: str, ref_date, stage: str) -> None:
    """대시보드 읽기전용 행. ref_date 기준 경과/잔여 표시."""
    cust = customer_by_id.get(p.customer_id)
    ct = contract_by_id.get(p.contract_id)
    d_left = (ref_date - today).days if ref_date else 0
    if d_left < 0:
        timing_col, timing_txt = C_RED, f"{-d_left}일 경과"
    elif d_left == 0:
        timing_col, timing_txt = C_AMBER, "오늘"
    else:
        timing_col = C_AMBER if d_left <= 7 else C_LABEL
        timing_txt = f"D-{d_left}"
    date_str = ref_date.isoformat() if ref_date else "—"
    with st.container(border=True):
        cols = st.columns([3, 2.2, 2, 2, 2.2])
        with cols[0]:
            st.markdown(
                f"<div style='font-weight:600;color:{C_TEXT};'>🏢 {cust.name if cust else '?'}</div>"
                f"<div style='font-size:0.76rem;color:{C_MUTED};'>"
                f"{(ct.order_form_name if ct else '?')} · {p.label}</div>",
                unsafe_allow_html=True,
            )
        with cols[1]:
            st.markdown(f"<div style='margin-bottom:3px;'>{stage_badge(stage)}</div>"
                        + label_block("AM", (ct.am if ct and ct.am else "—")),
                        unsafe_allow_html=True)
        with cols[2]:
            st.markdown(label_block(f"{date_label} {date_str}", timing_txt, color=timing_col),
                        unsafe_allow_html=True)
        with cols[3]:
            st.markdown(money_dual(p.amount, p.currency, align="left"), unsafe_allow_html=True)


# ────────────────── 🏠 대시보드 ──────────────────
with tab_dash:
    if not periods_all:
        st.info("active 계약이 없거나 계약 기간/금액이 비어 있어 수금 예정을 계산할 수 없습니다. "
                "**📋 계약** 탭에서 계약을 등록·수정하세요.")
    else:
        cur_qi = quarter_index(today)
        if "dash_qi" not in st.session_state:
            st.session_state["dash_qi"] = cur_qi

        # ① 분기 네비 + 크게
        nav = st.columns([1.1, 4, 1.1])
        with nav[0]:
            if st.button("◀ 이전 분기", use_container_width=True):
                st.session_state["dash_qi"] -= 1
                st.rerun()
        with nav[2]:
            if st.button("다음 분기 ▶", use_container_width=True):
                st.session_state["dash_qi"] += 1
                st.rerun()
        qi = st.session_state["dash_qi"]
        qstart, qend, qlabel = quarter_range(qi)
        tag = "이번 분기" if qi == cur_qi else ("지난 분기" if qi < cur_qi else "다음 분기")
        with nav[1]:
            st.markdown(
                f"<div style='text-align:center;'>"
                f"<div style='font-size:0.8rem;color:{C_MUTED};'>{tag}</div>"
                f"<div style='font-size:2.4rem;font-weight:800;color:{C_TEXT};letter-spacing:-0.02em;line-height:1.1;'>{qlabel}</div>"
                f"<div style='font-size:0.74rem;color:{C_LABEL};'>{qstart} ~ {qend}</div>"
                f"</div>", unsafe_allow_html=True)

        # 직전 분기 범위
        pstart, pend, plabel = quarter_range(qi - 1)

        # 이번 분기 청구/수금 (수금 예정일이 이 분기)
        q_periods = [p for p in periods_all if qstart <= _due(p) <= qend]
        coll_q = [p for p in q_periods if period_paid(p)]
        q_billed_usd, q_billed_krw = _sum_usd(q_periods), _sum_krw(q_periods)
        collect_usd, collect_krw = _sum_usd(coll_q), _sum_krw(coll_q)

        # 직전 분기 수금 (= 이번 분기 파트너 배분의 기준 금액 · 배분은 다음 분기에 정산)
        prev_periods = [p for p in periods_all if pstart <= _due(p) <= pend]
        coll_prev = [p for p in prev_periods if period_paid(p)]
        prev_collect_usd, prev_collect_krw = _sum_usd(coll_prev), _sum_krw(coll_prev)

        # 이번 분기 파트너 배분 = 직전 분기 수금분 × 배분율
        pq_amt = []  # (native_amount, currency)
        for p in coll_prev:
            ct = contract_by_id.get(p.contract_id)
            if not ct:
                continue
            for rs in ct.revenue_shares:
                if rs.ratio > 0:
                    pq_amt.append((p.amount * rs.ratio, p.currency))
        payout_usd = sum(to_usd(a, c) for a, c in pq_amt)
        payout_krw = sum(to_krw(a, c) for a, c in pq_amt)
        net_usd, net_krw = collect_usd - payout_usd, collect_krw - payout_krw

        st.markdown("")
        # 1~4) 핵심 지표
        m = st.columns(4)
        with m[0]:
            arr_sub = f"진행중 계약 {len(running_contracts)}개"
            if expired_active:
                arr_sub += f" · 만료 {len(expired_active)}개 제외"
            st.markdown(kpi_card("📈 총 ARR", arr_usd, arr_krw, arr_sub), unsafe_allow_html=True)
        with m[1]:
            st.markdown(kpi_card(f"💰 {qlabel} 총 수금", collect_usd, collect_krw,
                                 f"청구 {fmt_usd(q_billed_usd)} 중 · {len(coll_q)}/{len(q_periods)}건",
                                 accent=C_GREEN if coll_q else C_TEXT), unsafe_allow_html=True)
        with m[2]:
            st.markdown(kpi_card(f"💸 {qlabel} 파트너 배분", payout_usd, payout_krw,
                                 f"직전분기({plabel}) 수금 × 배분율", accent=C_BLUE if pq_amt else C_TEXT),
                        unsafe_allow_html=True)
        with m[3]:
            st.markdown(kpi_card(f"✅ {qlabel} 총 순수익", net_usd, net_krw,
                                 "총 수금 − 파트너 배분", accent=C_GREEN), unsafe_allow_html=True)

        st.divider()

        # 5) 고객사별 수금 — 이번 분기 (먼저)
        st.markdown(f"##### 🏢 고객사별 수금 · 이번 분기 {qlabel}")
        st.caption("계약상 분기 청구액 대비 실제 수금")
        cagg: dict[str, list] = {}  # cid -> [billed, collected]
        for p in q_periods:
            g = cagg.setdefault(p.customer_id, [0.0, 0.0])
            g[0] += to_usd(p.amount, p.currency)
            if period_paid(p):
                g[1] += to_usd(p.amount, p.currency)
        crows = []
        for cid, (bu, cu) in sorted(cagg.items(), key=lambda x: -x[1][0]):
            cust = customer_by_id.get(cid)
            out = bu - cu
            status = "🟢 수금완료" if out <= 1 else ("🟠 일부수금" if cu > 1 else "🔴 미수금")
            crows.append({
                "고객사": cust.name if cust else "?",
                "분기 청구": fmt_usd(bu),
                "수금": fmt_usd(cu),
                "미수금": fmt_usd(out) if out > 1 else "—",
                "상태": status,
            })
        if crows:
            st.dataframe(pd.DataFrame(crows), hide_index=True, use_container_width=True)
        else:
            st.caption(f"{qlabel}에 청구 예정인 수금이 없습니다.")

        st.markdown("")
        # 5-2) 고객사별 수금 — 직전 분기 (수금완료 여부 포함, 이번 분기 배분의 기준)
        st.markdown(f"##### 🏢 고객사별 수금 · 직전 분기 {plabel}")
        st.caption(f"이 수금액이 이번 분기({qlabel}) 파트너 배분의 기준 · 수금 완료 여부 포함")
        pagg: dict[str, list] = {}  # cid -> [billed, collected]
        for p in prev_periods:
            g = pagg.setdefault(p.customer_id, [0.0, 0.0])
            g[0] += to_usd(p.amount, p.currency)
            if period_paid(p):
                g[1] += to_usd(p.amount, p.currency)
        if pagg:
            prows = []
            for cid, (bu, cu) in sorted(pagg.items(), key=lambda x: -x[1][0]):
                cust = customer_by_id.get(cid)
                out = bu - cu
                status = "🟢 수금완료" if out <= 1 else ("🟠 일부수금" if cu > 1 else "🔴 미수금")
                prows.append({
                    "고객사": cust.name if cust else "?",
                    "분기 청구": fmt_usd(bu),
                    "수금": fmt_usd(cu),
                    "미수금": fmt_usd(out) if out > 1 else "—",
                    "상태": status,
                })
            st.dataframe(pd.DataFrame(prows), hide_index=True, use_container_width=True)
        else:
            st.caption(f"{plabel}에 청구 예정인 수금이 없습니다.")

        st.markdown("")
        # 6) 파트너사별 배분 — 파트너별 요약(총 배분 중 완료) + 자세히 보기(계약별 상세)
        st.markdown(f"##### 💸 파트너사별 배분 · 이번 분기 {qlabel}")
        st.caption(f"직전분기({plabel}) 수금 기준 · 파트너별 총 배분 중 완료 / 펼치면 고객사·계약별 상세")
        # owner -> {"total","done","items": {(cust,ct): [payout, done]}}
        owner_agg: dict[str, dict] = {}
        for p in coll_prev:
            ct = contract_by_id.get(p.contract_id)
            cust = customer_by_id.get(p.customer_id)
            if not ct:
                continue
            rec = collections.get(p.key)
            for rs in ct.revenue_shares:
                if rs.ratio <= 0:
                    continue
                payout = to_usd(p.amount * rs.ratio, p.currency)
                done = payout if ar_models.is_owner_paid_out(rec, rs.owner) else 0.0
                g = owner_agg.setdefault(rs.owner, {"total": 0.0, "done": 0.0, "items": {}})
                g["total"] += payout
                g["done"] += done
                key = (cust.name if cust else "?", ct.order_form_name or ct.id, rs.ratio)
                e = g["items"].setdefault(key, [0.0, 0.0])
                e[0] += payout
                e[1] += done
        if not owner_agg:
            st.caption(f"직전분기({plabel}) 수금분이 없어 이번 분기 배분 대상이 없습니다.")
        else:
            for owner in sorted(owner_agg, key=lambda o: -owner_agg[o]["total"]):
                g = owner_agg[owner]
                total, done = g["total"], g["done"]
                pending = total - done
                if pending <= 1:
                    summary = f"✅ 전액 배분완료 {fmt_usd(total)}"
                elif done > 1:
                    summary = f"배분 {fmt_usd(total)} 중 ✅ {fmt_usd(done)} 완료 · 🟠 {fmt_usd(pending)} 미완료"
                else:
                    summary = f"배분 {fmt_usd(total)} · 🟠 전액 미완료"
                with st.expander(f"💳 **{owner}** — {summary}", expanded=False):
                    rows = []
                    for (cust_nm, ct_nm, ratio), (pay, dn) in sorted(g["items"].items(), key=lambda x: -x[1][0]):
                        pend = pay - dn
                        stt = "🟢 완료" if pend <= 1 else ("🟠 일부완료" if dn > 1 else "🔴 미완료")
                        rows.append({
                            "고객사": cust_nm,
                            "계약(주문서)": ct_nm,
                            "배분율": f"{ratio*100:.0f}%",
                            "배분액": fmt_usd(pay),
                            "완료": fmt_usd(dn) if dn > 1 else "—",
                            "미완료": fmt_usd(pend) if pend > 1 else "—",
                            "상태": stt,
                        })
                    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

        st.divider()

        # 7) Alert — 현재 처리 필요한 것들 (분기와 무관, 실시간 상태)
        st.markdown("##### 🔔 Alert  ·  지금 처리해야 할 것")
        udue_usd, udue_krw = _sum_usd(unpaid_due), _sum_krw(unpaid_due)

        # 계약 만료 알림 (12개월 이내 또는 만료) — 위험도 분류
        def _ct_end(ct):
            return ar_models.parse_iso(ct.subscription_end_date) or ar_models.parse_iso(ct.termination_date)

        def _expiry_risk(d: int, auto: bool):
            """자동 갱신 계약은 만료 안 됨(None). 수동 갱신만 3·6·9·12개월 위험도."""
            if auto:
                return None
            if d < 0:
                return ("만료됨", C_RED)
            if d > 365:
                return None
            if d <= 90:
                return ("위험·3개월", C_RED)
            if d <= 180:
                return ("주의·6개월", C_AMBER)
            if d <= 270:
                return ("관심·9개월", "#EAB308")
            return ("여유·12개월", C_BLUE)

        expiring = []
        for ct in active_contracts:
            end = _ct_end(ct)
            if not end:
                continue
            d = (end - today).days
            r = _expiry_risk(d, ct.auto_renewal)
            if r:
                expiring.append((d, ct, end, r))
        expiring.sort(key=lambda x: x[0])
        exp_urgent = sum(1 for d, ct, _, _ in expiring if d < 0 or d <= 90)
        exp_color = C_RED if exp_urgent else (C_AMBER if expiring else C_AMBER)

        a = st.columns(5)
        with a[0]:
            st.markdown(alert_card("💰", "미수금", len(unpaid_due), udue_usd, udue_krw, C_AMBER),
                        unsafe_allow_html=True)
        with a[1]:
            st.markdown(alert_card("💸", "미배분", len(payouts), pay_usd, pay_krw, C_BLUE),
                        unsafe_allow_html=True)
        with a[2]:
            st.markdown(alert_card("🧾", "발행 필요", len(need_invoice), ni_usd, ni_krw, C_AMBER),
                        unsafe_allow_html=True)
        with a[3]:
            st.markdown(alert_card("🔥", "입금 연체", len(pay_overdue), po_usd, po_krw, C_RED),
                        unsafe_allow_html=True)
        with a[4]:
            on = len(expiring) > 0
            c = exp_color if on else "#6B7280"
            bg = f"{exp_color}14" if on else "rgba(255,255,255,0.02)"
            bd = f"{exp_color}44" if on else "rgba(255,255,255,0.08)"
            st.markdown(
                f"<div style='border:1px solid {bd};border-radius:12px;padding:12px 14px;background:{bg};'>"
                f"<div style='font-size:0.8rem;color:{C_MUTED};'>📑 계약 만료</div>"
                f"<div style='font-size:1.5rem;font-weight:800;color:{c};line-height:1.2;margin-top:2px;'>{len(expiring)}건</div>"
                f"<div style='font-size:0.72rem;color:{C_MUTED};'>12개월 내 · 긴급 {exp_urgent}건</div>"
                f"</div>", unsafe_allow_html=True)

        # 계약 만료 상세 (직관적 위험도 표시)
        if expiring:
            st.markdown("**📑 계약 만료 알림**  ·  3·6·9·12개월 위험도 / 자동갱신 없으면 긴급")
            for d, ct, end, (label, color) in expiring:
                cust = customer_by_id.get(ct.customer_id)
                mo = max(0, round(d / 30.4))
                if d < 0:
                    dtxt = f"{-d}일 지남 (만료)"
                else:
                    dtxt = f"D-{d} · 약 {mo}개월 남음"
                renew = "🔁 자동갱신" if ct.auto_renewal else "✋ 수동갱신"
                with st.container(border=True):
                    cc = st.columns([3.2, 2.4, 1.6, 3])
                    with cc[0]:
                        st.markdown(
                            f"<div style='font-weight:600;color:{C_TEXT};'>🏢 {cust.name if cust else '?'}</div>"
                            f"<div style='font-size:0.76rem;color:{C_MUTED};'>{ct.order_form_name or '(제목 없음)'}</div>",
                            unsafe_allow_html=True)
                    with cc[1]:
                        st.markdown(label_block(f"만료일 {end}", dtxt, color=color), unsafe_allow_html=True)
                    with cc[2]:
                        rc = C_GREEN if ct.auto_renewal else C_RED
                        st.markdown(f"<div style='font-size:0.85rem;color:{rc};'>{renew}</div>",
                                    unsafe_allow_html=True)
                    with cc[3]:
                        st.markdown(f"<div style='text-align:right;'>{badge(label, color)}</div>",
                                    unsafe_allow_html=True)

        st.markdown("")
        if need_invoice or pay_overdue or payout_inprogress:
            with st.expander("🔎 미수금·미배분 상세 목록 보기"):
                LIMIT = 10
                if need_invoice:
                    st.markdown("**🧾 인보이스 발행 필요**")
                    for p in need_invoice[:LIMIT]:
                        render_proc_row(p, date_label="도래일", ref_date=_due(p), stage="발행필요")
                if pay_overdue:
                    st.markdown("**🔥 입금 연체**")
                    for p in pay_overdue[:LIMIT]:
                        render_proc_row(p, date_label="입금기한", ref_date=payment_due_date(p), stage="입금연체")
                if payout_inprogress:
                    st.markdown("**💸 배분 진행(미배분)**")
                    for p in payout_inprogress[:LIMIT]:
                        render_proc_row(p, date_label="정산기한", ref_date=payout_due_date(p), stage="배분진행")
        elif not expiring:
            st.success("처리할 알림이 없습니다 👍")

        # 8) 월별 입출 현황 (예상) — 맨 아래 토글 (월별 + 분기별 + 연도별 요약)
        st.divider()
        with st.expander("📅 월별 입출 현황 (예상)  ·  월별 / 분기별 / 연도별", expanded=False):
            st.caption("진행중 계약의 수금 예정 = 입금, 그 수금분의 다음 분기 첫 달 파트너 배분 = 지출. "
                       "계약·배분율 기반 예상치(실제 수금/배분 상태와 무관).")

            def _add_b(d: dict, key, usd: float, krw: float) -> None:
                e = d.setdefault(key, [0.0, 0.0])
                e[0] += usd
                e[1] += krw

            in_by_m: dict = {}    # (y,m) -> [usd, krw] 수금(입금)
            out_by_m: dict = {}   # (y,m) -> [usd, krw] 파트너 배분(지출)
            for ct in running_contracts:
                ratio_sum = sum(rs.ratio for rs in ct.revenue_shares if rs.ratio > 0)
                for p in periods_by_contract.get(ct.id, []):
                    due = _due(p)
                    _add_b(in_by_m, (due.year, due.month),
                           to_usd(p.amount, p.currency), to_krw(p.amount, p.currency))
                    if ratio_sum > 0:
                        nstart, _, _ = quarter_range(quarter_index(due) + 1)
                        _add_b(out_by_m, (nstart.year, nstart.month),
                               to_usd(p.amount * ratio_sum, p.currency),
                               to_krw(p.amount * ratio_sum, p.currency))

            def _signed(v: float) -> str:
                return ("-" + fmt_usd(-v)) if v < 0 else fmt_usd(v)

            # (y,m) 데이터를 임의 버킷으로 굴려서 입금/지출/순/누적 표 생성
            def _roll(bucket_of, label_of, keys_filter=None):
                bin_in: dict = {}
                bin_out: dict = {}
                for (y, m), (u, w) in in_by_m.items():
                    bin_in[bucket_of(y, m)] = bin_in.get(bucket_of(y, m), 0.0) + u
                for (y, m), (u, w) in out_by_m.items():
                    bin_out[bucket_of(y, m)] = bin_out.get(bucket_of(y, m), 0.0) + u
                bkeys = sorted(set(bin_in) | set(bin_out))
                if keys_filter is not None:
                    bkeys = [b for b in bkeys if keys_filter(b)]
                rows, cum = [], 0.0
                for b in bkeys:
                    iu, ou = bin_in.get(b, 0.0), bin_out.get(b, 0.0)
                    net = iu - ou
                    cum += net
                    rows.append({
                        label_of[0]: label_of[1](b),
                        "입금(수금)": fmt_usd(iu) if iu else "—",
                        "지출(배분)": "-" + fmt_usd(ou) if ou else "—",
                        "순현금": _signed(net),
                        "누적 순현금": _signed(cum),
                    })
                return rows

            cur_my = (today.year, today.month)
            v_month, v_quarter, v_year = st.tabs(["월별", "분기별", "연도별"])

            with v_month:
                rows = _roll(lambda y, m: (y, m),
                             ("월", lambda b: f"{b[0]}-{b[1]:02d}"),
                             keys_filter=lambda b: b >= cur_my)
                rows = rows[:24]
                if rows:
                    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
                    st.caption(f"현재 월부터 향후 {len(rows)}개월")
                else:
                    st.caption("예상 입출 내역이 없습니다.")

            with v_quarter:
                rows = _roll(lambda y, m: (y, (m - 1) // 3 + 1),
                             ("분기", lambda b: f"{b[0]} Q{b[1]}"),
                             keys_filter=lambda b: b >= (today.year, (today.month - 1) // 3 + 1))
                if rows:
                    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
                else:
                    st.caption("예상 입출 내역이 없습니다.")

            with v_year:
                rows = _roll(lambda y, m: y,
                             ("연도", lambda b: f"{b}년"),
                             keys_filter=lambda b: b >= today.year)
                if rows:
                    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
                else:
                    st.caption("예상 입출 내역이 없습니다.")


# ────────────────── 🏢 고객사 ──────────────────
with tab_cust:
    head_cols = st.columns([3, 1])
    with head_cols[0]:
        st.markdown("##### 🏢 고객사 목록")
    with head_cols[1]:
        if st.button("➕ 신규 고객사 추가", type="primary", use_container_width=True, key="btn_add_cust"):
            dialog_add_customer()

    if not customers:
        st.caption("등록된 고객사가 없습니다. 위 버튼으로 추가하세요.")
    else:
        # 고객사별 미수금(도래·미입금) 합계
        out_usd_by_cust: dict[str, float] = {}
        out_krw_by_cust: dict[str, float] = {}
        for p in unpaid_due:
            out_usd_by_cust[p.customer_id] = out_usd_by_cust.get(p.customer_id, 0.0) + to_usd(p.amount, p.currency)
            out_krw_by_cust[p.customer_id] = out_krw_by_cust.get(p.customer_id, 0.0) + to_krw(p.amount, p.currency)

        q = st.text_input("🔍 고객사 검색", placeholder="고객사명 / 담당자명",
                          label_visibility="collapsed", key="cust_search").strip().lower()

        # 계약금액(ARR) 큰 순 → 미수금 → 계약 수 → 이름 순 정렬
        cust_rows = []
        for c in customers:
            ccts = [ct for ct in contracts if ct.customer_id == c.id]
            n_ct = len(ccts)
            arr_u = sum(to_usd(ct.yearly_fee, ct.currency) for ct in ccts if ct.status == "active")
            arr_k = sum(to_krw(ct.yearly_fee, ct.currency) for ct in ccts if ct.status == "active")
            cust_rows.append((c, n_ct,
                              out_usd_by_cust.get(c.id, 0.0),
                              out_krw_by_cust.get(c.id, 0.0),
                              arr_u, arr_k))
        cust_rows.sort(key=lambda t: (-t[4], -t[2], -t[1], t[0].name.lower()))

        total_arr_usd = sum(r[4] for r in cust_rows)
        total_out_usd = sum(r[2] for r in cust_rows)
        st.caption(
            f"고객사 {len(customers)}곳 · 총 계약금액(ARR) {fmt_usd(total_arr_usd)} ({fmt_krw(sum(r[5] for r in cust_rows))}) "
            f"· 총 미수금 {fmt_usd(total_out_usd)} ({fmt_krw(sum(r[3] for r in cust_rows))})"
        )

        shown = 0
        for c, n_ct, o_usd, o_krw, arr_u, arr_k in cust_rows:
            ar_s = staff_by_id.get(c.ar_manager_id)
            acc_s = staff_by_id.get(c.accounting_id)
            if q:
                hay = " ".join([c.name, c.biz_no, c.contact_name,
                                ar_s.name if ar_s else "", acc_s.name if acc_s else ""]).lower()
                if q not in hay:
                    continue
            shown += 1

            # 담당자: 있는 것만 한 줄로
            mgr_bits = []
            if ar_s:
                mgr_bits.append(f"<span style='color:{C_LABEL};'>AR</span> {ar_s.name}")
            if acc_s:
                mgr_bits.append(f"<span style='color:{C_LABEL};'>회계</span> {acc_s.name}")
            mgr_html = "  ·  ".join(mgr_bits) if mgr_bits else f"<span style='color:{C_LABEL};'>담당자 미지정</span>"

            sub = " · ".join([b for b in (c.biz_no, c.contact_name) if b])

            # 미수금: 0이면 — 로 깔끔하게
            if o_usd > 0:
                amt_inner = (
                    f"<span style='font-family:{MONO};font-weight:700;color:{C_AMBER};'>{fmt_usd(o_usd)}</span>"
                    f"<br><span style='font-size:0.72rem;color:{C_MUTED};font-family:{MONO};'>{fmt_krw(o_krw)}</span>"
                )
            else:
                amt_inner = f"<span style='color:{C_LABEL};'>—</span>"

            sub_line = (f"<div style='font-size:0.76rem;color:{C_MUTED};margin-top:1px;'>{sub}</div>"
                        if sub else "")
            flex_html = (
                "<div style='display:flex;align-items:center;gap:22px;flex-wrap:nowrap;'>"
                f"<div style='width:270px;flex:0 0 auto;'>"
                f"<div style='font-weight:600;color:{C_TEXT};font-size:1.0rem;'>🏢 {c.name}</div>{sub_line}</div>"
                f"<div style='width:200px;flex:0 0 auto;'>"
                f"<div style='font-size:0.86rem;color:{C_TEXT};'>{mgr_html}</div>"
                f"<div style='font-size:0.76rem;color:{C_LABEL};margin-top:2px;'>계약 {n_ct}건</div></div>"
                f"<div style='width:150px;flex:0 0 auto;'>"
                f"<div style='font-size:0.72rem;color:{C_LABEL};'>계약금액(ARR)</div>"
                f"<div style='font-family:{MONO};font-weight:700;color:{C_TEXT};font-size:0.92rem;'>{fmt_usd(arr_u)}</div>"
                f"<div style='font-size:0.72rem;color:{C_MUTED};font-family:{MONO};'>{fmt_krw(arr_k)}</div></div>"
                f"<div style='width:150px;flex:0 0 auto;'>"
                f"<div style='font-size:0.72rem;color:{C_LABEL};'>미수금</div>"
                f"<div style='font-size:0.92rem;'>{amt_inner}</div></div>"
                "</div>"
            )
            with st.container(border=True):
                cols = st.columns([9, 1.3, 1.7])
                with cols[0]:
                    st.markdown(flex_html, unsafe_allow_html=True)
                with cols[1]:
                    if st.button("✏️ 수정", key=f"edit_cust_{c.id}", use_container_width=True):
                        dialog_edit_customer(c.id)
        if q and shown == 0:
            st.caption("검색 결과가 없습니다.")


# ────────────────── 📋 계약 ──────────────────
with tab_ct:
    head_cols = st.columns([3, 1])
    with head_cols[0]:
        st.markdown("##### 📋 계약(주문서) 목록")
    with head_cols[1]:
        if st.button("➕ 신규 계약 추가", type="primary", use_container_width=True, key="btn_add_ct",
                     disabled=len(customers) == 0):
            dialog_add_contract()

    if not customers:
        st.warning("계약을 등록하려면 먼저 🏢 고객사를 등록해주세요.")
    elif not contracts:
        st.caption("등록된 계약이 없습니다. 위 버튼으로 추가하세요.")
    else:
        for ct in contracts:
            cust = customer_by_id.get(ct.customer_id)
            ct_periods = periods_by_contract.get(ct.id, []) if ct.status == "active" else []
            n_periods = len(ct_periods)
            n_done = sum(1 for p in ct_periods if period_paid(p))
            renew = "자동갱신" if ct.auto_renewal else "수동갱신"
            with st.container(border=True):
                cols = st.columns([3, 2.4, 2, 1.1, 1, 1.8])
                with cols[0]:
                    cust_html = f"🏢 {cust.name}" if cust else "⚠ 고객사 미지정"
                    cust_color = C_TEXT if cust else C_RED
                    st.markdown(
                        f"<div style='font-weight:700; color:{cust_color}; font-size:1.05rem;'>{cust_html}</div>"
                        f"<div style='font-size:0.86rem; color:{C_TEXT}; margin-top:2px;'>{ct.order_form_name or '(제목 없음)'}</div>"
                        f"<div style='font-size:0.74rem; color:{C_MUTED}; margin-top:1px;'>"
                        f"{ct.contract_type} · {ct.provided_data or '제공데이터 미기재'}</div>",
                        unsafe_allow_html=True,
                    )
                with cols[1]:
                    st.markdown(
                        label_block("기간(Effective~Sub.End)",
                                    f"{ct.effective_date or '—'} ~ {ct.subscription_end_date or '—'}")
                        + label_block("Billing · 갱신 · AM",
                                      f"{ct.billing_frequency} · {renew} · {ct.am or '—'}", top=4)
                        + label_block("기한",
                                      f"입금 {ct.payment_terms_days}일 · 배분 다음분기", top=4),
                        unsafe_allow_html=True,
                    )
                with cols[2]:
                    st.markdown(
                        f"<div style='font-size:0.74rem;color:{C_LABEL};'>ARR (Yearly Fee)</div>"
                        + f"<div style='font-family:{MONO};font-weight:700;color:{C_TEXT};font-size:0.95rem;'>{fmt_usd(to_usd(ct.yearly_fee, ct.currency))}</div>"
                        + f"<div style='font-size:0.72rem;color:{C_MUTED};font-family:{MONO};'>{fmt_krw(to_krw(ct.yearly_fee, ct.currency))}</div>"
                        + label_block("수금 확인", f"{n_done}/{n_periods}회", top=4),
                        unsafe_allow_html=True,
                    )
                with cols[3]:
                    st.markdown(contract_badge(ct), unsafe_allow_html=True)
                with cols[4]:
                    if st.button("✏️ 수정", key=f"edit_ct_{ct.id}", use_container_width=True):
                        dialog_edit_contract(ct.id)


# ────────────────── 📊 수금 확인 ──────────────────
# 체크박스는 on_change 콜백으로 처리 — 명시적 st.rerun() 없이 한 번만 다시 그려져
# 화면 깜빡임/점프를 막는다. (Streamlit이 위젯 변경 시 자동으로 1회 rerun)
def _sync_paid(rec: dict) -> None:
    if ar_models.is_paid(rec):
        if not rec.get("paid_at"):
            rec["paid_at"] = today.isoformat()
    else:
        rec["paid_at"] = ""


def _cb_invoice(pkey: str) -> None:
    val = bool(st.session_state.get(f"inv_{pkey}"))
    rec = collections.get(pkey) or ar_models.empty_record()
    rec["invoiced"] = val
    rec["invoiced_at"] = today.isoformat() if val else ""
    if not val:
        # 발행 해제 시 입금 3자 체크도 해제 (위젯 상태까지 동기화)
        for r in ar_models.COLLECTION_ROLES:
            rec[r] = False
            st.session_state[f"chk_{pkey}_{r}"] = False
    _sync_paid(rec)
    collections[pkey] = rec
    ar_models.save_collections(collections)


def _cb_role(pkey: str, role: str) -> None:
    val = bool(st.session_state.get(f"chk_{pkey}_{role}"))
    rec = collections.get(pkey) or ar_models.empty_record()
    rec[role] = val
    _sync_paid(rec)
    collections[pkey] = rec
    ar_models.save_collections(collections)


def _cb_payout(pkey: str, owner: str, step: str) -> None:
    val = bool(st.session_state.get(f"po_{pkey}_{owner}_{step}"))
    rec = collections.get(pkey) or ar_models.empty_record()
    ar_models.set_payout_step(rec, owner, step, val)
    if not val:
        # 앞 단계 해제 시 뒤 단계도 해제 (순차)
        steps = ar_models.PAYOUT_STEPS
        for later in steps[steps.index(step) + 1:]:
            ar_models.set_payout_step(rec, owner, later, False)
            st.session_state[f"po_{pkey}_{owner}_{later}"] = False
    collections[pkey] = rec
    ar_models.save_collections(collections)


def collect_stage(p) -> str:
    """수금 단계만 (배분 제외): 예정 / 발행필요 / 입금대기 / 입금연체 / 입금완료."""
    s = collections.get(p.key) or {}
    if not s.get("invoiced"):
        return "예정" if _due(p) > today else "발행필요"
    if not ar_models.is_paid(s):
        pdd = payment_due_date(p)
        return "입금연체" if (pdd and today > pdd) else "입금대기"
    return "입금완료"


with tab_collect:
    st.markdown("##### 📊 수금 확인  ·  ① 인보이스 발행 → ② 입금(책임자·회계·AR 3자 체크)")
    st.caption("발행해야 입금 체크가 열립니다. 입금(3자 체크)이 끝나면 **💸 배분 확인** 탭으로 넘어갑니다.")
    if not periods_all:
        st.caption("active 계약에서 계산된 수금 예정이 없습니다. 계약의 기간·금액·Billing Frequency를 확인하세요.")
    else:
        top = st.columns([2.4, 1.6, 2])
        with top[0]:
            ct_opts = ["(전체)"] + [c.id for c in active_contracts]
            ct_sel = st.selectbox(
                "계약 선택", ct_opts,
                format_func=lambda x: "전체 계약" if x == "(전체)" else contract_by_id[x].order_form_name,
                key="coll_ct",
            )
        with top[1]:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            include_future = st.toggle("🔜 미도래(예정) 포함", value=False, key="coll_future",
                                       help="끄면 수금일이 도래한 항목만, 켜면 미래 예정분까지 표시합니다.")
        with top[2]:
            sort_sel = st.segmented_control(
                "정렬", ["예정일 빠른순", "예정일 늦은순"],
                default="예정일 빠른순", key="coll_sort",
            )

        base = [p for p in periods_all if ct_sel == "(전체)" or p.contract_id == ct_sel]
        if not include_future:
            base = [p for p in base if collect_stage(p) != "예정"]

        stage_opts = ["발행필요", "입금대기", "입금연체", "입금완료"]
        if include_future:
            stage_opts = ["예정"] + stage_opts
        counts = {s: 0 for s in stage_opts}
        for p in base:
            cs = collect_stage(p)
            if cs in counts:
                counts[cs] += 1
        default_stages = [s for s in stage_opts if s not in ("입금완료", "예정") and counts[s] > 0]
        stage_sel = st.segmented_control(
            "단계 필터  ·  버튼으로 켜고 끄기 (여러 개)",
            stage_opts, selection_mode="multi",
            default=default_stages or stage_opts,
            format_func=lambda s, c=counts: f"{STAGE[s][0]}  {c[s]}",
            key="coll_stage",
        )

        rows = [p for p in base if (not stage_sel) or collect_stage(p) in stage_sel]
        rows.sort(key=_due, reverse=(bool(sort_sel) and "늦은" in sort_sel))

        st.caption(f"{len(rows)}건 · 합계 {fmt_usd(_sum_usd(rows))} ({fmt_krw(_sum_krw(rows))})")

        role_labels = ar_models.COLLECTION_ROLE_LABELS
        for p in rows[:60]:
            cust = customer_by_id.get(p.customer_id)
            ct = contract_by_id.get(p.contract_id)
            s = collections.get(p.key) or ar_models.empty_record()
            stage = collect_stage(p)
            invoiced = bool(s.get("invoiced"))

            with st.container(border=True):
                head = st.columns([3, 1.6, 2.6])
                with head[0]:
                    st.markdown(
                        f"<div style='font-weight:600;color:{C_TEXT};'>🏢 {cust.name if cust else '?'}</div>"
                        f"<div style='font-size:0.76rem;color:{C_MUTED};'>"
                        f"{(ct.order_form_name if ct else '?')} · {p.label} · 도래 {p.due_date}</div>",
                        unsafe_allow_html=True,
                    )
                with head[1]:
                    st.markdown(money_dual(p.amount, p.currency, align="left"), unsafe_allow_html=True)
                with head[2]:
                    if stage in ("입금대기", "입금연체"):
                        dd = payment_due_date(p)
                        dl = (dd - today).days if dd else 0
                        note = (f"입금기한 {dd} · " + (f"{-dl}일 경과" if dl < 0 else (f"D-{dl}" if dl > 0 else "오늘"))) if dd else ""
                        col = C_RED if stage == "입금연체" else C_MUTED
                    elif stage == "발행필요":
                        dl = (_due(p) - today).days
                        note = f"도래 {-dl}일 경과" if dl < 0 else "오늘 도래"
                        col = C_AMBER
                    else:
                        note = ""
                        col = C_MUTED
                    st.markdown(
                        f"<div style='text-align:right;'>{stage_badge(stage)}"
                        + (f"<div style='font-size:0.72rem;color:{col};margin-top:4px;'>{note}</div>" if note else "")
                        + "</div>", unsafe_allow_html=True)

                # 처리: 발행 → 입금 3자 (on_change 콜백 → 깜빡임 없음)
                ctrl = st.columns([1.7, 1, 1, 1, 1.7])
                with ctrl[0]:
                    st.checkbox("🧾 인보이스 발행", value=invoiced, key=f"inv_{p.key}",
                                on_change=_cb_invoice, args=(p.key,))
                for j, role in enumerate(ar_models.COLLECTION_ROLES):
                    with ctrl[1 + j]:
                        st.checkbox(role_labels[role], value=bool(s.get(role)),
                                    key=f"chk_{p.key}_{role}", disabled=not invoiced,
                                    on_change=_cb_role, args=(p.key, role))
        if len(rows) > 60:
            st.caption("⚠️ 표시 한계 60건. 계약/단계 필터로 좁혀주세요.")


# ────────────────── 💸 배분 확인 ──────────────────
def payout_owner_stage(p, owner) -> str:
    rec = collections.get(p.key) or {}
    return "배분완료" if ar_models.is_owner_paid_out(rec, owner) else "배분진행"


with tab_payout:
    st.markdown("##### 💸 배분 확인  ·  세금계산서 발행 → 지출결의 → 입금 (파트너사별)")
    st.caption("수금(입금) 완료분만 표시됩니다. 정산 기한 = **수금 완료 분기의 다음 분기 시작일** "
               "(예: 1~3월 수금 → 4/1). 세 단계를 순서대로 체크하면 배분 완료.")

    # 입금 완료 + 파트너 배분이 있는 (기간 × 파트너) 행
    payout_items = []  # (p, ct, rs)
    for p in periods_all:
        if not period_paid(p):
            continue
        ct = contract_by_id.get(p.contract_id)
        if not ct:
            continue
        for rs in ct.revenue_shares:
            if rs.ratio > 0:
                payout_items.append((p, ct, rs))

    if not payout_items:
        st.caption("입금 완료된 건 중 파트너 배분 대상이 없습니다. (수금 확인 탭에서 입금까지 완료 필요)")
    else:
        fcol = st.columns([2.4, 2])
        with fcol[0]:
            pct_opts = ["(전체)"] + sorted({rs.owner for _, _, rs in payout_items})
            owner_sel = st.selectbox("파트너사", pct_opts, key="po_owner")
        with fcol[1]:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)

        stages = ["배분진행", "배분완료"]
        counts = {s: 0 for s in stages}
        for p, ct, rs in payout_items:
            counts[payout_owner_stage(p, rs.owner)] += 1
        default_po = [s for s in stages if s != "배분완료" and counts[s] > 0]
        po_stage_sel = st.segmented_control(
            "상태 필터", stages, selection_mode="multi",
            default=default_po or stages,
            format_func=lambda s, c=counts: f"{STAGE[s][0]}  {c[s]}", key="po_stage",
        )

        rows = [(p, ct, rs) for (p, ct, rs) in payout_items
                if (owner_sel == "(전체)" or rs.owner == owner_sel)
                and (not po_stage_sel or payout_owner_stage(p, rs.owner) in po_stage_sel)]
        rows.sort(key=lambda t: payout_due_date(t[0]) or today)

        tot_usd = sum(to_usd(p.amount * rs.ratio, p.currency) for p, _, rs in rows)
        st.caption(f"{len(rows)}건 · 배분액 합계 {fmt_usd(tot_usd)} "
                   f"({fmt_krw(sum(to_krw(p.amount * rs.ratio, p.currency) for p, _, rs in rows))})")

        step_labels = ar_models.PAYOUT_STEP_LABELS
        for p, ct, rs in rows[:60]:
            cust = customer_by_id.get(p.customer_id)
            rec = collections.get(p.key) or ar_models.empty_record()
            steps = ar_models.get_payout_steps(rec, rs.owner)
            stage = payout_owner_stage(p, rs.owner)
            amt = p.amount * rs.ratio
            due = payout_due_date(p)
            dl = (due - today).days if due else 0

            with st.container(border=True):
                head = st.columns([3, 1.7, 2.5])
                with head[0]:
                    st.markdown(
                        f"<div style='font-weight:600;color:{C_TEXT};'>💸 {rs.owner}"
                        f"<span style='font-weight:400;color:{C_MUTED};font-size:0.8rem;'> · {rs.ratio*100:.0f}%</span></div>"
                        f"<div style='font-size:0.76rem;color:{C_MUTED};'>"
                        f"🏢 {cust.name if cust else '?'} · {(ct.order_form_name if ct else '?')} · {p.label}</div>",
                        unsafe_allow_html=True,
                    )
                with head[1]:
                    st.markdown(money_dual(amt, p.currency, align="left"), unsafe_allow_html=True)
                with head[2]:
                    if due:
                        note = f"정산기한 {payout_due_label(p)} · " + (f"{-dl}일 경과" if dl < 0 else (f"D-{dl}" if dl > 0 else "오늘"))
                    else:
                        note = ""
                    col = C_GREEN if stage == "배분완료" else C_MUTED
                    st.markdown(
                        f"<div style='text-align:right;'>{stage_badge(stage)}"
                        + (f"<div style='font-size:0.72rem;color:{col};margin-top:4px;'>{note}</div>" if note else "")
                        + "</div>", unsafe_allow_html=True)

                # 처리: 세금계산서 → 지출결의 → 입금 (순차, on_change 콜백)
                ctrl = st.columns([1.4, 1.4, 1.4, 2])
                prev_done = True
                for j, step in enumerate(ar_models.PAYOUT_STEPS):
                    with ctrl[j]:
                        st.checkbox(step_labels[step], value=bool(steps.get(step)),
                                    key=f"po_{p.key}_{rs.owner}_{step}",
                                    disabled=not prev_done,
                                    on_change=_cb_payout, args=(p.key, rs.owner, step))
                    prev_done = prev_done and bool(steps.get(step))
        if len(rows) > 60:
            st.caption("⚠️ 표시 한계 60건. 파트너/상태 필터로 좁혀주세요.")


# ────────────────── 📑 정산 현황표 ──────────────────
with tab_settle:
    _sh = st.columns([3, 1.4])
    with _sh[0]:
        st.markdown("##### 📑 정산 현황표  ·  배분사 × 계약별 분기 정산")
        st.caption("정산액 = 해당 분기 수금분 × 배분율 (진행중 계약·배분율 기반 예상치). 금액은 원화(₩) 기준.")
    with _sh[1]:
        try:
            from ar_app import notion_sync as _nsync
        except Exception:
            import notion_sync as _nsync  # type: ignore
        if _nsync.enabled():
            if st.button("🔄 노션으로 내보내기", use_container_width=True, key="btn_notion_sync"):
                with st.spinner("노션 동기화 중…"):
                    try:
                        res = _nsync.sync(today)
                        st.success(f"노션 동기화 완료 · 신규 {res['created']} · 갱신 {res['updated']} · "
                                   f"보관 {res['archived']} (총 {res['total']}건)")
                    except Exception as e:
                        st.error(f"동기화 실패: {e}")
        else:
            st.caption("🔗 노션 연동하려면 secrets에 `NOTION_TOKEN` 설정")

    # 연도 옵션 (진행중 계약의 수금 예정 연도)
    _years = sorted({_due(p).year
                     for ct in running_contracts
                     for p in periods_by_contract.get(ct.id, [])})
    if not _years:
        st.info("진행중 계약의 정산 예정 내역이 없습니다.")
    else:
        _yidx = _years.index(today.year) if today.year in _years else len(_years) - 1
        sel_year = st.selectbox("연도", _years, index=_yidx, key="settle_year",
                                format_func=lambda y: f"{y}년")

        # owner -> {"q":[4], "contracts": {ct_id: {"ratio","label","q":[4]}}}
        sdata: dict = {}
        for ct in running_contracts:
            cust = customer_by_id.get(ct.customer_id)
            label = f"{cust.name if cust else '?'} · {ct.order_form_name or '(제목 없음)'}"
            for rs in ct.revenue_shares:
                if rs.ratio <= 0:
                    continue
                for p in periods_by_contract.get(ct.id, []):
                    due = _due(p)
                    if due.year != sel_year:
                        continue
                    qi2 = (due.month - 1) // 3
                    amt = to_krw(p.amount * rs.ratio, p.currency)
                    o = sdata.setdefault(rs.owner, {"q": [0.0, 0.0, 0.0, 0.0], "contracts": {}})
                    o["q"][qi2] += amt
                    c = o["contracts"].setdefault(
                        ct.id, {"ratio": rs.ratio, "label": label, "q": [0.0, 0.0, 0.0, 0.0]})
                    c["q"][qi2] += amt

        if not sdata:
            st.info(f"{sel_year}년 정산 예정 내역이 없습니다. (계약에 파트너 배분율이 있어야 표시됩니다)")
        else:
            owners = sorted(sdata, key=lambda o: -sum(sdata[o]["q"]))
            totals = [sum(sdata[o]["q"][qi2] for o in owners) for qi2 in range(4)]
            cur_q = (today.month - 1) // 3 if sel_year == today.year else -1
            _bd = "rgba(241,245,249,0.12)"
            qh = ["1/4분기", "2/4분기", "3/4분기", "4/4분기"]

            def _kw(v: float) -> str:
                return f"₩{v:,.0f}"

            h = "<table style='width:100%;border-collapse:collapse;font-size:0.9rem;'>"
            h += "<tr>"
            h += (f"<th style='text-align:left;padding:10px 12px;border-bottom:2px solid {_bd};"
                  f"color:{C_MUTED};font-weight:700;'>{sel_year}년</th>")
            for qi2 in range(4):
                tag = " <span style='color:#60A5FA;'>(진행 중)</span>" if qi2 == cur_q else ""
                bg = "background:rgba(59,130,246,0.16);" if qi2 == cur_q else ""
                h += (f"<th style='text-align:right;padding:10px 12px;border-bottom:2px solid {_bd};"
                      f"color:{C_TEXT};font-weight:700;{bg}'>{qh[qi2]}{tag}</th>")
            h += (f"<th style='text-align:right;padding:10px 12px;border-bottom:2px solid {_bd};"
                  f"color:{C_MUTED};font-weight:700;'>연 합계</th>")
            h += "</tr>"
            # 전체 총계 (강조)
            h += "<tr style='background:#FDE047;color:#111827;font-weight:800;'>"
            h += "<td style='text-align:left;padding:10px 12px;'>전체 총계</td>"
            for qi2 in range(4):
                h += f"<td style='text-align:right;padding:10px 12px;'>{_kw(totals[qi2])}</td>"
            h += f"<td style='text-align:right;padding:10px 12px;'>{_kw(sum(totals))}</td>"
            h += "</tr>"
            # 배분사별
            for o in owners:
                q = sdata[o]["q"]
                h += "<tr>"
                h += (f"<td style='text-align:left;padding:9px 12px;border-bottom:1px solid {_bd};"
                      f"color:{C_TEXT};font-weight:600;'>{o}</td>")
                for qi2 in range(4):
                    cbg = "background:rgba(59,130,246,0.06);" if qi2 == cur_q else ""
                    h += (f"<td style='text-align:right;padding:9px 12px;border-bottom:1px solid {_bd};"
                          f"color:{C_TEXT};{cbg}'>{_kw(q[qi2])}</td>")
                h += (f"<td style='text-align:right;padding:9px 12px;border-bottom:1px solid {_bd};"
                      f"color:{C_MUTED};'>{_kw(sum(q))}</td>")
                h += "</tr>"
            h += "</table>"
            st.markdown(h, unsafe_allow_html=True)

            st.markdown("")
            st.markdown("**배분사별 · 계약 상세**")
            st.caption("각 배분사를 펼치면 어떤 계약에서 배분율 몇 %로 분기별 얼마를 정산하는지 표시됩니다.")
            for o in owners:
                od = sdata[o]
                with st.expander(f"💳 **{o}**  —  연 {_kw(sum(od['q']))}", expanded=False):
                    rows = []
                    for cid, c in sorted(od["contracts"].items(), key=lambda x: -sum(x[1]["q"])):
                        cq = c["q"]
                        rows.append({
                            "고객사 · 계약": c["label"],
                            "배분율": f"{c['ratio']*100:.0f}%",
                            "1/4": _kw(cq[0]) if cq[0] else "—",
                            "2/4": _kw(cq[1]) if cq[1] else "—",
                            "3/4": _kw(cq[2]) if cq[2] else "—",
                            "4/4": _kw(cq[3]) if cq[3] else "—",
                            "연 합계": _kw(sum(cq)),
                        })
                    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)


# ────────────────── 👥 담당자 ──────────────────
with tab_staff:
    head_cols = st.columns([3, 1])
    with head_cols[0]:
        st.markdown("##### 👥 담당자 목록")
    with head_cols[1]:
        if st.button("➕ 신규 담당자 추가", type="primary", use_container_width=True, key="btn_add_staff"):
            dialog_add_staff()

    if not staff:
        st.caption("등록된 담당자가 없습니다. 고객사 등록 시 드롭다운에서 선택할 수 있도록 먼저 추가하세요.")
    else:
        for s in staff:
            n_cust = sum(1 for c in customers if c.ar_manager_id == s.id or c.accounting_id == s.id)
            with st.container(border=True):
                cols = st.columns([2, 2, 2, 1])
                with cols[0]:
                    st.markdown(
                        f"<div style='font-weight:600; color:{C_TEXT};'>{s.name}</div>"
                        f"<div style='font-size:0.78rem; color:{C_MUTED};'>{s.email}</div>",
                        unsafe_allow_html=True,
                    )
                with cols[1]:
                    st.markdown(label_block("역할", s.role or "—"), unsafe_allow_html=True)
                with cols[2]:
                    st.markdown(label_block("담당 고객사", f"{n_cust}개"), unsafe_allow_html=True)
                with cols[3]:
                    if st.button("✏️ 수정", key=f"edit_staff_{s.id}", use_container_width=True):
                        dialog_edit_staff(s.id)


# ─────────────────────────────────────────────────────────────
# 푸터
# ─────────────────────────────────────────────────────────────
st.divider()
_fc = st.columns([3, 1])
with _fc[0]:
    st.caption(
        f"📝 현재 **{_my_email}** 님이 편집 중 (한 번에 한 명만 사용). "
        "데이터는 이 PC의 `ar_app/data/*.json`에 자동 저장됩니다."
    )
with _fc[1]:
    if st.button("🔓 사용 종료 (넘기기)", use_container_width=True,
                 help="다른 분이 사용할 수 있도록 편집 권한을 내려놓습니다."):
        ar_lock.release(_lock_id)
        st.session_state["_ar_released"] = True
        st.rerun()
