"""
AR Management — 메인 페이지 (v2).

UI 원칙:
  - ID는 내부 키로만 사용, 화면에서 숨김
  - 추가/수정은 모달(st.dialog)로
  - 담당자는 별도 풀(Staff)에서 드롭다운 선택
  - 인보이스 자동 생성 없음 — 계약 등록 시 수동, 수금 일정은 명시적 액션
  - ARR 기준 KPI
"""
from __future__ import annotations

import sys
from dataclasses import asdict
from datetime import date, datetime, timedelta
from pathlib import Path

# 패키지 import
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
except ImportError:
    import models as ar_models           # type: ignore
    import schedule as ar_schedule       # type: ignore


# ─────────────────────────────────────────────────────────────
# 헤더
# ─────────────────────────────────────────────────────────────
st.markdown(
    """
    <div style="margin:6px 0 18px 0;">
      <div style="font-size:1.6rem; font-weight:700; color:#F1F5F9; letter-spacing:-0.02em;">
        💰 AR Management
      </div>
      <div style="font-size:0.85rem; color:rgba(241,245,249,0.65); margin-top:4px;">
        고객사 · 계약 · 수금 · 데이터 오너 배분 통합 관리.
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ─────────────────────────────────────────────────────────────
# 데이터 로드 + 자동 상태 갱신
# ─────────────────────────────────────────────────────────────
customers = ar_models.load_customers()
contracts = ar_models.load_contracts()
invoices = ar_models.load_invoices()
staff = ar_models.load_staff()

today = date.today()
invoices = [ar_models.update_invoice_status(i, today) for i in invoices]

# lookup
customer_by_id = {c.id: c for c in customers}
contract_by_id = {c.id: c for c in contracts}
staff_by_id = {s.id: s for s in staff}


# 담당자 드롭다운용 헬퍼
def staff_options(role_filter: str = "") -> list[str]:
    """Staff ID 목록 (옵션: role 필터)."""
    out = [""]  # 빈 옵션 (미지정)
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
# ARR + KPI
# ─────────────────────────────────────────────────────────────
def annualize_contract(ct: ar_models.Contract) -> float:
    """active 계약의 ARR 환산. one-time은 0."""
    if ct.status != "active":
        return 0.0
    if ct.billing_frequency == "one-time":
        return 0.0
    s = ar_models.parse_iso(ct.start_date)
    e = ar_models.parse_iso(ct.end_date)
    if not s or not e:
        return 0.0
    months = max(1, (e.year - s.year) * 12 + (e.month - s.month) + 1)
    return ct.total_amount * 12.0 / months


arr = sum(annualize_contract(c) for c in contracts)
active_count = sum(1 for c in contracts if c.status == "active")


def _in_month(d_str: str, y: int, m: int) -> bool:
    d = ar_models.parse_iso(d_str)
    return d is not None and d.year == y and d.month == m


now_y, now_m = today.year, today.month
this_month_pending_amt = sum(
    i.amount for i in invoices
    if _in_month(i.issue_date, now_y, now_m) and i.status == "pending"
)
this_month_pending_cnt = sum(
    1 for i in invoices
    if _in_month(i.issue_date, now_y, now_m) and i.status == "pending"
)
overdue_amt = sum(i.amount - i.paid_amount for i in invoices if i.status == "overdue")
overdue_cnt = sum(1 for i in invoices if i.status == "overdue")
upcoming_7d_amt = sum(
    i.amount for i in invoices
    if i.status in ("issued", "pending")
    and (ar_models.parse_iso(i.due_date) or today) <= today + timedelta(days=7)
    and (ar_models.parse_iso(i.due_date) or today) >= today
)

kpi_cols = st.columns(4)
with kpi_cols[0]:
    st.metric("📈 ARR", f"{arr/1e6:,.1f}M원",
              f"{active_count}개 active 계약", delta_color="off",
              help="active 계약의 연환산 매출. 일회성 계약은 제외.")
with kpi_cols[1]:
    st.metric("🧾 이번 달 발행 예정", f"{this_month_pending_amt/1e6:,.1f}M원",
              f"{this_month_pending_cnt}건", delta_color="off")
with kpi_cols[2]:
    st.metric("🔥 연체 미수금", f"{overdue_amt/1e6:,.1f}M원",
              f"{overdue_cnt}건",
              delta_color="inverse" if overdue_amt > 0 else "off")
with kpi_cols[3]:
    st.metric("⏰ 7일 내 만기", f"{upcoming_7d_amt/1e6:,.1f}M원",
              "곧 수금/연체", delta_color="off")

st.markdown("")


# ═══════════════════════════════════════════════════════════
# 모달 (st.dialog) — 추가/수정
# ═══════════════════════════════════════════════════════════
@st.dialog("👥 신규 담당자 추가")
def dialog_add_staff():
    with st.form("dlg_add_staff"):
        name = st.text_input("이름 *", placeholder="홍길동")
        email = st.text_input("이메일 *", placeholder="hong@mandata.kr")
        role = st.selectbox("역할", ["AR 담당", "회계", "고객 담당", "기타"], index=0)
        notes = st.text_area("메모", height=68)
        if st.form_submit_button("저장", type="primary"):
            if not name.strip() or not email.strip():
                st.error("이름과 이메일은 필수")
                return
            new_s = ar_models.Staff(
                id=ar_models.next_staff_id(staff),
                name=name.strip(), email=email.strip(),
                role=role, notes=notes.strip(),
                created_at=today.isoformat(),
            )
            staff.append(new_s)
            ar_models.save_staff(staff)
            st.rerun()


@st.dialog("👥 담당자 수정")
def dialog_edit_staff(staff_id: str):
    s = staff_by_id.get(staff_id)
    if not s:
        st.error("담당자 없음")
        return
    with st.form("dlg_edit_staff"):
        name = st.text_input("이름 *", value=s.name)
        email = st.text_input("이메일 *", value=s.email)
        role = st.selectbox(
            "역할", ["AR 담당", "회계", "고객 담당", "기타"],
            index=["AR 담당", "회계", "고객 담당", "기타"].index(s.role) if s.role in ["AR 담당", "회계", "고객 담당", "기타"] else 3,
        )
        notes = st.text_area("메모", value=s.notes, height=68)
        cols = st.columns([1, 1])
        with cols[0]:
            if st.form_submit_button("저장", type="primary", use_container_width=True):
                for x in staff:
                    if x.id == staff_id:
                        x.name = name.strip()
                        x.email = email.strip()
                        x.role = role
                        x.notes = notes.strip()
                ar_models.save_staff(staff)
                st.rerun()
        with cols[1]:
            if st.form_submit_button("🗑 삭제", use_container_width=True):
                # 사용 중인지 확인
                in_use = any(
                    c.ar_manager_id == staff_id or c.accounting_id == staff_id
                    for c in customers
                )
                if in_use:
                    st.error("이 담당자는 고객사에 배정돼 있어 삭제 불가. 먼저 고객사에서 변경하세요.")
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
        ar_id = st.selectbox(
            "AR 담당자", options=staff_options(),
            format_func=staff_label, key="add_cust_ar",
        )
        acc_id = st.selectbox(
            "회계 담당자", options=staff_options(),
            format_func=staff_label, key="add_cust_acc",
        )
        notes = st.text_area("메모", height=68)

        if st.form_submit_button("저장", type="primary"):
            if not name.strip():
                st.error("고객사명은 필수")
                return
            new_c = ar_models.Customer(
                id=ar_models.next_customer_id(customers),
                name=name.strip(), biz_no=biz_no.strip(),
                contact_name=contact_name.strip(),
                contact_email=contact_email.strip(),
                ar_manager_id=ar_id, accounting_id=acc_id,
                notes=notes.strip(),
                created_at=today.isoformat(),
            )
            customers.append(new_c)
            ar_models.save_customers(customers)
            st.rerun()


@st.dialog("🏢 고객사 수정")
def dialog_edit_customer(customer_id: str):
    c = customer_by_id.get(customer_id)
    if not c:
        st.error("고객사 없음")
        return
    with st.form("dlg_edit_cust"):
        name = st.text_input("고객사명 *", value=c.name)
        biz_no = st.text_input("사업자번호", value=c.biz_no)
        contact_name = st.text_input("고객사 담당자 이름", value=c.contact_name)
        contact_email = st.text_input("고객사 담당자 이메일", value=c.contact_email)

        st.markdown("**우리 쪽 담당자**")
        opts = staff_options()
        ar_idx = opts.index(c.ar_manager_id) if c.ar_manager_id in opts else 0
        acc_idx = opts.index(c.accounting_id) if c.accounting_id in opts else 0
        ar_id = st.selectbox("AR 담당자", options=opts, index=ar_idx,
                              format_func=staff_label, key="edit_cust_ar")
        acc_id = st.selectbox("회계 담당자", options=opts, index=acc_idx,
                               format_func=staff_label, key="edit_cust_acc")
        notes = st.text_area("메모", value=c.notes, height=68)

        cols = st.columns([1, 1])
        with cols[0]:
            if st.form_submit_button("저장", type="primary", use_container_width=True):
                for x in customers:
                    if x.id == customer_id:
                        x.name = name.strip()
                        x.biz_no = biz_no.strip()
                        x.contact_name = contact_name.strip()
                        x.contact_email = contact_email.strip()
                        x.ar_manager_id = ar_id
                        x.accounting_id = acc_id
                        x.notes = notes.strip()
                ar_models.save_customers(customers)
                st.rerun()
        with cols[1]:
            if st.form_submit_button("🗑 삭제", use_container_width=True):
                # 계약/인보이스에 묶여있는지 확인
                n_ct = sum(1 for ct in contracts if ct.customer_id == customer_id)
                if n_ct > 0:
                    st.error(f"이 고객사에 계약이 {n_ct}건 있어 삭제 불가. 먼저 계약을 정리하세요.")
                else:
                    customers[:] = [x for x in customers if x.id != customer_id]
                    ar_models.save_customers(customers)
                    st.rerun()


@st.dialog("📋 신규 계약 추가")
def dialog_add_contract():
    if not customers:
        st.warning("먼저 고객사를 등록해주세요.")
        return
    with st.form("dlg_add_ct"):
        cust_id = st.selectbox(
            "고객사 *", options=[c.id for c in customers],
            format_func=lambda x: customer_by_id[x].name,
        )
        title = st.text_input("계약 제목 *", placeholder="2026 데이터 라이선스")
        ct_type = st.selectbox("계약 유형", ["annual", "monthly", "one-time", "custom"], index=0)
        dcols = st.columns(2)
        with dcols[0]:
            start = st.date_input("시작일 *", value=today)
        with dcols[1]:
            end = st.date_input("종료일", value=today + timedelta(days=365))
        amt_cols = st.columns([2, 1])
        with amt_cols[0]:
            amount = st.number_input("총 금액 *", min_value=0.0, step=100_000.0, value=12_000_000.0)
        with amt_cols[1]:
            currency = st.selectbox("통화", ["KRW", "USD"], index=0)
        freq = st.selectbox(
            "Billing Frequency *",
            ["monthly", "quarterly", "annually", "one-time"], index=0,
        )
        fcols = st.columns(2)
        with fcols[0]:
            bday = st.number_input("발행일", min_value=1, max_value=28, value=1)
        with fcols[1]:
            terms = st.number_input("수금 기한 (일)", min_value=0, max_value=180, value=30)
        url = st.text_input("계약서/오더폼 URL")
        notes = st.text_area("메모", height=68)

        st.markdown("**💼 데이터 오너 배분 (옵션)**")
        st.caption("합계 1.0 이하 (나머지는 회사 몫). 최대 4명.")
        rs_rows = []
        for k in range(4):
            rs_cols = st.columns([2, 1, 2])
            with rs_cols[0]:
                owner = st.text_input(f"오너 #{k+1}", key=f"add_ct_rs_o_{k}", label_visibility="collapsed",
                                       placeholder=f"오너 #{k+1} 이름")
            with rs_cols[1]:
                ratio = st.number_input(f"비율 #{k+1}", min_value=0.0, max_value=1.0,
                                         step=0.05, value=0.0, key=f"add_ct_rs_r_{k}",
                                         label_visibility="collapsed")
            with rs_cols[2]:
                owner_email = st.text_input(f"이메일 #{k+1}", key=f"add_ct_rs_e_{k}",
                                             label_visibility="collapsed",
                                             placeholder=f"이메일 #{k+1}")
            if owner.strip() and ratio > 0:
                rs_rows.append(ar_models.RevenueShare(
                    owner=owner.strip(), ratio=float(ratio),
                    contact_email=owner_email.strip(),
                ))

        if st.form_submit_button("저장", type="primary"):
            if not title.strip():
                st.error("계약 제목 필수")
                return
            if amount <= 0:
                st.error("금액은 0보다 커야 함")
                return
            if freq != "one-time" and not end:
                st.error("종료일 필수 (일회성 외)")
                return
            if sum(rs.ratio for rs in rs_rows) > 1.0001:
                st.error("배분율 합계가 1.0을 초과")
                return
            new_ct = ar_models.Contract(
                id=ar_models.next_contract_id(contracts),
                customer_id=cust_id,
                title=title.strip(),
                contract_type=ct_type,
                start_date=start.isoformat(),
                end_date=end.isoformat() if end else "",
                total_amount=float(amount), currency=currency,
                billing_frequency=freq,
                billing_day_of_month=int(bday),
                payment_terms_days=int(terms),
                revenue_shares=rs_rows,
                status="active",
                order_form_url=url.strip(),
                notes=notes.strip(),
                created_at=today.isoformat(),
            )
            contracts.append(new_ct)
            ar_models.save_contracts(contracts)
            st.rerun()


@st.dialog("📋 계약 수정")
def dialog_edit_contract(contract_id: str):
    ct = contract_by_id.get(contract_id)
    if not ct:
        st.error("계약 없음")
        return
    with st.form("dlg_edit_ct"):
        cust_opts = [c.id for c in customers]
        cust_idx = cust_opts.index(ct.customer_id) if ct.customer_id in cust_opts else 0
        cust_id = st.selectbox("고객사", options=cust_opts, index=cust_idx,
                                format_func=lambda x: customer_by_id[x].name)
        title = st.text_input("계약 제목", value=ct.title)
        status = st.selectbox("상태", ["active", "paused", "ended"],
                               index=["active", "paused", "ended"].index(ct.status)
                               if ct.status in ("active", "paused", "ended") else 0)
        dcols = st.columns(2)
        with dcols[0]:
            start = st.date_input("시작일",
                                   value=ar_models.parse_iso(ct.start_date) or today)
        with dcols[1]:
            end = st.date_input("종료일",
                                 value=ar_models.parse_iso(ct.end_date) or today)
        amt_cols = st.columns([2, 1])
        with amt_cols[0]:
            amount = st.number_input("총 금액", min_value=0.0, step=100_000.0,
                                      value=float(ct.total_amount))
        with amt_cols[1]:
            currency = st.selectbox("통화", ["KRW", "USD"],
                                     index=["KRW", "USD"].index(ct.currency)
                                     if ct.currency in ("KRW", "USD") else 0)
        freq = st.selectbox(
            "Billing Frequency",
            ["monthly", "quarterly", "annually", "one-time"],
            index=["monthly", "quarterly", "annually", "one-time"].index(ct.billing_frequency)
            if ct.billing_frequency in ("monthly", "quarterly", "annually", "one-time") else 0,
        )
        fcols = st.columns(2)
        with fcols[0]:
            bday = st.number_input("발행일", min_value=1, max_value=28,
                                    value=int(ct.billing_day_of_month or 1))
        with fcols[1]:
            terms = st.number_input("수금 기한 (일)", min_value=0, max_value=180,
                                     value=int(ct.payment_terms_days or 30))
        url = st.text_input("계약서/오더폼 URL", value=ct.order_form_url)
        notes = st.text_area("메모", value=ct.notes, height=68)

        cols = st.columns([1, 1])
        with cols[0]:
            if st.form_submit_button("저장", type="primary", use_container_width=True):
                for x in contracts:
                    if x.id == contract_id:
                        x.customer_id = cust_id
                        x.title = title.strip()
                        x.status = status
                        x.start_date = start.isoformat()
                        x.end_date = end.isoformat() if end else ""
                        x.total_amount = float(amount)
                        x.currency = currency
                        x.billing_frequency = freq
                        x.billing_day_of_month = int(bday)
                        x.payment_terms_days = int(terms)
                        x.order_form_url = url.strip()
                        x.notes = notes.strip()
                ar_models.save_contracts(contracts)
                st.rerun()
        with cols[1]:
            if st.form_submit_button("🗑 삭제 (인보이스도 함께)", use_container_width=True):
                contracts[:] = [x for x in contracts if x.id != contract_id]
                invoices[:] = [i for i in invoices if i.contract_id != contract_id]
                ar_models.save_contracts(contracts)
                ar_models.save_invoices(invoices)
                st.rerun()


@st.dialog("📊 수금 일정 생성")
def dialog_generate_schedule(contract_id: str):
    """명시적으로 계약의 수금 일정(인보이스)을 자동 생성."""
    ct = contract_by_id.get(contract_id)
    if not ct:
        st.error("계약 없음")
        return
    existing = [i for i in invoices if i.contract_id == contract_id]
    if existing:
        st.warning(f"이 계약에 이미 수금 항목이 {len(existing)}건 있습니다.")
    st.markdown(
        f"**{ct.title}** · {customer_by_id.get(ct.customer_id, ar_models.Customer(id='', name='')).name} · "
        f"{ct.billing_frequency} · {ct.total_amount:,.0f} {ct.currency}"
    )

    preview = ar_schedule.generate_invoice_schedule(ct, invoices)
    st.caption(f"미리보기: {len(preview)}건 자동 생성됩니다. 회당 약 {preview[0].amount if preview else 0:,.0f} {ct.currency}.")

    if preview:
        df_preview = pd.DataFrame([{
            "발행일": p.issue_date, "만기일": p.due_date,
            "금액": f"{p.amount:,.0f}",
        } for p in preview[:6]])
        st.dataframe(df_preview, hide_index=True, use_container_width=True)
        if len(preview) > 6:
            st.caption(f"...외 {len(preview) - 6}건")

    cols = st.columns([1, 1])
    with cols[0]:
        if st.button("➕ 새로 생성 (기존 pending 덮어쓰기)", type="primary", use_container_width=True):
            updated = ar_schedule.regenerate_for_contract(ct, invoices)
            invoices[:] = updated
            ar_models.save_invoices(invoices)
            st.rerun()
    with cols[1]:
        if st.button("취소", use_container_width=True):
            st.rerun()


@st.dialog("📊 수금 항목 추가 (수동)")
def dialog_add_invoice():
    if not contracts:
        st.warning("먼저 계약을 등록해주세요.")
        return
    with st.form("dlg_add_inv"):
        ct_id = st.selectbox(
            "계약 *", options=[c.id for c in contracts],
            format_func=lambda x: f"{contract_by_id[x].title} ({customer_by_id.get(contract_by_id[x].customer_id, ar_models.Customer(id='', name='')).name})",
        )
        ct_sel = contract_by_id.get(ct_id)
        issue_d = st.date_input("발행일 *", value=today)
        due_d = st.date_input("만기일 *",
                               value=today + timedelta(days=ct_sel.payment_terms_days if ct_sel else 30))
        amount = st.number_input("금액 *", min_value=0.0, step=100_000.0,
                                  value=float(ct_sel.total_amount) if ct_sel else 0.0)
        currency = st.selectbox("통화", ["KRW", "USD"],
                                 index=["KRW", "USD"].index(ct_sel.currency) if ct_sel and ct_sel.currency in ("KRW", "USD") else 0)
        notes = st.text_area("메모", height=68)
        if st.form_submit_button("저장", type="primary"):
            new_inv = ar_models.Invoice(
                id=ar_models.next_invoice_id(ct_id, issue_d.isoformat(), invoices),
                contract_id=ct_id,
                customer_id=ct_sel.customer_id if ct_sel else "",
                issue_date=issue_d.isoformat(), due_date=due_d.isoformat(),
                amount=float(amount), currency=currency,
                status="pending", notes=notes.strip(),
                auto_generated=False,
            )
            invoices.append(new_inv)
            ar_models.save_invoices(invoices)
            st.rerun()


# ═══════════════════════════════════════════════════════════
# 탭
# ═══════════════════════════════════════════════════════════
tab_dash, tab_cust, tab_ct, tab_inv, tab_staff = st.tabs([
    "🏠 대시보드", "🏢 고객사", "📋 계약", "📊 수금", "👥 담당자",
])


# ────────────────── 🏠 대시보드 ──────────────────
with tab_dash:
    if not invoices:
        st.info(
            "아직 수금 항목이 없습니다. **📋 계약** 탭에서 계약을 등록하고 "
            "**📊 수금 일정 생성** 또는 **수금 탭에서 수동 추가**를 진행하세요."
        )
    else:
        # 7일 내 만기
        st.markdown("##### ⏰ 7일 내 만기")
        soon = []
        for i in invoices:
            due = ar_models.parse_iso(i.due_date)
            if due and i.status in ("issued", "pending"):
                d_left = (due - today).days
                if 0 <= d_left <= 7:
                    soon.append((d_left, i))
        soon.sort(key=lambda x: x[0])
        if not soon:
            st.caption("없음")
        else:
            for d_left, inv in soon:
                cust = customer_by_id.get(inv.customer_id)
                cust_name = cust.name if cust else "?"
                color = "#EF4444" if d_left <= 1 else ("#F59E0B" if d_left <= 3 else "#94A3B8")
                day_str = "오늘" if d_left == 0 else ("내일" if d_left == 1 else f"D-{d_left}")
                st.markdown(
                    f"""
                    <div style="padding:8px 12px; border-left:3px solid {color}; margin:4px 0;
                                background:rgba(255,255,255,0.02); border-radius:4px;
                                display:flex; justify-content:space-between; align-items:center;">
                      <div>
                        <b style="color:#F1F5F9;">{cust_name}</b>
                        <div style="font-size:0.78rem; color:rgba(241,245,249,0.55); margin-top:2px;">
                          만기 {inv.due_date} · 상태 <b>{inv.status}</b>
                        </div>
                      </div>
                      <div style="text-align:right;">
                        <div style="color:{color}; font-weight:700;">{day_str}</div>
                        <div style="color:#F1F5F9; font-family:'JetBrains Mono',monospace;">
                          {inv.amount:,.0f} {inv.currency}
                        </div>
                      </div>
                    </div>
                    """, unsafe_allow_html=True,
                )

        # 연체
        st.markdown("##### 🔥 연체")
        overdue = [i for i in invoices if i.status == "overdue"]
        if not overdue:
            st.caption("없음 👍")
        else:
            rows = []
            for i in overdue:
                cust = customer_by_id.get(i.customer_id)
                ar_s = staff_by_id.get(cust.ar_manager_id) if cust else None
                due = ar_models.parse_iso(i.due_date)
                rows.append({
                    "고객사": cust.name if cust else "?",
                    "담당자": ar_s.name if ar_s else "—",
                    "만기일": i.due_date,
                    "경과": f"D+{(today - due).days}" if due else "—",
                    "금액": f"{i.amount:,.0f} {i.currency}",
                })
            st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)


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
        # 카드형 목록
        for c in customers:
            ar_s = staff_by_id.get(c.ar_manager_id)
            acc_s = staff_by_id.get(c.accounting_id)
            n_ct = sum(1 for ct in contracts if ct.customer_id == c.id)
            outstanding = sum(
                i.amount - i.paid_amount for i in invoices
                if i.customer_id == c.id and i.status in ("issued", "overdue")
            )
            with st.container(border=True):
                cols = st.columns([3, 2, 2, 1, 1])
                with cols[0]:
                    st.markdown(
                        f"<div style='font-weight:600; color:#F1F5F9; font-size:1.0rem;'>{c.name}</div>"
                        f"<div style='font-size:0.78rem; color:rgba(241,245,249,0.6);'>"
                        f"{c.biz_no or '사업자번호 없음'} · {c.contact_name or '담당자 미지정'}</div>",
                        unsafe_allow_html=True,
                    )
                with cols[1]:
                    st.markdown(
                        f"<div style='font-size:0.78rem; color:rgba(241,245,249,0.55);'>AR 담당</div>"
                        f"<div style='color:#F1F5F9; font-size:0.88rem;'>{ar_s.name if ar_s else '—'}</div>"
                        f"<div style='font-size:0.78rem; color:rgba(241,245,249,0.55); margin-top:4px;'>회계</div>"
                        f"<div style='color:#F1F5F9; font-size:0.88rem;'>{acc_s.name if acc_s else '—'}</div>",
                        unsafe_allow_html=True,
                    )
                with cols[2]:
                    st.markdown(
                        f"<div style='font-size:0.78rem; color:rgba(241,245,249,0.55);'>계약</div>"
                        f"<div style='color:#F1F5F9; font-weight:600;'>{n_ct}건</div>"
                        f"<div style='font-size:0.78rem; color:rgba(241,245,249,0.55); margin-top:4px;'>미수금</div>"
                        f"<div style='color:#F59E0B; font-weight:600; font-family:\"JetBrains Mono\",monospace;'>"
                        f"{outstanding:,.0f}</div>",
                        unsafe_allow_html=True,
                    )
                with cols[3]:
                    if st.button("✏️ 수정", key=f"edit_cust_{c.id}", use_container_width=True):
                        dialog_edit_customer(c.id)


# ────────────────── 📋 계약 ──────────────────
with tab_ct:
    head_cols = st.columns([3, 1])
    with head_cols[0]:
        st.markdown("##### 📋 계약 목록")
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
            n_inv = sum(1 for i in invoices if i.contract_id == ct.id)
            n_paid = sum(1 for i in invoices if i.contract_id == ct.id and i.status == "paid")
            status_color = {"active": "#22C55E", "paused": "#F59E0B", "ended": "#94A3B8"}.get(ct.status, "#94A3B8")
            with st.container(border=True):
                cols = st.columns([3, 2, 2, 1.2, 1])
                with cols[0]:
                    st.markdown(
                        f"<div style='font-weight:600; color:#F1F5F9; font-size:1.0rem;'>{ct.title}</div>"
                        f"<div style='font-size:0.78rem; color:rgba(241,245,249,0.6);'>"
                        f"{cust.name if cust else '?'} · {ct.contract_type}</div>",
                        unsafe_allow_html=True,
                    )
                with cols[1]:
                    st.markdown(
                        f"<div style='font-size:0.78rem; color:rgba(241,245,249,0.55);'>기간</div>"
                        f"<div style='color:#F1F5F9; font-size:0.88rem;'>{ct.start_date} ~ {ct.end_date or '—'}</div>"
                        f"<div style='font-size:0.78rem; color:rgba(241,245,249,0.55); margin-top:4px;'>빈도</div>"
                        f"<div style='color:#F1F5F9; font-size:0.88rem;'>{ct.billing_frequency} · 매월 {ct.billing_day_of_month}일</div>",
                        unsafe_allow_html=True,
                    )
                with cols[2]:
                    st.markdown(
                        f"<div style='font-size:0.78rem; color:rgba(241,245,249,0.55);'>금액</div>"
                        f"<div style='color:#F1F5F9; font-weight:600; font-family:\"JetBrains Mono\",monospace;'>"
                        f"{ct.total_amount:,.0f} {ct.currency}</div>"
                        f"<div style='font-size:0.78rem; color:rgba(241,245,249,0.55); margin-top:4px;'>수금</div>"
                        f"<div style='color:#F1F5F9; font-size:0.88rem;'>{n_paid}/{n_inv}건 완료</div>",
                        unsafe_allow_html=True,
                    )
                with cols[3]:
                    st.markdown(
                        f"<div style='display:inline-block; padding:3px 10px; border-radius:10px; "
                        f"background:{status_color}25; color:{status_color}; font-weight:600; font-size:0.8rem;'>"
                        f"{ct.status}</div>",
                        unsafe_allow_html=True,
                    )
                    if st.button("📊 수금 일정", key=f"gen_sched_{ct.id}", use_container_width=True):
                        dialog_generate_schedule(ct.id)
                with cols[4]:
                    if st.button("✏️ 수정", key=f"edit_ct_{ct.id}", use_container_width=True):
                        dialog_edit_contract(ct.id)


# ────────────────── 📊 수금 ──────────────────
with tab_inv:
    head_cols = st.columns([3, 1])
    with head_cols[0]:
        st.markdown("##### 📊 수금 항목 목록")
    with head_cols[1]:
        if st.button("➕ 수동 추가", type="primary", use_container_width=True, key="btn_add_inv",
                      disabled=len(contracts) == 0):
            dialog_add_invoice()

    if not invoices:
        st.caption(
            "등록된 수금 항목이 없습니다. **📋 계약 탭에서 [📊 수금 일정] 버튼**으로 자동 생성하거나 "
            "위 [➕ 수동 추가] 버튼으로 등록하세요."
        )
    else:
        # 필터
        f_cols = st.columns([1, 1, 1, 1])
        with f_cols[0]:
            status_filter = st.multiselect(
                "상태", ["pending", "issued", "paid", "overdue", "void"],
                default=["pending", "issued", "overdue"],
            )
        with f_cols[1]:
            cust_filter = st.selectbox(
                "고객사", ["(전체)"] + [c.id for c in customers],
                format_func=lambda x: "(전체)" if x == "(전체)" else customer_by_id[x].name,
            )
        with f_cols[2]:
            month_filter = st.selectbox("월", ["(전체)", "이번 달", "다음 달", "지난 달"])
        with f_cols[3]:
            sort_key = st.selectbox("정렬", ["만기일 가까운 순", "발행일 빠른 순", "금액 큰 순"])

        filtered = list(invoices)
        if status_filter:
            filtered = [i for i in filtered if i.status in status_filter]
        if cust_filter != "(전체)":
            filtered = [i for i in filtered if i.customer_id == cust_filter]
        if month_filter != "(전체)":
            target_y, target_m = now_y, now_m
            if month_filter == "다음 달":
                target_m = now_m + 1
                if target_m > 12:
                    target_m, target_y = 1, target_y + 1
            elif month_filter == "지난 달":
                target_m = now_m - 1
                if target_m < 1:
                    target_m, target_y = 12, target_y - 1
            filtered = [i for i in filtered if _in_month(i.issue_date, target_y, target_m)]

        if sort_key == "만기일 가까운 순":
            filtered.sort(key=lambda i: ar_models.parse_iso(i.due_date) or date.max)
        elif sort_key == "발행일 빠른 순":
            filtered.sort(key=lambda i: ar_models.parse_iso(i.issue_date) or date.max)
        else:
            filtered.sort(key=lambda i: -i.amount)

        st.caption(f"{len(filtered)}건 / 전체 {len(invoices)}건")

        color_map = {
            "pending": "#94A3B8", "issued": "#3B82F6", "paid": "#22C55E",
            "overdue": "#EF4444", "void": "#71717A",
        }
        for inv in filtered[:50]:
            cust = customer_by_id.get(inv.customer_id)
            ct = contract_by_id.get(inv.contract_id)
            sc = color_map.get(inv.status, "#94A3B8")
            with st.container(border=True):
                cols = st.columns([3, 2, 1.5, 2])
                with cols[0]:
                    st.markdown(
                        f"<div style='font-weight:600; color:#F1F5F9;'>{cust.name if cust else '?'}</div>"
                        f"<div style='font-size:0.78rem; color:rgba(241,245,249,0.6);'>"
                        f"{ct.title if ct else '?'}</div>",
                        unsafe_allow_html=True,
                    )
                with cols[1]:
                    st.markdown(
                        f"<div style='font-size:0.85rem; color:rgba(241,245,249,0.85);'>"
                        f"발행 {inv.issue_date}<br>만기 {inv.due_date}</div>",
                        unsafe_allow_html=True,
                    )
                with cols[2]:
                    st.markdown(
                        f"<div style='font-family:\"JetBrains Mono\",monospace; font-weight:700; "
                        f"color:#F1F5F9; text-align:right;'>{inv.amount:,.0f}"
                        f"<br><span style='font-size:0.78rem; font-weight:400; color:rgba(241,245,249,0.6);'>{inv.currency}</span></div>",
                        unsafe_allow_html=True,
                    )
                with cols[3]:
                    st.markdown(
                        f"<div style='display:inline-block; padding:3px 10px; border-radius:10px; "
                        f"background:{sc}25; color:{sc}; font-weight:600; font-size:0.8rem; margin-bottom:6px;'>"
                        f"{inv.status}</div>",
                        unsafe_allow_html=True,
                    )
                    if inv.status == "pending":
                        if st.button("📤 발행", key=f"act_iss_{inv.id}", use_container_width=True):
                            for i in invoices:
                                if i.id == inv.id:
                                    i.status = "issued"
                                    i.issued_at = today.isoformat()
                            ar_models.save_invoices(invoices)
                            st.rerun()
                    elif inv.status in ("issued", "overdue"):
                        sub_cols = st.columns([1, 1])
                        with sub_cols[0]:
                            if st.button("✅ 수금", key=f"act_paid_{inv.id}", use_container_width=True):
                                for i in invoices:
                                    if i.id == inv.id:
                                        i.status = "paid"
                                        i.paid_at = today.isoformat()
                                        i.paid_amount = i.amount
                                ar_models.save_invoices(invoices)
                                st.rerun()
                        with sub_cols[1]:
                            if st.button("❌ void", key=f"act_void_{inv.id}", use_container_width=True):
                                for i in invoices:
                                    if i.id == inv.id:
                                        i.status = "void"
                                ar_models.save_invoices(invoices)
                                st.rerun()

        if len(filtered) > 50:
            st.caption(f"⚠️ 표시 한계 50건. 더 보려면 필터를 좁혀주세요.")


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
                        f"<div style='font-weight:600; color:#F1F5F9;'>{s.name}</div>"
                        f"<div style='font-size:0.78rem; color:rgba(241,245,249,0.6);'>{s.email}</div>",
                        unsafe_allow_html=True,
                    )
                with cols[1]:
                    st.markdown(
                        f"<div style='font-size:0.78rem; color:rgba(241,245,249,0.55);'>역할</div>"
                        f"<div style='color:#F1F5F9; font-size:0.9rem;'>{s.role or '—'}</div>",
                        unsafe_allow_html=True,
                    )
                with cols[2]:
                    st.markdown(
                        f"<div style='font-size:0.78rem; color:rgba(241,245,249,0.55);'>담당 고객사</div>"
                        f"<div style='color:#F1F5F9; font-weight:600;'>{n_cust}개</div>",
                        unsafe_allow_html=True,
                    )
                with cols[3]:
                    if st.button("✏️ 수정", key=f"edit_staff_{s.id}", use_container_width=True):
                        dialog_edit_staff(s.id)


# ─────────────────────────────────────────────────────────────
# 푸터
# ─────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "📂 데이터: `ar_app/data/{customers,contracts,invoices,staff}.json` · "
    "Streamlit Cloud는 재배포 시 파일이 사라지는 ephemeral fs라, "
    "변경사항은 로컬에서 git commit & push 로 영구 보존하세요."
)
