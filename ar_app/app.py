"""
AR Management — 메인 페이지.

상단 KPI + 4 탭 (대시보드 / 고객사 / 계약 / 인보이스).

Phase 1 범위: 계약 등록 + 인보이스 자동 일정 + 수금 상태 추적.
Phase 2 예정: 이메일 알림, 배분 송금서, 월별 리포트.
"""
from __future__ import annotations

import sys
from dataclasses import asdict, replace
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
        고객사 계약 · 인보이스 자동 일정 · 수금 추적 · 데이터 오너 배분.
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ─────────────────────────────────────────────────────────────
# 데이터 로드 (매 rerun)
# ─────────────────────────────────────────────────────────────
customers = ar_models.load_customers()
contracts = ar_models.load_contracts()
invoices = ar_models.load_invoices()

# 상태 자동 갱신 (overdue 표시)
today = date.today()
invoices = [ar_models.update_invoice_status(i, today) for i in invoices]

# lookup
customer_by_id = {c.id: c for c in customers}
contract_by_id = {c.id: c for c in contracts}


# ─────────────────────────────────────────────────────────────
# 상단 KPI 4개
# ─────────────────────────────────────────────────────────────
def _amount_in_month(invs: list, year: int, month: int, status_filter: list[str]) -> float:
    total = 0.0
    for i in invs:
        d = ar_models.parse_iso(i.issue_date)
        if d and d.year == year and d.month == month and i.status in status_filter:
            total += i.amount
    return total

now_y, now_m = today.year, today.month
this_month_pending = _amount_in_month(invoices, now_y, now_m, ["pending"])
this_month_issued = _amount_in_month(invoices, now_y, now_m, ["issued", "paid"])
overdue_amount = sum(i.amount - i.paid_amount for i in invoices if i.status == "overdue")
upcoming_7d = sum(
    i.amount for i in invoices
    if i.status in ("issued", "pending")
    and (ar_models.parse_iso(i.due_date) or today) <= today + timedelta(days=7)
    and (ar_models.parse_iso(i.due_date) or today) >= today
)

# Monthly Recurring Revenue 추정 — 현재 active 월간/분기/연간 계약의 월환산
mrr = 0.0
for ct in contracts:
    if ct.status != "active":
        continue
    if ct.billing_frequency == "monthly":
        s = ar_models.parse_iso(ct.start_date)
        e = ar_models.parse_iso(ct.end_date)
        if s and e:
            n = max(1, (e.year - s.year) * 12 + (e.month - s.month) + 1)
            mrr += ct.total_amount / n
    elif ct.billing_frequency == "quarterly":
        s = ar_models.parse_iso(ct.start_date)
        e = ar_models.parse_iso(ct.end_date)
        if s and e:
            n_months = max(1, (e.year - s.year) * 12 + (e.month - s.month) + 1)
            mrr += ct.total_amount / n_months
    elif ct.billing_frequency == "annually":
        s = ar_models.parse_iso(ct.start_date)
        e = ar_models.parse_iso(ct.end_date)
        if s and e:
            n_months = max(12, (e.year - s.year) * 12 + (e.month - s.month) + 1)
            mrr += ct.total_amount / n_months


kpi_cols = st.columns(4)
with kpi_cols[0]:
    st.metric("📈 MRR (월환산)", f"{mrr/1e6:,.1f}M원", f"{len([c for c in contracts if c.status=='active'])}개 active 계약",
              delta_color="off",
              help="현재 active 계약의 월환산 매출 추정. 일회성 계약은 제외.")
with kpi_cols[1]:
    st.metric("🧾 이번 달 발행 예정", f"{this_month_pending/1e6:,.1f}M원",
              f"{len([i for i in invoices if (ar_models.parse_iso(i.issue_date) or today).year==now_y and (ar_models.parse_iso(i.issue_date) or today).month==now_m and i.status=='pending'])}건",
              delta_color="off")
with kpi_cols[2]:
    st.metric("🔥 연체 미수금", f"{overdue_amount/1e6:,.1f}M원",
              f"{len([i for i in invoices if i.status=='overdue'])}건",
              delta_color="inverse" if overdue_amount > 0 else "off",
              help="만기일 지났는데 아직 수금 안 된 인보이스 합계.")
with kpi_cols[3]:
    st.metric("⏰ 7일 내 만기", f"{upcoming_7d/1e6:,.1f}M원",
              "곧 수금/연체",
              delta_color="off")


st.markdown("")


# ─────────────────────────────────────────────────────────────
# 탭
# ─────────────────────────────────────────────────────────────
tab_dash, tab_cust, tab_contract, tab_invoice = st.tabs([
    "🏠 대시보드", "🏢 고객사", "📋 계약", "🧾 인보이스",
])


# ════════════════════════════════════════════════════════════
# 탭 1: 대시보드
# ════════════════════════════════════════════════════════════
with tab_dash:
    if not invoices:
        st.info("아직 등록된 인보이스가 없습니다. **📋 계약** 탭에서 계약을 등록하면 자동으로 인보이스 일정이 생성됩니다.")
    else:
        # 임박 (7일 내 만기)
        st.markdown("##### ⏰ 7일 내 만기 인보이스")
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
                cust_name = cust.name if cust else inv.customer_id
                color = "#EF4444" if d_left <= 1 else ("#F59E0B" if d_left <= 3 else "#94A3B8")
                day_str = "오늘" if d_left == 0 else ("내일" if d_left == 1 else f"D-{d_left}")
                st.markdown(
                    f"""
                    <div style="padding:8px 12px; border-left:3px solid {color}; margin:4px 0;
                                background:rgba(255,255,255,0.02); border-radius:4px;
                                display:flex; justify-content:space-between; align-items:center;">
                      <div>
                        <b style="color:#F1F5F9;">{cust_name}</b> · {inv.id}
                        <div style="font-size:0.8rem; color:rgba(241,245,249,0.55); margin-top:2px;">
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
                    """,
                    unsafe_allow_html=True,
                )

        # 연체
        st.markdown("##### 🔥 연체 인보이스")
        overdue = [i for i in invoices if i.status == "overdue"]
        if not overdue:
            st.caption("없음 👍")
        else:
            rows = []
            for i in overdue:
                cust = customer_by_id.get(i.customer_id)
                due = ar_models.parse_iso(i.due_date)
                days_overdue = (today - due).days if due else 0
                rows.append({
                    "인보이스": i.id,
                    "고객사": cust.name if cust else i.customer_id,
                    "담당자": (cust.ar_manager if cust else "") or "—",
                    "만기일": i.due_date,
                    "경과(일)": f"D+{days_overdue}",
                    "금액": f"{i.amount:,.0f} {i.currency}",
                })
            st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

        # 이번 달 수금 완료
        st.markdown("##### ✅ 이번 달 수금 완료")
        paid_this = [
            i for i in invoices
            if i.status == "paid"
            and ar_models.parse_iso(i.paid_at or "")
            and ar_models.parse_iso(i.paid_at).year == now_y
            and ar_models.parse_iso(i.paid_at).month == now_m
        ]
        if not paid_this:
            st.caption("없음")
        else:
            paid_rows = []
            for i in paid_this:
                cust = customer_by_id.get(i.customer_id)
                paid_rows.append({
                    "수금일": i.paid_at,
                    "고객사": cust.name if cust else i.customer_id,
                    "인보이스": i.id,
                    "수금액": f"{i.paid_amount:,.0f} {i.currency}",
                })
            st.dataframe(pd.DataFrame(paid_rows), hide_index=True, use_container_width=True)
            total_paid = sum(i.paid_amount for i in paid_this)
            st.caption(f"💰 이번 달 누적 수금: **{total_paid:,.0f}원** · {len(paid_this)}건")


# ════════════════════════════════════════════════════════════
# 탭 2: 고객사
# ════════════════════════════════════════════════════════════
with tab_cust:
    cust_l, cust_r = st.columns([1.4, 1])

    with cust_l:
        st.markdown("##### 🏢 고객사 목록")
        if not customers:
            st.info("등록된 고객사가 없습니다. 우측 폼에서 추가하세요.")
        else:
            rows = []
            for c in customers:
                n_contracts = sum(1 for ct in contracts if ct.customer_id == c.id)
                outstanding = sum(
                    i.amount - i.paid_amount for i in invoices
                    if i.customer_id == c.id and i.status in ("issued", "overdue")
                )
                rows.append({
                    "ID": c.id,
                    "고객사": c.name,
                    "사업자번호": c.biz_no or "—",
                    "담당자(우리)": c.ar_manager or "—",
                    "계약 수": n_contracts,
                    "미수금": f"{outstanding:,.0f}",
                })
            st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

    with cust_r:
        st.markdown("##### ➕ 신규 고객사 등록")
        with st.form("add_customer", clear_on_submit=True):
            new_name = st.text_input("고객사명 *", placeholder="㈜OO")
            new_biz = st.text_input("사업자번호", placeholder="123-45-67890")
            new_contact_name = st.text_input("담당자(고객) 이름")
            new_contact_email = st.text_input("담당자(고객) 이메일", placeholder="kim@customer.com")
            new_ar_mgr = st.text_input("AR 담당자(우리) 이메일", placeholder="yonghan@mandata.kr")
            new_acc = st.text_input("회계 담당자 이메일", placeholder="accounting@mandata.kr")
            new_notes = st.text_area("메모", height=68)
            submit_cust = st.form_submit_button("등록", type="primary")
            if submit_cust:
                if not new_name.strip():
                    st.error("고객사명은 필수입니다.")
                else:
                    new_cust = ar_models.Customer(
                        id=ar_models.next_customer_id(customers),
                        name=new_name.strip(),
                        biz_no=new_biz.strip(),
                        contact_name=new_contact_name.strip(),
                        contact_email=new_contact_email.strip(),
                        ar_manager=new_ar_mgr.strip(),
                        accounting_email=new_acc.strip(),
                        notes=new_notes.strip(),
                        created_at=today.isoformat(),
                    )
                    customers.append(new_cust)
                    ar_models.save_customers(customers)
                    st.success(f"✅ {new_cust.name} ({new_cust.id}) 등록 완료")
                    st.rerun()


# ════════════════════════════════════════════════════════════
# 탭 3: 계약
# ════════════════════════════════════════════════════════════
with tab_contract:
    if not customers:
        st.warning("계약을 등록하려면 먼저 **🏢 고객사** 탭에서 고객사를 등록해주세요.")
    else:
        ct_l, ct_r = st.columns([1.4, 1])

        with ct_l:
            st.markdown("##### 📋 계약 목록")
            if not contracts:
                st.info("등록된 계약이 없습니다. 우측 폼에서 추가하세요.")
            else:
                rows = []
                for ct in contracts:
                    cust = customer_by_id.get(ct.customer_id)
                    rows.append({
                        "ID": ct.id,
                        "고객사": cust.name if cust else ct.customer_id,
                        "제목": ct.title,
                        "기간": f"{ct.start_date} ~ {ct.end_date or '—'}",
                        "금액": f"{ct.total_amount:,.0f} {ct.currency}",
                        "빈도": ct.billing_frequency,
                        "상태": ct.status,
                    })
                st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

                # 계약 삭제 / 인보이스 재생성
                with st.expander("🔧 계약 관리 (선택)", expanded=False):
                    target_id = st.selectbox(
                        "계약 선택", [ct.id for ct in contracts],
                        format_func=lambda x: f"{x} — {contract_by_id[x].title}",
                        key="ct_action_target",
                    )
                    a_cols = st.columns(3)
                    with a_cols[0]:
                        if st.button("🔄 인보이스 재생성 (pending만)", key="ct_regen", use_container_width=True):
                            target = contract_by_id[target_id]
                            invoices = ar_schedule.regenerate_for_contract(target, invoices)
                            ar_models.save_invoices(invoices)
                            st.success(f"{target_id} 인보이스 재생성 완료")
                            st.rerun()
                    with a_cols[1]:
                        new_status = st.selectbox(
                            "상태 변경", ["active", "paused", "ended"],
                            index=["active", "paused", "ended"].index(contract_by_id[target_id].status)
                            if contract_by_id[target_id].status in ("active", "paused", "ended") else 0,
                            key="ct_status_change",
                        )
                        if st.button("적용", key="ct_status_apply", use_container_width=True):
                            for ct in contracts:
                                if ct.id == target_id:
                                    ct.status = new_status
                            ar_models.save_contracts(contracts)
                            st.success("상태 변경 완료")
                            st.rerun()
                    with a_cols[2]:
                        if st.button("🗑 계약+인보이스 삭제", key="ct_delete", type="secondary",
                                      use_container_width=True):
                            contracts = [ct for ct in contracts if ct.id != target_id]
                            invoices = [i for i in invoices if i.contract_id != target_id]
                            ar_models.save_contracts(contracts)
                            ar_models.save_invoices(invoices)
                            st.success("삭제 완료")
                            st.rerun()

        with ct_r:
            st.markdown("##### ➕ 신규 계약 등록")
            with st.form("add_contract", clear_on_submit=True):
                cust_id_pick = st.selectbox(
                    "고객사 *",
                    [c.id for c in customers],
                    format_func=lambda x: f"{customer_by_id[x].name} ({x})",
                )
                ct_title = st.text_input("계약 제목 *", placeholder="2026 데이터 라이선스")
                ct_type = st.selectbox("계약 유형", ["annual", "monthly", "one-time", "custom"], index=0)
                date_cols = st.columns(2)
                with date_cols[0]:
                    ct_start = st.date_input("시작일 *", value=today)
                with date_cols[1]:
                    ct_end = st.date_input("종료일", value=today + timedelta(days=365))
                amt_cols = st.columns([2, 1])
                with amt_cols[0]:
                    ct_amount = st.number_input("총 금액 *", min_value=0.0, step=100_000.0, value=12_000_000.0)
                with amt_cols[1]:
                    ct_currency = st.selectbox("통화", ["KRW", "USD"], index=0)
                ct_freq = st.selectbox(
                    "Billing Frequency *",
                    ["monthly", "quarterly", "annually", "one-time"],
                    index=0,
                )
                freq_cols = st.columns(2)
                with freq_cols[0]:
                    ct_bday = st.number_input("발행일 (매월/매분기/매년)", min_value=1, max_value=28, value=1)
                with freq_cols[1]:
                    ct_terms = st.number_input("수금 기한 (일)", min_value=0, max_value=180, value=30)
                ct_url = st.text_input("계약서/오더폼 URL", placeholder="https://drive.google.com/...")
                ct_notes = st.text_area("메모", height=68)

                with st.expander("💼 데이터 오너 배분 (옵션)", expanded=False):
                    st.caption("배분율 합계는 1.0 이하여야 합니다 (나머지는 회사 몫). 최대 4명까지 등록.")
                    rs_rows = []
                    for k in range(4):
                        rs_cols = st.columns([2, 1, 2])
                        with rs_cols[0]:
                            owner = st.text_input(f"오너 #{k+1} 이름", key=f"rs_owner_{k}")
                        with rs_cols[1]:
                            ratio = st.number_input(f"비율 #{k+1}", min_value=0.0, max_value=1.0,
                                                     step=0.05, value=0.0, key=f"rs_ratio_{k}")
                        with rs_cols[2]:
                            owner_email = st.text_input(f"이메일 #{k+1}", key=f"rs_email_{k}")
                        if owner.strip() and ratio > 0:
                            rs_rows.append(ar_models.RevenueShare(
                                owner=owner.strip(),
                                ratio=float(ratio),
                                contact_email=owner_email.strip(),
                            ))

                submit_ct = st.form_submit_button("등록 + 인보이스 자동 생성", type="primary")
                if submit_ct:
                    if not ct_title.strip():
                        st.error("계약 제목은 필수입니다.")
                    elif ct_amount <= 0:
                        st.error("금액은 0보다 커야 합니다.")
                    elif ct_freq != "one-time" and not ct_end:
                        st.error("종료일을 입력해주세요 (일회성 외에는 필수).")
                    elif sum(rs.ratio for rs in rs_rows) > 1.0001:
                        st.error("배분율 합계가 1.0을 초과합니다.")
                    else:
                        new_ct = ar_models.Contract(
                            id=ar_models.next_contract_id(contracts),
                            customer_id=cust_id_pick,
                            title=ct_title.strip(),
                            contract_type=ct_type,
                            start_date=ct_start.isoformat(),
                            end_date=ct_end.isoformat() if ct_end else "",
                            total_amount=float(ct_amount),
                            currency=ct_currency,
                            billing_frequency=ct_freq,
                            billing_day_of_month=int(ct_bday),
                            payment_terms_days=int(ct_terms),
                            revenue_shares=rs_rows,
                            status="active",
                            order_form_url=ct_url.strip(),
                            notes=ct_notes.strip(),
                            created_at=today.isoformat(),
                        )
                        contracts.append(new_ct)
                        ar_models.save_contracts(contracts)
                        # 인보이스 자동 생성
                        new_invs = ar_schedule.generate_invoice_schedule(new_ct, invoices)
                        invoices = invoices + new_invs
                        ar_models.save_invoices(invoices)
                        st.success(
                            f"✅ {new_ct.title} ({new_ct.id}) 등록 + "
                            f"인보이스 {len(new_invs)}건 자동 생성"
                        )
                        st.rerun()


# ════════════════════════════════════════════════════════════
# 탭 4: 인보이스
# ════════════════════════════════════════════════════════════
with tab_invoice:
    if not invoices:
        st.info("등록된 인보이스가 없습니다. 계약을 등록하면 자동으로 생성됩니다.")
    else:
        # 필터
        f_cols = st.columns([1, 1, 1, 1])
        with f_cols[0]:
            status_filter = st.multiselect(
                "상태 필터",
                ["pending", "issued", "paid", "overdue", "void"],
                default=["pending", "issued", "overdue"],
            )
        with f_cols[1]:
            cust_filter = st.selectbox(
                "고객사 필터",
                ["(전체)"] + [c.id for c in customers],
                format_func=lambda x: "(전체)" if x == "(전체)" else customer_by_id[x].name,
            )
        with f_cols[2]:
            month_filter = st.selectbox(
                "월 필터",
                ["(전체)", "이번 달", "다음 달", "지난 달"],
            )
        with f_cols[3]:
            sort_key = st.selectbox(
                "정렬",
                ["만기일 가까운 순", "발행일 빠른 순", "금액 큰 순"],
            )

        # 필터링
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
                    target_m = 1
                    target_y += 1
            elif month_filter == "지난 달":
                target_m = now_m - 1
                if target_m < 1:
                    target_m = 12
                    target_y -= 1
            def _in_month(i):
                d = ar_models.parse_iso(i.issue_date)
                return d and d.year == target_y and d.month == target_m
            filtered = [i for i in filtered if _in_month(i)]

        # 정렬
        if sort_key == "만기일 가까운 순":
            filtered.sort(key=lambda i: ar_models.parse_iso(i.due_date) or date.max)
        elif sort_key == "발행일 빠른 순":
            filtered.sort(key=lambda i: ar_models.parse_iso(i.issue_date) or date.max)
        else:  # 금액 큰 순
            filtered.sort(key=lambda i: -i.amount)

        st.caption(f"{len(filtered)}건 / 전체 {len(invoices)}건")

        # 인보이스 테이블 + 상태 변경 액션
        for inv in filtered[:50]:  # too many면 잘라서
            cust = customer_by_id.get(inv.customer_id)
            ct = contract_by_id.get(inv.contract_id)
            color_map = {
                "pending": "#94A3B8",
                "issued": "#3B82F6",
                "paid": "#22C55E",
                "overdue": "#EF4444",
                "void": "#71717A",
            }
            sc = color_map.get(inv.status, "#94A3B8")

            with st.container(border=True):
                cols = st.columns([3, 2, 1.5, 2])
                with cols[0]:
                    st.markdown(
                        f"<div style='font-weight:600; color:#F1F5F9;'>{inv.id}</div>"
                        f"<div style='font-size:0.78rem; color:rgba(241,245,249,0.6);'>"
                        f"{cust.name if cust else inv.customer_id} · "
                        f"{ct.title if ct else inv.contract_id}</div>",
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
                        f"<div style='font-family:\"JetBrains Mono\",monospace; font-weight:700; color:#F1F5F9; text-align:right;'>"
                        f"{inv.amount:,.0f}<br><span style='font-size:0.78rem; font-weight:400; color:rgba(241,245,249,0.6);'>{inv.currency}</span></div>",
                        unsafe_allow_html=True,
                    )
                with cols[3]:
                    st.markdown(
                        f"<div style='display:inline-block; padding:3px 10px; border-radius:10px; "
                        f"background:{sc}25; color:{sc}; font-weight:600; font-size:0.8rem; "
                        f"margin-bottom:6px;'>{inv.status}</div>",
                        unsafe_allow_html=True,
                    )
                    # 상태 변경 액션
                    if inv.status == "pending":
                        if st.button("📤 발행", key=f"act_issue_{inv.id}", use_container_width=True):
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


# ─────────────────────────────────────────────────────────────
# 푸터 — 데이터 영속성 안내
# ─────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "📂 데이터: `ar_app/data/{customers,contracts,invoices}.json` · "
    "Streamlit Cloud는 재배포 시 파일이 사라지는 ephemeral fs라, "
    "변경사항은 로컬에서 git commit & push 로 영구 보존하세요."
)
