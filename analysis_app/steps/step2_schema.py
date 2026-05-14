"""Step 2 — Schema Intelligence (컬럼 역할 추론 + 매핑 UI)."""
from __future__ import annotations

import streamlit as st

from modules.common.foundation import (
    infer_schema, ROLE_OPTIONS, ROLE_LABEL, ROLE_COLOR, ROLE_DESCRIPTION,
    role_label, role_help_text, normalize_role_map, user_role_options,
)
from modules.analysis.guides import render_guide
from analysis_app.navigation import go_to


# ── 역할 가이드 그룹 (expander 안에서 카테고리별 표시) ─────────────────────────
_ROLE_GROUPS = [
    ("📅 시간",           ["transaction_date"]),
    ("🏢 식별",           ["company_name", "brand_name", "sku_name"]),
    ("🗂 카테고리 계층",   ["category_large", "category_medium", "category_small", "category_name"]),
    ("💰 매출 메트릭",     ["sales_amount", "sales_quantity", "sales_count", "unit_price"]),
    ("👥 데모그래픽",      ["gender", "age_group", "region"]),
    ("🛒 채널·점포·고객", ["channel", "store_id", "customer_id"]),
    ("🔄 리텐션",         ["retention_flag"]),
    ("📈 종목 매핑",       ["stock_code", "security_code"]),
]


def _render_role_guide() -> None:
    """역할 가이드 expander — 각 역할로 매핑하면 어떤 분석에 활용되는지."""
    with st.expander(
        "📖 역할 가이드 — 이 역할로 매핑하면 어떤 분석이 가능한지 (펼쳐서 확인)",
        expanded=False,
    ):
        st.caption(
            "각 컬럼을 어떤 역할로 매핑할지 헷갈리면 아래를 참고하세요. "
            "각 역할로 매핑하면 어떤 분석이 활성화되고, 무엇을 알 수 있는지 정리되어 있습니다."
        )
        import pandas as pd
        for group_label, role_keys in _ROLE_GROUPS:
            st.markdown(f"##### {group_label}")
            rows = []
            for r in role_keys:
                info = ROLE_DESCRIPTION.get(r, {})
                rows.append({
                    "역할":      ROLE_LABEL.get(r, r),
                    "의미":      info.get("what", "—"),
                    "이 역할로 매핑하면 활용 가능한 분석":
                                  info.get("for", "—"),
                })
            st.dataframe(
                pd.DataFrame(rows), hide_index=True, use_container_width=True,
            )
            st.write("")


def render() -> None:
    st.subheader("Step 2 — Schema Intelligence")
    render_guide("step2")

    df = st.session_state.get("raw_df")
    if df is None:
        st.warning("먼저 파일을 업로드하세요.")
        go_to(1)

    # ── 최초 1회만 추론, 이후 rerun은 session_state 사용 ─────────────────────
    if "schema_rows" not in st.session_state:
        st.session_state["schema_rows"] = infer_schema(df)

    schema_rows = st.session_state["schema_rows"]

    # ── 헤더 ──────────────────────────────────────────────────────────────────
    hcol, bcol = st.columns([5, 1])
    with hcol:
        st.caption("역할을 수정하거나 불필요한 컬럼을 제외하세요. "
                   "역할 의미가 헷갈리면 ↓ 가이드 펼쳐보세요.")
    with bcol:
        if st.button("🔄 재추론"):
            st.session_state.pop("schema_rows", None)
            st.rerun()

    # ── 역할 가이드 expander (각 역할의 의미 + 활용처) ──────────────────────
    _render_role_guide()

    # ── 필수 역할 할당 현황 경고 ─────────────────────────────────────────────
    cur_roles = {r["final_role"] for r in schema_rows}
    for req in ["transaction_date", "sales_amount"]:
        if req not in cur_roles:
            st.error(f"❌ `{req}` 역할이 없습니다 — 아래에서 직접 지정하세요.")

    # ── 컬럼별 편집 UI ────────────────────────────────────────────────────────
    h = st.columns([0.4, 1.8, 2.0, 2.4, 1.5, 0.6, 1.5])
    for label, col in zip(
        ["포함", "컬럼명", "샘플값 (상위 3)", "역할 ✏️", "현재 매핑", "신뢰도", "근거"],
        h
    ):
        col.markdown(f"<div style='font-size:12px;font-weight:600;color:#6b7280;"
                     f"padding-bottom:4px;border-bottom:1px solid #e5e7eb;'>"
                     f"{label}</div>", unsafe_allow_html=True)

    st.write("")

    updated_rows = []
    for i, row in enumerate(schema_rows):
        c_chk, c_col, c_smp, c_role, c_badge, c_conf, c_rsn = st.columns(
            [0.4, 1.8, 2.0, 2.4, 1.5, 0.6, 1.5]
        )

        # 포함 체크박스
        inc_key = f"inc_{i}"
        if inc_key not in st.session_state:
            st.session_state[inc_key] = True
        included = c_chk.checkbox("", value=st.session_state[inc_key], key=inc_key,
                                  label_visibility="collapsed")

        # 컬럼명
        c_col.markdown(
            f"<div style='padding:6px 0;font-size:13px;"
            f"{'color:#9ca3af;text-decoration:line-through' if not included else ''}'>"
            f"<code>{row['column_name']}</code><br>"
            f"<small style='color:#9ca3af'>{row['dtype']}</small></div>",
            unsafe_allow_html=True,
        )

        # 샘플값
        c_smp.markdown(
            f"<div style='padding:6px 0;font-size:12px;color:#374151;"
            f"line-height:1.45;word-break:break-all'>"
            f"{row['sample']}</div>",
            unsafe_allow_html=True,
        )

        # 역할 selectbox (한국어 라벨로 표시 — 백엔드 alias는 숨김)
        role_key = f"role_{i}"
        # 사용자에게 보일 옵션만 — quantity/number_of_tx 같은 alias 숨김
        opts = user_role_options()
        # 옛 세션·신규 추론이 alias라면 부모(sales_quantity/sales_count)로 정규화.
        # 정규화는 selectbox 호출 *전에* session_state에 반영 — 안 그러면
        # Streamlit이 session_state 값을 options에서 못 찾아 ValueError 발생.
        _ALIAS_TO_PARENT = {"quantity": "sales_quantity",
                            "number_of_tx": "sales_count"}

        # 초기값 세팅: 추론 결과를 부모로 정규화 후 저장
        if role_key not in st.session_state:
            init_val = _ALIAS_TO_PARENT.get(row["final_role"], row["final_role"])
            if init_val not in opts:
                init_val = "unknown"
            st.session_state[role_key] = init_val
        else:
            # 기존 session_state 값이 alias 또는 옵션 밖이면 정규화
            cur = st.session_state[role_key]
            cur = _ALIAS_TO_PARENT.get(cur, cur)
            if cur not in opts:
                cur = "unknown"
            st.session_state[role_key] = cur

        new_role = c_role.selectbox(
            "", opts,
            key=role_key,                  # session_state 기반 — index 불필요
            format_func=role_label,
            label_visibility="collapsed",
            disabled=not included,
            help=role_help_text(st.session_state[role_key]),
        )

        # 현재 매핑된 역할 뱃지
        if included and new_role != "unknown":
            bg = ROLE_COLOR.get(new_role, "#f3f4f6")
            lbl = ROLE_LABEL.get(new_role, new_role)
            c_badge.markdown(
                f"<div style='padding:8px 0'>"
                f"<span style='background:{bg};border-radius:6px;padding:5px 10px;"
                f"font-size:12.5px;font-weight:600;color:#1f2937;display:inline-block;"
                f"white-space:nowrap'>{lbl}</span></div>",
                unsafe_allow_html=True,
            )
        else:
            c_badge.markdown(
                f"<div style='padding:8px 0;font-size:11px;color:#9ca3af'>—</div>",
                unsafe_allow_html=True,
            )

        # 신뢰도 배지
        conf = row["confidence"]
        conf_color = "#16a34a" if conf >= 70 else ("#d97706" if conf >= 40 else "#9ca3af")
        c_conf.markdown(
            f"<div style='padding:8px 0;font-size:12px;font-weight:600;color:{conf_color}'>"
            f"{conf}%</div>",
            unsafe_allow_html=True,
        )

        # 근거
        c_rsn.markdown(
            f"<div style='padding:6px 0;font-size:11px;color:#6b7280'>"
            f"{row['reason']}</div>",
            unsafe_allow_html=True,
        )

        updated_rows.append({**row, "final_role": new_role, "included": included})

    st.session_state["schema_rows"] = updated_rows

    # ── role_map 구성 + alias 자동 등록 ──────────────────────────────────────
    raw_role_map = {}
    for r in updated_rows:
        if not r["included"]:
            continue
        role = r["final_role"]
        if role != "unknown" and role not in raw_role_map:
            raw_role_map[role] = r["column_name"]
    role_map = normalize_role_map(raw_role_map)
    st.session_state["role_map"] = role_map

    # ── 매핑 결과 요약 ────────────────────────────────────────────────────────
    st.divider()
    if raw_role_map:
        tag_html = " ".join(
            f"<span style='background:{ROLE_COLOR.get(role,'#f3f4f6')};"
            f"border-radius:6px;padding:5px 12px;font-size:13px;margin:3px;display:inline-block;"
            f"font-weight:600;color:#1f2937'>"
            f"{ROLE_LABEL.get(role, role)} → <code style='font-size:11px;color:#4b5563'>{col}</code></span>"
            for role, col in raw_role_map.items()
        )
        st.markdown(
            f"<div style='font-size:13px;font-weight:700;color:#0f172a;"
            f"margin-bottom:8px'>📋 매핑된 역할 ({len(raw_role_map)}개)</div>"
            f"{tag_html}",
            unsafe_allow_html=True,
        )

        added_aliases = sorted(set(role_map) - set(raw_role_map))
        if added_aliases:
            alias_str = ", ".join(f"{ROLE_LABEL.get(a, a)}" for a in added_aliases)
            st.caption(f"↳ 호환 alias 자동 등록: {alias_str}")

        excluded = [r["column_name"] for r in updated_rows if not r["included"]]
        if excluded:
            st.caption(f"제외된 컬럼: {', '.join(excluded)}")
    else:
        st.info("역할이 매핑되지 않았습니다.")

    st.write("")
    c_prev, c_next = st.columns(2)
    with c_prev:
        if st.button("← Data Upload"):
            go_to(1)
    with c_next:
        has_required = all(r in role_map for r in ["transaction_date", "sales_amount"])
        if st.button(
            "다음 → Data Validation",
            type="primary",
            disabled=not has_required,
        ):
            go_to(3)
