"""Step 2 — Schema Intelligence (컬럼 역할 추론 + 매핑 UI)."""
from __future__ import annotations

import streamlit as st

from modules.common.foundation import (
    infer_schema, ROLE_OPTIONS, ROLE_LABEL, ROLE_COLOR, ROLE_DESCRIPTION,
    role_label, role_help_text, normalize_role_map,
    user_role_options, ALIAS_TO_PARENT, normalize_to_user_role,
)
from modules.analysis.guides import render_guide
from analysis_app.navigation import go_to


# ── 역할 가이드 그룹 (expander 안에서 카테고리별 표시) ─────────────────────────
_ROLE_GROUPS = [
    ("📅 시간",           ["transaction_date"]),
    ("🏢 식별",           ["company_name", "brand_name", "sku_name"]),
    ("🗂 카테고리 계층",   ["category_large", "category_medium", "category_small", "category_name"]),
    ("💰 매출 메트릭",     ["sales_amount", "sales_quantity", "sales_count", "unit_price"]),
    ("👥 이용자 메트릭",   ["active_users"]),
    ("📊 데모그래픽",      ["gender", "age_group", "region"]),
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
        try:
            with st.spinner("스키마 추론 중... (패턴 매칭 + AI 보강 — 약 5~15초)"):
                st.session_state["schema_rows"] = infer_schema(df)
        except Exception as e:
            st.error(f"스키마 추론 실패: {type(e).__name__}: {str(e)[:200]}")
            # 최소한의 빈 schema로 폴백
            st.session_state["schema_rows"] = [
                {"column_name": c, "dtype": str(df[c].dtype),
                 "sample": "", "null_pct": 0, "n_unique": 0,
                 "inferred_role": "unknown", "confidence": 0,
                 "reason": "추론 실패 — 수동 매핑", "final_role": "unknown"}
                for c in df.columns
            ]

    schema_rows = st.session_state["schema_rows"]

    # ── AI 사용 가능 여부 표시 (실패해도 앱 중단 안 됨) ───────────────────────
    ai_enabled = False
    n_llm_enhanced = 0
    try:
        from modules.common.schema_ai import is_available as _ai_ok
        ai_enabled = _ai_ok()
        n_llm_enhanced = sum(1 for r in schema_rows if r.get("llm_reasoning"))
    except Exception:
        pass

    # ── 헤더 ──────────────────────────────────────────────────────────────────
    hcol, bcol = st.columns([5, 1])
    with hcol:
        if ai_enabled:
            st.caption(
                f"역할을 수정하거나 불필요한 컬럼을 제외하세요. "
                f"역할 의미가 헷갈리면 ↓ 가이드 펼쳐보세요.  "
                f"🤖 **AI 보강 활성** — 패턴이 약한 컬럼은 Claude가 자동으로 의미·분석 활용처를 추론합니다 "
                f"(보강된 컬럼: {n_llm_enhanced}개)."
            )
        else:
            st.caption(
                "역할을 수정하거나 불필요한 컬럼을 제외하세요. "
                "역할 의미가 헷갈리면 ↓ 가이드 펼쳐보세요.  "
                "⚠️ AI 스마트 추론 비활성화 — `ANTHROPIC_API_KEY`를 Streamlit Secrets에 추가하면 "
                "낯선 컬럼도 자동 분석됩니다."
            )
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

        # 역할 selectbox (한국어 라벨 + 백엔드 alias 숨김 + 방어적 정규화)
        role_key = f"role_{i}"
        try:
            opts = user_role_options()
        except Exception:
            opts = ["unknown"]
        if "unknown" not in opts:
            opts = list(opts) + ["unknown"]

        # session_state 강제 정규화 (selectbox 호출 전에 반드시)
        # 단일 출처 normalize_to_user_role 사용 — alias/invalid → 'unknown' 또는 부모
        if role_key not in st.session_state:
            st.session_state[role_key] = normalize_to_user_role(
                row.get("final_role"), opts
            )
        else:
            st.session_state[role_key] = normalize_to_user_role(
                st.session_state[role_key], opts
            )

        try:
            new_role = c_role.selectbox(
                "", opts,
                key=role_key,
                format_func=role_label,
                label_visibility="collapsed",
                disabled=not included,
                help=role_help_text(st.session_state.get(role_key, "unknown")),
            )
        except Exception as e:
            # 어떤 상황이든 selectbox가 죽지 않도록 — fallback display
            new_role = st.session_state.get(role_key, "unknown")
            c_role.markdown(
                f"<div style='font-size:11px;color:#dc2626'>"
                f"역할 선택 UI 오류 (현재: {new_role})</div>",
                unsafe_allow_html=True,
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

        # 근거 + AI 분석 힌트 (방어적 — 어떤 키든 빠져도 OK)
        try:
            rsn_html = (
                f"<div style='padding:6px 0;font-size:11px;color:#6b7280'>"
                f"{row.get('reason', '')}</div>"
            )
            hint = row.get("llm_analysis_hint") or ""
            if hint:
                rsn_html += (
                    f"<div style='font-size:11px;color:#1e40af;background:#eff6ff;"
                    f"border-radius:4px;padding:3px 6px;margin-top:2px;line-height:1.4'>"
                    f"💡 {hint}</div>"
                )
            c_rsn.markdown(rsn_html, unsafe_allow_html=True)
        except Exception:
            c_rsn.markdown(
                f"<div style='font-size:11px;color:#9ca3af'>—</div>",
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

        # ── 🎯 활성화된 파생 메트릭 / 가능 분석 ──────────────────────────
        try:
            from modules.common.derived_metrics import suggest_derived
            derived = suggest_derived(raw_role_map)
            active   = [d for d in derived if d["active_now"]]
            possible = [d for d in derived if not d["active_now"]]

            if active:
                st.markdown(
                    "<div style='margin-top:14px;font-size:13px;font-weight:700;"
                    "color:#0f172a'>✅ 자동으로 가능해진 파생 분석</div>",
                    unsafe_allow_html=True,
                )
                for d in active:
                    st.markdown(
                        f"<div style='background:#ecfdf5;border-left:3px solid #10b981;"
                        f"padding:8px 12px;margin:4px 0;border-radius:4px;font-size:12.5px'>"
                        f"<b>{d['name']}</b> &nbsp; "
                        f"<code style='font-size:11px;color:#065f46'>{d['formula']}</code><br>"
                        f"<span style='color:#475569;line-height:1.5'>{d['describes']}</span><br>"
                        f"<span style='color:#94a3b8;font-size:11px'>예: {d['example']}</span>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )
            if possible:
                with st.expander(
                    f"💡 추가로 매핑하면 가능해질 분석 ({len(possible)}개)",
                    expanded=False,
                ):
                    for d in possible:
                        missing = [m for m in d["requires"]
                                   if m not in raw_role_map]
                        st.markdown(
                            f"<div style='background:#fffbeb;border-left:3px solid #fbbf24;"
                            f"padding:8px 12px;margin:4px 0;border-radius:4px;font-size:12.5px'>"
                            f"<b>{d['name']}</b><br>"
                            f"<span style='color:#475569'>{d['describes']}</span><br>"
                            f"<span style='font-size:11px;color:#92400e'>"
                            f"필요 역할: {', '.join('`'+m+'`' for m in missing)}</span>"
                            f"</div>",
                            unsafe_allow_html=True,
                        )
        except Exception:
            pass
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
