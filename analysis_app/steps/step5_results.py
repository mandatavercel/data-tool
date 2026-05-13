"""Step 5 — Results (분석 모듈별 탭 + Investor Dashboard)."""
from __future__ import annotations

import streamlit as st

from analysis_app.navigation import go_to
from analysis_app.dashboard  import render_investor_dashboard
from analysis_app.config     import ANALYSIS_OPTIONS, RENDERERS


STATUS_ICON = {"success": "✅", "warning": "⚠️", "failed": "❌"}
DASH_LABEL  = "🎯 Investor Dashboard"


def render() -> None:
    st.subheader("Step 5 — Results")

    results = st.session_state.get("results", {})

    if not results:
        st.info("아직 실행된 분석이 없습니다. Step 4에서 모듈을 선택하고 실행하세요.")
        if st.button("← 분석 설정으로"):
            go_to(4)
        st.stop()

    # ── 탭 라벨 + Dashboard ──────────────────────────────────────────────────
    tab_keys   = list(results.keys())
    tab_labels = [ANALYSIS_OPTIONS.get(k, k) for k in tab_keys]
    all_views  = [DASH_LABEL] + tab_labels

    if "step5_active" not in st.session_state:
        st.session_state["step5_active"] = DASH_LABEL
    # 잘못된 active state 방어
    if st.session_state["step5_active"] not in all_views:
        st.session_state["step5_active"] = DASH_LABEL

    cur_idx = all_views.index(st.session_state["step5_active"])
    if hasattr(st, "segmented_control"):
        sel_view = st.segmented_control(
            "View", all_views, default=st.session_state["step5_active"],
            key="step5_view_sel", label_visibility="collapsed",
        )
        if sel_view is not None and sel_view != st.session_state["step5_active"]:
            st.session_state["step5_active"] = sel_view
            st.rerun()
    else:
        sel_view = st.radio(
            "View", all_views, index=cur_idx, horizontal=True,
            key="step5_view_radio", label_visibility="collapsed",
        )
        if sel_view != st.session_state["step5_active"]:
            st.session_state["step5_active"] = sel_view
            st.rerun()

    active = st.session_state["step5_active"]

    if active == DASH_LABEL:
        render_investor_dashboard(results, analysis_options=ANALYSIS_OPTIONS)
    else:
        sel_idx = tab_labels.index(active)
        sel_key = tab_keys[sel_idx]
        try:
            RENDERERS[sel_key](results[sel_key])
        except Exception as render_exc:
            st.error(
                f"렌더링 오류 — {ANALYSIS_OPTIONS.get(sel_key, sel_key)}\n\n"
                f"`{type(render_exc).__name__}: {str(render_exc)[:200]}`\n\n"
                "분석은 완료됐지만 차트 표시 중 문제가 발생했습니다. 다른 모듈은 정상 작동합니다."
            )

    st.divider()
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("← 분석 설정"):
            go_to(4)
    with c2:
        if st.button("🗑 결과 초기화"):
            st.session_state["results"] = {}
            for key in ANALYSIS_OPTIONS:
                st.session_state.pop(f"sel_{key}", None)
            go_to(4)
    with c3:
        if st.button("→ Signal Dashboard", type="primary"):
            go_to(6)
