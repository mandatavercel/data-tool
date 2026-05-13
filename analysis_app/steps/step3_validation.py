"""Step 3 — Data Validation (데이터 품질 검사)."""
from __future__ import annotations

import streamlit as st

from modules.common.foundation import validate_data
from modules.analysis.guides import render_guide
from analysis_app.navigation import go_to


def render() -> None:
    st.subheader("Step 3 — Data Validation")
    render_guide("step3")

    df       = st.session_state.get("raw_df")
    role_map = st.session_state.get("role_map", {})

    if df is None or not role_map:
        st.warning("이전 단계를 완료하세요.")
        go_to(2)

    # validate_data()가 Streamlit 렌더링을 직접 수행
    result = validate_data(df, role_map)
    st.session_state["quality_score"]     = result["score"]
    st.session_state["validation_result"] = result

    c_prev, c_next = st.columns(2)
    with c_prev:
        if st.button("← Schema Intelligence"):
            go_to(2)
    with c_next:
        if st.button("다음 → Capability Map", type="primary"):
            go_to(4)
