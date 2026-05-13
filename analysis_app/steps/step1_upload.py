"""Step 1 — Data Upload (CSV/XLSX 업로드)."""
from __future__ import annotations

import streamlit as st
import pandas as pd

from modules.analysis.guides import render_guide
from analysis_app.navigation import go_to


def render() -> None:
    st.subheader("Step 1 — Data Upload")
    render_guide("step1")

    uploaded = st.file_uploader(
        "데이터 파일 업로드 (xlsx / csv)",
        type=["xlsx", "csv"],
        key="upload_file",
    )

    if not uploaded:
        st.info("xlsx 또는 csv 파일을 업로드하세요.")
        st.stop()

    df = (
        pd.read_excel(uploaded)
        if uploaded.name.endswith(".xlsx")
        else pd.read_csv(uploaded)
    )

    # 파일이 바뀌면 이전 스키마 캐시 + 개별 위젯 상태 초기화
    prev_file = st.session_state.get("_uploaded_filename")
    if prev_file != uploaded.name:
        # schema_rows 개수만큼 생성된 inc_N / role_N 키도 함께 삭제
        old_rows = st.session_state.get("schema_rows", [])
        for i in range(len(old_rows)):
            st.session_state.pop(f"inc_{i}", None)
            st.session_state.pop(f"role_{i}", None)
        for key in ["schema_rows", "role_map", "quality_score",
                    "results", "selected_analysis"]:
            st.session_state.pop(key, None)
        st.session_state["_uploaded_filename"] = uploaded.name

    st.session_state["raw_df"] = df
    st.success(f"✅ {len(df):,}행 × {len(df.columns)}열 로드 완료")

    with st.expander("미리보기 (상위 5행)", expanded=True):
        st.dataframe(df.head(5))

    if st.button("다음 → Schema Intelligence", type="primary"):
        go_to(2)
