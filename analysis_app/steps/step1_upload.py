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

    # 파일 읽기 — 메모리 cap (Streamlit Cloud 무료 티어 1GB 대응)
    # 핵심: 읽을 때부터 행 제한해서 OOM 자체를 막음 (다운샘플 후가 아니라 읽기 단계에서)
    _ROW_HARD_CAP = 500_000   # 50만행으로 보수적 — 안전 첫째

    file_size_mb = (uploaded.size or 0) / (1024 * 1024) if hasattr(uploaded, "size") else 0
    is_large = file_size_mb > 50    # 50MB 넘으면 대용량 모드

    try:
        with st.spinner(f"파일 읽는 중... ({uploaded.name}, {file_size_mb:.1f}MB)"):
            if uploaded.name.endswith(".xlsx"):
                # 첫 행 수 체크
                if is_large:
                    # xlsx는 chunksize 미지원 → 일단 nrows로 cap해서 읽음
                    df = pd.read_excel(uploaded, nrows=_ROW_HARD_CAP)
                    truncated_at_read = True
                else:
                    df = pd.read_excel(uploaded)
                    truncated_at_read = False
            else:  # csv
                if is_large:
                    df = pd.read_csv(uploaded, nrows=_ROW_HARD_CAP)
                    truncated_at_read = True
                else:
                    df = pd.read_csv(uploaded)
                    truncated_at_read = False
    except MemoryError:
        st.error(
            "❌ **메모리 부족** — 파일이 클라우드 한계(1GB RAM)를 초과합니다.\n\n"
            "**해결 방법**:\n"
            "1. **파일을 더 작게 분할** 후 업로드 (예: 월별 분할)\n"
            "2. **CSV로 변환** 후 업로드 (xlsx보다 메모리 효율적)\n"
            "3. **유료 티어** ($20/월, 16GB RAM) 업그레이드"
        )
        st.stop()
    except Exception as e:
        st.error(
            f"❌ 파일 읽기 실패: **{type(e).__name__}**: {str(e)[:200]}\n\n"
            "→ 파일 형식 오류 또는 메모리 초과 가능성."
        )
        st.stop()

    n_orig = len(df)
    if truncated_at_read:
        st.warning(
            f"⚠️ **대용량 파일 감지** ({file_size_mb:.1f}MB) — "
            f"메모리 안전을 위해 **상위 {n_orig:,}행만 로드**했습니다.\n\n"
            "전체 데이터 분석이 필요하면: (1) 파일을 분할하거나, "
            "(2) Streamlit Cloud 유료 티어 사용, "
            "(3) CSV 형식 사용."
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
