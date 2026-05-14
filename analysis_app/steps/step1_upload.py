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

    # 파일 읽기 — Cloud에서만 메모리 cap 적용, 로컬은 무제한
    import os
    # Streamlit Cloud 환경 감지: /mount/src/ 디렉토리 존재 여부 (Cloud 표준 mount path)
    _ON_CLOUD = os.path.isdir("/mount/src")
    _ROW_HARD_CAP = 500_000

    file_size_mb = (uploaded.size or 0) / (1024 * 1024) if hasattr(uploaded, "size") else 0
    # Cloud + 50MB 이상일 때만 nrows cap 적용
    apply_cap = _ON_CLOUD and file_size_mb > 50

    try:
        env_label = "Cloud" if _ON_CLOUD else "Local"
        with st.spinner(f"파일 읽는 중... ({uploaded.name}, {file_size_mb:.1f}MB · {env_label})"):
            read_kwargs = {"nrows": _ROW_HARD_CAP} if apply_cap else {}
            if uploaded.name.endswith(".xlsx"):
                df = pd.read_excel(uploaded, **read_kwargs)
            else:
                df = pd.read_csv(uploaded, **read_kwargs)
    except MemoryError:
        st.error(
            "❌ **메모리 부족** — 파일이 환경 메모리 한계를 초과합니다.\n\n"
            "**해결 방법**:\n"
            "1. **파일을 더 작게 분할** 후 업로드 (예: 월별 분할)\n"
            "2. **CSV로 변환** 후 업로드 (xlsx보다 메모리 효율적)\n"
            "3. **Streamlit Cloud 유료 티어** ($25/월, 2.5GB RAM) 업그레이드"
        )
        st.stop()
    except Exception as e:
        st.error(
            f"❌ 파일 읽기 실패: **{type(e).__name__}**: {str(e)[:200]}\n\n"
            "→ 파일 형식 오류 또는 메모리 초과 가능성."
        )
        st.stop()

    n_orig = len(df)
    if apply_cap:
        st.warning(
            f"⚠️ **클라우드 메모리 안전 모드** — 대용량 파일 ({file_size_mb:.1f}MB)이라 "
            f"**상위 {n_orig:,}행만 로드**했습니다.\n\n"
            "전체 데이터 분석이 필요하면: (1) 로컬 실행 (`streamlit run streamlit_app.py`), "
            "(2) 파일 분할, (3) 유료 티어 사용."
        )
    elif _ON_CLOUD:
        st.caption(f"🟢 클라우드 환경 · 파일 크기 안전 범위 ({file_size_mb:.1f}MB · {n_orig:,}행)")
    else:
        st.caption(f"💻 로컬 환경 · 메모리 cap 없음 · 전체 {n_orig:,}행 로드")

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
