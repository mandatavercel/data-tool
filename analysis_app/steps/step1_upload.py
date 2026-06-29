"""Step 1 — Data Upload.

두 가지 입력 모드:
    A. 브라우저 업로드 (xlsx/csv/parquet) — 환경별 한도:
       - Local: 50 GB 가능 (메모리 한도 안에서)
       - Cloud: 1~2 GB 권장 (이상이면 타임아웃 위험)
    B. 로컬 경로 입력 — 초대용량 파일 (10~50 GB 이상) 권장.
       업로드 우회 → 메모리 복사 0회 → 즉시 디스크 스트리밍 읽기.
       Cloud에서는 사용 불가 (사용자 파일시스템 접근 불가).
"""
from __future__ import annotations

import os
import streamlit as st
import pandas as pd
from pathlib import Path

from modules.analysis.guides import render_guide
from analysis_app.navigation import go_to


# Cloud 환경 감지 — /mount/src/ 디렉토리는 Streamlit Cloud 표준 mount
def _is_cloud() -> bool:
    return os.path.isdir("/mount/src")


def _read_dataframe(source, name: str, nrows: int | None = None) -> pd.DataFrame:
    """파일/경로/업로드 객체에서 DataFrame 읽기. nrows로 행 제한 가능."""
    kwargs = {"nrows": nrows} if nrows else {}
    name_lc = name.lower()
    if name_lc.endswith(".parquet"):
        # parquet은 nrows 미지원 → 전체 읽고 head로 cap
        df = pd.read_parquet(source)
        if nrows:
            df = df.head(nrows)
        return df
    if name_lc.endswith(".xlsx") or name_lc.endswith(".xls"):
        return pd.read_excel(source, **kwargs)
    return pd.read_csv(source, **kwargs)


def _reset_session_for_new_file(new_name: str) -> None:
    """파일이 바뀌면 이전 분석 캐시 깨끗이 초기화."""
    prev_file = st.session_state.get("_uploaded_filename")
    if prev_file != new_name:
        old_rows = st.session_state.get("schema_rows", [])
        for i in range(len(old_rows)):
            st.session_state.pop(f"inc_{i}", None)
            st.session_state.pop(f"role_{i}", None)
        for key in ["schema_rows", "role_map", "quality_score",
                    "results", "selected_analysis"]:
            st.session_state.pop(key, None)
        st.session_state["_uploaded_filename"] = new_name


def render() -> None:
    st.subheader("Step 1 — Data Upload")
    render_guide("step1")

    on_cloud = _is_cloud()
    _ROW_HARD_CAP = 500_000   # Cloud 안전 cap

    # ── 입력 모드 선택 ──────────────────────────────────────────────────
    if on_cloud:
        st.info(
            "🟢 **클라우드 환경** — 파일 업로드 한도 50 GB. "
            "단, 1~2 GB 초과 시 메모리 부족으로 실패할 수 있어요. "
            "**대용량(10 GB+)은 로컬 실행 권장** (`.command` 더블클릭)."
        )
        mode = "upload"
    else:
        st.caption(
            "💻 **로컬 환경** · 초대용량(10 GB+) 파일은 "
            "**경로 입력 모드** 권장 (브라우저 업로드 우회 → 즉시 읽기)."
        )
        mode_label = st.radio(
            "입력 방식",
            ["📁 브라우저 업로드", "🛣 로컬 경로 입력 (대용량 권장)"],
            horizontal=True,
            label_visibility="collapsed",
        )
        mode = "path" if "경로" in mode_label else "upload"

    df: pd.DataFrame | None = None
    source_display = ""
    file_size_mb = 0.0

    # ── 모드 A: 브라우저 업로드 ─────────────────────────────────────────
    if mode == "upload":
        uploaded = st.file_uploader(
            "데이터 파일 (xlsx / csv / parquet · 최대 50 GB)",
            type=["xlsx", "csv", "parquet"],
            key="upload_file",
        )
        if not uploaded:
            st.info("xlsx · csv · parquet 파일 업로드.")
            st.stop()

        source_display = uploaded.name
        file_size_mb = (uploaded.size or 0) / (1024 * 1024) if hasattr(uploaded, "size") else 0
        apply_cap = on_cloud and file_size_mb > 50

        try:
            with st.spinner(f"파일 읽는 중... ({source_display}, {file_size_mb:.1f} MB)"):
                df = _read_dataframe(
                    uploaded, source_display,
                    nrows=_ROW_HARD_CAP if apply_cap else None,
                )
        except MemoryError:
            st.error(
                "❌ **메모리 부족** — 파일이 환경 메모리 한계를 초과합니다.\n\n"
                "**해결 방법**:\n"
                "1. **로컬 실행** + **경로 입력 모드** 사용 (메모리 복사 0회)\n"
                "2. **파일 분할** (월별/회사별)\n"
                "3. **parquet 형식 변환** (xlsx보다 10배 가벼움)"
            )
            st.stop()
        except Exception as e:
            st.error(f"❌ 파일 읽기 실패: **{type(e).__name__}**: {str(e)[:300]}")
            st.stop()

        if apply_cap:
            st.warning(
                f"⚠️ **클라우드 안전 모드** — 대용량({file_size_mb:.1f} MB)이라 "
                f"상위 {len(df):,}행만 로드. 전체는 로컬 실행 권장."
            )

    # ── 모드 B: 경로 입력 (로컬 전용) ───────────────────────────────────
    else:
        # 최근 사용 경로 기억
        last_path = st.session_state.get("_last_local_path", "")
        path_str = st.text_input(
            "📂 절대 경로 입력 (예: /Users/yonghan/Downloads/big_data.parquet)",
            value=last_path,
            placeholder="/Users/yonghan/Desktop/data/transactions_2024.parquet",
            help="홈 디렉토리는 ~/ 또는 $HOME 사용 가능. "
                 "지원 형식: parquet (최고 빠름·작음) > csv > xlsx",
        )

        # 빠른 선택 — Desktop / Downloads 흔한 경로
        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("📂 Desktop 탐색", use_container_width=True):
                st.session_state["_browse_dir"] = str(Path.home() / "Desktop")
        with c2:
            if st.button("📥 Downloads 탐색", use_container_width=True):
                st.session_state["_browse_dir"] = str(Path.home() / "Downloads")
        with c3:
            if st.button("🏠 홈 폴더 탐색", use_container_width=True):
                st.session_state["_browse_dir"] = str(Path.home())

        # 디렉토리 탐색 — 지원 형식만 나열
        browse_dir = st.session_state.get("_browse_dir")
        if browse_dir and not path_str:
            try:
                p = Path(browse_dir).expanduser()
                if p.is_dir():
                    files = sorted(
                        [
                            f for f in p.iterdir()
                            if f.is_file() and f.suffix.lower() in {".parquet", ".csv", ".xlsx", ".xls"}
                        ],
                        key=lambda f: f.stat().st_mtime,
                        reverse=True,
                    )[:30]
                    if files:
                        st.caption(f"📂 `{p}` (최신 {len(files)}개 파일)")
                        for f in files:
                            size_mb = f.stat().st_size / (1024 * 1024)
                            if st.button(
                                f"📄 {f.name}  ·  {size_mb:.1f} MB",
                                key=f"pick_{f.name}",
                                use_container_width=True,
                            ):
                                st.session_state["_last_local_path"] = str(f)
                                st.rerun()
                    else:
                        st.caption(f"📭 `{p}`에 지원 형식(parquet/csv/xlsx) 파일 없음.")
            except Exception as e:
                st.caption(f"⚠️ 디렉토리 읽기 실패: {e}")

        if not path_str:
            st.info("경로 입력하거나 위 ‘탐색’ 버튼으로 파일 선택.")
            st.stop()

        # 경로 정규화 + 검증
        try:
            path = Path(path_str).expanduser().resolve()
        except Exception as e:
            st.error(f"❌ 경로 형식 오류: {e}")
            st.stop()

        if not path.exists():
            st.error(f"❌ 파일을 찾을 수 없음: `{path}`")
            st.stop()
        if not path.is_file():
            st.error(f"❌ 파일이 아닌 경로 (폴더 등): `{path}`")
            st.stop()
        if path.suffix.lower() not in {".parquet", ".csv", ".xlsx", ".xls"}:
            st.error(f"❌ 지원 안 하는 형식: `{path.suffix}` (parquet/csv/xlsx 만)")
            st.stop()

        st.session_state["_last_local_path"] = str(path)
        source_display = path.name
        file_size_mb = path.stat().st_size / (1024 * 1024)

        size_label = f"{file_size_mb:.1f} MB" if file_size_mb < 1024 else f"{file_size_mb/1024:.2f} GB"
        st.success(f"✅ 파일 확인: `{path}` ({size_label})")

        # 대용량 경고
        if file_size_mb > 5_000:
            st.warning(
                f"⚠️ 매우 큰 파일 ({size_label}). 메모리 사용량 주의. "
                "RAM이 부족하면 Python 프로세스가 종료될 수 있어요. "
                "parquet 형식이면 csv/xlsx보다 5~10배 효율적."
            )

        try:
            with st.spinner(f"파일 읽는 중... ({source_display}, {size_label})"):
                df = _read_dataframe(path, path.name, nrows=None)
        except MemoryError:
            st.error(
                "❌ **메모리 부족** — 파일이 RAM을 초과합니다. "
                "parquet 형식으로 변환하거나 파일을 분할하세요."
            )
            st.stop()
        except Exception as e:
            st.error(f"❌ 파일 읽기 실패: **{type(e).__name__}**: {str(e)[:300]}")
            st.stop()

    # ── 공통: df 검증 + 세션 저장 ───────────────────────────────────────
    if df is None or df.empty:
        st.error("❌ 빈 DataFrame.")
        st.stop()

    n_rows = len(df)
    n_cols = len(df.columns)
    if on_cloud:
        st.caption(f"🟢 클라우드 · {file_size_mb:.1f} MB · {n_rows:,}행 × {n_cols}열")
    else:
        st.caption(f"💻 로컬 · {file_size_mb:,.1f} MB · 전체 {n_rows:,}행 × {n_cols}열 로드")

    _reset_session_for_new_file(source_display)
    st.session_state["raw_df"] = df
    st.success(f"✅ {n_rows:,}행 × {n_cols}열 로드 완료")

    with st.expander("미리보기 (상위 5행)", expanded=True):
        st.dataframe(df.head(5))

    if st.button("다음 → Schema Intelligence", type="primary"):
        go_to(2)
