"""
데이터 매핑 앱 — 원천 데이터를 회사 표준 레이아웃으로 변환

📂 위치: mapping_app/app.py
📋 역할:
    ① 원천 raw + 표준 레이아웃 파일 업로드
    ② 표준 컬럼 기준으로 raw 컬럼 자동 매핑(사용자 수정 가능)
    ③ 회사명 → ISIN/단축코드/시장 KRX 자동 매칭
    ④ 데이터 검증 (날짜·금액 컬럼 기반)
    ⑤ 표준 레이아웃 형식 그대로 변환·다운로드
🚀 실행: `🗂 데이터매핑 실행.command` 더블클릭 (포트 8502)

⚠️  매핑 앱은 '변환'만 책임진다. 분석은 데이터 분석 앱(8501)에서 수행한다.
"""
import sys
from pathlib import Path

# 프로젝트 루트(data-tool/)를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import io

import pandas as pd
import streamlit as st

from modules.mapping.column_mapper import (
    KIND_LABEL,
    auto_map,
    infer_column_kind,
    raw_metadata,
    read_standard_layout,
)
from modules.mapping.dart_lookup import (
    dart_summary,
    fetch_dart_corp_master,
    fetch_jurir_nos_batch,
    match_dart_companies,
)
from modules.mapping.lookup import (
    isin_compute_from_dart_match,
    isin_from_dart_match,
    load_user_master,
)
from modules.mapping.sources import (
    KIND_DEFAULT_VSRC,
    VIRTUAL_SOURCES,
    NO_MAP as _NO_MAP,
    is_translate_source,
    is_virtual,
    make_translate_source,
)
from modules.mapping import translation_db as _trans_db
from modules.mapping.translation import pipeline as _trans_pipeline

# ── Phase A 리팩토링: 공통 유틸 별도 모듈로 분리 ──────────────────────────────
# mapping_app.{ui_common, output, validation} 가 헬퍼 보유. 이하 alias 는 기존
# 호출처와의 호환을 위한 thin wrapper — 외부 시그니처는 그대로 유지.
from mapping_app import output as _out
from mapping_app import ui_common as _ui
from mapping_app import validation as _val
from mapping_app import master_builder as _master

# streamlit hot-reload 가 새 심볼을 못 잡을 때 강제 reload — 한 번만
def _ensure_master_fresh():
    """master_builder + dart_lookup 등 모듈에 신규 함수가 누락됐으면 reload."""
    needed = (
        "llm_classify_gics", "llm_find_parents", "llm_largest_shareholder",
        "save_ksic_gics_cache_rows", "save_subsidiary_cache_rows",
        "load_subsidiary_cache", "format_subsidiary_status",
        "apply_subsidiary_to_row", "build_list_only_xlsx",
        "resolve_shareholder_from_dart_master",
        "build_list_rows",   # 리팩토링 신규
    )
    import importlib
    if not all(hasattr(_master, n) for n in needed):
        globals()["_master"] = importlib.reload(_master)

    # dart_lookup 도 신규 시그니처 확인 (max_workers / listed_only_codes)
    from modules.mapping import dart_lookup as _dart_mod
    import inspect
    try:
        params = inspect.signature(_dart_mod.fetch_largest_shareholders_batch).parameters
        if "max_workers" not in params or "listed_only_codes" not in params:
            importlib.reload(_dart_mod)
    except (AttributeError, ValueError):
        importlib.reload(_dart_mod)


_ensure_master_fresh()

# ── 페이지 설정 ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="🗂 데이터 매핑 — Mandata",
    page_icon="🗂",
    layout="wide",
)

STEPS = [
    "파일 업로드",
    "컬럼 매핑",         # ② raw 키 매핑
    "DART 매칭",         # ③ 회사명 → corp_code · 영문명 · 단축코드
    "ISIN 매핑",         # ④ 단축코드 → KRX ISIN
    "브랜드·제품 영문화", # ⑤ KIPRIS + Claude LLM + 로마자 폴백 + 사용자 검수
    "최종 매핑",         # ⑥ 영문 변환·법인등록번호·ISIN 등 가상 소스 매핑
    "데이터 검증",       # ⑦
    "변환·다운로드",     # ⑧
]

# ── Information & List 모드의 step 흐름 ────────────────────────────────────
# 회사명 → DART/ISIN/GICS 자동 매핑 → LIST_PR 1-시트 xlsx 생성
MASTER_STEPS = [
    "회사 입력",        # M1
    "DART 매칭",        # M2
    "ISIN 산출",        # M3
    "GICS 매핑 검수",   # M4
    "xlsx 생성",        # M5
]

# 매핑이 권장되는 핵심 유형 (없어도 진행 가능, 다만 경고)
RECOMMENDED_KINDS = ["date", "amount"]


# ══════════════════════════════════════════════════════════════════════════════
# Alias — 모든 wrapper 를 한 곳에 모음 (분리 모듈 → 기존 호출처 호환)
# ══════════════════════════════════════════════════════════════════════════════
# ui_common (Streamlit UI · session)
go_to                        = _ui.go_to
_save_secret                 = _ui.save_secret
_reset_downstream            = _ui.reset_downstream
_kind_badge                  = _ui.kind_badge
_raw_col_for_kind            = _ui.raw_col_for_kind
_render_company_key_selector = _ui.render_company_key_selector
_trans_get_keys              = _ui.get_translation_keys

# output (표준 레이아웃 빌드 · XLSX/CSV)
_build_output_df  = _out.build_output_df
_df_to_xlsx_bytes = _out.df_to_xlsx_bytes
_df_to_csv_bytes  = _out.df_to_csv_bytes
_XLSX_MAX_ROWS    = _out.XLSX_MAX_ROWS


def render_stepper():
    """STEPS 를 전달하는 thin wrapper."""
    _ui.render_stepper(STEPS)


# ══════════════════════════════════════════════════════════════════════════════
# Step 1 — 파일 업로드 (raw + 표준 레이아웃)
# ══════════════════════════════════════════════════════════════════════════════

def render_step_upload():
    st.subheader("① 파일 업로드")
    st.caption(
        "왼쪽에는 데이터원천사가 보낸 **raw 데이터**, "
        "오른쪽에는 우리 회사 **표준 레이아웃** 파일을 올려주세요. "
        "표준 레이아웃은 헤더(컬럼명)만 있는 빈 양식이어도 됩니다."
    )

    c_left, c_right = st.columns(2)

    # ── (a) Raw 데이터 ────────────────────────────────────────────────────────
    with c_left:
        st.markdown("##### 📥 1) 원천 raw 데이터")
        up_raw = st.file_uploader(
            "raw 파일 (CSV / XLSX)",
            type=["csv", "xlsx", "xls"],
            key="up_raw_file",
        )
        if up_raw is not None:
            fid = f"raw__{up_raw.name}__{up_raw.size}"
            if st.session_state.get("file_id_raw") != fid:
                try:
                    if up_raw.name.lower().endswith(".csv"):
                        df = pd.read_csv(up_raw)
                    else:
                        df = pd.read_excel(up_raw)
                except Exception as e:
                    st.error(f"❌ raw 파일 읽기 실패: {e}")
                    st.stop()
                st.session_state["raw_df"]       = df
                st.session_state["file_id_raw"]  = fid
                st.session_state["file_name_raw"] = up_raw.name
                _reset_downstream(after_step=1)

        if "raw_df" in st.session_state:
            df = st.session_state["raw_df"]
            st.success(
                f"✅ **{st.session_state['file_name_raw']}** — "
                f"{len(df):,}행 × {len(df.columns)}열"
            )
            with st.expander("상위 5행 미리보기", expanded=False):
                st.dataframe(df.head(5), width="stretch")

    # ── (b) 표준 레이아웃 ──────────────────────────────────────────────────────
    with c_right:
        st.markdown("##### 📐 2) 회사 표준 레이아웃")
        up_std = st.file_uploader(
            "표준 레이아웃 파일 (CSV / XLSX)",
            type=["csv", "xlsx", "xls"],
            key="up_std_file",
        )
        if up_std is not None:
            fid = f"std__{up_std.name}__{up_std.size}"
            if st.session_state.get("file_id_std") != fid:
                try:
                    parse = read_standard_layout(up_std)
                except Exception as e:
                    st.error(f"❌ 표준 레이아웃 파일 읽기 실패: {e}")
                    st.session_state["std_parse_error"] = str(e)
                    parse = {"columns": [], "tried": []}

                # backward compatibility — 옛 버전은 list 반환했음
                if isinstance(parse, list):
                    parse = {"columns": parse, "sheet": None,
                             "header_row": 0, "tried": []}
                elif not isinstance(parse, dict):
                    parse = {"columns": [], "tried": []}

                cols = parse.get("columns", [])
                st.session_state["std_columns"]    = cols
                st.session_state["std_kinds"]      = [infer_column_kind(c) for c in cols]
                st.session_state["file_id_std"]    = fid
                st.session_state["file_name_std"]  = up_std.name
                st.session_state["std_parse_info"] = parse
                _reset_downstream(after_step=1)

        # 표준 컬럼 처리 결과 — 무조건 어떤 상태든 화면에 표시
        if "std_columns" in st.session_state or "std_parse_info" in st.session_state:
            cols  = st.session_state.get("std_columns", [])
            kinds = st.session_state.get("std_kinds", [])
            info  = st.session_state.get("std_parse_info", {})

            if cols:
                meta_parts = []
                if info.get("sheet"):
                    meta_parts.append(f"시트 `{info['sheet']}`")
                if info.get("header_row") is not None:
                    meta_parts.append(f"헤더 {info['header_row']+1}행")
                meta_str = " · ".join(meta_parts)
                st.success(
                    f"✅ **{st.session_state.get('file_name_std', '?')}** — "
                    f"{len(cols)}개 표준 컬럼 인식"
                    + (f"  ({meta_str})" if meta_str else "")
                )
                preview = pd.DataFrame({
                    "표준 컬럼": cols,
                    "추론된 유형": [KIND_LABEL.get(k, KIND_LABEL['text'])[0] for k in kinds],
                })
                with st.expander("표준 컬럼 목록 + 유형 추론", expanded=False):
                    st.dataframe(preview, width="stretch", hide_index=True)
            else:
                st.error(
                    "❌ 표준 레이아웃 파일에서 컬럼을 인식하지 못했습니다. "
                    "헤더(컬럼명)가 시트 어딘가에 있는지 아래 진단을 확인하세요."
                )
                # 진단 — 시도 로그 무조건 펼침
                tried = info.get("tried", [])
                with st.expander("🔧 진단 — 시도 로그 (어느 시트/행을 봤는지)", expanded=True):
                    if tried:
                        st.dataframe(
                            pd.DataFrame(tried),
                            width="stretch",
                            hide_index=True,
                        )
                    else:
                        st.write("(진단 로그가 비어있음 — read_standard_layout 호출 자체가 실패했거나 옛 캐시 코드가 돌고 있을 수 있습니다)")

                    # 파싱 에러가 있었으면 같이 표시
                    err = st.session_state.get("std_parse_error")
                    if err:
                        st.code(err, language="text")

                    # 메모리에 로드된 모듈 버전 확인 (옛 캐시 진단)
                    from modules.mapping import column_mapper as _cm
                    import inspect
                    src = inspect.getsource(_cm.read_standard_layout)
                    return_type_hint = "dict" if "-> dict" in src else "list" if "-> list" in src else "?"
                    st.caption(
                        f"현재 메모리의 `read_standard_layout` 반환 타입: **{return_type_hint}** "
                        f"(dict 가 정상. list 면 옛 버전이 캐시된 것 — streamlit 완전 재시작 필요)"
                    )

    # ── 진행 버튼 ─────────────────────────────────────────────────────────────
    ready_raw = "raw_df" in st.session_state
    ready_std = "std_columns" in st.session_state

    st.divider()
    _, _, right = st.columns([5, 1, 1])
    with right:
        if st.button(
            "다음 →",
            type="primary",
            disabled=not (ready_raw and ready_std),
            key="up_next",
            width="stretch",
        ):
            go_to(2)

    if not (ready_raw and ready_std):
        missing = []
        if not ready_raw: missing.append("raw 데이터")
        if not ready_std: missing.append("표준 레이아웃")
        st.info(f"⏳ {', '.join(missing)} 파일을 모두 업로드하면 다음 단계로 진행할 수 있습니다.")


# ══════════════════════════════════════════════════════════════════════════════
# Step 2 — 컬럼 매핑 (표준 ← raw 또는 가상 소스)
# ══════════════════════════════════════════════════════════════════════════════


def render_step_mapping():
    st.subheader("② 컬럼 매핑")
    st.caption(
        "표준 레이아웃의 각 컬럼에 어떤 raw 컬럼을 매핑할지 선택합니다. "
        "자동 추론값이 채워져 있으니 필요한 부분만 수정하세요."
    )

    if "raw_df" not in st.session_state or "std_columns" not in st.session_state:
        st.warning("먼저 ① 단계에서 raw + 표준 레이아웃 파일을 업로드하세요.")
        if st.button("← 업로드로"):
            go_to(1)
        st.stop()

    raw_df: pd.DataFrame = st.session_state["raw_df"]
    std_cols: list[str]  = st.session_state["std_columns"]
    std_kinds: list[str] = st.session_state["std_kinds"]
    raw_cols: list[str]  = [str(c) for c in raw_df.columns]

    # ── 자동 매핑 1회 (재실행 시 캐시) ────────────────────────────────────────
    auto_key = (tuple(std_cols), tuple(raw_cols))
    if st.session_state.get("auto_map_key") != auto_key:
        meta = raw_metadata(raw_df)
        st.session_state["raw_meta"]     = meta
        st.session_state["auto_map"]     = auto_map(std_cols, raw_cols, meta)
        st.session_state["auto_map_key"] = auto_key
        # 자동 매핑 결과로 초기값 세팅 — raw 컬럼만 (가상 소스는 ⑥에서 자동 적용)
        for entry in st.session_state["auto_map"]:
            k = f"std_to_raw__{entry['std_col']}"
            if k not in st.session_state:
                st.session_state[k] = entry["raw_col"] or _NO_MAP

    auto_results: list[dict] = st.session_state["auto_map"]
    raw_meta: dict[str, dict] = st.session_state.get("raw_meta", {})

    # ── 매핑 UI ───────────────────────────────────────────────────────────────
    st.markdown("#### 표준 컬럼 ← raw 컬럼")
    st.info(
        "📌 이 단계에서는 **raw 컬럼만** 매핑합니다. 회사명 raw 매핑이 곧 "
        "③ DART · ④ ISIN 매칭의 **키** 역할을 합니다.\n\n"
        "**최종 변환(회사명 영문화·법인등록번호·ISIN 채움 등)은 ⑥ 변환 단계에서 자동 적용**됩니다."
    )
    options = [_NO_MAP] + raw_cols   # 가상 소스는 ⑥ 에서 처리

    std_to_raw: dict[str, str] = {}
    # 헤더 — 샘플 컬럼 폭을 넉넉히
    header_cols = st.columns([2, 1, 2, 3])
    header_cols[0].markdown("**표준 컬럼**")
    header_cols[1].markdown("**유형**")
    header_cols[2].markdown("**raw 컬럼**")
    header_cols[3].markdown("**예시 값 · 사유**")

    for entry in auto_results:
        c1, c2, c3, c4 = st.columns([2, 1, 2, 3])
        std_col   = entry["std_col"]
        kind      = entry["kind"]
        suggested = entry["raw_col"]
        reason    = entry["reason"]
        score     = entry["score"]

        with c1:
            st.markdown(f"`{std_col}`")
        with c2:
            st.markdown(_kind_badge(kind), unsafe_allow_html=True)
        with c3:
            key = f"std_to_raw__{std_col}"
            current = st.session_state.get(key) or _NO_MAP
            if current not in options:
                current = _NO_MAP
            idx = options.index(current)
            chosen = st.selectbox(
                std_col,
                options=options,
                index=idx,
                key=key,
                label_visibility="collapsed",
            )
            if chosen != _NO_MAP:
                std_to_raw[std_col] = chosen
        with c4:
            # 가상 소스가 선택된 경우 안내
            if chosen in VIRTUAL_SOURCES:
                if chosen.startswith("[KRX]"):
                    src_origin = "KRX 매칭 결과"
                    next_step  = "③ ISIN 매칭"
                elif chosen.startswith("[DART]"):
                    src_origin = "DART 매칭 결과"
                    next_step  = "④ DART 매칭"
                else:  # [변환] ...
                    src_origin = "DART 영문명으로 변환"
                    next_step  = "④ DART 매칭 (실패 시 입력 한글 유지)"
                st.markdown(
                    f"<div style='font-size:12.5px;line-height:1.5;color:#374151;'>"
                    f"🔗 <b>{src_origin}</b> 에서 채움"
                    f"<br><span style='color:#6b7280;'>"
                    f"회사명 컬럼을 키로, {next_step} 단계에서 매칭된 값을 사용합니다."
                    f"</span></div>",
                    unsafe_allow_html=True,
                )
                if suggested:
                    st.caption(f"💡 raw 후보 `{suggested}` 가 있지만 가상 소스로 덮음 (selectbox에서 raw 선택 가능)")
            # 현재 선택된 raw 컬럼의 예시 값 + 메타 (raw 가 바뀔 때마다 자동 갱신)
            elif chosen != _NO_MAP and chosen in raw_df.columns:
                m = raw_meta.get(chosen, {})
                samples = m.get("samples") or []

                # ── Fallback: 캐시에 samples 없으면 raw_df 에서 직접 ───────────
                if not samples:
                    try:
                        col_s = raw_df[chosen].dropna()
                        # 큰 데이터에서도 빠르도록 head(50) 먼저 자르고 unique
                        if len(col_s) > 50:
                            col_s = col_s.head(50)
                        uniq = col_s.drop_duplicates().head(4)
                        samples = [str(v) for v in uniq.tolist()]
                    except Exception:
                        samples = []

                # 메타 정보도 raw_meta 가 비어있으면 즉시 계산
                dtype_str = m.get("dtype") or str(raw_df[chosen].dtype)
                if "n_unique" in m:
                    n_unique = m["n_unique"]
                else:
                    try:
                        n_unique = int(raw_df[chosen].nunique())
                    except Exception:
                        n_unique = -1
                null_pct = m.get("null_pct", 0.0)

                samples_str = " · ".join(
                    s if len(s) <= 18 else s[:15] + "…"
                    for s in samples[:4]
                ) or "(빈 값)"
                null_tail = f" · 결측 {null_pct:.1f}%" if null_pct > 0 else ""
                uniq_tail = f" · 고유 {n_unique:,}개" if n_unique >= 0 else ""
                meta_line = f"{dtype_str}{uniq_tail}{null_tail}"

                st.markdown(
                    f"<div style='font-size:12.5px;line-height:1.5;color:#374151;'>"
                    f"📋 <b>예:</b> {samples_str}"
                    f"<br><span style='color:#6b7280;'>{meta_line}</span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )
                # 자동 추론 사유 (사용자가 그대로 두면 정확, 바꿨으면 참고용)
                if chosen == suggested and reason and reason != "매칭 없음":
                    st.caption(f"💡 자동: {reason} (신뢰도 {score})")
                elif suggested and chosen != suggested:
                    st.caption(f"💡 자동 추천은 `{suggested}` — 사용자가 변경함")
                elif not suggested:
                    st.caption("💡 자동 매칭 후보 없음 (수동 선택)")
            else:
                # 매핑 안 함 — 자동 추천이 있었으면 안내
                if suggested:
                    sm = raw_meta.get(suggested, {})
                    sm_samples = sm.get("samples") or []
                    sm_str = " · ".join(s[:15] for s in sm_samples[:3])
                    st.caption(
                        f"💡 자동 추천: `{suggested}` "
                        f"(예: {sm_str or '?'}) — 사용자가 비활성화"
                    )
                else:
                    st.caption("자동 매칭 후보 없음")

    # ── 중복 사용 검사 ────────────────────────────────────────────────────────
    used_raw: dict[str, list[str]] = {}
    for std, raw in std_to_raw.items():
        used_raw.setdefault(raw, []).append(std)
    duplicates = {raw: stds for raw, stds in used_raw.items() if len(stds) > 1}

    if duplicates:
        for raw, stds in duplicates.items():
            st.warning(
                f"⚠️ raw 컬럼 **`{raw}`** 이 여러 표준 컬럼에 중복 매핑됨: "
                f"{', '.join(stds)} — 보통 1:1 로 하는 것이 좋습니다."
            )

    # ── 권장 유형 충족 여부 ──────────────────────────────────────────────────
    mapped_kinds = {
        std_kinds[std_cols.index(std)]
        for std in std_to_raw.keys()
        if std in std_cols
    }
    missing_rec = [k for k in RECOMMENDED_KINDS if k not in mapped_kinds]
    if missing_rec:
        labels = ", ".join(KIND_LABEL[k][0] for k in missing_rec)
        st.info(f"💡 권장 유형 누락: {labels} — 매핑하면 검증·변환 품질이 올라갑니다.")
    else:
        st.success("✅ 권장 유형(날짜·금액) 모두 매핑됨")

    # 가상 소스로 자동 채워질 표준 컬럼 안내 (사용자 안내용)
    auto_fill_kinds = {"isin","stock_code","corp_code","name_eng","company","brand"}
    auto_fill_std = [
        std for std, kind in zip(std_cols, std_kinds)
        if kind in auto_fill_kinds and std not in std_to_raw
    ]
    if auto_fill_std:
        st.caption(
            "ℹ️ raw 에 직접 매칭 없는 다음 표준 컬럼은 ⑥ 변환 단계에서 자동으로 "
            "**KRX/DART 매칭 결과**로 채워집니다: "
            + ", ".join(f"`{s}`" for s in auto_fill_std)
        )

    # 매핑 변경 시 다운스트림 캐시 무효화
    if st.session_state.get("std_to_raw") != std_to_raw:
        st.session_state["std_to_raw"] = std_to_raw
        _reset_downstream(after_step=2)

    # ── 진행 버튼 ─────────────────────────────────────────────────────────────
    st.divider()
    left, _, right = st.columns([1, 4, 1])
    with left:
        if st.button("← 이전", key="map_prev", width="stretch"):
            go_to(1)
    with right:
        if st.button(
            "다음 →",
            type="primary",
            disabled=(len(std_to_raw) == 0),
            key="map_next",
            width="stretch",
        ):
            go_to(3)


# ══════════════════════════════════════════════════════════════════════════════
# Step 3 — ISIN 매핑
# ══════════════════════════════════════════════════════════════════════════════

def render_step_isin():
    st.subheader("④ ISIN 매핑")
    st.caption(
        "③ DART 매칭에서 얻은 **법인등록번호 + 단축코드**를 기반으로 ISIN 을 생성합니다. "
        "방법을 먼저 선택하고 **🚀 매핑 시작** 버튼을 누르세요. "
        "자동 매칭이 안 된 항목은 표에서 **직접 입력**할 수 있습니다."
    )

    if "std_to_raw" not in st.session_state:
        st.warning("먼저 ② 컬럼 매핑을 완료하세요.")
        if st.button("← 매핑으로"):
            go_to(2)
        st.stop()

    raw_df     = st.session_state["raw_df"]
    dart_match = st.session_state.get("dart_match")

    # ── DART 매칭 결과가 없으면 안내 ──────────────────────────────────────────
    if dart_match is None or dart_match.empty:
        st.warning(
            "⚠️ ③ DART 매칭이 완료되지 않았습니다. ISIN 매핑은 DART 단축코드를 키로 "
            "사용하므로 먼저 ③ 을 완료해 주세요."
        )
        st.divider()
        left, _, right = st.columns([1, 4, 1])
        with left:
            if st.button("← DART 매칭으로", key="isin_back_no_dart",
                         width="stretch"):
                go_to(3)
        with right:
            if st.button("건너뛰기 →", type="primary",
                         key="isin_skip_no_dart", width="stretch"):
                st.session_state.pop("isin_match", None)
                go_to(5)
        st.stop()

    dart_info = st.session_state.get("dart_company_info") or {}

    # ── 매핑 방법 선택 ────────────────────────────────────────────────────────
    st.markdown("#### 매핑 방법")

    METHOD_AUTO = "자동 산출 (ISO 6166) — DART 단축코드에서 ISIN 직접 계산  ✨ 권장"
    METHOD_FILE = "사용자 매핑 파일 — 회사명·종목코드·ISIN xlsx/csv 업로드"

    method = st.radio(
        "ISIN 을 어떻게 채울지 선택하세요",
        options=[METHOD_AUTO, METHOD_FILE],
        index=0,
        key="isin_method",
        label_visibility="collapsed",
    )

    # 사용자 파일 모드면 업로더 노출
    user_master = None
    if method == METHOD_FILE:
        with st.container(border=True):
            up_master = st.file_uploader(
                "📁 매핑 파일 업로드 (헤더: name/회사명, stock_code/종목코드, isin)",
                type=["csv","xlsx","xls"], key="user_master_file",
            )
            if up_master is not None:
                try:
                    user_master = load_user_master(up_master)
                    st.success(f"✅ {len(user_master):,}개 종목 로드됨")
                    with st.expander("미리보기 (상위 5행)"):
                        st.dataframe(user_master.head(5), width="stretch", hide_index=True)
                except Exception as e:
                    st.error(f"❌ 매핑 파일 읽기 실패: {e}")
    elif method == METHOD_AUTO:
        st.caption(
            "ℹ️ ISO 6166 알고리즘으로 `KR7 + 단축코드 + 00 + check_digit` 12자리 생성. "
            "**보통주는 100% 정확**합니다. 우선주·신주·펀드는 분류 코드(`00`)가 다를 수 있어 직접 수정 권장."
        )

    # ── 매핑 시작 버튼 ────────────────────────────────────────────────────────
    can_start = (method == METHOD_AUTO) or (method == METHOD_FILE and user_master is not None)

    st.divider()
    btn_col, msg_col = st.columns([1, 3])
    with btn_col:
        if st.button("🚀 ISIN 매핑 시작", type="primary", disabled=not can_start,
                     width="stretch", key="isin_start"):
            with st.spinner(f"{len(dart_match):,}개 회사 ISIN 처리 중…"):
                if method == METHOD_AUTO:
                    st.session_state["isin_match"] = isin_compute_from_dart_match(
                        dart_match, dart_company_info=dart_info
                    )
                else:
                    st.session_state["isin_match"] = isin_from_dart_match(
                        dart_match, user_master, dart_company_info=dart_info
                    )
            st.session_state["isin_match_method"] = method
            st.rerun()
    with msg_col:
        if not can_start and method == METHOD_FILE:
            st.caption("📂 위에서 매핑 파일을 먼저 업로드하세요.")

    # ── 결과 (있을 때만) ──────────────────────────────────────────────────────
    if "isin_match" in st.session_state and st.session_state["isin_match"] is not None:
        try:
            _render_isin_result(dart_match, dart_info)
        except Exception as e:
            # 결과 렌더링이 실패해도 다음 진행 버튼은 보여야 함
            st.error(f"결과 표시 중 오류: {type(e).__name__}: {e}")
            with st.expander("상세", expanded=False):
                import traceback
                st.code(traceback.format_exc(), language="text")

    # ── 진행 버튼 (결과 렌더링 성공 여부와 무관하게 항상 노출) ────────────────
    st.divider()
    left, _, right = st.columns([1, 4, 1])
    with left:
        if st.button("← 이전", key="isin_prev", width="stretch"):
            go_to(3)
    with right:
        if st.button("다음 →", type="primary", key="isin_next", width="stretch"):
            go_to(5)


def _render_isin_result(dart_match: pd.DataFrame, dart_info: dict):
    """ISIN 매핑 결과 표시 + 수동 입력 UI. render_step_isin 내부에서 호출."""
    # 수동 입력 override 병합
    match_df = st.session_state["isin_match"]
    manual: dict[str, str] = st.session_state.get("isin_manual_override", {}) or {}
    if manual:
        match_df = match_df.copy()
        for i, row in match_df.iterrows():
            inp = row["input_name"]
            if inp in manual and manual[inp].strip():
                match_df.at[i, "isin"]   = manual[inp].strip()
                match_df.at[i, "status"] = "manual"
                match_df.at[i, "source"] = "수동 입력"
        st.session_state["isin_match"] = match_df

    n_total     = len(match_df)
    n_with_isin = int((match_df["isin"].astype(str).str.len() > 0).sum())
    n_manual    = int((match_df["source"] == "수동 입력").sum())
    n_no_stock  = int((match_df["source"] == "DART (비상장)").sum())
    n_failed    = int((match_df["source"] == "DART 매칭 실패").sum())

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("입력 회사", f"{n_total:,}")
    m2.metric("✅ ISIN 채워짐", f"{n_with_isin:,}",
              delta=f"{(n_with_isin/n_total*100 if n_total else 0):.1f}%",
              delta_color="off")
    m3.metric("✏️ 수동 입력", f"{n_manual:,}")
    m4.metric("비상장", f"{n_no_stock:,}")
    m5.metric("DART 실패", f"{n_failed:,}")

    st.markdown("#### 매칭 결과 (법인등록번호 기준)")
    badge = {"exact":"✅ 매핑","manual":"✏️ 수동","none":"❌ 없음"}
    display_df = match_df.assign(상태=match_df["status"].map(badge).fillna(match_df["status"])).rename(columns={
        "input_name":   "입력 회사명",
        "jurir_no":     "법인등록번호",
        "matched_name": "한글명",
        "stock_code":   "단축코드",
        "isin":         "ISIN",
        "market":       "시장",
        "source":       "출처",
    })[["상태","법인등록번호","입력 회사명","한글명","단축코드","ISIN","시장","출처"]]
    st.dataframe(display_df, width="stretch", hide_index=True, height=420)

    # 수동 입력 UI — expander 로 감싸 기본 접힘, 100개 초과 시 일부만 노출
    failed_rows = match_df[match_df["isin"].astype(str).str.len() == 0]
    if not failed_rows.empty:
        n_failed = len(failed_rows)
        MANUAL_LIMIT = 50

        with st.expander(
            f"✏️ 수동 ISIN 입력 — {n_failed}개 미매칭 (펼쳐서 직접 입력)",
            expanded=(n_failed <= 10),   # 적을 때만 자동 펼침
        ):
            if n_failed > MANUAL_LIMIT:
                st.caption(
                    f"⚠️ 미매칭이 {n_failed}개로 많아 **처음 {MANUAL_LIMIT}개만** 입력 UI에 노출합니다. "
                    "보통 비상장 법인은 ISIN 이 없으므로 빈 채로 두어도 됩니다."
                )
                shown = failed_rows.head(MANUAL_LIMIT)
            else:
                st.caption(
                    "자동 매칭 안 된 회사에 ISIN 을 직접 입력하세요. "
                    "비워두면 빈 ISIN 으로 출력됩니다. ISIN 은 보통 `KR7` 로 시작하는 12자리 문자열."
                )
                shown = failed_rows

            manual_inputs: dict[str, str] = {}
            cols = st.columns(2)
            for idx, (_, row) in enumerate(shown.iterrows()):
                inp = row["input_name"]
                with cols[idx % 2]:
                    label = f"`{inp}`"
                    if row.get("jurir_no"):
                        label += f"  ·  법인 `{row['jurir_no']}`"
                    if row.get("source"):
                        label += f"  ·  _{row['source']}_"
                    prev = manual.get(inp, "")
                    val = st.text_input(
                        label, value=prev,
                        key=f"isin_manual__{inp}",
                        placeholder="KR7005930003", max_chars=12,
                    )
                    manual_inputs[inp] = val

            # 펼친 행에 대한 변경만 반영
            new_override = dict(manual)   # 기존 보존
            for k, v in manual_inputs.items():
                if v.strip():
                    new_override[k] = v.strip()
                elif k in new_override:
                    del new_override[k]

            if new_override != manual:
                st.session_state["isin_manual_override"] = new_override
                st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# Step 3 — DART 매칭 (법인등록번호 + 영문회사명 + 단축코드)
# ══════════════════════════════════════════════════════════════════════════════

def render_step_dart():
    st.subheader("③ DART 매칭")
    st.caption(
        "DART 공시정보로 회사명 → **법인등록번호 + 영문회사명 + 단축코드** 자동 매칭. "
        "회사명 키 컬럼을 정하고 **🚀 DART 매칭 시작** 버튼을 누르면 실행됩니다."
    )

    if "std_to_raw" not in st.session_state:
        st.warning("먼저 ② 컬럼 매핑을 완료하세요.")
        if st.button("← 매핑으로"):
            go_to(2)
        st.stop()

    raw_df = st.session_state["raw_df"]

    # 회사명 매칭 키 컬럼 선택
    comp_col = _render_company_key_selector("dart")

    if not comp_col:
        st.warning(
            "⚠️ 회사명으로 사용할 raw 컬럼을 선택해야 DART 매칭을 진행할 수 있습니다."
        )
        left, _, right = st.columns([1, 4, 1])
        with left:
            if st.button("← 이전", key="dart_back_no_comp", width="stretch"):
                go_to(2)
        with right:
            if st.button("건너뛰기 →", type="primary", key="dart_skip", width="stretch"):
                st.session_state.pop("dart_match", None)
                go_to(4)
        st.stop()

    # ── DART API 인증키 ──────────────────────────────────────────────────────
    try:
        dart_secret = st.secrets.get("DART_API_KEY", "")
    except Exception:
        dart_secret = ""
    default_key = st.session_state.get("dart_api_key", dart_secret) or ""

    with st.expander(
        "🔑 DART Open API 인증키 " + ("(secrets.toml 에서 자동 로드됨)" if dart_secret else "(필수)"),
        expanded=not bool(default_key),
    ):
        st.caption(
            "DART Open API (https://opendart.fss.or.kr) 에서 발급받은 인증키를 넣어주세요. "
            "오른쪽 **🔒 영구 저장** 버튼을 누르면 `secrets.toml` 에 기록되어 "
            "다음 실행부터 자동 로드됩니다."
        )
        cin, csave = st.columns([4, 1])
        with cin:
            key_input = st.text_input(
                "DART_API_KEY",
                value=default_key,
                type="password",
                key="dart_api_input",
                label_visibility="collapsed",
                placeholder="발급받은 DART 인증키 붙여넣기",
            )
        with csave:
            st.write("")
            if st.button("🔒 영구 저장", key="dart_save_key", width="stretch",
                         disabled=not key_input.strip(),
                         help="현재 입력된 키를 mapping_app/.streamlit/secrets.toml 에 저장"):
                try:
                    p = _save_secret("DART_API_KEY", key_input.strip())
                    st.success(f"✅ `{p.relative_to(Path(__file__).resolve().parent)}` 에 저장됨. "
                               f"다음 실행부터 자동 로드됩니다.")
                    st.caption("⚠️ git 사용 중이면 `.gitignore` 에 `.streamlit/secrets.toml` 을 꼭 추가하세요.")
                except Exception as e:
                    st.error(f"저장 실패: {e}")
        if key_input != st.session_state.get("dart_api_key", ""):
            st.session_state["dart_api_key"] = key_input
            fetch_dart_corp_master.clear()

    api_key = (st.session_state.get("dart_api_key") or "").strip()
    if not api_key:
        st.info("📝 DART 인증키를 입력하면 자동 매칭이 시작됩니다. 인증키가 없으면 건너뛰고 다음 단계로 진행할 수 있어요.")
        left, _, right = st.columns([1, 4, 1])
        with left:
            if st.button("← 이전", key="dart_back_no_key", width="stretch"):
                go_to(2)
        with right:
            if st.button("건너뛰기 →", type="primary", key="dart_skip_no_key", width="stretch"):
                st.session_state.pop("dart_match", None)
                go_to(4)
        st.stop()

    # ── 주요 액션 버튼 (한 줄에) ──────────────────────────────────────────────
    has_match = st.session_state.get("dart_match") is not None
    matched_cc = sorted({
        cc for cc in (st.session_state.get("dart_match", pd.DataFrame())
                        .get("corp_code", pd.Series([], dtype=str))
                        .astype(str).tolist())
        if cc
    }) if has_match else []
    info_map_now = st.session_state.get("dart_company_info", {})
    n_jurir_done = sum(1 for cc in matched_cc if cc in info_map_now)
    n_jurir_left = len(matched_cc) - n_jurir_done

    st.markdown("#### ① 매칭")
    a1, a2 = st.columns(2)
    with a1:
        do_match = st.button(
            "🚀 DART 매칭 시작" if not has_match else "🔁 매칭 다시 실행",
            type="primary" if not has_match else "secondary",
            width="stretch",
            key="dart_btn_match",
            help="회사명 → corp_code/한글정식명/영문명/단축코드 일괄 매칭",
        )
    with a2:
        do_refresh = st.button(
            "🔄 마스터 재다운로드",
            width="stretch",
            key="dart_btn_refresh",
            help="DART corpCode 캐시 무효화 후 다시 받기",
        )
    # do_jurir 는 동명 후보 선택 UI 뒤(②)에서 노출 — 기본값 False 로 초기화.
    do_jurir = False

    # ── 액션 처리 ────────────────────────────────────────────────────────────
    if do_refresh:
        fetch_dart_corp_master.clear()
        st.session_state.pop("dart_match", None)
        st.session_state.pop("dart_match_key", None)
        st.rerun()

    if do_match:
        try:
            with st.spinner("DART 마스터 다운로드 중…"):
                dart_master = fetch_dart_corp_master(api_key)
        except Exception as e:
            st.error("❌ DART 마스터를 가져오지 못했습니다.")
            with st.expander("상세 에러", expanded=True):
                st.code(f"{type(e).__name__}: {e}", language="text")
            st.stop()
        unique_names = sorted({
            str(x).strip() for x in raw_df[comp_col].dropna() if str(x).strip()
        })
        with st.spinner(f"{len(unique_names):,}개 회사 DART 매칭 중…"):
            st.session_state["dart_match"]     = match_dart_companies(unique_names, dart_master)
            st.session_state["dart_match_key"] = (comp_col, len(unique_names),
                                                    tuple(unique_names[:10]), len(dart_master))
        st.rerun()

    # do_jurir 핸들러는 후보 선택 UI 뒤(②)에서 처리 — 버튼이 거기서 정의되므로
    # 시각적 위치와 클릭 처리 순서를 맞추기 위해 아래로 이동.

    # ── 매칭 안 됐으면 안내 ───────────────────────────────────────────────────
    if not has_match:
        st.info("📌 위 **🚀 DART 매칭 시작** 버튼을 눌러 매칭을 진행하세요.")
        st.divider()
        left, _, right = st.columns([1, 4, 1])
        with left:
            if st.button("← 이전", key="dart_prev_pre", width="stretch"):
                go_to(2)
        with right:
            if st.button("건너뛰기 →", type="primary", key="dart_skip_pre", width="stretch"):
                go_to(4)
        st.stop()

    # ── 매칭 결과 ────────────────────────────────────────────────────────────
    match_df = st.session_state["dart_match"]
    summary  = dart_summary(match_df)

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("입력 회사 수", f"{summary['total']:,}")
    m2.metric("✅ 완전 매칭", f"{summary['exact']:,}")
    m3.metric("🟡 부분 매칭", f"{summary['partial']:,}")
    m4.metric("❓ 동명 후보", f"{summary['ambiguous']:,}",
              help="후보가 2개 이상 — 아래에서 정확한 회사를 선택")
    m5.metric("❌ 매칭 실패", f"{summary['none']:,}",
              delta=f"{summary['rate']*100:.1f}% 커버", delta_color="off")

    # ── 동명 회사 후보 선택 UI ────────────────────────────────────────────────
    ambiguous_rows = match_df[match_df["n_candidates"] > 1]
    if not ambiguous_rows.empty:
        st.markdown("#### ❓ 동명 회사 후보 — 어느 회사인지 선택")
        st.caption(
            "DART에 같은(또는 비슷한) 이름의 회사가 여러 개입니다. "
            "각 입력 회사명에 대해 정확한 한 곳을 선택해 주세요."
        )
        # session 에 사용자 선택 보존: dart_pick[input_name] = corp_code
        picks: dict = st.session_state.setdefault("dart_pick", {})

        for _, row in ambiguous_rows.iterrows():
            inp = row["input_name"]
            cands = row["candidates"]
            options_labels = [
                f"{c['corp_code']}  ·  {c['corp_name']} "
                f"{('('+c['corp_name_eng']+')') if c.get('corp_name_eng') else ''} "
                f"{'📈상장' if c.get('stock_code') else '비상장'}"
                for c in cands
            ]
            label_to_idx = {l: i for i, l in enumerate(options_labels)}

            # 기존 선택값
            existing_cc = picks.get(inp, row["corp_code"])
            default_idx = next(
                (i for i, c in enumerate(cands) if c.get("corp_code") == existing_cc),
                0,
            )

            # 큰 회사명 라벨 — selectbox label 은 collapsed 처리
            st.markdown(
                f"<div style='margin-top:14px;margin-bottom:4px;font-size:18px;"
                f"font-weight:700;color:#111827;'>"
                f"🏢 <span style='color:#1e40af;'>{inp}</span> "
                f"<span style='font-size:13px;color:#6b7280;font-weight:500;'>"
                f"— {len(cands)}개 후보</span></div>",
                unsafe_allow_html=True,
            )
            chosen_label = st.selectbox(
                f"`{inp}` 후보 선택",
                options=options_labels,
                index=default_idx,
                key=f"dart_pick__{inp}",
                label_visibility="collapsed",
            )
            chosen_cand = cands[label_to_idx[chosen_label]]
            picks[inp] = chosen_cand.get("corp_code", "")

            # match_df 의 primary 컬럼들도 사용자 선택으로 덮어쓰기
            mask = match_df["input_name"] == inp
            match_df.loc[mask, "corp_code"]     = chosen_cand.get("corp_code", "")
            match_df.loc[mask, "corp_name"]     = chosen_cand.get("corp_name", "")
            match_df.loc[mask, "corp_name_eng"] = chosen_cand.get("corp_name_eng", "")
            match_df.loc[mask, "stock_code"]    = chosen_cand.get("stock_code", "")
        st.session_state["dart_match"] = match_df

    # 동명 후보 선택이 끝났을 수 있으므로, 변경된 corp_code 기준으로 재계산
    matched_cc = sorted({
        str(cc) for cc in match_df["corp_code"].astype(str).tolist() if cc
    })
    n_jurir_done = sum(1 for cc in matched_cc if cc in info_map_now)
    n_jurir_left = len(matched_cc) - n_jurir_done

    # ── ② 법인등록번호 조회 ──────────────────────────────────────────────────
    st.markdown("#### ② 법인등록번호 조회")
    if not ambiguous_rows.empty:
        st.caption(
            "위에서 **회사 선택을 마친 뒤** 실행하세요. "
            "선택된 corp_code 기준으로 DART 회사 상세를 호출해 **법인등록번호(jurir_no)** 를 가져옵니다."
        )
    else:
        st.caption(
            "매칭된 corp_code 기준으로 DART 회사 상세를 호출해 "
            "**법인등록번호(jurir_no)** 를 가져옵니다."
        )
    jc1, jc2 = st.columns([1, 3])
    with jc1:
        do_jurir = st.button(
            f"📡 조회 시작 ({n_jurir_left}개 남음)" if n_jurir_left else "📡 조회 완료",
            type="primary" if n_jurir_left else "secondary",
            width="stretch",
            key="dart_btn_jurir",
            disabled=n_jurir_left == 0,
            help="매칭된 회사들의 jurir_no(법인등록번호) 조회 — 동명 후보 선택 후 실행",
        )
    with jc2:
        if n_jurir_left == 0 and matched_cc:
            st.success(f"✅ {len(matched_cc)}개 회사 법인등록번호 조회 완료")
        elif n_jurir_done:
            st.caption(
                f"진행: **{n_jurir_done} / {len(matched_cc)}** 완료 · "
                f"남은 {n_jurir_left}개 조회 가능"
            )
        else:
            st.caption(f"매칭된 {len(matched_cc)}개 회사 모두 미조회 — 위 버튼으로 시작")

    # 조회 버튼 클릭 처리
    if do_jurir and matched_cc:
        remaining = [cc for cc in matched_cc if cc not in info_map_now]
        prog = st.progress(0.0, text="조회 중…")
        results = dict(info_map_now)

        def _cb(i: int, total: int, info: dict):
            prog.progress(i / total, text=f"{i} / {total} — {info.get('corp_name', '')}")

        results.update(fetch_jurir_nos_batch(api_key, remaining, progress_callback=_cb))
        st.session_state["dart_company_info"] = results
        prog.empty()
        st.rerun()

    # ── 매칭 결과 테이블 ──────────────────────────────────────────────────────
    st.markdown("#### ③ 매칭 결과")
    info_map = st.session_state.get("dart_company_info", {})
    badge_map = {"exact": "✅ 완전", "partial": "🟡 부분", "none": "❌ 실패"}
    rows_view = []
    for _, r in match_df.iterrows():
        cc = r["corp_code"]
        jurir = (info_map.get(cc) or {}).get("jurir_no", "")
        rows_view.append({
            "상태":         badge_map.get(r["status"], r["status"]),
            "입력 회사명":   r["input_name"],
            "DART 한글명":   r["corp_name"],
            "영문명":       r["corp_name_eng"],
            "법인등록번호":  jurir or "(미조회)",
            "corp_code":   cc,
            "단축코드":     r["stock_code"],
            "후보":         r["n_candidates"],
        })
    display_df = pd.DataFrame(rows_view)
    if not display_df.empty:
        display_df = display_df.sort_values(
            by="상태",
            key=lambda s: s.map({"❌ 실패": 0, "🟡 부분": 1, "✅ 완전": 2}),
        ).reset_index(drop=True)
    st.dataframe(display_df, width="stretch", hide_index=True, height=420)

    # ── 진행 버튼 ─────────────────────────────────────────────────────────────
    st.divider()
    left, _, right = st.columns([1, 4, 1])
    with left:
        if st.button("← 이전", key="dart_prev", width="stretch"):
            go_to(2)
    with right:
        if st.button("다음 →", type="primary", key="dart_next", width="stretch"):
            go_to(4)


# ══════════════════════════════════════════════════════════════════════════════
# Step 5 — 브랜드·제품 영문화 (KIPRIS + LLM + 로마자 폴백 + 사용자 검수)
# ══════════════════════════════════════════════════════════════════════════════

def _trans_nav(suffix: str = "top"):
    """⑤ 영문화 step 상/하단에 공통으로 두는 navigation row."""
    left, _, right = st.columns([1, 4, 1])
    with left:
        if st.button("← 이전 (④ ISIN)", key=f"trans_prev_{suffix}", width="stretch"):
            go_to(4)
    with right:
        if st.button("다음 → (⑥ 최종 매핑)", type="primary",
                     key=f"trans_next_{suffix}", width="stretch"):
            go_to(6)


def render_step_brand_product_en():
    st.subheader("⑤ 브랜드·제품 영문화 + 자유 영문화")
    st.caption(
        "raw 데이터의 **브랜드·제품(SKU)** 은 KIPRIS·LLM·로마자로, "
        "**카테고리·섹터·지역 등 임의 컬럼**은 LLM 자유 영문화로 매핑합니다. "
        "확정한 영문명은 SQLite 에 저장돼 다음 raw 처리에 자동 사용돼요."
    )

    if "std_to_raw" not in st.session_state:
        st.warning("먼저 ② 컬럼 매핑을 완료하세요.")
        if st.button("← 매핑으로"):
            go_to(2)
        st.stop()

    raw_df: pd.DataFrame = st.session_state["raw_df"]

    # raw 컬럼에서 브랜드/제품 추론 (카테고리는 자유 영문화 섹션에서 multiselect)
    brand_col = _raw_col_for_kind("brand")
    sku_col   = _raw_col_for_kind("sku")

    # 매뉴얼 override 도 제공 (사용자가 다른 컬럼 선택 가능)
    raw_cols = [str(c) for c in raw_df.columns]

    with st.expander(
        "🔑 브랜드/제품 raw 키 컬럼",
        expanded=not (brand_col or sku_col),
    ):
        c1, c2 = st.columns(2)
        with c1:
            brand_pick = st.selectbox(
                "브랜드 raw 컬럼",
                options=[""] + raw_cols,
                index=(raw_cols.index(brand_col) + 1) if brand_col in raw_cols else 0,
                key="trans_brand_col",
            )
            brand_col = brand_pick or None
        with c2:
            sku_pick = st.selectbox(
                "제품(SKU) raw 컬럼",
                options=[""] + raw_cols,
                index=(raw_cols.index(sku_col) + 1) if sku_col in raw_cols else 0,
                key="trans_sku_col",
            )
            sku_col = sku_pick or None

    # ⑤ 의 사용자 선택을 ⑧ 변환에서 그대로 쓰도록 명시 저장 — 자동 추론 우회.
    st.session_state["mapping_brand_key"] = brand_col
    st.session_state["mapping_sku_key"]   = sku_col
    # 카테고리 단일 키는 더 이상 사용하지 않음 (자유 영문화에서 multiselect 사용)
    st.session_state.pop("mapping_category_key", None)

    kipris_key, anthropic_key = _trans_get_keys()

    # anthropic SDK 설치 여부 체크
    try:
        import anthropic as _anthropic_check   # noqa: F401
        sdk_ok = True
    except ImportError:
        sdk_ok = False

    badges = []
    badges.append("🔐 KIPRIS ✓" if kipris_key else "⚠️ KIPRIS 키 없음")
    if anthropic_key:
        badges.append("🤖 Claude ✓" if sdk_ok else "⚠️ Claude 키 OK / **SDK 미설치**")
    else:
        badges.append("⚠️ Claude 키 없음")
    badges.append("📦 SQLite ✓")
    st.caption(" · ".join(badges))

    # ── SDK 미설치 안내 ──────────────────────────────────────────────────────
    if anthropic_key and not sdk_ok:
        st.error(
            "❌ **anthropic Python SDK 가 설치되어 있지 않습니다.** "
            "LLM 폴백이 동작하지 않아 외래어(예: 버드와이저)는 로마자 표기(`Beodeuwaijeo`) 로만 채워집니다.\n\n"
            "**터미널에서 한 번만 실행하세요:**\n\n"
            "```bash\npip install anthropic --break-system-packages\n```\n\n"
            "설치 후 매핑 앱을 재시작하면 Claude 호출이 활성화됩니다."
        )

    # ── 환경 진단 expander (1회 테스트 호출) ─────────────────────────────────
    with st.expander("🧪 환경 진단 — KIPRIS / Claude 한 번 테스트 호출", expanded=False):
        st.caption("'버드와이저' 로 KIPRIS · Claude 를 각각 한 번 호출해 결과를 보여줍니다.")
        if st.button("진단 실행", key="trans_diag"):
            test_name = "버드와이저"
            # KIPRIS
            if kipris_key:
                try:
                    from modules.mapping.translation import kipris as _kp
                    krx_results = _kp.lookup_brand_en(test_name, kipris_key)
                    st.write(f"**KIPRIS** ({len(krx_results)}개): "
                             + (", ".join(r["candidate_en"] for r in krx_results) or "결과 없음"))
                except Exception as e:
                    st.error(f"KIPRIS 호출 실패: {type(e).__name__}: {e}")
            else:
                st.caption("KIPRIS 키 없음 — skip")
            # LLM
            if anthropic_key:
                try:
                    from modules.mapping.translation import llm as _llm
                    r = _llm.llm_translate_brand(test_name, anthropic_key)
                    if r:
                        st.write(f"**Claude**: `{r['candidate_en']}` (conf={r['confidence']:.2f})")
                    else:
                        st.warning("Claude 호출은 됐지만 응답 파싱 실패")
                except Exception as e:
                    st.error(f"Claude 호출 실패: {type(e).__name__}: {e}")
            else:
                st.caption("Claude 키 없음 — skip")
            # Romanizer
            from modules.mapping.translation import romanizer as _rom
            st.write(f"**Romanizer**: `{_rom.romanize_brand(test_name)}`")

    # unique 값 추출 헬퍼 (자유 영문화 섹션 등에서도 재사용)
    def _unique_from(col):
        if not col:
            return []
        return sorted({
            str(x).strip() for x in raw_df[col].dropna() if str(x).strip()
        })
    unique_brands = _unique_from(brand_col)
    unique_skus   = _unique_from(sku_col)

    # ── 상단 네비게이션 (검수 항목이 많아도 빠른 이동) ──────────────────────────
    _trans_nav("top")

    # 브랜드/제품 키도 없고 자유 영문화 컬럼도 없으면 건너뛰기 안내만
    if not (brand_col or sku_col) and not st.session_state.get("free_trans_cols"):
        st.info(
            "브랜드/제품 컬럼을 라벨링하거나, 아래 **🌐 LLM 자유 영문화** 에 "
            "카테고리·섹터·지역 등 임의 컬럼을 추가하세요. 건너뛰어도 됩니다."
        )

    m1, m2, m3 = st.columns(3)
    m1.metric("입력 브랜드", f"{len(unique_brands):,}")
    m2.metric("입력 제품(SKU)", f"{len(unique_skus):,}")
    db_stats = _trans_db.stats()
    m3.metric(
        "DB 확정 (브랜드/제품/카테고리 공용)",
        f"{db_stats['brand_confirmed']:,} / "
        f"{db_stats['product_confirmed']:,} / "
        f"{db_stats.get('category_confirmed', 0):,}",
    )

    # ── 브랜드/제품 영문화 실행 ─────────────────────────────────────────────
    st.markdown("#### ① 브랜드 · 제품 영문화")
    a1, a2 = st.columns(2)
    with a1:
        do_brand = st.button(
            f"🚀 브랜드 영문화 ({len(unique_brands)}개)",
            type="primary",
            disabled=(len(unique_brands) == 0),
            width="stretch",
            key="trans_btn_brand",
        )
    with a2:
        do_sku = st.button(
            f"🚀 제품 영문화 ({len(unique_skus)}개)",
            type="primary",
            disabled=(len(unique_skus) == 0),
            width="stretch",
            key="trans_btn_sku",
        )

    if do_brand and unique_brands:
        prog = st.progress(0.0, text="브랜드 배치 영문화 중…")
        def _cb(i, total, name):
            prog.progress(i / max(1, total), text=f"{i}/{total} — {name}")
        try:
            with st.spinner(
                f"Claude 배치 호출 + KIPRIS 병렬 ({len(unique_brands)}개)…"
            ):
                _trans_pipeline.collect_brands_batch(
                    unique_brands,
                    kipris_key=kipris_key,
                    llm_key=anthropic_key,
                    skip_confirmed=True,
                    progress_callback=_cb,
                )
        except Exception as e:
            st.error(f"⚠️ 배치 실행 실패: {type(e).__name__}: {e}")
        prog.empty()
        err = _trans_pipeline.get_last_llm_error("brand")
        if err:
            st.error(f"⚠️ Claude LLM 호출 문제 — `{err}`")
        st.success(f"✅ {len(unique_brands)}개 브랜드 처리 완료 (이미 확정된 항목 자동 skip)")
        st.rerun()

    if do_sku and unique_skus:
        prog = st.progress(0.0, text="제품 배치 영문화 중…")
        def _cb_sku(i, total, name):
            prog.progress(i / max(1, total), text=f"{i}/{total} — {name}")
        try:
            with st.spinner(
                f"Claude 배치 호출 ({len(unique_skus)}개 제품, 50개씩 묶음 × 4 워커 병렬)…"
            ):
                _trans_pipeline.collect_products_batch(
                    unique_skus,
                    known_brands_kr=unique_brands,
                    llm_key=anthropic_key,
                    skip_confirmed=True,
                    progress_callback=_cb_sku,
                )
        except Exception as e:
            st.error(f"⚠️ 배치 실행 실패: {type(e).__name__}: {e}")
        prog.empty()
        err = _trans_pipeline.get_last_llm_error("product")
        if err:
            st.error(
                f"⚠️ Claude LLM 호출 문제 — `{err}`\n\n"
                "이 상태에서는 LLM 결과가 없어 후보가 로마자/규칙기반만 남습니다. "
                "위 `🧪 환경 진단` expander 에서 한 번 테스트해 보세요."
            )
        st.success(f"✅ {len(unique_skus)}개 제품 처리 완료 (이미 확정된 항목 자동 skip)")
        st.rerun()

    # ══════════════════════════════════════════════════════════════════════════
    # ② 🌐 LLM 자유 영문화 — 브랜드/제품 아래 위치 (대/중/소 분류·섹터·지역·등)
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("#### ② 🌐 LLM 자유 영문화")
    st.caption(
        "카테고리·대/중/소 분류·섹터·지역·채널 등 raw 컬럼을 자유롭게 추가해 영문 매핑합니다. "
        "각 컬럼의 한글 고유값이 영문으로 변환되어 SQLite `category` 테이블에 공용 저장되고, "
        "⑥ 최종 매핑에서 **[번역::컬럼명]** 가상 소스로 선택할 수 있어요."
    )

    used_for_trans = {c for c in (brand_col, sku_col) if c}
    free_candidate_cols = [c for c in raw_cols if c not in used_for_trans]
    prev_free = st.session_state.get("free_trans_cols", [])
    prev_free = [c for c in prev_free if c in free_candidate_cols]

    free_picks = st.multiselect(
        "자유 영문화 대상 raw 컬럼",
        options=free_candidate_cols,
        default=prev_free,
        key="trans_free_cols_select",
        help="브랜드/제품으로 잡힌 컬럼은 옵션에서 제외됩니다.",
    )
    st.session_state["free_trans_cols"] = free_picks

    free_unique: dict[str, list[str]] = {}
    if free_picks:
        preview_rows = []
        for col in free_picks:
            uniq = _unique_from(col)
            free_unique[col] = uniq
            preview_rows.append({
                "raw 컬럼": col,
                "고유 한글값": f"{len(uniq):,}개",
                "예시": " · ".join(uniq[:3]) + ("…" if len(uniq) > 3 else ""),
            })
        st.dataframe(pd.DataFrame(preview_rows), width="stretch", hide_index=True)

        total_free = sum(len(v) for v in free_unique.values())
        run_free = st.button(
            f"🚀 선택한 {len(free_picks)}개 컬럼 일괄 영문화 (총 {total_free:,}개 한글값)",
            type="primary",
            width="stretch",
            key="trans_btn_free",
            disabled=(total_free == 0),
        )
        if run_free:
            all_unique = sorted({v for vs in free_unique.values() for v in vs})
            prog = st.progress(0.0, text="자유 영문화 중…")
            def _cb_free(i, total, name):
                prog.progress(i / max(1, total), text=f"{i}/{total} — {name}")
            try:
                with st.spinner(
                    f"Claude 배치 호출 ({len(all_unique):,}개 unique 값, "
                    f"80개씩 묶음 × 4 워커 병렬)…"
                ):
                    _trans_pipeline.collect_categories_batch(
                        all_unique,
                        llm_key=anthropic_key,
                        skip_confirmed=True,
                        progress_callback=_cb_free,
                    )
            except Exception as e:
                st.error(f"⚠️ 자유 영문화 실행 실패: {type(e).__name__}: {e}")
            prog.empty()
            err = _trans_pipeline.get_last_llm_error("category")
            if err:
                st.error(f"⚠️ Claude LLM 호출 문제 — `{err}`")
            st.success(
                f"✅ {len(all_unique):,}개 한글값 처리 완료 (이미 확정된 값 자동 skip)"
            )
            st.rerun()
    else:
        st.caption("자유 영문화할 컬럼이 없어요. 대/중/소 분류 등 한글 컬럼을 추가하세요.")

    # ── 일괄 작업 ────────────────────────────────────────────────────────────
    if unique_brands or unique_skus or free_picks:
        st.markdown("#### ③ 일괄 작업")
        bb1, bb2, bb3, bb4, bb5 = st.columns(5)
        with bb1:
            if st.button("🎯 일괄 확정 (브랜드)", width="stretch",
                         key="trans_bulk_brand",
                         disabled=not unique_brands):
                n = _trans_db.bulk_select_top("brand", reviewer="auto")
                st.success(f"✅ 브랜드 {n}개 자동 확정 — 1순위 후보로")
                st.rerun()
        with bb2:
            if st.button("🎯 일괄 확정 (제품)", width="stretch",
                         key="trans_bulk_sku",
                         disabled=not unique_skus):
                n = _trans_db.bulk_select_top("product", reviewer="auto")
                st.success(f"✅ 제품 {n}개 자동 확정 — 1순위 후보로")
                st.rerun()
        with bb3:
            if st.button("🎯 일괄 확정 (자유 영문화)", width="stretch",
                         key="trans_bulk_free",
                         disabled=not free_picks,
                         help="multiselect 한 모든 자유 영문화 컬럼의 1순위 후보를 일괄 확정"):
                n = _trans_db.bulk_select_top("category", reviewer="auto")
                st.success(f"✅ 자유 영문화 {n}개 자동 확정 — 1순위 후보로")
                st.rerun()
        with bb4:
            if st.button("🧹 옛 규칙기반·한글 후보 정리", width="stretch",
                         key="trans_purge_partial",
                         help="규칙기반(official_site) 후보 + 한글 섞인 후보를 모두 삭제하고 "
                              "해당 항목 확정도 해제합니다. 이후 🚀 영문화 다시 실행 권장."):
                r1 = _trans_db.purge_partial_korean_candidates("product")
                r2 = _trans_db.purge_partial_korean_candidates("brand")
                r3 = _trans_db.purge_partial_korean_candidates("category")
                total_del = r1["candidates_deleted"] + r2["candidates_deleted"] + r3["candidates_deleted"]
                total_unc = r1["entities_unconfirmed"] + r2["entities_unconfirmed"] + r3["entities_unconfirmed"]
                st.success(
                    f"✅ 옛 후보 {total_del}건 삭제 · 확정 {total_unc}건 해제.\n\n"
                    "**다음 단계**: 🚀 제품 영문화 다시 실행 → 🎯 일괄 확정 (제품)"
                )
                st.rerun()
        with bb5:
            # 마스터 다운로드 — 확정된 영문 매핑 xlsx (3시트)
            try:
                bdf = _trans_db.export_master_dict("brand")
                pdf = _trans_db.export_master_dict("product")
                cdf = _trans_db.export_master_dict("category")
                buf = io.BytesIO()
                def _write_sheet(w, df, name, empty_msg):
                    if df is None or df.empty:
                        pd.DataFrame({"info": [empty_msg]}).to_excel(
                            w, sheet_name=name, index=False)
                    else:
                        df.to_excel(w, sheet_name=name, index=False)
                with pd.ExcelWriter(buf, engine="openpyxl") as w:
                    _write_sheet(w, bdf, "brand",    "(no confirmed brand)")
                    _write_sheet(w, pdf, "product",  "(no confirmed product)")
                    _write_sheet(w, cdf, "category", "(no confirmed category)")
                st.download_button(
                    "📥 마스터 xlsx 다운로드",
                    data=buf.getvalue(),
                    file_name="brand_product_category_master.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    width="stretch",
                    key="trans_master_dl",
                    help="확정된 영문 매핑을 회사 표준 사전 파일로 export (3시트)",
                )
            except Exception as e:
                st.caption(f"마스터 export 실패: {e}")

        st.caption(
            "💡 1순위 일괄 확정 후 개별 항목에서 다른 후보를 선택해 변경 가능합니다. "
            "확정된 영문명은 모두 **언더바 형식** (예: Samsung_Electronics) 으로 저장됩니다."
        )

    # ── 검수 UI ──────────────────────────────────────────────────────────────
    if unique_brands:
        st.markdown("#### ④ 브랜드 검수")
        _render_translation_review("brand", unique_brands)

    if unique_skus:
        st.markdown("#### ⑤ 제품(SKU) 검수")
        _render_translation_review("product", unique_skus)

    # 자유 영문화 컬럼별 검수 — 각 expander 헤더에 1순위 일괄 확정 버튼
    free_cols = st.session_state.get("free_trans_cols", [])
    if free_cols:
        st.markdown("#### ⑥ 자유 영문화 검수 (대/중/소 분류 등)")
        for col in free_cols:
            uniq = _unique_from(col)
            if not uniq:
                continue
            with st.expander(
                f"📂 `{col}` — {len(uniq)}개 고유값",
                expanded=(len(uniq) <= 10),
            ):
                if st.button(
                    f"🎯 `{col}` 1순위 일괄 확정",
                    key=f"trans_bulk_free_col_{col}",
                    help=f"{col} 의 모든 한글값에 대해 1순위 후보를 자동 확정",
                ):
                    n = _trans_db.bulk_select_top("category", reviewer="auto")
                    st.success(f"✅ category 풀 전체 {n}개 자동 확정 (`{col}` 포함)")
                    st.rerun()
                _render_translation_review("category", uniq, key_suffix=f"free_{col}")

    # ── 하단 진행 버튼 ───────────────────────────────────────────────────────
    st.divider()
    _trans_nav("bottom")


def _render_translation_review(
    entity_type: str,
    names: list[str],
    key_suffix: str = "",
):
    """후보 검수 — 항목별로 후보 selectbox + 수동 입력.

    key_suffix: widget key 충돌 방지용. 같은 entity_type 으로 여러 그룹을 그릴 때
                (예: 자유 영문화 컬럼별 검수) 호출자가 고유 접미사를 넘긴다.
    """
    if not names:
        return

    ns = f"{entity_type}{('_' + key_suffix) if key_suffix else ''}"

    # 페이지네이션 — 한 화면에 50개씩
    PAGE = 50
    page_key = f"trans_page_{ns}"
    cur_page = st.session_state.get(page_key, 1)
    total_pages = max(1, (len(names) + PAGE - 1) // PAGE)

    if total_pages > 1:
        cur_page = st.number_input(
            f"페이지 ({ns}, 총 {total_pages}쪽)",
            min_value=1, max_value=total_pages, value=cur_page,
            step=1, key=f"trans_page_input_{ns}",
        )
        st.session_state[page_key] = cur_page

    start = (cur_page - 1) * PAGE
    end   = min(start + PAGE, len(names))
    shown = names[start:end]

    # entity_type → en column name 매핑
    _EN_COL = {
        "brand":    "name_en",
        "product":  "name_en_assembled",
        "category": "name_en",
    }
    _TABLE_OF = {
        "brand":    "brand",
        "product":  "product",
        "category": "category",
    }

    def _get_entity(name_kr: str):
        """entity_type 에 맞는 row 1건 조회. 없으면 None."""
        with _trans_db.connect() as _c:
            row = _c.execute(
                f"SELECT * FROM {_TABLE_OF[entity_type]} WHERE name_kr = ?",
                (name_kr,),
            ).fetchone()
            return dict(row) if row else None

    for name_kr in shown:
        # DB 에서 후보 가져오기
        entity = _get_entity(name_kr)
        if not entity:
            # 아직 후보 수집 안 됨
            st.caption(f"`{name_kr}` — 후보 미수집 (위 🚀 버튼 실행 필요)")
            continue

        candidates = _trans_db.list_candidates(entity_type, entity["id"])
        if not candidates:
            continue

        # 현재 확정된 영문명
        en_col = _EN_COL[entity_type]
        confirmed = entity.get(en_col) or ""

        with st.container(border=True):
            c1, c2 = st.columns([2, 3])
            with c1:
                st.markdown(f"**`{name_kr}`**")
                if confirmed:
                    st.markdown(f"✅ 확정: **{confirmed}**")
            with c2:
                # 후보 selectbox
                options = [
                    f"{c['candidate_en']}  ·  [{c['source']}]  conf={c['confidence']:.2f}"
                    + ("  ⭐" if c["is_selected"] else "")
                    for c in candidates
                ] + ["✏️ 직접 입력…"]

                default_idx = next(
                    (i for i, c in enumerate(candidates) if c["is_selected"]),
                    0,
                )
                pick = st.selectbox(
                    "후보",
                    options=options,
                    index=default_idx,
                    key=f"trans_pick_{ns}_{entity['id']}",
                    label_visibility="collapsed",
                )

                if pick == "✏️ 직접 입력…":
                    manual = st.text_input(
                        "직접 입력 영문명 (공백 자동 _ 변환)",
                        value=confirmed,
                        key=f"trans_manual_{ns}_{entity['id']}",
                        label_visibility="collapsed",
                    )
                    if st.button(
                        "💾 저장",
                        key=f"trans_save_manual_{ns}_{entity['id']}",
                    ):
                        if manual.strip():
                            cid = _trans_db.add_candidate(
                                entity_type, entity["id"], manual.strip(),
                                "manual", 1.0,
                            )
                            _trans_db.select_candidate(cid, reviewer="user")
                            st.rerun()
                else:
                    chosen_idx = options.index(pick)
                    chosen_cand = candidates[chosen_idx]
                    # 미확정이거나, 다른 후보 선택 시 — 확정 버튼 노출
                    cur_selected_id = next(
                        (c["id"] for c in candidates if c["is_selected"]),
                        None,
                    )
                    if cur_selected_id != chosen_cand["id"]:
                        btn_label = ("✅ 이 후보로 변경"
                                     if cur_selected_id else "✅ 이 후보 확정")
                        if st.button(
                            btn_label,
                            key=f"trans_confirm_{ns}_{chosen_cand['id']}",
                        ):
                            _trans_db.select_candidate(
                                chosen_cand["id"], reviewer="user"
                            )
                            st.rerun()
                    # 확정된 경우 — 확정 취소 옵션도 제공
                    if confirmed and cur_selected_id == chosen_cand["id"]:
                        if st.button(
                            "↩️ 확정 취소",
                            key=f"trans_unconfirm_{ns}_{entity['id']}",
                        ):
                            # 해당 엔티티 모든 선택 해제 + brand/product/category 영문 null
                            with _trans_db.connect() as _c:
                                _c.execute(
                                    "UPDATE name_candidate SET is_selected = 0, "
                                    "reviewer = NULL, reviewed_at = NULL "
                                    "WHERE entity_type = ? AND entity_id = ?",
                                    (entity_type, entity["id"]),
                                )
                                table = _TABLE_OF[entity_type]
                                en_c  = _EN_COL[entity_type]
                                _c.execute(
                                    f"UPDATE {table} SET {en_c} = NULL WHERE id = ?",
                                    (entity["id"],),
                                )
                            st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# Step 6 — 최종 매핑 (영문 변환·법인등록번호·ISIN 등 가상 소스 적용)
# ══════════════════════════════════════════════════════════════════════════════

def render_step_final_mapping():
    st.subheader("⑥ 최종 매핑")
    st.caption(
        "각 표준 컬럼의 최종 값을 어디서 가져올지 확정합니다. "
        "**회사명/ISIN/법인등록번호 등은 자동으로 KRX·DART 변환 결과(가상 소스)로 채워집니다.** "
        "raw 값을 그대로 쓰고 싶으면 selectbox에서 raw 컬럼을 선택하세요."
    )

    if "std_to_raw" not in st.session_state:
        st.warning("먼저 ② 컬럼 매핑을 완료하세요.")
        if st.button("← 매핑으로", key="final_back_no_map"):
            go_to(2)
        st.stop()

    raw_df    = st.session_state["raw_df"]
    std_cols  = st.session_state["std_columns"]
    std_kinds = st.session_state["std_kinds"]
    base_map  = dict(st.session_state.get("std_to_raw", {}))   # ②의 raw 매핑

    raw_cols = [str(c) for c in raw_df.columns]

    # ⑤ 에서 등록한 자유 영문화 컬럼들 → [번역::col] 동적 가상 소스
    free_trans_cols = [
        c for c in st.session_state.get("free_trans_cols", []) if c in raw_cols
    ]
    free_trans_vsrcs = [make_translate_source(c) for c in free_trans_cols]

    options = [_NO_MAP] + raw_cols + VIRTUAL_SOURCES + free_trans_vsrcs

    auto_fill_kinds = set(KIND_DEFAULT_VSRC.keys())
    final_map: dict[str, str] = {}

    # 매칭 상태 안내
    has_dart = st.session_state.get("dart_match") is not None
    has_isin = st.session_state.get("isin_match") is not None
    badges = []
    badges.append("✅ DART 매칭 완료" if has_dart else "⚠️ DART 미완료 — ③ 단계를 거치는 게 좋아요")
    badges.append("✅ ISIN 매핑 완료" if has_isin else "⚠️ ISIN 미완료 — ④ 단계를 거치는 게 좋아요")
    st.caption("  ·  ".join(badges))

    st.markdown("#### 표준 컬럼 ← 최종 소스")

    from modules.mapping.column_mapper import KIND_LABEL
    grid = st.columns(2)
    for i, std in enumerate(std_cols):
        with grid[i % 2]:
            kind = std_kinds[i]
            key = f"final_pick__{std}"
            # 첫 진입 default 결정
            if key not in st.session_state:
                if kind in auto_fill_kinds:
                    st.session_state[key] = KIND_DEFAULT_VSRC[kind]
                else:
                    st.session_state[key] = base_map.get(std, _NO_MAP) or _NO_MAP

            current = st.session_state[key]
            if current not in options:
                current = _NO_MAP
            idx = options.index(current)
            kind_label = KIND_LABEL.get(kind, KIND_LABEL["text"])[0]
            chosen = st.selectbox(
                f"`{std}` · {kind_label}",
                options=options,
                index=idx,
                key=key,
            )
            # 옵션별 안내
            if chosen.startswith("[변환]"):
                st.caption("🌐 DART 영문명 우선, 매칭 실패 시 raw 한글 유지")
            elif chosen.startswith("[번역::"):
                st.caption(
                    f"🌐 ⑤ 자유 영문화에서 등록한 raw 컬럼의 한글값을 영문으로 치환"
                )
            elif chosen.startswith("[번역]"):
                st.caption("🌐 ⑤ 영문화 파이프라인 결과 (브랜드/제품/카테고리)")
            elif chosen.startswith("[KRX]"):
                st.caption("📈 ④ ISIN 매칭 결과에서 채움")
            elif chosen.startswith("[DART]"):
                st.caption("🏛 ③ DART 매칭 결과에서 채움")
            elif chosen == _NO_MAP:
                pass
            else:
                # raw 컬럼 선택 — 샘플 미리보기
                try:
                    sample = (
                        raw_df[chosen].dropna().drop_duplicates().head(3)
                        .astype(str).tolist()
                    )
                    st.caption(f"📋 raw 예: {' · '.join(sample) or '(빈 값)'}")
                except Exception:
                    pass
            if chosen != _NO_MAP:
                final_map[std] = chosen

    # 최종 매핑 저장
    if st.session_state.get("final_std_to_raw") != final_map:
        st.session_state["final_std_to_raw"] = final_map
        st.session_state.pop("validation", None)

    # ── 가상 소스 사용 현황 요약 ──────────────────────────────────────────────
    vsrc_used = [(std, v) for std, v in final_map.items() if is_virtual(v)]
    if vsrc_used:
        st.markdown("#### 적용될 가상 소스 변환")
        for std, vsrc in vsrc_used:
            st.markdown(f"- `{std}` ← **{vsrc}**")

    # ── 진행 버튼 ─────────────────────────────────────────────────────────────
    st.divider()
    left, _, right = st.columns([1, 4, 1])
    with left:
        if st.button("← 이전", key="final_prev", width="stretch"):
            go_to(5)
    with right:
        if st.button("다음 →", type="primary", key="final_next", width="stretch"):
            go_to(7)


# ══════════════════════════════════════════════════════════════════════════════
# Step 6 — 데이터 검증 (kind=date/amount 기반)
# ══════════════════════════════════════════════════════════════════════════════

def _parse_date_series(s: pd.Series) -> pd.Series:
    """YYYYMMDD 정수 / 'YYYY-MM-DD' / datetime 등 다양한 형식을 안전하게 파싱.

    ⚠️ `pd.to_datetime(20240101)` 처럼 정수를 그대로 넣으면 epoch 이후 ns 로 해석돼
       1970년대 날짜가 나오는 버그. 정수형이면 문자열 → format='%Y%m%d' 로 명시.
    """
    # 본문은 mapping_app.validation.parse_date_series 로 이전됨.
    return _val.parse_date_series(s)


def _simple_validate(raw_df: pd.DataFrame) -> dict:
    """date/amount kind 컬럼 자동 추론 후 mapping_app.validation 호출."""
    return _val.simple_validate(
        raw_df,
        date_col=_raw_col_for_kind("date"),
        amount_col=_raw_col_for_kind("amount"),
    )


def render_step_validation():
    st.subheader("⑦ 데이터 검증")
    st.caption("매핑된 날짜·금액 컬럼을 기준으로 결측·파싱·중복을 점검합니다.")

    if "std_to_raw" not in st.session_state:
        st.warning("먼저 ② 컬럼 매핑을 완료하세요.")
        if st.button("← 매핑으로"):
            go_to(2)
        st.stop()

    raw_df = st.session_state["raw_df"]
    if "validation" not in st.session_state:
        st.session_state["validation"] = _simple_validate(raw_df)
    val = st.session_state["validation"]

    ICON = {"critical": "🔴", "error": "🟠", "warning": "🟡", "info": "🔵", "ok": "✅"}
    BG   = {"critical": "#fef2f2", "error": "#fff7ed", "warning": "#fefce8",
            "info": "#eff6ff", "ok": "#f0fdf4"}

    for chk in val["checks"]:
        sev = chk["severity"]
        st.markdown(
            f"<div style='background:{BG[sev]};border-radius:8px;padding:10px 16px;"
            f"margin-bottom:6px;font-size:14px'>"
            f"{ICON[sev]} &nbsp; <b>{chk['label']}</b> &nbsp;·&nbsp; {chk['detail']}"
            f"</div>",
            unsafe_allow_html=True,
        )

    st.divider()
    left, _, right = st.columns([1, 4, 1])
    with left:
        if st.button("← 이전", key="val_prev", width="stretch"):
            go_to(6)
    with right:
        if st.button("다음 →", type="primary", key="val_next", width="stretch"):
            go_to(8)


# ══════════════════════════════════════════════════════════════════════════════
# Step 8 — 변환 & 다운로드
# ══════════════════════════════════════════════════════════════════════════════

def render_step_download():
    st.subheader("⑧ 변환 & 다운로드")
    st.caption(
        "업로드한 **표준 레이아웃의 컬럼 순서·이름 그대로** 변환됩니다. "
        "ISIN·DART 매칭 결과는 회사명 기준 left-join 으로 자동 합쳐집니다."
    )

    if "std_to_raw" not in st.session_state:
        st.warning("먼저 ② 컬럼 매핑을 완료하세요.")
        if st.button("← 매핑으로"):
            go_to(2)
        st.stop()

    raw_df:     pd.DataFrame      = st.session_state["raw_df"]
    std_cols:   list[str]         = st.session_state["std_columns"]
    std_kinds:  list[str]         = st.session_state["std_kinds"]
    isin_match: pd.DataFrame|None = st.session_state.get("isin_match")
    dart_match: pd.DataFrame|None = st.session_state.get("dart_match")
    dart_info:  dict              = st.session_state.get("dart_company_info") or {}

    # ⑤ 최종 매핑 단계에서 결정된 매핑을 그대로 사용 (없으면 ② raw 매핑 폴백)
    std_to_raw: dict[str, str] = dict(
        st.session_state.get("final_std_to_raw")
        or st.session_state.get("std_to_raw", {})
    )

    if not st.session_state.get("final_std_to_raw"):
        st.info(
            "ℹ️ ⑤ 최종 매핑 단계를 거치지 않아 ②의 raw 매핑이 그대로 사용됩니다. "
            "회사명을 영문으로 변환하거나 ISIN·법인등록번호를 채우려면 ← 이전 으로 가서 "
            "⑤ 최종 매핑 단계를 거쳐 주세요."
        )

    # 표준 컬럼에 매핑된 raw 컬럼 (가상 소스는 제외)
    mapped_originals = {
        v for v in std_to_raw.values()
        if v in raw_df.columns
    }
    unmapped_cols = [c for c in raw_df.columns if c not in mapped_originals]

    extra_keep: list[str] = []
    if unmapped_cols:
        st.markdown("#### 추가로 포함할 raw 컬럼")
        st.caption("표준 레이아웃에 없지만 출력에 그대로 포함하려는 raw 컬럼을 선택하세요.")
        prev_keep = set(st.session_state.get("extra_keep", []))
        cols = st.columns(3)
        for i, c in enumerate(unmapped_cols):
            with cols[i % 3]:
                if st.checkbox(c, value=(c in prev_keep), key=f"keep_{c}"):
                    extra_keep.append(c)
        st.session_state["extra_keep"] = extra_keep
    else:
        st.info("모든 원본 컬럼이 표준 컬럼에 매핑되었습니다.")

    # ── 변환 시작 버튼 ────────────────────────────────────────────────────────
    # 6백만 행 같은 대용량은 변환에 시간 걸리므로 자동 실행하지 않고 사용자 트리거
    config_key = (
        tuple(std_to_raw.items()),
        tuple(extra_keep),
        id(isin_match), id(dart_match),
        st.session_state.get("comp_col_override"),
        st.session_state.get("mapping_brand_key"),
        st.session_state.get("mapping_sku_key"),
        st.session_state.get("mapping_category_key"),
    )

    is_built = (
        st.session_state.get("out_df_ready")
        and st.session_state.get("out_df_config") == config_key
    )

    # ── 변환 실행 전 미리보기 ────────────────────────────────────────────────
    # 어떤 std 컬럼이 어떤 소스(raw / 가상)에서 어떤 값을 받게 되는지 표로 미리 보여줌.
    # 행 수 늘리지 않고 raw_df 상위 3행만 가공해 샘플 출력 — 가벼움.
    st.markdown("#### 최종 레이아웃 미리보기 (변환 실행 전)")
    st.caption(
        "각 표준 컬럼이 어떤 소스에서 어떤 값을 받을지 미리 확인하세요. "
        "**잘못된 매핑이면 ← 이전(⑥)에서 수정**한 뒤 변환 실행해 주세요."
    )
    try:
        sample_raw = raw_df.head(3)
        sample_out = _build_output_df(
            sample_raw, std_cols, std_kinds, std_to_raw, extra_keep,
            isin_match=isin_match,
            dart_match=dart_match,
            dart_company_info=dart_info,
            company_override=st.session_state.get("comp_col_override"),
            brand_key_col=st.session_state.get("mapping_brand_key"),
            sku_key_col=st.session_state.get("mapping_sku_key"),
            category_key_col=st.session_state.get("mapping_category_key"),
        )
        # 매핑 요약표: std 컬럼 / kind / 소스 / 예시값
        kind_labels = {
            "company":"🏢 회사", "brand":"🏷 브랜드", "sku":"📦 제품",
            "category":"🗂 카테고리", "isin":"📈 ISIN", "stock_code":"📊 단축코드",
            "corp_code":"🆔 corp_code", "name_eng":"🌐 영문회사명",
            "date":"📅 날짜", "amount":"💰 금액", "count":"🔢 수량",
            "text":"📝 텍스트",
        }
        rows = []
        for std, kind in zip(std_cols, std_kinds):
            src = std_to_raw.get(std, "")
            if not src:
                src_disp = "— (매핑 안 함) —"
            elif src in raw_df.columns:
                src_disp = f"raw: {src}"
            elif is_translate_source(src):
                src_disp = f"🌐 자유 영문화: {src}"
            elif src in VIRTUAL_SOURCES:
                src_disp = src
            else:
                src_disp = src
            if std in sample_out.columns:
                vals = [str(v) for v in sample_out[std].head(3).tolist()]
                sample_str = " · ".join(v if len(v) <= 25 else v[:22]+"…" for v in vals)
            else:
                sample_str = "(빈 값)"
            rows.append({
                "표준 컬럼": std,
                "유형":     kind_labels.get(kind, kind),
                "소스":     src_disp,
                "예시 (상위 3행)": sample_str or "(빈 값)",
            })
        preview_df = pd.DataFrame(rows)
        st.dataframe(preview_df, width="stretch", hide_index=True)
    except Exception as e:
        st.warning(f"⚠️ 미리보기 생성 실패 (변환은 그래도 시도 가능): {type(e).__name__}: {e}")

    st.markdown("#### 변환 실행")
    bcol, mcol = st.columns([1, 3])
    with bcol:
        do_build = st.button(
            "🛠 변환 결과 생성" if not is_built else "🔁 다시 생성",
            type="primary" if not is_built else "secondary",
            width="stretch",
            key="dl_btn_build",
            help="표준 레이아웃 데이터를 생성합니다. 대용량은 수초~수십초 소요될 수 있어요.",
        )
    with mcol:
        if is_built:
            n_rows = len(st.session_state["out_df_cache"])
            st.caption(f"✅ 변환 완료 — {n_rows:,}행. 아래에서 다운로드하세요.")
        else:
            st.caption("📌 위 미리보기를 확인한 뒤, 위 버튼을 눌러 전체 변환을 시작하세요.")

    if do_build:
        with st.spinner(f"변환 중 ({len(raw_df):,}행)…"):
            try:
                out_df = _build_output_df(
                    raw_df, std_cols, std_kinds, std_to_raw, extra_keep,
                    isin_match=isin_match,
                    dart_match=dart_match,
                    dart_company_info=dart_info,
                    company_override=st.session_state.get("comp_col_override"),
                    brand_key_col=st.session_state.get("mapping_brand_key"),
                    sku_key_col=st.session_state.get("mapping_sku_key"),
                    category_key_col=st.session_state.get("mapping_category_key"),
                )
                st.session_state["out_df_cache"]  = out_df
                st.session_state["out_df_config"] = config_key
                st.session_state["out_df_ready"]  = True
                # 새 변환이 완료되면 직렬화 캐시는 무효화
                for k in ("csv_bytes_cache", "xlsx_bytes_cache", "xlsx_truncated_cache"):
                    st.session_state.pop(k, None)
            except Exception as e:
                st.error(f"❌ 변환 실패: {type(e).__name__}: {e}")
                import traceback
                with st.expander("상세 traceback"):
                    st.code(traceback.format_exc(), language="text")
                st.session_state["out_df_ready"] = False
        st.rerun()

    # ── 변환 결과 미리보기 + 다운로드 ─────────────────────────────────────────
    if is_built:
        out_df: pd.DataFrame = st.session_state["out_df_cache"]
        n_rows = len(out_df)

        st.divider()
        st.markdown("#### 변환 결과 미리보기")

        def _filled_for_vsrc(vsrc: str) -> int:
            matched_std = [s for s, v in std_to_raw.items() if v == vsrc]
            if not matched_std:
                return -1
            col = matched_std[0]
            if col not in out_df.columns:
                return -1
            return int((out_df[col].astype(str).str.len() > 0).sum())

        c1, c2, c3 = st.columns(3)
        c1.metric("출력 행 수", f"{n_rows:,}")
        c2.metric("출력 열 수", f"{len(out_df.columns):,}")
        c3.metric("표준 / 추가", f"{len(std_cols)} / {len(extra_keep)}")

        # 자유 번역 가상 소스도 metric 에 포함
        all_vsrcs_used = sorted({
            v for v in std_to_raw.values()
            if v in VIRTUAL_SOURCES or is_translate_source(v)
        })
        vsrc_metrics: list[tuple[str, int]] = []
        for vsrc in all_vsrcs_used:
            n = _filled_for_vsrc(vsrc)
            if n >= 0:
                vsrc_metrics.append((vsrc, n))
        if vsrc_metrics:
            st.caption("**가상 소스 채움 현황** (사용자가 매핑한 표준 컬럼만)")
            cols = st.columns(min(4, len(vsrc_metrics)))
            for i, (vsrc, n) in enumerate(vsrc_metrics):
                short = vsrc.replace("[KRX] ", "🔐").replace("[DART] ", "🏛")
                cols[i % len(cols)].metric(short, f"{n:,} / {n_rows:,}")

        st.dataframe(out_df.head(20), width="stretch")

        # ── 다운로드 ──────────────────────────────────────────────────────────
        st.markdown("#### 다운로드")
        if n_rows > _XLSX_MAX_ROWS - 1:
            st.info(
                f"💡 출력 행 수가 **{n_rows:,}** 개입니다. "
                f"**전체 행은 CSV** 로만 받을 수 있어요 (XLSX 는 1,048,575행 제한)."
            )

        base_name = Path(st.session_state.get("file_name_raw", "output")).stem + "__mapped"

        # ── lazy 직렬화 — 사용자가 명시 버튼 클릭 시에만 생성 ──────────────────
        # 매 rerun 마다 무거운 CSV/XLSX 생성이 자동 실행되지 않도록 분리.
        csv_bytes      = st.session_state.get("csv_bytes_cache")
        xlsx_bytes     = st.session_state.get("xlsx_bytes_cache")
        xlsx_truncated = st.session_state.get("xlsx_truncated_cache", False)

        st.markdown("##### 1) 파일 생성")
        g1, g2 = st.columns(2)
        with g1:
            do_make_csv = st.button(
                "📦 CSV 파일 생성 (전체 행, 권장)" if csv_bytes is None
                else f"✅ CSV 생성됨 ({len(csv_bytes)/1024/1024:.1f} MB)",
                type="primary" if csv_bytes is None else "secondary",
                width="stretch",
                key="dl_make_csv",
                disabled=csv_bytes is not None,
            )
        with g2:
            xlsx_will_truncate = n_rows > _XLSX_MAX_ROWS - 1
            do_make_xlsx = st.button(
                ("📦 XLSX 파일 생성"
                 + (" (1M행 잘림)" if xlsx_will_truncate else "")
                 if xlsx_bytes is None
                 else f"✅ XLSX 생성됨 ({len(xlsx_bytes)/1024/1024:.1f} MB)"),
                type="secondary",
                width="stretch",
                key="dl_make_xlsx",
                disabled=xlsx_bytes is not None,
            )

        if do_make_csv:
            try:
                with st.spinner("CSV 변환 중… (대용량은 수십초 소요)"):
                    csv_bytes = _df_to_csv_bytes(out_df)
                st.session_state["csv_bytes_cache"] = csv_bytes
                st.rerun()
            except Exception as e:
                st.error(f"CSV 변환 실패: {type(e).__name__}: {e}")

        if do_make_xlsx:
            try:
                with st.spinner("XLSX 변환 중… (대용량은 수십초 소요)"):
                    xlsx_bytes, xlsx_truncated = _df_to_xlsx_bytes(out_df)
                st.session_state["xlsx_bytes_cache"]     = xlsx_bytes
                st.session_state["xlsx_truncated_cache"] = xlsx_truncated
                st.rerun()
            except Exception as e:
                st.warning(f"⚠️ XLSX 변환 실패: {type(e).__name__}: {e}")

        csv_ready  = csv_bytes is not None
        xlsx_ready = xlsx_bytes is not None

        if csv_ready or xlsx_ready:
            st.markdown("##### 2) 다운로드")
        d1, d2, _ = st.columns([1, 1, 3])
        with d1:
            if csv_ready:
                st.download_button(
                    "📥 CSV 다운로드 (전체 행)",
                    data=csv_bytes,
                    file_name=f"{base_name}.csv",
                    mime="text/csv",
                    type="primary",
                    width="stretch",
                    key="dl_csv",
                )
        with d2:
            if xlsx_ready:
                st.download_button(
                    "📥 XLSX 다운로드" + (" (1M 행 잘림)" if xlsx_truncated else ""),
                    data=xlsx_bytes,
                    file_name=f"{base_name}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary" if not xlsx_truncated else "secondary",
                    width="stretch",
                    key="dl_xlsx",
                )
            else:
                st.button("CSV 변환 실패", disabled=True, width="stretch", key="dl_csv_dis")

    # ── 진행 버튼 (변환 성공/실패와 무관하게 항상 노출) ──────────────────────
    st.divider()
    left, _ = st.columns([1, 5])
    with left:
        if st.button("← 이전", key="dl_prev", width="stretch"):
            go_to(7)


# ══════════════════════════════════════════════════════════════════════════════
# 모드 B — 회사 마스터 빌더 (POS 표준 4-시트 xlsx)
# ══════════════════════════════════════════════════════════════════════════════

def _master_nav(suffix: str, prev_step: int | None, next_step: int | None,
                next_label: str = "다음 →", next_disabled: bool = False):
    """master 모드 step 의 상/하단 nav."""
    left, _, right = st.columns([1, 4, 1])
    with left:
        if prev_step is not None:
            if st.button("← 이전", key=f"m_prev_{suffix}", width="stretch"):
                st.session_state["master_step"] = prev_step
                st.rerun()
    with right:
        if next_step is not None:
            if st.button(next_label, type="primary",
                         disabled=next_disabled,
                         key=f"m_next_{suffix}", width="stretch"):
                st.session_state["master_step"] = next_step
                st.rerun()


def render_master_step_input():
    """M1 — 입력 레벨 선택 (회사 / 회사+브랜드 / 회사+브랜드+제품) + 페어 입력."""
    st.subheader("M1. 회사 입력")

    # ── 입력 레벨 선택 ───────────────────────────────────────────────────────
    LEVEL_OPTS = {
        "회사만 (1열)":              1,
        "회사 + 브랜드 (2열)":       2,
        "회사 + 브랜드 + 제품 (3열)": 3,
    }
    cur_level = st.session_state.get("master_input_level", 2)
    pick = st.radio(
        "어느 레벨까지 LIST 행으로 만들까요?",
        options=list(LEVEL_OPTS.keys()),
        index=[v for v in LEVEL_OPTS.values()].index(cur_level),
        horizontal=True,
        key="m_level_radio",
    )
    level = LEVEL_OPTS[pick]
    if level != cur_level:
        st.session_state["master_input_level"] = level
        # 레벨 바뀌면 입력 데이터프레임 리셋
        st.session_state.pop("master_pairs_df", None)
        st.rerun()

    if level == 1:
        cols = ["회사명"]
    elif level == 2:
        cols = ["회사명", "브랜드(LOB)"]
    else:
        cols = ["회사명", "브랜드(LOB)", "제품(SKU)"]

    # ── 업로드 예시 ──────────────────────────────────────────────────────────
    with st.expander("📋 업로드 예시 (펼쳐서 확인)", expanded=False):
        if level == 1:
            example = pd.DataFrame({
                "회사명": ["농심", "오뚜기", "CJ제일제당", "동원F&B", "KT&G"],
            })
            st.caption("회사 한글명 한 줄에 하나. 모든 회사는 `_ALL` 1행으로 처리됩니다.")
        elif level == 2:
            example = pd.DataFrame({
                "회사명":     ["KT&G","KT&G","KT&G","KT&G","농심","농심"],
                "브랜드(LOB)": ["전체","에쎄","릴","THE ONE","전체","신라면"],
            })
            st.caption(
                "회사명 + 브랜드(LOB). 같은 회사가 여러 브랜드면 행 분리. "
                "브랜드가 빈 값이거나 '전체'면 `_ALL` 로 처리. 회사당 001/002/003 순번."
            )
        else:
            example = pd.DataFrame({
                "회사명":     ["농심","농심","농심","농심","KT&G","KT&G"],
                "브랜드(LOB)": ["전체","신라면","신라면","새우깡","전체","에쎄"],
                "제품(SKU)":  ["전체","전체","큰사발면","전체","전체","에쎄1mg"],
            })
            st.caption(
                "회사 + 브랜드 + 제품 3계층. mandata_brand_name 은 "
                "`{회사}_{브랜드}_{제품}` 형식. 회사당 001/002/003 순번."
            )
        st.dataframe(example, width="stretch", hide_index=True)

    tab_editor, tab_file = st.tabs(["📝 직접 입력", "📁 파일 업로드"])

    with tab_editor:
        pairs_df = st.session_state.get("master_pairs_df")
        if pairs_df is None or len(pairs_df) == 0 or list(pairs_df.columns) != cols:
            pairs_df = pd.DataFrame({c: [""] * 5 for c in cols})
        edited = st.data_editor(
            pairs_df,
            num_rows="dynamic",
            width="stretch", height=420,
            key="m_pairs_editor",
        )
        st.session_state["master_pairs_df"] = edited

    with tab_file:
        st.caption(f"CSV/XLSX — {len(cols)}열 (헤더 무관): {' / '.join(cols)}")
        up = st.file_uploader(
            "파일 업로드",
            type=["csv", "xlsx", "xls"],
            key="master_upload",
        )
        if up is not None:
            try:
                if up.name.lower().endswith(".csv"):
                    df = pd.read_csv(up, header=None)
                else:
                    df = pd.read_excel(up, header=None)
                df = df.iloc[:, :len(cols)].copy()
                df.columns = cols
                df = df.fillna("").astype(str)
                first = df.iloc[0]
                if any(k in str(first.get("회사명","")).lower()
                       for k in ("company","회사","name","corp")):
                    df = df.iloc[1:].reset_index(drop=True)
                st.success(f"✅ {len(df)}행 로드됨")
                st.session_state["master_pairs_df"] = df
                with st.expander("미리보기 (상위 10)"):
                    st.dataframe(df.head(10), width="stretch", hide_index=True)
            except Exception as e:
                st.error(f"읽기 실패: {e}")

    # 페어 정규화
    edited = st.session_state.get("master_pairs_df")
    pairs: list[dict] = []
    if edited is not None:
        for _, r in edited.iterrows():
            c = str(r.get("회사명", "") or "").strip()
            b = str(r.get("브랜드(LOB)", "") or "").strip() if level >= 2 else ""
            p = str(r.get("제품(SKU)", "") or "").strip() if level >= 3 else ""
            if not c:
                continue
            pairs.append({"company": c, "brand": b, "product": p})

    st.session_state["master_pairs"] = pairs
    companies = list(dict.fromkeys(p["company"] for p in pairs))
    st.session_state["master_companies"] = companies

    if pairs:
        n_brands   = sum(1 for p in pairs if p["brand"]) if level >= 2 else 0
        n_products = sum(1 for p in pairs if p["product"]) if level >= 3 else 0
        msg = f"📋 총 **{len(pairs)}행** · 고유 회사 {len(companies)}"
        if level >= 2: msg += f" · 브랜드 지정 {n_brands}"
        if level >= 3: msg += f" · 제품 지정 {n_products}"
        st.info(msg)

    _master_nav("input", prev_step=0, next_step=2,
                next_disabled=(len(pairs) == 0))


def render_master_step_dart():
    """M2 — DART 매칭. 기존 ③ DART 로직 재사용."""
    st.subheader("M2. DART 매칭")
    st.caption(
        "DART 공시정보로 회사명 → **법인등록번호 + 영문회사명 + 단축코드 + 업종코드** 자동 매칭. "
        "동명 회사가 있으면 정확한 한 곳을 선택해 주세요."
    )

    companies = st.session_state.get("master_companies", [])
    if not companies:
        st.warning("M1 에서 회사 이름을 입력하세요.")
        _master_nav("dart_no_input", prev_step=1, next_step=None)
        st.stop()

    # DART API 키
    try:
        dart_secret = st.secrets.get("DART_API_KEY", "")
    except Exception:
        dart_secret = ""
    api_key = (st.session_state.get("dart_api_key", dart_secret) or "").strip()
    if not api_key:
        st.error("DART API 키가 없습니다. ③ DART 매칭 step 에서 키를 먼저 등록하세요.")
        _master_nav("dart_no_key", prev_step=1, next_step=None)
        st.stop()

    # ── 액션 버튼 ────────────────────────────────────────────────────────
    has_match = st.session_state.get("master_dart_match") is not None
    do_match = st.button(
        "🚀 DART 매칭 시작" if not has_match else "🔁 다시 매칭",
        type="primary" if not has_match else "secondary",
        key="m_dart_run",
    )

    if do_match:
        try:
            with st.spinner("DART 마스터 다운로드 중…"):
                dart_master = fetch_dart_corp_master(api_key)
        except Exception as e:
            st.error(f"❌ DART 마스터를 가져오지 못했습니다: {type(e).__name__}: {e}")
            st.stop()
        with st.spinner(f"{len(companies)}개 회사 DART 매칭 중…"):
            match_df = match_dart_companies(companies, dart_master)
            st.session_state["master_dart_match"] = match_df
        st.rerun()

    if not has_match:
        st.info("📌 위 **🚀 DART 매칭 시작** 버튼을 눌러 진행하세요.")
        _master_nav("dart_pre", prev_step=1, next_step=None)
        st.stop()

    match_df: pd.DataFrame = st.session_state["master_dart_match"]
    summary = dart_summary(match_df)
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("입력 회사", f"{summary['total']:,}")
    m2.metric("✅ 완전 매칭", f"{summary['exact']:,}")
    m3.metric("❓ 동명 후보", f"{summary['ambiguous']:,}")
    m4.metric("❌ 매칭 실패", f"{summary['none']:,}")

    # ── 동명 후보 선택 UI ───────────────────────────────────────────────
    ambiguous_rows = match_df[match_df["n_candidates"] > 1]
    if not ambiguous_rows.empty:
        st.markdown("#### ❓ 동명 회사 후보 선택")
        picks: dict = st.session_state.setdefault("master_dart_pick", {})
        for _, row in ambiguous_rows.iterrows():
            inp = row["input_name"]
            cands = row["candidates"]
            labels = [
                f"{c['corp_code']} · {c['corp_name']} "
                f"{('('+c['corp_name_eng']+')') if c.get('corp_name_eng') else ''} "
                f"{'📈상장' if c.get('stock_code') else '비상장'}"
                for c in cands
            ]
            existing_cc = picks.get(inp, row["corp_code"])
            default_idx = next(
                (i for i, c in enumerate(cands) if c.get("corp_code") == existing_cc), 0
            )
            st.markdown(
                f"<div style='margin-top:14px;margin-bottom:4px;font-size:18px;"
                f"font-weight:700;color:#111827;'>"
                f"🏢 <span style='color:#1e40af;'>{inp}</span> "
                f"<span style='font-size:13px;color:#6b7280;font-weight:500;'>"
                f"— {len(cands)}개 후보</span></div>",
                unsafe_allow_html=True,
            )
            chosen = st.selectbox(
                f"{inp} 후보 선택",
                options=labels, index=default_idx,
                key=f"m_dart_pick__{inp}",
                label_visibility="collapsed",
            )
            chosen_cand = cands[labels.index(chosen)]
            picks[inp] = chosen_cand.get("corp_code", "")
            mask = match_df["input_name"] == inp
            match_df.loc[mask, "corp_code"]     = chosen_cand.get("corp_code", "")
            match_df.loc[mask, "corp_name"]     = chosen_cand.get("corp_name", "")
            match_df.loc[mask, "corp_name_eng"] = chosen_cand.get("corp_name_eng", "")
            match_df.loc[mask, "stock_code"]    = chosen_cand.get("stock_code", "")
        st.session_state["master_dart_match"] = match_df

    # ── 회사 상세 (induty_code, corp_cls, jurir_no) 조회 ─────────────────
    matched_cc = sorted({
        str(cc) for cc in match_df["corp_code"].astype(str).tolist() if cc
    })
    info_map_now = st.session_state.get("master_dart_info", {})
    n_left = sum(1 for cc in matched_cc if cc not in info_map_now)

    st.markdown("#### 회사 상세 조회 (법인등록번호 · 업종코드 · 시장구분)")
    if n_left == 0 and matched_cc:
        st.success(f"✅ {len(matched_cc)}개 회사 상세 조회 완료")
    else:
        st.caption(f"미조회 {n_left}개 — 아래 버튼으로 시작")

    if st.button(
        f"📡 상세 조회 시작 ({n_left}개 남음)" if n_left else "📡 조회 완료",
        type="primary" if n_left else "secondary",
        disabled=n_left == 0,
        key="m_dart_info_run",
    ):
        remaining = [cc for cc in matched_cc if cc not in info_map_now]
        prog = st.progress(0.0, text="조회 중…")

        def _cb(i, total, info):
            prog.progress(i / total, text=f"{i}/{total} — {info.get('corp_name', '')}")
        results = dict(info_map_now)
        results.update(fetch_jurir_nos_batch(api_key, remaining, progress_callback=_cb))
        st.session_state["master_dart_info"] = results
        prog.empty()
        st.rerun()

    # 결과 테이블
    rows_view = []
    for _, r in match_df.iterrows():
        cc = r["corp_code"]
        info = info_map_now.get(cc) or {}
        rows_view.append({
            "입력":          r["input_name"],
            "DART 한글":     r["corp_name"],
            "영문":          r["corp_name_eng"],
            "법인등록번호":  info.get("jurir_no", "(미조회)"),
            "단축코드":      r["stock_code"] or info.get("stock_code", ""),
            "업종코드":      info.get("induty_code", ""),
            "시장":          info.get("corp_cls", ""),
            "corp_code":     cc,
        })
    st.dataframe(pd.DataFrame(rows_view), width="stretch", hide_index=True, height=360)

    _master_nav("dart", prev_step=1, next_step=3,
                next_disabled=(n_left > 0 and len(matched_cc) > 0))


def render_master_step_isin():
    """M3 — ISIN 산출. compute_isin_from_stock_code 재사용."""
    _ensure_master_fresh()   # streamlit 모듈 캐시 우회 — load_subsidiary_cache 등 보장
    st.subheader("M3. ISIN 산출")
    st.caption(
        "단축코드 → ISO 6166 알고리즘으로 ISIN 12자리 자동 계산. "
        "상장사는 자동, 비상장사는 ISIN 없음 (security_code 빈 값)."
    )

    match_df = st.session_state.get("master_dart_match")
    info_map = st.session_state.get("master_dart_info") or {}
    if match_df is None:
        st.warning("M2 DART 매칭을 먼저 완료하세요.")
        _master_nav("isin_no_match", prev_step=2, next_step=None)
        st.stop()

    # ISIN 산출
    isin_map: dict[str, str] = st.session_state.get("master_isin", {}) or {}
    if not isin_map:
        from modules.mapping.lookup import compute_isin_from_stock_code
        for _, r in match_df.iterrows():
            stk = (r.get("stock_code") or "").strip()
            if not stk:
                # info_map 에서 가져오기
                stk = (info_map.get(r["corp_code"]) or {}).get("stock_code", "").strip()
            if stk and stk.isdigit() and len(stk) == 6:
                try:
                    isin_map[r["input_name"]] = compute_isin_from_stock_code(stk)
                except Exception:
                    isin_map[r["input_name"]] = ""
            else:
                isin_map[r["input_name"]] = ""
        st.session_state["master_isin"] = isin_map

    # 표시
    rows = []
    for _, r in match_df.iterrows():
        cc = r["corp_code"]
        info = info_map.get(cc) or {}
        rows.append({
            "회사":      r["input_name"],
            "영문명":    r["corp_name_eng"],
            "단축코드":  r["stock_code"] or info.get("stock_code", ""),
            "ISIN":      isin_map.get(r["input_name"], ""),
            "시장":      _master.CORP_CLS_TO_STATUS.get(info.get("corp_cls", ""), "Not listed"),
        })
    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True, height=360)
    n_isin = sum(1 for v in isin_map.values() if v)
    st.caption(f"✅ ISIN 채움: {n_isin} / {len(isin_map)} (비상장은 빈 값)")

    # ── 비상장/Delisted → 모회사 매핑 ──────────────────────────────────────
    nonlisted: list[str] = []
    for _, r in match_df.iterrows():
        info = info_map.get(r["corp_code"]) or {}
        is_listed = (info.get("corp_cls") or "") in ("Y", "K")  # KOSPI / KOSDAQ
        own_isin  = isin_map.get(r["input_name"], "")
        if not is_listed or not own_isin:
            nonlisted.append(r["input_name"])

    parent_overrides: dict = st.session_state.setdefault("master_parent_overrides", {})
    # 캐시에서 자동 채우기 (한 번만)
    if not st.session_state.get("master_parent_cache_loaded") and nonlisted:
        sub_cache = _master.load_subsidiary_cache()
        for inp in nonlisted:
            if inp in sub_cache and inp not in parent_overrides:
                parent_overrides[inp] = dict(sub_cache[inp])
        st.session_state["master_parent_cache_loaded"] = True

    if nonlisted:
        st.markdown("---")
        st.markdown("#### 🔗 비상장·Delisted 회사 → 모회사 매핑")
        st.caption(
            f"{len(nonlisted)}개 회사가 직접 상장되지 않았어요. **모회사가 있으면** 그 회사의 "
            "ISIN 으로 `security_code` 를 채우고, `listing_status` 에 "
            "`Subsidiary of {parent} ({parent_isin})` 로 서술합니다. "
            "캐시(`mapping_app/data/subsidiary_cache.csv`) 에 누적돼 다음 빌드에 자동 적용돼요."
        )

        _kipris_key, anthropic_key = _trans_get_keys()
        n_filled = sum(1 for inp in nonlisted
                       if (parent_overrides.get(inp) or {}).get("parent_kr"))
        cc1, cc2 = st.columns([1, 3])
        with cc1:
            do_llm = st.button(
                f"🤖 Claude 로 모회사 추정 ({len(nonlisted) - n_filled}개)",
                type="primary",
                disabled=(len(nonlisted) - n_filled == 0 or not anthropic_key),
                width="stretch",
                key="m_parent_llm",
                help="Anthropic API 키 필요. 비상장 회사들의 모회사를 LLM 추정.",
            )
        with cc2:
            if not anthropic_key:
                st.caption("⚠️ Anthropic API 키 없음")
            else:
                st.caption(
                    f"매핑 완료 {n_filled} / {len(nonlisted)} "
                    "— 누락 행만 LLM 추정 / 직접 입력 모두 가능"
                )

        if do_llm:
            _ensure_master_fresh()
            if not hasattr(_master, "llm_find_parents"):
                st.error(
                    "❌ 모듈 캐시 오류 — 앱을 완전 재시작해 주세요 "
                    "(Ctrl+C 후 다시 실행)."
                )
                st.stop()
            items = []
            need = [inp for inp in nonlisted
                    if not (parent_overrides.get(inp) or {}).get("parent_kr")]
            for inp in need:
                row_dart = match_df[match_df["input_name"] == inp].iloc[0]
                info = info_map.get(row_dart["corp_code"]) or {}
                items.append({
                    "name_kr":     inp,
                    "name_en":     row_dart.get("corp_name_eng", "") or "",
                    "stock_code":  row_dart.get("stock_code", "") or "",
                    "induty_code": info.get("induty_code", ""),
                })
            with st.spinner(f"Claude 호출 중 ({len(items)}개)…"):
                results = _master.llm_find_parents(items, anthropic_key)
            n_ok = 0
            for inp, res in zip(need, results):
                if not res or not res.get("parent_kr"):
                    continue
                parent_overrides[inp] = {
                    "parent_kr":         res["parent_kr"],
                    "parent_en":         res.get("parent_en", ""),
                    "parent_stock_code": res.get("parent_stock_code", ""),
                    "parent_isin":       res.get("parent_isin", ""),
                    "status_kind":       res.get("status_kind", "subsidiary"),
                }
                n_ok += 1
            st.session_state["master_parent_overrides"] = parent_overrides
            st.success(f"✅ LLM 추정 {n_ok}/{len(items)} 채움. 아래에서 검수.")
            st.rerun()

        # 검수 테이블 — st.data_editor
        rows_p = []
        for inp in nonlisted:
            p = parent_overrides.get(inp) or {}
            rows_p.append({
                "회사 (비상장)":     inp,
                "parent_kr":         p.get("parent_kr", ""),
                "parent_en":         p.get("parent_en", ""),
                "parent_stock_code": p.get("parent_stock_code", ""),
                "parent_isin":       p.get("parent_isin", ""),
                "status_kind":       p.get("status_kind", "subsidiary"),
            })
        edit_p = pd.DataFrame(rows_p)
        edited_p = st.data_editor(
            edit_p,
            column_config={
                "회사 (비상장)": st.column_config.TextColumn(disabled=True),
                "status_kind":   st.column_config.SelectboxColumn(
                    options=["subsidiary", "delisted", "international"],
                    help="subsidiary=일반 자회사 / delisted=상장폐지 / international=해외상장",
                ),
            },
            width="stretch", hide_index=True, height=360,
            key="m_parent_editor",
        )
        for _, r in edited_p.iterrows():
            parent_overrides[r["회사 (비상장)"]] = {
                "parent_kr":         r["parent_kr"],
                "parent_en":         r["parent_en"],
                "parent_stock_code": r["parent_stock_code"],
                "parent_isin":       r["parent_isin"],
                "status_kind":       r["status_kind"],
            }
        st.session_state["master_parent_overrides"] = parent_overrides

        # parent_stock_code 가 입력됐는데 parent_isin 이 비어있으면 자동 산출 안내
        # (수동으로 KR7로 시작하는 ISIN 을 직접 넣어도 됨)
        st.caption(
            "💡 `parent_stock_code` 만 입력하면 아래 버튼으로 ISIN 자동 산출 가능."
        )
        if st.button("⚡ parent_stock_code → parent_isin 자동 산출",
                     key="m_parent_isin_auto"):
            from modules.mapping.lookup import compute_isin_from_stock_code
            n_done = 0
            for inp, p in list(parent_overrides.items()):
                if p.get("parent_stock_code") and not p.get("parent_isin"):
                    stk = str(p["parent_stock_code"]).strip().zfill(6)
                    if stk.isdigit() and len(stk) == 6:
                        try:
                            p["parent_isin"] = compute_isin_from_stock_code(stk)
                            n_done += 1
                        except Exception:
                            pass
            st.session_state["master_parent_overrides"] = parent_overrides
            st.success(f"✅ {n_done}개 ISIN 자동 산출")
            st.rerun()

        # 캐시 저장 + 다음으로
        if st.button("💾 자회사 매핑 캐시에 저장 + 다음으로",
                     type="primary", width="stretch", key="m_parent_save_next"):
            save_rows: list[dict] = []
            for inp, p in parent_overrides.items():
                if not p.get("parent_kr"):
                    continue
                save_rows.append({
                    "company_kr":        inp,
                    "parent_kr":         p["parent_kr"],
                    "parent_en":         p.get("parent_en", ""),
                    "parent_stock_code": p.get("parent_stock_code", ""),
                    "parent_isin":       p.get("parent_isin", ""),
                    "status_kind":       p.get("status_kind", "subsidiary"),
                    "source":            "manual",
                    "confidence":        1.0,
                })
            if save_rows:
                n = _master.save_subsidiary_cache_rows(save_rows)
                st.success(f"✅ {n}개 자회사 매핑 캐시에 저장. 다음 단계로.")
            st.session_state["master_step"] = 4
            st.rerun()

    _master_nav("isin", prev_step=2, next_step=4)


def render_master_step_gics():
    """M4 — GICS 매핑 검수. induty_code → GICS 4컬럼.

    하이브리드:
      ① 캐시 csv (사용자 누적 확정 매핑) + builtin KSIC→GICS 표 → 자동 매핑
      ② 누락 row 는 Claude LLM 으로 추론 (사용자 버튼 클릭)
      ③ 사용자 검수 후 '다음 →' 시 매핑을 캐시에 누적
    """
    st.subheader("M4. GICS 산업분류 검수")
    st.caption(
        "1) 캐시(누적 csv) + builtin KSIC→GICS 표로 자동 매핑 → "
        "2) 누락 row 는 🤖 Claude 로 일괄 채우기 → 3) 검수 후 다음 단계로 가면 "
        "확정된 매핑이 캐시에 누적돼 다음 빌드에 자동 적용됩니다."
    )

    match_df = st.session_state.get("master_dart_match")
    info_map = st.session_state.get("master_dart_info") or {}
    if match_df is None:
        st.warning("M2 를 먼저 완료하세요.")
        _master_nav("gics_no", prev_step=3, next_step=None)
        st.stop()

    # 회사별 GICS — 자동 매핑 후 사용자 override 가능
    gics_map: dict = st.session_state.setdefault("master_gics", {})
    # 회사명 → KSIC induty_code 매핑 (LLM 호출·캐시 저장 시 사용)
    company_ksic: dict[str, str] = {}
    for _, r in match_df.iterrows():
        inp = r["input_name"]
        cc  = r["corp_code"]
        info = info_map.get(cc) or {}
        induty = str(info.get("induty_code", "")).strip()
        company_ksic[inp] = induty
        if inp not in gics_map:
            g = _master.map_ksic_to_gics(induty)
            if g:
                gics_map[inp] = {
                    "gics_industry_code":     g[0],
                    "gics_industry":          g[1],
                    "gics_sub_industry_code": g[2],
                    "gics_sub_industry":      g[3],
                }
            else:
                gics_map[inp] = {
                    "gics_industry_code": "", "gics_industry": "",
                    "gics_sub_industry_code": "", "gics_sub_industry": "",
                }

    # 누락 행 식별
    missing = [
        r["input_name"] for _, r in match_df.iterrows()
        if not gics_map.get(r["input_name"], {}).get("gics_industry_code")
    ]

    # ── 🤖 LLM 일괄 채우기 ──────────────────────────────────────────────────
    _kipris_key, anthropic_key = _trans_get_keys()
    cc1, cc2 = st.columns([1, 3])
    with cc1:
        do_llm = st.button(
            f"🤖 Claude 로 누락 GICS 채우기 ({len(missing)}개)",
            type="primary",
            disabled=(len(missing) == 0 or not anthropic_key),
            width="stretch",
            key="m_gics_llm",
            help="Anthropic API 키 필요. 회사명+KSIC+영문명 컨텍스트로 GICS 추론.",
        )
    with cc2:
        if not anthropic_key:
            st.caption("⚠️ Anthropic API 키 없음 — 자유 영문화 step 에서 키 등록 필요.")
        elif missing:
            st.caption(
                f"누락 {len(missing)}개 — LLM 으로 일괄 추론. "
                "결과는 검수 후 캐시에 저장돼 다음 빌드에 자동 적용됨."
            )
        else:
            st.caption("✅ 누락 없음 — 모두 자동 매핑됨")

    if do_llm and missing:
        _ensure_master_fresh()
        if not hasattr(_master, "llm_classify_gics"):
            st.error(
                "❌ 모듈 캐시 오류 — 앱을 완전 재시작해 주세요 (Ctrl+C 후 다시 실행). "
                "이전 버전 코드가 메모리에 남아 새 함수를 못 찾고 있어요."
            )
            st.stop()
        items = []
        for inp in missing:
            row_dart = match_df[match_df["input_name"] == inp].iloc[0]
            items.append({
                "ksic_code":  company_ksic.get(inp, ""),
                "name_kr":    inp,
                "name_en":    row_dart.get("corp_name_eng", "") or "",
                "stock_code": row_dart.get("stock_code", "") or "",
            })
        with st.spinner(f"Claude 호출 중 ({len(items)}개)…"):
            llm_results = _master.llm_classify_gics(items, anthropic_key)
        n_ok = 0
        for inp, res in zip(missing, llm_results):
            if not res or not res.get("gics_industry_code"):
                continue
            gics_map[inp] = {
                "gics_industry_code":     res["gics_industry_code"],
                "gics_industry":          res["gics_industry"],
                "gics_sub_industry_code": res["gics_sub_industry_code"],
                "gics_sub_industry":      res["gics_sub_industry"],
            }
            n_ok += 1
        st.success(f"✅ LLM 추론 {n_ok}/{len(items)} 채움. 아래 테이블에서 검수 후 다음 →")
        st.session_state["master_gics"] = gics_map
        st.rerun()

    # ── 검수 테이블 ────────────────────────────────────────────────────────
    rows = []
    for _, r in match_df.iterrows():
        inp = r["input_name"]
        induty = company_ksic.get(inp, "")
        g = gics_map[inp]
        rows.append({
            "회사":      inp,
            "업종코드":  induty or "(없음)",
            "gics_industry_code":     g["gics_industry_code"],
            "gics_industry":          g["gics_industry"],
            "gics_sub_industry_code": g["gics_sub_industry_code"],
            "gics_sub_industry":      g["gics_sub_industry"],
        })
    edit_df = pd.DataFrame(rows)
    edited = st.data_editor(
        edit_df,
        column_config={
            "회사":     st.column_config.TextColumn(disabled=True),
            "업종코드": st.column_config.TextColumn(disabled=True),
        },
        width="stretch", hide_index=True, height=420,
        key="m_gics_editor",
    )
    # 변경사항을 session 에 저장
    for _, r in edited.iterrows():
        gics_map[r["회사"]] = {
            "gics_industry_code":     r["gics_industry_code"],
            "gics_industry":          r["gics_industry"],
            "gics_sub_industry_code": r["gics_sub_industry_code"],
            "gics_sub_industry":      r["gics_sub_industry"],
        }
    st.session_state["master_gics"] = gics_map

    n_filled = sum(1 for v in gics_map.values() if v.get("gics_industry_code"))
    st.caption(f"📊 GICS 매핑 완료: {n_filled} / {len(gics_map)}")

    # ── 다음 → 시 캐시에 누적 저장 ─────────────────────────────────────────
    if st.button("💾 검수 결과 KSIC→GICS 캐시에 저장 + 다음으로",
                 type="primary", width="stretch", key="m_gics_save_next"):
        save_rows: list[dict] = []
        for inp, g in gics_map.items():
            ksic = company_ksic.get(inp, "")
            if not ksic or not g.get("gics_industry_code"):
                continue
            save_rows.append({
                "ksic_code":              ksic,
                "gics_industry_code":     g["gics_industry_code"],
                "gics_industry":          g["gics_industry"],
                "gics_sub_industry_code": g["gics_sub_industry_code"],
                "gics_sub_industry":      g["gics_sub_industry"],
                "source":                 "manual",   # 사용자 검수 = manual 우선순위
                "confidence":             1.0,
            })
        if save_rows:
            n = _master.save_ksic_gics_cache_rows(save_rows)
            st.success(f"✅ {n}개 KSIC→GICS 매핑 캐시에 저장. 다음 단계로 이동.")
        st.session_state["master_step"] = 5
        st.rerun()

    _master_nav("gics", prev_step=3, next_step=5,
                next_label="저장 없이 다음 →")


def render_master_step_download():
    """M5 — LIST_PR 단일 시트 xlsx 생성 + 다운로드."""
    st.subheader("M5. LIST_PR xlsx 생성")
    st.caption(
        "지금까지 입력된 회사 마스터 정보로 **LIST_PR 단일 시트** xlsx 를 생성합니다."
    )

    match_df = st.session_state.get("master_dart_match")
    info_map = st.session_state.get("master_dart_info") or {}
    isin_map = st.session_state.get("master_isin") or {}
    gics_map = st.session_state.get("master_gics") or {}
    parent_overrides = st.session_state.get("master_parent_overrides") or {}

    if match_df is None:
        st.warning("이전 단계를 완료하세요.")
        _master_nav("dl_no", prev_step=4, next_step=None)
        st.stop()

    # ── 페어 가져오기 (M1) — 회사 / 브랜드 / 제품 3-계층 ────────────────────
    pairs = st.session_state.get("master_pairs") or []
    if not pairs:
        pairs = [{"company": r["input_name"], "brand": "", "product": ""}
                 for _, r in match_df.iterrows()]

    # ── 영문화 실행 (수동 트리거 — 회사·브랜드·제품 모두) ────────────────────
    st.markdown("#### 🔤 영문화 실행 (회사 + 브랜드 + 제품)")
    _kipris_key0, _anth0 = _trans_get_keys()
    n_brands_kr  = len({p["brand"]   for p in pairs if p["brand"]   and p["brand"]   not in ("전체","ALL","All","all")})
    n_products_kr= len({p["product"] for p in pairs if p["product"] and p["product"] not in ("전체","ALL","All","all")})
    n_companies_kr = len({p["company"] for p in pairs if p["company"]})

    bcol, mcol = st.columns([1, 3])
    with bcol:
        do_translate = st.button(
            f"🚀 영문화 실행 (브랜드 {n_brands_kr} · 제품 {n_products_kr})",
            type="primary",
            disabled=(not _anth0 or (n_brands_kr + n_products_kr == 0)),
            width="stretch",
            key="m_translate_run",
            help="브랜드·제품 한글값을 LLM 으로 영문 변환. 회사 영문은 DART 결과 사용.",
        )
    with mcol:
        if not _anth0:
            st.caption("⚠️ Anthropic API 키 없음 — 영문화 안 됨 (한글 그대로 사용)")
        else:
            st.caption(f"회사 {n_companies_kr}개 · 브랜드 {n_brands_kr}개 · 제품 {n_products_kr}개")

    if do_translate:
        # 브랜드 영문화
        if n_brands_kr:
            unique_brands_kr = sorted({p["brand"] for p in pairs
                                        if p["brand"] and p["brand"] not in ("전체","ALL","All","all")})
            with st.spinner(f"브랜드 영문화 {len(unique_brands_kr)}개…"):
                try:
                    _trans_pipeline.collect_brands_batch(
                        unique_brands_kr, llm_key=_anth0, skip_confirmed=True,
                    )
                    from modules.mapping import translation_db as _db
                    _db.bulk_select_top("brand", reviewer="auto")
                except Exception as e:
                    st.error(f"브랜드 영문화 실패: {e}")
        # 제품 영문화
        if n_products_kr:
            unique_products_kr = sorted({p["product"] for p in pairs
                                          if p["product"] and p["product"] not in ("전체","ALL","All","all")})
            with st.spinner(f"제품 영문화 {len(unique_products_kr)}개…"):
                try:
                    _trans_pipeline.collect_products_batch(
                        unique_products_kr, llm_key=_anth0, skip_confirmed=True,
                    )
                    from modules.mapping import translation_db as _db
                    _db.bulk_select_top("product", reviewer="auto")
                except Exception as e:
                    st.error(f"제품 영문화 실패: {e}")
        st.success("✅ 영문화 완료 — 아래 미리보기에 반영됨")
        st.rerun()

    def _brand_to_en(brand_kr: str) -> str:
        if not brand_kr or brand_kr in ("전체","ALL","All","all"):
            return "ALL"
        en = _trans_pipeline.lookup_brand_en(brand_kr)
        if en:
            return en
        from modules.mapping.translation import romanizer as _rom
        return _rom.romanize_brand(brand_kr) or brand_kr

    def _product_to_en(product_kr: str) -> str:
        if not product_kr or product_kr in ("전체","ALL","All","all"):
            return "ALL"
        en = _trans_pipeline.lookup_product_en(product_kr)
        if en:
            return en
        from modules.mapping.translation import romanizer as _rom
        return _rom.romanize_product(product_kr) or product_kr

    # LIST 행 빌드 — master_builder.build_list_rows 에 위임
    shareholder_map_now = st.session_state.get("master_shareholder") or {}
    list_rows, pair_companies = _master.build_list_rows(
        pairs=pairs,
        match_df=match_df,
        info_map=info_map,
        isin_map=isin_map,
        gics_map=gics_map,
        parent_overrides=parent_overrides,
        shareholder_map=shareholder_map_now,
        brand_to_en=_brand_to_en,
        product_to_en=_product_to_en,
    )
    list_df = pd.DataFrame(list_rows, columns=_master.LIST_COLUMNS)

    # 최대주주·정의 컬럼은 build_list_rows 가 이미 회사 단위로 채워줌
    shareholder_map: dict = st.session_state.setdefault("master_shareholder", {})

    # ── 📡 DART 최대주주 1차 조회 ──────────────────────────────────────────
    st.markdown("#### 📡 최대주주 조회")
    st.caption(
        "**1순위 — DART**: `hyslrSttus.json` 사업보고서 기반 최대주주 한글명을 가져와 "
        "dart_master 에서 영문/상장정보/ISIN 자동 산출. "
        "**2순위 — LLM (Claude)**: DART 매칭 실패 / 해외 모회사 / 정의(mandata_brand_name_definition) "
        "는 LLM 으로 fallback."
    )

    try:
        dart_secret = st.secrets.get("DART_API_KEY", "")
    except Exception:
        dart_secret = ""
    dart_api_key = (st.session_state.get("dart_api_key", dart_secret) or "").strip()

    # 고유 회사 단위로 누락 체크 (같은 회사 여러 행이면 한 번만)
    missing_sh: list[str] = []
    _seen_sh: set = set()
    for i in range(len(list_df)):
        c = pair_companies[i] if i < len(pair_companies) else ""
        if not c or c in _seen_sh:
            continue
        _seen_sh.add(c)
        if not (list_df.iloc[i]["largest_shareholder_company_name_en"] or "").strip():
            missing_sh.append(c)

    dc1, dc2 = st.columns([1, 3])
    with dc1:
        do_dart_sh = st.button(
            f"📡 DART 최대주주 조회 ({len(missing_sh)}개)",
            type="primary",
            disabled=(len(missing_sh) == 0 or not dart_api_key),
            width="stretch",
            key="m_sh_dart",
            help="DART hyslrSttus.json 호출 → dart_master 자동 매칭",
        )
    with dc2:
        if not dart_api_key:
            st.caption("⚠️ DART API 키 없음")
        elif missing_sh:
            st.caption(f"누락 {len(missing_sh)}개 — DART 사업보고서 기준 최대주주 1차 조회")
        else:
            st.caption("✅ 누락 없음")

    if do_dart_sh and missing_sh:
        # dart_master 캐시에서 가져오기
        try:
            with st.spinner("DART 마스터 준비…"):
                dart_master = fetch_dart_corp_master(dart_api_key)
        except Exception as e:
            st.error(f"DART 마스터 실패: {e}")
            st.stop()
        # 모듈 캐시 우회 — reload 후 함수 참조
        import importlib
        from modules.mapping import dart_lookup as _dart_mod
        importlib.reload(_dart_mod)
        fetch_largest_shareholders_batch = _dart_mod.fetch_largest_shareholders_batch
        corp_codes = []
        inp_to_cc = {}
        listed_codes: set[str] = set()
        for inp in missing_sh:
            cc = str(match_df[match_df["input_name"] == inp].iloc[0]["corp_code"]).zfill(8)
            corp_codes.append(cc); inp_to_cc[inp] = cc
            # 상장사만 hyslrSttus 호출 — 비상장은 응답 없으므로 skip
            inf = info_map.get(cc) or {}
            if (inf.get("corp_cls") or "") in ("Y", "K"):
                listed_codes.add(cc)
        n_listed   = len(listed_codes)
        n_skipped  = len(corp_codes) - n_listed
        prog = st.progress(
            0.0,
            text=f"DART 최대주주 호출 중… (상장 {n_listed}개 호출, 비상장 {n_skipped}개 skip)",
        )
        def _cb(i, total, info):
            status = info.get("status", "")
            tag = "skip" if status == "skipped_nonlisted" else info.get("nm", "")
            prog.progress(i / max(1, total), text=f"{i}/{total} — {tag}")
        sh_results = fetch_largest_shareholders_batch(
            dart_api_key, corp_codes, progress_callback=_cb,
            max_workers=8, listed_only_codes=listed_codes,
        )
        prog.empty()
        n_ok = 0
        for inp in missing_sh:
            cc = inp_to_cc[inp]
            r  = sh_results.get(cc) or {}
            nm = (r.get("nm") or "").strip()
            if not nm:
                # DART 응답 자체가 없음 — verify 표에 남기기
                shareholder_map.setdefault(inp, {})
                shareholder_map[inp]["_source"]  = "DART no_data"
                shareholder_map[inp]["_dart_nm"] = ""
                shareholder_map[inp]["_dart_qota_rt"] = 0.0
                continue
            resolved = _master.resolve_shareholder_from_dart_master(nm, dart_master)
            shareholder_map[inp] = {
                "largest_shareholder_company_name_en":
                    resolved.get("largest_shareholder_company_name_en", nm),
                "largest_shareholder_listing_status":
                    resolved.get("largest_shareholder_listing_status", ""),
                "largest_shareholder_security_code":
                    resolved.get("largest_shareholder_security_code", ""),
                "mandata_brand_name_definition": "",
                # 검증 메타 — 별도 표에서 보여줌
                "_source":         resolved.get("_source", "DART unmatched"),
                "_master_match":   resolved.get("_master_match", ""),
                "_dart_nm":        nm,
                "_dart_qota_rt":   r.get("qota_rt", 0.0),
                "_dart_year":      r.get("year", ""),
            }
            n_ok += 1
        st.session_state["master_shareholder"] = shareholder_map
        st.success(f"✅ DART 에서 {n_ok}/{len(missing_sh)} 최대주주 채움")
        st.rerun()

    # ── 🔍 최대주주 매칭 검증 — DART 응답 원본 + 매칭 경로 표시 ───────────────
    verify_rows = []
    for inp in (st.session_state.get("master_companies") or []):
        sh = shareholder_map.get(inp) or {}
        if not sh.get("_source"):
            continue
        verify_rows.append({
            "회사":          inp,
            "DART 한글 (원본)": sh.get("_dart_nm", ""),
            "DART master 매칭": sh.get("_master_match", "(없음)"),
            "→ 영문":        sh.get("largest_shareholder_company_name_en", ""),
            "상장":          sh.get("largest_shareholder_listing_status", ""),
            "ISIN":          sh.get("largest_shareholder_security_code", ""),
            "지분율(%)":     sh.get("_dart_qota_rt", 0),
            "source":        sh.get("_source", ""),
            "LLM 보강 필드":  sh.get("_llm_filled", ""),
        })
    if verify_rows:
        with st.expander(
            f"🔍 최대주주 매칭 검증 ({len(verify_rows)}건) — DART 응답 vs 영문 변환 결과",
            expanded=False,
        ):
            st.caption(
                "**source** 컬럼: `DART exact` 정확 일치 / `DART prefix_suffix` 부분 일치 / "
                "`DART unmatched` 매칭 실패 / `Individual (heuristic)` 자연인 / "
                "`Institution (NPS)` 기관 / `LLM` 보강 / `Manual` 수동 / `DART no_data` 응답 없음"
            )
            st.dataframe(pd.DataFrame(verify_rows), width="stretch", hide_index=True, height=320)

    # ── 🤖 LLM fallback (DART 매칭 실패 + 정의 항목) ────────────────────────
    _kipris_key, anthropic_key = _trans_get_keys()
    # 정의 비어있거나, 영문주주명이 한글로 남아있는 회사만 (고유 단위)
    missing_llm: list[str] = []
    _seen_llm: set = set()
    for i in range(len(list_df)):
        inp = pair_companies[i] if i < len(pair_companies) else ""
        if not inp or inp in _seen_llm:
            continue
        _seen_llm.add(inp)
        sh  = shareholder_map.get(inp) or {}
        en  = (sh.get("largest_shareholder_company_name_en") or
               list_df.iloc[i]["largest_shareholder_company_name_en"] or "")
        de  = sh.get("mandata_brand_name_definition", "") or \
              list_df.iloc[i]["mandata_brand_name_definition"]
        has_korean = any('가' <= c <= '힣' for c in str(en))
        if has_korean or not str(de).strip():
            missing_llm.append(inp)

    sc1, sc2 = st.columns([1, 3])
    with sc1:
        do_sh = st.button(
            f"🤖 LLM fallback ({len(missing_llm)}개)",
            type="secondary",
            disabled=(len(missing_llm) == 0 or not anthropic_key),
            width="stretch",
            key="m_sh_llm",
            help="DART 매칭 실패한 행과 정의(definition) 컬럼을 LLM 으로 보강.",
        )
    with sc2:
        if not anthropic_key:
            st.caption("⚠️ Anthropic API 키 없음")
        elif missing_llm:
            st.caption(
                f"DART 매칭 실패 또는 정의 누락 {len(missing_llm)}개 → LLM 보강"
            )
        else:
            st.caption("✅ LLM 보강 필요 없음")

    if do_sh and missing_llm:
        _ensure_master_fresh()
        if not hasattr(_master, "llm_largest_shareholder"):
            st.error("❌ 모듈 캐시 오류 — 앱 재시작 필요 (Ctrl+C 후 다시 실행)")
            st.stop()
        items = []
        for inp in missing_llm:
            row_dart = match_df[match_df["input_name"] == inp].iloc[0]
            info = info_map.get(row_dart["corp_code"]) or {}
            # list_df 는 페어 단위(같은 회사 여러 행) — pair_companies 인덱스 기반으로 첫 행 선택
            mandata = ""
            for i, c in enumerate(pair_companies):
                if c == inp:
                    mandata = list_df.iloc[i]["mandata_brand_name"]
                    break
            items.append({
                "name_kr":            inp,
                "name_en":            row_dart.get("corp_name_eng", "") or "",
                "mandata_brand_name": mandata or "",
                "induty_code":        info.get("induty_code", ""),
            })
        with st.spinner(f"Claude 호출 중 ({len(items)}개)…"):
            results = _master.llm_largest_shareholder(items, anthropic_key)
        n_ok = 0
        for inp, res in zip(missing_llm, results):
            if not res:
                continue
            existing = shareholder_map.get(inp) or {}
            llm_filled_fields = []
            # DART 결과 보존, LLM 은 빈 칸만 채움
            def _take(key):
                v = existing.get(key) or res.get(key, "")
                if not existing.get(key) and res.get(key):
                    llm_filled_fields.append(key)
                return v
            updated = {
                "largest_shareholder_company_name_en": _take("largest_shareholder_company_name_en"),
                "largest_shareholder_listing_status":  _take("largest_shareholder_listing_status"),
                "largest_shareholder_security_code":   _take("largest_shareholder_security_code"),
                # 정의는 항상 LLM 결과로 채움
                "mandata_brand_name_definition":
                    res.get("mandata_brand_name_definition", "") or
                    existing.get("mandata_brand_name_definition", ""),
                # 검증 메타 보존 + LLM 보강 표시
                "_dart_nm":      existing.get("_dart_nm", ""),
                "_dart_qota_rt": existing.get("_dart_qota_rt", 0.0),
                "_dart_year":    existing.get("_dart_year", ""),
                "_master_match": existing.get("_master_match", ""),
                "_source":       (existing.get("_source", "") + " + LLM")
                                  if existing.get("_source") and llm_filled_fields
                                  else ("LLM" if llm_filled_fields else existing.get("_source", "")),
                "_llm_filled":   ",".join(llm_filled_fields),
            }
            shareholder_map[inp] = updated
            n_ok += 1
        st.session_state["master_shareholder"] = shareholder_map
        st.success(f"✅ {n_ok}/{len(items)} 채움. 아래 표에서 검수.")
        st.rerun()

    # ── 미리보기 + 편집 (data_editor) ────────────────────────────────────
    st.markdown("#### 미리보기 — LIST_PR (편집 가능)")
    st.caption(
        "맨 뒤 4개 컬럼 (`largest_shareholder_*` · `mandata_brand_name_definition`) 은 "
        "직접 수정 가능합니다. 수정한 값으로 xlsx 가 생성돼요."
    )
    edited_df = st.data_editor(
        list_df,
        column_config={
            # 앞 10개는 readonly (M2-M4 결과)
            c: st.column_config.TextColumn(disabled=True)
            for c in _master.LIST_COLUMNS[:10]
        },
        width="stretch", hide_index=True, height=420,
        key="m_list_editor",
    )
    # 사용자 수정값을 session 에 반영 (회사 단위 — 같은 회사 마지막 행 값으로)
    for i, r in edited_df.iterrows():
        company_kr = pair_companies[i] if i < len(pair_companies) else ""
        if not company_kr:
            continue
        shareholder_map[company_kr] = {
            "largest_shareholder_company_name_en": r["largest_shareholder_company_name_en"],
            "largest_shareholder_listing_status":  r["largest_shareholder_listing_status"],
            "largest_shareholder_security_code":   r["largest_shareholder_security_code"],
            "mandata_brand_name_definition":       r["mandata_brand_name_definition"],
        }
    st.session_state["master_shareholder"] = shareholder_map
    list_df = edited_df   # xlsx 생성 시 편집본 사용

    # LIST_PR 단일 시트 xlsx 빌드
    if st.button("🛠 LIST_PR xlsx 생성", type="primary", key="m_build_xlsx"):
        _ensure_master_fresh()
        with st.spinner("xlsx 생성 중…"):
            data = _master.build_list_only_xlsx(list_df)
        st.session_state["master_xlsx_bytes"] = data
        st.success(f"✅ 생성 완료 ({len(data):,} bytes)")

    xlsx_bytes = st.session_state.get("master_xlsx_bytes")
    if xlsx_bytes:
        from datetime import datetime
        fname = f"LIST_PR_{datetime.now():%Y%m%d}.xlsx"
        st.download_button(
            "📥 xlsx 다운로드",
            data=xlsx_bytes, file_name=fname,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
            width="stretch", key="m_dl_xlsx",
        )

    _master_nav("dl", prev_step=4, next_step=None)


# ══════════════════════════════════════════════════════════════════════════════
# 모드 선택 (홈)
# ══════════════════════════════════════════════════════════════════════════════

def render_mode_select():
    """홈 — 두 모드 중 선택."""
    st.title("🗂 데이터 매핑 — Alt-Data Intelligence")
    st.caption("어떤 작업을 진행할지 선택하세요.")

    st.markdown("---")
    c1, c2 = st.columns(2)
    with c1:
        with st.container(border=True):
            st.markdown("### 📊 원천 데이터 → 표준 레이아웃")
            st.write(
                "데이터원천사가 보낸 **raw 데이터 파일**을 우리 회사 "
                "표준 레이아웃으로 변환합니다. 8단계 마법사 워크플로우."
            )
            st.caption(
                "• 컬럼 매핑 · DART/ISIN 자동 매칭\n\n"
                "• 브랜드/제품/카테고리 영문화\n\n"
                "• XLSX/CSV 출력"
            )
            if st.button("▶ 시작하기", type="primary", width="stretch", key="mode_a"):
                st.session_state["app_mode"] = "raw_to_standard"
                st.session_state["step"] = 1
                st.rerun()
    with c2:
        with st.container(border=True):
            st.markdown("### 🏢 Information & List")
            st.write(
                "**회사 한글 이름**만 입력하면 LIST_PR xlsx 를 자동 생성합니다. "
                "DART + ISIN + GICS 매핑 → 다운로드."
            )
            st.caption(
                "• LIST_PR 단일 시트 (10 컬럼)\n\n"
                "• 5단계 wizard"
            )
            if st.button("▶ 시작하기", type="primary", width="stretch", key="mode_b"):
                st.session_state["app_mode"] = "company_master"
                st.session_state["master_step"] = 1
                st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# Main router
# ══════════════════════════════════════════════════════════════════════════════

if "app_mode" not in st.session_state:
    st.session_state["app_mode"] = None

# 사이드바 — 현재 모드 표시 + 변경
with st.sidebar:
    cur_mode = st.session_state.get("app_mode")
    if cur_mode:
        st.markdown("### 모드")
        st.write(
            "📊 원천 → 표준" if cur_mode == "raw_to_standard"
            else "🏢 Information & List"
        )
        if st.button("🏠 모드 선택으로", key="sb_home", width="stretch"):
            st.session_state["app_mode"] = None
            st.rerun()

app_mode = st.session_state.get("app_mode")

if app_mode is None:
    render_mode_select()

elif app_mode == "raw_to_standard":
    if "step" not in st.session_state:
        st.session_state["step"] = 1

    st.title("🗂 데이터 매핑 — Alt-Data Intelligence")
    st.caption(
        "원천 raw 데이터를 우리 회사 **표준 레이아웃** 그대로 변환합니다. "
        "표준 레이아웃을 업로드하면 그 컬럼 정의가 매핑 기준이 됩니다."
    )

    render_stepper()
    step = st.session_state["step"]
    if   step == 1: render_step_upload()
    elif step == 2: render_step_mapping()
    elif step == 3: render_step_dart()
    elif step == 4: render_step_isin()
    elif step == 5: render_step_brand_product_en()
    elif step == 6: render_step_final_mapping()
    elif step == 7: render_step_validation()
    elif step == 8: render_step_download()
    else:           go_to(1)

elif app_mode == "company_master":
    if "master_step" not in st.session_state:
        st.session_state["master_step"] = 1

    st.title("🏢 Information & List")
    st.caption(
        "회사 한글명만 주면 POS 표준 4-시트 xlsx (INFORMATION_PR/BT + LIST_PR/BT) 를 "
        "자동 생성합니다."
    )

    # master stepper
    _ui.render_stepper(MASTER_STEPS)
    # render_stepper 가 'step' 세션을 읽으니, master_step 임시로 'step' 에도 박아 stepper 가 잘 그리도록
    st.session_state["step"] = st.session_state["master_step"]

    mstep = st.session_state["master_step"]
    if   mstep == 1: render_master_step_input()
    elif mstep == 2: render_master_step_dart()
    elif mstep == 3: render_master_step_isin()
    elif mstep == 4: render_master_step_gics()
    elif mstep == 5: render_master_step_download()
    else:            st.session_state["master_step"] = 1; st.rerun()

else:
    st.session_state["app_mode"] = None
    st.rerun()
