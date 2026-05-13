"""
mapping_app/ui_common.py — 매핑 앱 전역 공통 UI/세션 헬퍼.

⚠️ Streamlit 의존. modules/* (analysis 앱·매핑 코어) 와는 별개로 mapping_app 만 사용.
"""
from __future__ import annotations

from pathlib import Path

import streamlit as st

from modules.mapping.column_mapper import KIND_LABEL, infer_column_kind


# ── 페이지·세션 헬퍼 ──────────────────────────────────────────────────────────
def go_to(step: int):
    """현재 step 을 변경하고 즉시 rerun."""
    st.session_state["step"] = step
    st.rerun()


def render_stepper(steps: list[str]):
    """상단 스텝 인디케이터. (Markdown 코드블록 해석 회피용 한 줄 HTML)"""
    current = st.session_state.get("step", 1)
    items = ""
    for i, label in enumerate(steps, start=1):
        if i < current:
            state, icon = "done", "✓"
        elif i == current:
            state, icon = "active", str(i)
        else:
            state, icon = "future", str(i)
        items += (
            f'<div class="step {state}">'
            f'<div class="circle">{icon}</div>'
            f'<div class="label">{label}</div>'
            f'</div>'
        )
        if i < len(steps):
            items += "<div class='connector'></div>"

    style = """
<style>
.stepper { display: flex; align-items: center; padding: 16px 0 24px 0; gap: 0; }
.step { display: flex; flex-direction: column; align-items: center; flex: 0 0 auto; }
.connector { flex: 1; height: 2px; background: #d1d5db; margin: 0 4px; margin-bottom: 20px; }
.circle { width: 32px; height: 32px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 13px; font-weight: 700; }
.label { font-size: 11px; margin-top: 4px; white-space: nowrap; color: #6b7280; }
.step.done   .circle { background: #1e40af; color: #fff; }
.step.active .circle { background: #3b82f6; color: #fff; box-shadow: 0 0 0 3px #bfdbfe; }
.step.future .circle { background: #e5e7eb; color: #9ca3af; }
.step.active .label  { color: #1e40af; font-weight: 600; }
.step.done   .label  { color: #1e40af; }
</style>
"""
    st.markdown(style + f'<div class="stepper">{items}</div>', unsafe_allow_html=True)


def reset_downstream(after_step: int):
    """Step N 이후의 하위 단계 캐시만 비운다.
    순서: ① 업로드 / ② 매핑 / ③ DART / ④ ISIN / ⑤ 영문화 / ⑥ 최종매핑 / ⑦ 검증 / ⑧ 변환
    """
    keys: list[str] = []

    def _wipe_final_picks():
        for k in list(st.session_state.keys()):
            if isinstance(k, str) and k.startswith("final_pick__"):
                st.session_state.pop(k, None)

    if after_step <= 1:
        keys += ["std_to_raw", "auto_map", "auto_map_key",
                 "dart_match", "dart_match_key", "dart_company_info",
                 "isin_match", "isin_match_key", "isin_manual_override",
                 "final_std_to_raw",
                 "validation", "extra_keep",
                 "out_df_cache", "out_df_config", "out_df_ready"]
        _wipe_final_picks()
    elif after_step <= 2:
        keys += ["dart_match", "dart_match_key", "dart_company_info",
                 "isin_match", "isin_match_key", "isin_manual_override",
                 "final_std_to_raw", "validation"]
        _wipe_final_picks()
    elif after_step <= 3:
        keys += ["isin_match", "isin_match_key", "isin_manual_override",
                 "final_std_to_raw", "validation"]
        _wipe_final_picks()
    elif after_step <= 4:
        keys += ["final_std_to_raw", "validation"]
        _wipe_final_picks()
    elif after_step <= 5:
        keys += ["validation"]
    for k in keys:
        st.session_state.pop(k, None)


# ── 뱃지·라벨 ────────────────────────────────────────────────────────────────
def kind_badge(kind: str) -> str:
    """kind → HTML 뱃지 (st.markdown 으로 렌더링)."""
    label, color = KIND_LABEL.get(kind, KIND_LABEL["text"])
    return (
        f"<span style='background:{color}22;color:{color};border-radius:4px;"
        f"padding:2px 8px;font-size:11px;font-weight:600;'>{label}</span>"
    )


def badge_source(src: str) -> str:
    """후보 출처 → 짧은 라벨 (영문화 검수 UI 등)."""
    return {
        "internal_db":   "🗂 내부 DB",
        "kipris":        "🏛 KIPRIS",
        "llm":           "🤖 Claude",
        "romanizer":     "🔤 로마자",
        "manual":        "✏️ 수동",
        "official_site": "📋 규칙기반",
        "gs1":           "🧾 GS1",
    }.get(src, src)


# ── secrets.toml 입출력 ──────────────────────────────────────────────────────
def save_secret(key_name: str, value: str) -> Path:
    """mapping_app/.streamlit/secrets.toml 에 key='value' 추가/갱신.
    streamlit 은 시작 시 한 번 secrets.toml 을 읽으므로 저장 후엔 재시작 필요.
    """
    if not value:
        raise ValueError("빈 값은 저장할 수 없습니다")
    secrets_path = Path(__file__).resolve().parent / ".streamlit" / "secrets.toml"
    secrets_path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    if secrets_path.exists():
        lines = secrets_path.read_text(encoding="utf-8").splitlines()

    found = False
    new_lines: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith(key_name) and "=" in stripped:
            head = stripped.split("=", 1)[0].strip()
            if head == key_name:
                new_lines.append(f'{key_name} = "{value}"')
                found = True
                continue
        new_lines.append(line)
    if not found:
        if new_lines and new_lines[-1].strip() != "":
            new_lines.append("")
        new_lines.append(f'{key_name} = "{value}"')

    secrets_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    return secrets_path


def get_translation_keys() -> tuple[str | None, str | None]:
    """secrets.toml 에서 (KIPRIS_PLUS_KEY, ANTHROPIC_API_KEY) 읽기."""
    try:
        kipris = st.secrets.get("KIPRIS_PLUS_KEY", "") or None
    except Exception:
        kipris = None
    try:
        anthr = st.secrets.get("ANTHROPIC_API_KEY", "") or None
    except Exception:
        anthr = None
    return kipris, anthr


# ── 회사명 매칭 키 컬럼 (DART·ISIN 매칭에서 공용) ─────────────────────────────
def raw_col_for_kind(
    target_kind: str,
    std_to_raw: dict[str, str] | None = None,
    std_cols: list[str] | None = None,
    std_kinds: list[str] | None = None,
    raw_df=None,
) -> str | None:
    """target_kind 의 raw 컬럼 찾기.
    1) std_to_raw 가 직접 매핑한 raw 컬럼
    2) raw_df 컬럼명에서 kind 자동 추론 (fallback)
    """
    if std_to_raw is None: std_to_raw = st.session_state.get("std_to_raw", {})
    if std_cols   is None: std_cols   = st.session_state.get("std_columns", [])
    if std_kinds  is None: std_kinds  = st.session_state.get("std_kinds", [])
    if raw_df     is None: raw_df     = st.session_state.get("raw_df")

    raw_columns = list(raw_df.columns) if raw_df is not None else []

    for std, kind in zip(std_cols, std_kinds):
        if kind == target_kind:
            src = std_to_raw.get(std)
            if src in raw_columns:
                return src

    if raw_columns:
        for c in raw_columns:
            if infer_column_kind(c) == target_kind:
                return c
    return None


def company_raw_col(**kw) -> str | None:
    """회사명 raw 컬럼 — 우선순위:
      1) 사용자 ③/④ override (comp_col_override)
      2) std_to_raw + std_kinds 의 kind='company' / 'brand'
      3) raw_df 자동 추론
    """
    raw_df = kw.get("raw_df")
    if raw_df is None:
        raw_df = st.session_state.get("raw_df")
    override = st.session_state.get("comp_col_override")
    if override and raw_df is not None and override in raw_df.columns:
        return override
    return raw_col_for_kind("company", **kw) or raw_col_for_kind("brand", **kw)


def render_company_key_selector(step_key: str) -> str | None:
    """③/④ step 상단 회사명 키 컬럼 selectbox + 미리보기."""
    raw_df = st.session_state.get("raw_df")
    if raw_df is None:
        return None
    raw_cols = [str(c) for c in raw_df.columns]
    if not raw_cols:
        return None

    auto = raw_col_for_kind("company") or raw_col_for_kind("brand")
    current = st.session_state.get("comp_col_override") or auto or raw_cols[0]
    if current not in raw_cols:
        current = raw_cols[0]

    chosen = st.selectbox(
        "🔑 회사명 매칭 키 컬럼 (raw)",
        options=raw_cols,
        index=raw_cols.index(current),
        key=f"comp_col_select__{step_key}",
        help=(
            "KRX/DART 매칭에 사용할 raw 컬럼입니다. "
            "보통 한글 회사명이 들어있는 컬럼을 선택하세요. "
            "여기서 바꾸면 ③·④ 모두에 즉시 반영됩니다."
        ),
    )
    if chosen != st.session_state.get("comp_col_override"):
        st.session_state["comp_col_override"] = chosen
        st.session_state.pop("isin_match", None)
        st.session_state.pop("isin_match_key", None)
        st.session_state.pop("dart_match", None)
        st.session_state.pop("dart_match_key", None)

    try:
        sample = (
            raw_df[chosen].dropna().drop_duplicates().head(5).astype(str).tolist()
        )
        n_unique = int(raw_df[chosen].nunique())
        st.caption(
            f"📋 예시 값: {' · '.join(sample) or '(빈 값)'}  "
            f"&nbsp;|&nbsp;  고유 {n_unique:,}개"
        )
    except Exception:
        pass
    return chosen
