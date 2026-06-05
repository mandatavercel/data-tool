"""
Mandata Contract Builder — 표준 계약서 생성 도구
===================================================
질문 답변 → docx 초안 자동 생성. 첫 양식은 국내 데이터 공급 계약서(DSA Domestic v1.0).
신규 양식은 contract_app/templates/<key>/ 폴더에 manifest.json + schema.json + template.docx
세 파일만 추가하면 자동으로 인식된다.

로컬 실행:
    streamlit run contract_app/app.py --server.port 8506
또는 통합 런처의 ▶실행.
"""
from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path

import streamlit as st

# 상위 디렉토리를 path에 추가 (단독 실행 시)
import sys
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from contract_app import generator, registry


# ─────────────────────────────────────────────────────────────
# Page config (런처에서 import되면 무시됨)
# ─────────────────────────────────────────────────────────────
try:
    st.set_page_config(
        page_title="계약서 생성기",
        page_icon="📜",
        layout="wide",
        initial_sidebar_state="expanded",
    )
except Exception:
    pass


# ─────────────────────────────────────────────────────────────
# 헬퍼
# ─────────────────────────────────────────────────────────────
SS_KEY_PREFIX = "contract_app::"


def _ss_key(template_key: str, var: str) -> str:
    return f"{SS_KEY_PREFIX}{template_key}::{var}"


def _safe_filename(name: str) -> str:
    """파일명에 안전한 문자만 남김."""
    name = re.sub(r"[^\w가-힣\-_.\s]", "", name).strip()
    return re.sub(r"\s+", "_", name)


def _collect_answers(template_key: str, schema: dict) -> dict[str, str]:
    """session_state에서 답안을 수집. 빈 값이면 default로 폴백."""
    out: dict[str, str] = {}
    for g in schema.get("groups", []):
        for sec in g.get("sections", []):
            for f in sec.get("fields", []):
                k = _ss_key(template_key, f["var"])
                v = st.session_state.get(k, "")
                if v is None or (isinstance(v, str) and not v.strip()):
                    v = f.get("default", "")
                out[f["var"]] = "" if v is None else str(v)
    return out


def _fill_pct(template_key: str, schema: dict) -> tuple[int, int]:
    filled = 0
    total = 0
    for g in schema.get("groups", []):
        for sec in g.get("sections", []):
            for f in sec.get("fields", []):
                total += 1
                k = _ss_key(template_key, f["var"])
                v = st.session_state.get(k, "")
                if (v and str(v).strip()) or f.get("default"):
                    filled += 1
    return filled, total


# ─────────────────────────────────────────────────────────────
# CSS — 가벼운 추가 스타일
# ─────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    .ct-card {
        background: rgba(255,255,255,0.03);
        border: 1px solid rgba(255,255,255,0.10);
        border-radius: 12px;
        padding: 18px 22px;
        margin-bottom: 14px;
    }
    .ct-pill {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 999px;
        background: rgba(59,130,246,0.15);
        color: #93C5FD;
        font-size: 0.78rem;
        margin-right: 6px;
        font-weight: 600;
    }
    .ct-pill.scope { background: rgba(34,197,94,0.15); color: #86EFAC; }
    .ct-pill.version { background: rgba(168,85,247,0.15); color: #D8B4FE; }
    .ct-section-title {
        color: #FBBF24;
        font-weight: 700;
        font-size: 1.05rem;
        margin: 10px 0 6px 0;
    }
    .ct-field-help { color: #94A3B8; font-size: 0.82rem; line-height: 1.4; }
    .ct-var { font-family: 'Menlo','Consolas',monospace; color:#60A5FA; font-size:0.78rem; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ─────────────────────────────────────────────────────────────
# 양식 선택
# ─────────────────────────────────────────────────────────────
st.title("📜 계약서 생성기")
st.caption("질문에 답하면 맨데이터 표준 양식의 계약서 초안(.docx)이 생성됩니다.")

templates = registry.list_templates(include_drafts=True)
if not templates:
    st.error(
        "등록된 계약서 양식이 없습니다. `contract_app/templates/<key>/` 아래에 "
        "`manifest.json`, `schema.json`, `template.docx` 세 파일을 두면 자동으로 인식됩니다."
    )
    st.stop()

with st.sidebar:
    st.markdown("### 양식 선택")
    options = [m.key for m in templates]
    labels = {m.key: f"{m.name}  v{m.version}" + ("" if m.status == "active" else f"  ({m.status})")
              for m in templates}
    selected_key = st.selectbox(
        "사용할 계약서 양식",
        options=options,
        format_func=lambda k: labels.get(k, k),
        key="contract_app::selected_template",
    )

    st.markdown("---")
    st.markdown("### 입력값 백업")
    uploaded = st.file_uploader("이전 입력값(JSON) 불러오기", type=["json"], key="contract_app::upload_json")
    if uploaded is not None:
        try:
            payload = json.loads(uploaded.read().decode("utf-8"))
            for var, val in payload.get("answers", {}).items():
                st.session_state[_ss_key(selected_key, var)] = val
            st.success(f"{len(payload.get('answers', {}))}개 항목을 불러왔습니다.")
        except Exception as e:
            st.error(f"불러오기 실패: {e}")

    st.markdown("---")
    st.markdown("### 사용 안내")
    st.caption(
        "1) 좌측에서 양식 선택  \n"
        "2) 그룹별 탭에서 답안 입력  \n"
        "3) 하단에서 .docx 다운로드  \n\n"
        "비워두면 기본값으로 자동 채워집니다."
    )


meta = registry.get_template(selected_key)
schema = registry.load_schema(meta)

# 양식 카드
party_a = meta.party_a_role or "갑"
party_b = meta.party_b_role or "을"
st.markdown(
    f"""
    <div class="ct-card">
      <div style="display:flex; align-items:center; gap:14px; flex-wrap:wrap;">
        <div style="font-size:1.35rem; font-weight:700;">{meta.name}</div>
        <span class="ct-pill version">v{meta.version}</span>
        <span class="ct-pill">{meta.type}</span>
        <span class="ct-pill scope">{meta.scope}</span>
        <span class="ct-pill">{meta.language.upper()}</span>
        <span style="color:#64748B; font-size:0.85rem;">{meta.doc_code}</span>
      </div>
      <div style="color:#CBD5E1; margin-top:8px;">{meta.description}</div>
      <div style="color:#94A3B8; margin-top:8px; font-size:0.88rem;">
        <b>갑:</b> {party_a} &nbsp;·&nbsp; <b>을:</b> {party_b}
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# 진척도
filled, total = _fill_pct(selected_key, schema)
pct = int((filled / total) * 100) if total else 0
colA, colB = st.columns([3, 1])
with colA:
    st.progress(pct / 100, text=f"입력 진척도  {filled} / {total}  ({pct}%)")
with colB:
    if st.button("🔄 모든 입력 초기화", use_container_width=True):
        prefix = f"{SS_KEY_PREFIX}{selected_key}::"
        for k in list(st.session_state.keys()):
            if k.startswith(prefix):
                del st.session_state[k]
        st.rerun()


# ─────────────────────────────────────────────────────────────
# 입력 폼 — 그룹별 탭
# ─────────────────────────────────────────────────────────────
groups = schema.get("groups", [])
tab_labels = [f"{g.get('icon','•')} {g['name']}" for g in groups]
tabs = st.tabs(tab_labels)

for tab, g in zip(tabs, groups):
    with tab:
        for sec in g.get("sections", []):
            st.markdown(f"<div class='ct-section-title'>{sec['title']}</div>", unsafe_allow_html=True)
            for f in sec.get("fields", []):
                var = f["var"]
                key = _ss_key(selected_key, var)
                label = f["label"]
                help_text = f.get("desc") or None
                placeholder = f.get("placeholder", "")
                default = f.get("default", "")
                ftype = f.get("type", "text")

                # 라벨에 변수명 회색으로 같이 표시
                label_html = label
                # 초기 default 주입 (session_state에 값이 아예 없을 때만)
                if key not in st.session_state:
                    st.session_state[key] = default or ""

                if ftype == "select":
                    options = f.get("options", [])
                    # default가 옵션에 있으면 그 인덱스를 시작값으로
                    current = st.session_state.get(key, default)
                    if current not in options and default in options:
                        current = default
                    idx = options.index(current) if current in options else 0
                    st.selectbox(
                        f"{label}  ·  {{{{{var}}}}}",
                        options=options,
                        index=idx,
                        key=key,
                        help=help_text,
                    )
                elif ftype == "textarea":
                    st.text_area(
                        f"{label}  ·  {{{{{var}}}}}",
                        key=key,
                        placeholder=placeholder,
                        height=80,
                        help=help_text,
                    )
                else:  # text
                    st.text_input(
                        f"{label}  ·  {{{{{var}}}}}",
                        key=key,
                        placeholder=placeholder,
                        help=help_text,
                    )


# ─────────────────────────────────────────────────────────────
# 미리보기 / 산출
# ─────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("### 📄 초안 생성")

answers = _collect_answers(selected_key, schema)

# 미치환 변수 감지
unfilled = generator.find_unfilled(meta.template_path, answers)
if unfilled:
    with st.expander(f"⚠️ 템플릿에는 있는데 답안에 없는 변수 ({len(unfilled)}개) — 비워두면 빈 칸으로 출력됩니다.", expanded=False):
        st.code("\n".join(unfilled), language="text")

# 짧은 요약 미리보기
preview_keys_priority = [
    ("계약명", "계약명"),
    ("갑_회사명", "갑(원천사)"),
    ("을_회사명", "을(맨데이터)"),
    ("계약체결일", "체결일"),
    ("계약기간", "계약기간"),
    ("과금방식", "과금방식"),
    ("독점성", "독점성"),
    ("재공급_허용여부", "재공급 허용여부"),
]
preview_rows = []
for v, lbl in preview_keys_priority:
    val = (answers.get(v) or "").strip()
    if val:
        preview_rows.append((lbl, val))
if preview_rows:
    with st.expander("👀 핵심 항목 미리보기", expanded=True):
        for lbl, val in preview_rows:
            st.markdown(f"- **{lbl}** — {val}")


col1, col2 = st.columns([2, 1])

with col1:
    # 파일명 구성
    party_a_name = (answers.get("갑_회사명") or "원천사").strip()
    today_str = date.today().strftime("%Y%m%d")
    fname = _safe_filename(f"{meta.type}_{meta.scope}_{party_a_name}_{today_str}.docx") or "계약서_초안.docx"

    docx_bytes = generator.render(meta.template_path, answers)
    st.download_button(
        label=f"📥 .docx 초안 다운로드  —  {fname}",
        data=docx_bytes,
        file_name=fname,
        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        type="primary",
        use_container_width=True,
    )

with col2:
    # 답안 백업
    backup = {
        "template_key": meta.key,
        "template_name": meta.name,
        "version": meta.version,
        "saved_at": date.today().isoformat(),
        "answers": answers,
    }
    st.download_button(
        label="💾 입력값 백업(.json)",
        data=json.dumps(backup, ensure_ascii=False, indent=2),
        file_name=_safe_filename(f"contract_input_{meta.key}_{today_str}.json"),
        mime="application/json",
        use_container_width=True,
    )

# 도움말
with st.expander("ℹ️ 이 도구에 대해 / 새 양식 추가하기"):
    st.markdown(
        f"""
- 모든 양식은 `contract_app/templates/<key>/` 폴더에 있는 세 파일로 정의됩니다.
  - `manifest.json` — 양식 메타데이터(이름, 유형, 갑/을 역할 등)
  - `schema.json` — 질문지 정의(그룹·섹션·필드)
  - `template.docx` — `{{{{변수명}}}}` 자리표시자가 박힌 Word 템플릿
- 신규 양식 추가 절차:
  1. 위 세 파일을 새 폴더에 만든다 (예: `templates/dsa_global_v1/`)
  2. 페이지를 새로고침하면 사이드바 드롭다운에 자동 노출됨
- 향후 로드맵: 글로벌 공급 계약, NDA(상호/단방향), DDQ 응답서, 상품별 부속합의서.

현재 양식 경로:  `{meta.folder.relative_to(_ROOT)}`
"""
    )
