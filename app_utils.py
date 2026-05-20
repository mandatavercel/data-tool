"""
공용 유틸리티 — 환경 감지, 브랜딩(로고) 등.

streamlit_app.py / pages/*.py 어디서든 import 해서 씁니다.
새 헬퍼가 필요해지면 이 파일에 추가하세요 (별도 파일 분리는 함수가 5개+ 될 때).
"""
from __future__ import annotations

import base64
import os
from pathlib import Path


ROOT = Path(__file__).resolve().parent


# ─────────────────────────────────────────────────────────────
# 환경 감지
# ─────────────────────────────────────────────────────────────
def is_streamlit_cloud() -> bool:
    """Streamlit Community Cloud / Teams Cloud 환경인지 감지."""
    if os.environ.get("STREAMLIT_SHARING_MODE"):
        return True
    if "/mount/src" in str(ROOT):
        return True
    if os.environ.get("HOSTNAME", "").startswith("streamlit"):
        return True
    return False


# ─────────────────────────────────────────────────────────────
# 로고 자동 감지 — assets/ 폴더에서 우선순위 순으로 찾기
# 새 포맷을 우선순위로 넣고 싶으면 LOGO_CANDIDATES 수정
# ─────────────────────────────────────────────────────────────
LOGO_CANDIDATES: list[tuple[str, str]] = [
    ("mandata_logo.svg", "image/svg+xml"),
    ("mandata_logo.png", "image/png"),
    ("mandata_logo.jpg", "image/jpeg"),
    ("mandata_logo.jpeg", "image/jpeg"),
    ("logo.svg", "image/svg+xml"),
    ("logo.png", "image/png"),
]


def find_logo() -> tuple[bytes | None, str | None]:
    """assets/ 폴더에서 로고 파일 찾아 (bytes, mime) 반환. 없으면 (None, None)."""
    for name, mime in LOGO_CANDIDATES:
        p = ROOT / "assets" / name
        if p.exists():
            return p.read_bytes(), mime
    return None, None


def get_logo_html(height_px: int = 56, alt: str = "Mandata Data Intelligence") -> str:
    """로고를 inline data: URL로 임베드한 HTML <img>. 로고 파일 없으면 빈 문자열."""
    data, mime = find_logo()
    if not data:
        return ""
    b64 = base64.b64encode(data).decode("ascii")
    return (
        f'<img src="data:{mime};base64,{b64}" '
        f'style="height:{height_px}px;" alt="{alt}" />'
    )


# ─────────────────────────────────────────────────────────────
# Mandata Dark — 공용 테마 CSS 주입
# streamlit_app.py 진입점에서 한 번 호출 → multi-page 모든 페이지에 적용
# ─────────────────────────────────────────────────────────────
_MANDATA_DARK_CSS = """
<!-- Google Fonts: Inter (sans) + JetBrains Mono (code) -->
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">

<style>
/* ── 전역 폰트 ───────────────────────────────────────── */
html, body, [data-testid="stAppViewContainer"], [data-testid="stSidebar"], .stApp {
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
  font-feature-settings: 'cv11', 'ss03';
}
code, pre, .stCodeBlock, [data-testid="stMetricValue"] {
  font-family: 'JetBrains Mono', 'Menlo', 'Monaco', monospace !important;
}

/* ── 타이포 — 헤더 자간/굵기 ────────────────────────── */
h1 { font-size: 2.0rem !important; font-weight: 700 !important; letter-spacing: -0.025em !important; margin-top: 0.5rem !important; }
h2 { font-size: 1.5rem !important; font-weight: 600 !important; letter-spacing: -0.015em !important; }
h3 { font-size: 1.2rem !important; font-weight: 600 !important; }

/* ── 페이지 컨테이너 max-width ──────────────────────── */
.main .block-container {
  padding-top: 2rem !important;
  max-width: 1400px !important;
}
[data-testid="stHeader"] { background-color: transparent !important; }

/* ── 카드(border 컨테이너) ──────────────────────────── */
[data-testid="stVerticalBlockBorderWrapper"] {
  border-color: rgba(255, 255, 255, 0.08) !important;
  border-radius: 12px !important;
  background-color: rgba(255, 255, 255, 0.015) !important;
}

/* ── 입력박스 (TextInput / TextArea / NumberInput) ──── */
.stTextInput input, .stTextArea textarea, .stNumberInput input {
  background-color: #161618 !important;
  border: 1px solid rgba(255, 255, 255, 0.08) !important;
  border-radius: 8px !important;
  color: #F1F5F9 !important;
  transition: border-color 0.15s ease, box-shadow 0.15s ease !important;
}
.stTextInput input:focus, .stTextArea textarea:focus, .stNumberInput input:focus {
  border-color: #F59E0B !important;
  box-shadow: 0 0 0 1px #F59E0B !important;
  outline: none !important;
}

/* ── Selectbox / Multiselect ────────────────────────── */
.stSelectbox [data-baseweb="select"] > div,
.stMultiSelect [data-baseweb="select"] > div {
  background-color: #161618 !important;
  border-color: rgba(255, 255, 255, 0.08) !important;
  border-radius: 8px !important;
}

/* ── 버튼 ─────────────────────────────────────────── */
.stButton button, .stDownloadButton button, .stFormSubmitButton button {
  border-radius: 8px !important;
  font-weight: 500 !important;
  transition: all 0.15s ease !important;
  border: 1px solid rgba(255, 255, 255, 0.12) !important;
  background-color: transparent !important;
  color: #F1F5F9 !important;
}
.stButton button:hover, .stDownloadButton button:hover, .stFormSubmitButton button:hover {
  border-color: rgba(245, 158, 11, 0.5) !important;
  color: #F59E0B !important;
  background-color: rgba(245, 158, 11, 0.05) !important;
}
.stButton button[kind="primary"], .stDownloadButton button[kind="primary"], .stFormSubmitButton button[kind="primary"] {
  background-color: #F59E0B !important;
  color: #0A0A0B !important;
  border: 1px solid #F59E0B !important;
  font-weight: 600 !important;
}
.stButton button[kind="primary"]:hover, .stDownloadButton button[kind="primary"]:hover, .stFormSubmitButton button[kind="primary"]:hover {
  background-color: #FBBF24 !important;
  border-color: #FBBF24 !important;
  color: #0A0A0B !important;
  transform: translateY(-1px) !important;
}
.stButton button:disabled {
  opacity: 0.4 !important;
  cursor: not-allowed !important;
}

/* ── 사이드바 ──────────────────────────────────────── */
[data-testid="stSidebar"] {
  border-right: 1px solid rgba(255, 255, 255, 0.06) !important;
  background-color: #0D0D0E !important;
}
[data-testid="stSidebarNav"] a, [data-testid="stSidebarNav"] li > div {
  border-radius: 6px !important;
  transition: all 0.15s ease !important;
}
[data-testid="stSidebarNav"] a:hover {
  background-color: rgba(245, 158, 11, 0.08) !important;
}
[data-testid="stSidebarNav"] a[aria-current="page"] {
  background-color: rgba(245, 158, 11, 0.12) !important;
  color: #F59E0B !important;
}

/* ── 메트릭 (st.metric) ─────────────────────────────── */
[data-testid="stMetricValue"] {
  font-size: 1.75rem !important;
  font-weight: 600 !important;
  letter-spacing: -0.025em !important;
  color: #F1F5F9 !important;
}
[data-testid="stMetricLabel"] {
  font-size: 0.8rem !important;
  font-weight: 500 !important;
  color: rgba(241, 245, 249, 0.65) !important;
  text-transform: uppercase !important;
  letter-spacing: 0.06em !important;
}
[data-testid="stMetricDelta"] { font-weight: 500 !important; }

/* ── 캡션 ─────────────────────────────────────────── */
[data-testid="stCaptionContainer"], .stCaption, small {
  color: rgba(241, 245, 249, 0.55) !important;
}

/* ── 탭 (st.tabs) ─────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
  border-bottom: 1px solid rgba(255, 255, 255, 0.08) !important;
  gap: 6px !important;
}
.stTabs [data-baseweb="tab"] {
  font-weight: 500 !important;
  color: rgba(241, 245, 249, 0.55) !important;
  padding: 8px 14px !important;
}
.stTabs [data-baseweb="tab"]:hover { color: #F1F5F9 !important; }
.stTabs [data-baseweb="tab"][aria-selected="true"] {
  color: #F59E0B !important;
}
.stTabs [data-baseweb="tab-highlight"] { background-color: #F59E0B !important; }

/* ── DataFrame / Table ─────────────────────────────── */
.stDataFrame, [data-testid="stDataFrame"] {
  border-radius: 8px !important;
  border: 1px solid rgba(255, 255, 255, 0.06) !important;
  overflow: hidden !important;
}

/* ── 코드 블록 ─────────────────────────────────────── */
.stCodeBlock, pre {
  background-color: #161618 !important;
  border-radius: 8px !important;
  border: 1px solid rgba(255, 255, 255, 0.06) !important;
}

/* ── Alert (info/warning/success/error) ────────────── */
[data-testid="stAlert"], [data-testid="stNotification"] {
  border-radius: 10px !important;
  border-left-width: 4px !important;
}

/* ── Expander ─────────────────────────────────────── */
[data-testid="stExpander"] details {
  border-radius: 10px !important;
  border: 1px solid rgba(255, 255, 255, 0.08) !important;
}
.streamlit-expanderHeader { border-radius: 10px !important; }

/* ── File uploader ────────────────────────────────── */
[data-testid="stFileUploaderDropzone"] {
  background-color: #161618 !important;
  border: 1.5px dashed rgba(255, 255, 255, 0.12) !important;
  border-radius: 10px !important;
}
[data-testid="stFileUploaderDropzone"]:hover {
  border-color: rgba(245, 158, 11, 0.5) !important;
}

/* ── 링크 ─────────────────────────────────────────── */
a {
  color: #F59E0B !important;
  text-decoration: none !important;
}
a:hover { text-decoration: underline !important; }

/* ── 라디오/체크박스 라벨 (가독성) ─────────────────── */
.stRadio label, .stCheckbox label { font-weight: 500 !important; }

/* ── Progress bar ─────────────────────────────────── */
.stProgress > div > div > div > div { background-color: #F59E0B !important; }
</style>
"""


def inject_theme_css() -> None:
    """Mandata Dark 테마 CSS를 현재 페이지에 주입. streamlit_app.py 진입점에서 호출."""
    import streamlit as st  # 지연 import (app_utils가 다른 곳에서 streamlit 없이 import 될 수도)
    st.markdown(_MANDATA_DARK_CSS, unsafe_allow_html=True)
