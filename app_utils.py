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
