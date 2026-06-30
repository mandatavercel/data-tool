"""
kfnb_app/utils/secrets_store.py — 로컬 키 보관(DART 등) 헬퍼.

API 키를 매번 입력하지 않도록 사용자 홈(~/.kfnb/)에 평문으로 저장한다.
로컬 데스크톱 도구 전용 — 저장소(git)에 커밋되지 않으며, 우선순위는
st.secrets / 환경변수 > 저장파일 순으로 호출부에서 조합한다.
"""
from __future__ import annotations

import os
from pathlib import Path

_DIR = Path.home() / ".kfnb"


def _path(name: str) -> Path:
    return _DIR / f"{name}.key"


def load_key(name: str = "dart") -> str:
    """저장된 키 로드. 없으면 빈 문자열."""
    try:
        return _path(name).read_text(encoding="utf-8").strip()
    except Exception:                              # noqa: BLE001
        return ""


def save_key(value: str, name: str = "dart") -> bool:
    """키를 ~/.kfnb/<name>.key 에 저장(평문, 권한 600)."""
    try:
        _DIR.mkdir(parents=True, exist_ok=True)
        p = _path(name)
        p.write_text((value or "").strip(), encoding="utf-8")
        try:
            os.chmod(p, 0o600)
        except Exception:                          # noqa: BLE001
            pass
        return True
    except Exception:                              # noqa: BLE001
        return False


def clear_key(name: str = "dart") -> None:
    try:
        _path(name).unlink()
    except Exception:                              # noqa: BLE001
        pass
