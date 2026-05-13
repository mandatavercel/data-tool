"""
영구 저장 — DART API Key 등 사용자 비밀 정보를 디스크에 보존.

저장 위치: ~/.mandata_analysis/config.json (chmod 600 — 소유자만 읽기/쓰기)
파일 형식: JSON dict (key→value).

분석 앱 모듈 어디서나 import해 사용:
    from analysis_app.secrets_store import (
        PERSIST_PATH, load_persistent_secrets, save_persistent_secret,
        seed_session_state,
    )
"""
from __future__ import annotations

import json as _json
from pathlib import Path
import streamlit as st


# 저장 경로 (외부에서도 표시용으로 참조)
PERSIST_PATH = Path.home() / ".mandata_analysis" / "config.json"

# session_state에 시드할 영구→세션 키 매핑
# (디스크의 key명) → (Streamlit widget key명)
_SEED_KEYS: list[tuple[str, str]] = [
    ("dart_api_key", "p_dart_key"),
]


def load_persistent_secrets() -> dict:
    """디스크의 영구 저장소를 읽어 dict 반환. 파일 없거나 오류면 빈 dict."""
    try:
        if PERSIST_PATH.exists():
            return _json.loads(PERSIST_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def save_persistent_secret(key: str, value: str) -> bool:
    """key=value를 디스크에 저장. value 비어있으면 키 삭제. 성공 시 True."""
    try:
        PERSIST_PATH.parent.mkdir(parents=True, exist_ok=True)
        cfg = load_persistent_secrets()
        if value:
            cfg[key] = value
        else:
            cfg.pop(key, None)
        PERSIST_PATH.write_text(
            _json.dumps(cfg, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        try:
            PERSIST_PATH.chmod(0o600)   # 소유자만 읽기/쓰기
        except Exception:
            pass
        return True
    except Exception:
        return False


def _load_streamlit_secrets() -> dict:
    """Streamlit Cloud / .streamlit/secrets.toml에서 secrets 읽기.

    Cloud 배포 시 사용 — 사용자가 .streamlit/secrets.toml 또는 Streamlit Cloud
    Secrets management에 `DART_API_KEY = "..."` 형태로 저장하면 자동 로드.
    """
    out: dict = {}
    try:
        # st.secrets는 키 없으면 AttributeError/KeyError
        if "DART_API_KEY" in st.secrets:
            out["dart_api_key"] = str(st.secrets["DART_API_KEY"])
    except Exception:
        pass
    return out


def seed_session_state() -> None:
    """앱 세션 시작 시 1회 — 영구 저장된 secrets를 streamlit session_state로 시드.

    우선순위:
      1) Streamlit Cloud / secrets.toml의 DART_API_KEY (cloud 배포 시 자동)
      2) 로컬 영구 저장 ~/.mandata_analysis/config.json (로컬 macOS 환경)
      3) 사용자 직접 입력

    main app entry point에서 한 번 호출하면 모든 위젯이 기본값으로 사용.
    """
    if "_secrets_loaded" in st.session_state:
        return

    # 1) Streamlit Cloud / secrets.toml 우선 (cloud 배포 시)
    cloud_secrets = _load_streamlit_secrets()
    # 2) 로컬 영구 저장 (cloud secrets에 없는 키 채움)
    local_secrets = load_persistent_secrets()
    cfg = {**local_secrets, **cloud_secrets}   # cloud가 우선

    for persist_key, session_key in _SEED_KEYS:
        if persist_key in cfg and session_key not in st.session_state:
            st.session_state[session_key] = cfg[persist_key]
    st.session_state["_secrets_loaded"] = True
