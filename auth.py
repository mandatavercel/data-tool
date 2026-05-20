"""
권한(ACL) 모듈 — Mandata Data Intelligence
============================================
사용 흐름:
  1. acl.json 로드 → get_acl()
  2. 현재 사용자 이메일 확인 → get_current_email()
     - Streamlit Cloud: st.user.email (viewer auth)
     - 로컬 개발: 환경변수 MANDATA_DEV_EMAIL
  3. 권한 체크:
     - is_admin(email)
     - has_access(email, page_key)
     - accessible_pages(email) → 그 사람이 볼 수 있는 page_key 목록

ACL 스키마 (acl.json):
  {
    "admins": ["yonghan@mandata.kr"],
    "default_access": ["*@mandata.kr"],   # 도메인 와일드카드 OK
    "page_access": {
      "analysis": ["a@b.com"],            # 추가 invitee
      "mapping": [],
      ...
    }
  }
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Optional

import streamlit as st


# acl.json 위치 — streamlit_app.py와 같은 폴더
_ACL_PATH = Path(__file__).resolve().parent / "acl.json"


# ─────────────────────────────────────────────────────────────
# ACL load / save
# ─────────────────────────────────────────────────────────────
def _empty_acl() -> dict:
    return {
        "admins": [],
        "default_access": [],
        "page_access": {},
    }


def get_acl() -> dict:
    """acl.json을 로드. 파일 없거나 파싱 실패면 빈 ACL 반환."""
    if not _ACL_PATH.exists():
        return _empty_acl()
    try:
        data = json.loads(_ACL_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return _empty_acl()
    # 정합성 보정
    data.setdefault("admins", [])
    data.setdefault("default_access", [])
    data.setdefault("page_access", {})
    return data


def save_acl(data: dict) -> None:
    """ACL을 파일에 저장. Streamlit Cloud는 ephemeral fs라 보존 보장 안 됨 (admin 페이지에서 export 권장)."""
    # 내부 키 정리 (_schema, _notes는 보존)
    out = {k: v for k, v in data.items()}
    _ACL_PATH.write_text(
        json.dumps(out, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ─────────────────────────────────────────────────────────────
# 현재 사용자
# ─────────────────────────────────────────────────────────────
def get_current_email() -> Optional[str]:
    """
    현재 로그인된 사용자의 이메일 반환.
    우선순위:
      1. st.user.email (Streamlit Cloud viewer auth)
      2. 환경변수 MANDATA_DEV_EMAIL (로컬 개발용)
      3. st.session_state["_dev_email"] (UI에서 임시 로그인 입력)
      4. None (비로그인)
    """
    # 1) Streamlit Cloud의 viewer auth
    try:
        user = getattr(st, "user", None)
        if user is not None:
            email = getattr(user, "email", None)
            if email:
                return email.lower().strip()
    except Exception:
        pass

    # 2) 환경변수 (로컬)
    env_email = os.environ.get("MANDATA_DEV_EMAIL", "").strip()
    if env_email:
        return env_email.lower()

    # 3) session_state 임시 로그인 (개발용)
    sess_email = st.session_state.get("_dev_email", "").strip() if hasattr(st, "session_state") else ""
    if sess_email:
        return sess_email.lower()

    return None


def is_logged_in() -> bool:
    return get_current_email() is not None


# ─────────────────────────────────────────────────────────────
# 매칭 헬퍼 — 와일드카드 도메인 지원
# ─────────────────────────────────────────────────────────────
def _matches_rule(email: str, rule: str) -> bool:
    """
    rule이 email과 매칭되는지.
    예:
      _matches_rule("a@mandata.kr", "*@mandata.kr") → True
      _matches_rule("a@mandata.kr", "a@mandata.kr") → True
      _matches_rule("a@mandata.kr", "*") → True
      _matches_rule("a@mandata.kr", "b@mandata.kr") → False
    """
    if not email or not rule:
        return False
    email = email.lower().strip()
    rule = rule.lower().strip()
    if rule == "*":
        return True
    # 와일드카드 *@도메인  →  정규식
    if rule.startswith("*@"):
        domain = rule[2:]
        return email.endswith("@" + domain)
    return email == rule


def _matches_any(email: str, rules: list) -> bool:
    return any(_matches_rule(email, r) for r in (rules or []))


# ─────────────────────────────────────────────────────────────
# 권한 체크 API
# ─────────────────────────────────────────────────────────────
def is_admin(email: Optional[str] = None) -> bool:
    """admins 목록에 매칭되는지."""
    email = email or get_current_email()
    if not email:
        return False
    return _matches_any(email, get_acl().get("admins", []))


def has_access(page_key: str, email: Optional[str] = None) -> bool:
    """
    특정 페이지(page_key)에 접근 권한이 있는지.
    True 조건 (OR):
      - admin
      - default_access 규칙 매칭
      - page_access[page_key]에 매칭
    """
    email = email or get_current_email()
    if not email:
        return False
    acl = get_acl()
    if _matches_any(email, acl.get("admins", [])):
        return True
    if _matches_any(email, acl.get("default_access", [])):
        return True
    page_list = acl.get("page_access", {}).get(page_key, [])
    return _matches_any(email, page_list)


def accessible_pages(all_page_keys: list, email: Optional[str] = None) -> list:
    """전체 page_key 중 이 사용자가 접근 가능한 것들만 반환 (순서 유지)."""
    email = email or get_current_email()
    if not email:
        return []
    return [k for k in all_page_keys if has_access(k, email)]


# ─────────────────────────────────────────────────────────────
# UI 헬퍼 (선택)
# ─────────────────────────────────────────────────────────────
def render_login_widget(prompt: str = "이메일로 임시 로그인 (개발용)"):
    """
    Streamlit Cloud에 배포되지 않은 환경(로컬 등)에서 임시 로그인 입력.
    st.session_state["_dev_email"] 에 저장.
    """
    with st.form("_dev_login_form", clear_on_submit=False):
        email = st.text_input(prompt, value=st.session_state.get("_dev_email", ""))
        submitted = st.form_submit_button("로그인")
        if submitted:
            st.session_state["_dev_email"] = email.strip().lower()
            st.rerun()


def render_access_denied(page_name: str = "이 페이지"):
    """공통 '접근 권한 없음' 화면."""
    email = get_current_email() or "(비로그인)"
    st.error(f"❌ {page_name}에 접근 권한이 없습니다.")
    st.caption(f"현재 계정: `{email}`")
    st.caption("권한 요청은 관리자에게 문의해주세요.")


# ─────────────────────────────────────────────────────────────
# 페이지 wrapper 공통 헬퍼 — pages/ 안의 각 .py 파일에서 사용
# ─────────────────────────────────────────────────────────────
def gate(page_key: str, page_name: Optional[str] = None) -> str:
    """
    페이지 진입 시 권한 체크. 권한 없으면 render + st.stop().
    반환: 현재 사용자 이메일 (편의)
    """
    email = get_current_email()
    if not email:
        st.error("🔒 로그인이 필요합니다. 메인 페이지에서 로그인 후 다시 시도하세요.")
        st.stop()
    if not has_access(page_key, email):
        render_access_denied(page_name or page_key)
        st.stop()
    return email


def run_legacy_app(folder: str, entry_file: str) -> None:
    """
    기존 앱 폴더의 entry 파일을 in-process로 실행 (subprocess 없이, multi-page용).
    - sys.path에 앱 폴더 추가 (앱이 자기 모듈 import 할 수 있게)
    - cwd를 앱 폴더로 옮김 (.streamlit/config.toml, data/ 같은 상대경로 보존)
    - runpy로 실행, 실행 후 cwd 복귀
    - st.set_page_config 중복 호출은 자동으로 no-op 처리됨 (streamlit_app.py에서 monkey-patch)
    """
    import os
    import runpy
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parent
    app_dir = root / folder if folder else root
    entry = app_dir / entry_file

    if not entry.exists():
        st.error(f"앱 파일을 찾을 수 없어요: `{entry}`")
        st.stop()

    # sys.path에 ROOT와 앱 폴더 추가 (중복 방지)
    for p in (str(root), str(app_dir)):
        if p not in sys.path:
            sys.path.insert(0, p)

    old_cwd = os.getcwd()
    try:
        os.chdir(app_dir)
        runpy.run_path(str(entry), run_name="__main__")
    finally:
        try:
            os.chdir(old_cwd)
        except Exception:
            pass
