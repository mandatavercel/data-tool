"""
Mandata Data Intelligence — 메인 엔트리 포인트
==============================================
- 로컬: `streamlit run streamlit_app.py` (단일 진입)
- Streamlit Cloud: 동일 entry, viewer auth 로 사용자 식별

확장성:
  새 앱 추가 = pages_registry.py에 entry 1개 + pages/<key>.py wrapper 1개
  이 파일은 거의 손댈 일 없음.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import streamlit as st


# ─────────────────────────────────────────────────────────────
# 1) Page config (반드시 첫 streamlit 호출)
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Mandata Data Intelligence",
    page_icon="📊",
    layout="wide",
)

# 2) 이후 legacy 앱들이 set_page_config을 다시 부르면 silently 무시
#    (multi-page에서 모든 sub-app이 자기 page_config를 갖고 있어서 충돌 방지)
_orig_set_page_config = st.set_page_config

def _safe_set_page_config(*args, **kwargs):
    try:
        _orig_set_page_config(*args, **kwargs)
    except Exception:
        pass

st.set_page_config = _safe_set_page_config


# ─────────────────────────────────────────────────────────────
# 3) 모듈 import (page_config 이후)
# ─────────────────────────────────────────────────────────────
import auth  # noqa: E402
import app_utils  # noqa: E402
from pages_registry import (  # noqa: E402
    PAGES,
    PAGES_BY_KEY,
    CATEGORY_ICONS,
    all_categories_in_use,
)


# Mandata Dark 테마 CSS 주입 — 모든 페이지에 일관 적용
app_utils.inject_theme_css()


_running_on_cloud = app_utils.is_streamlit_cloud()


# ─────────────────────────────────────────────────────────────
# 5) 현재 사용자
# ─────────────────────────────────────────────────────────────
_current_email = auth.get_current_email()


# ─────────────────────────────────────────────────────────────
# 6) 사이드바 — 사용자 정보 (모든 페이지에 공통 표시)
# ─────────────────────────────────────────────────────────────
with st.sidebar:
    if _current_email:
        st.markdown(f"👤 **{_current_email}**")
        if auth.is_admin(_current_email):
            st.caption("🛡 관리자 권한")
        # 로그아웃 (cloud + 로컬 둘 다 — _dev_email session 기반)
        if st.session_state.get("_dev_email"):
            if st.button("🚪 로그아웃", use_container_width=True):
                st.session_state.pop("_dev_email", None)
                st.rerun()
    else:
        st.caption("🔒 로그인되지 않음")


# ─────────────────────────────────────────────────────────────
# 7) 로그인 화면 — st.navigation으로 단일 페이지 띄움 (사이드바 깨끗)
# ─────────────────────────────────────────────────────────────
def render_login_screen():
    """로그인되지 않은 사용자에게 보여줄 화면."""
    _logo_img = app_utils.get_logo_html(64)
    if _logo_img:
        st.markdown(
            f"<div style='display:flex; align-items:center; gap:18px; margin:16px 0 8px 0;'>{_logo_img}</div>",
            unsafe_allow_html=True,
        )
    st.title("Mandata Data Intelligence")

    # 공유 패스워드 (선택 — secrets.toml에 LOGIN_PASSWORD가 있으면 활성화)
    login_password = ""
    try:
        login_password = str(st.secrets.get("LOGIN_PASSWORD", "")).strip()
    except Exception:
        login_password = ""

    if login_password:
        st.info("초대받은 이메일로 로그인해주세요. 공유 패스워드 + 이메일이 필요합니다.")
    else:
        st.info(
            "초대받은 이메일로 로그인해주세요. "
            "권한이 있는 이메일만 접근 가능합니다 (관리자 명단 + 도메인 와일드카드 + 앱별 초대 기준)."
        )

    with st.form("_login_form", clear_on_submit=False):
        if login_password:
            pwd = st.text_input("공유 패스워드", type="password")
        else:
            pwd = ""
        email_in = st.text_input("이메일", placeholder="you@mandata.kr")
        submitted = st.form_submit_button("로그인", type="primary")
        if submitted:
            if login_password and pwd != login_password:
                st.error("패스워드가 틀려요.")
            elif not email_in.strip():
                st.error("이메일을 입력해주세요.")
            else:
                st.session_state["_dev_email"] = email_in.strip().lower()
                st.rerun()

    if _running_on_cloud:
        st.caption(
            "⚠️ Streamlit Community Cloud 무료 티어에는 빌트인 OAuth가 없어, "
            "이메일 입력 후 ACL(`acl.json`)로 권한을 확인합니다. "
            "정식 SSO가 필요하면 Streamlit Cloud Connect (유료) 또는 OIDC 구성을 고려하세요."
        )


if not _current_email:
    _login_page = st.Page(render_login_screen, title="Login", icon="🔒", default=True)
    _login_nav = st.navigation([_login_page], position="hidden")
    _login_nav.run()
    st.stop()


# ─────────────────────────────────────────────────────────────
# 8) 사용자가 접근 가능한 페이지 목록 만들기 (권한 필터)
# ─────────────────────────────────────────────────────────────
_is_admin = auth.is_admin(_current_email)


def _user_can_see(p) -> bool:
    """이 사용자에게 사이드바 nav에 노출할지."""
    if p.admin_only:
        return _is_admin
    if p.key == "launcher":
        return True  # 대시보드는 로그인된 모두에게
    return _is_admin or auth.has_access(p.key, _current_email)


visible_entries = [p for p in PAGES if _user_can_see(p)]

# 권한 0개면 안내
if not visible_entries or (len(visible_entries) == 1 and visible_entries[0].key == "launcher"
                           and not _is_admin and not any(
                               auth.has_access(p.key, _current_email)
                               for p in PAGES if not p.admin_only and p.key != "launcher"
                           )):
    # launcher만 보이는데 그 안에 아무 카드도 안 보이는 경우 = 사실상 권한 0개
    if not _is_admin and not any(
        auth.has_access(p.key, _current_email)
        for p in PAGES if not p.admin_only and p.key != "launcher"
    ):
        auth.render_access_denied("Mandata Data Intelligence")
        st.stop()


# ─────────────────────────────────────────────────────────────
# 9) st.navigation — 카테고리별 그룹으로 사이드바 구성
# ─────────────────────────────────────────────────────────────
def _category_label(cat: str) -> str:
    if not cat:
        return "기타"
    icon = CATEGORY_ICONS.get(cat, "")
    return f"{icon} {cat}".strip() if icon else cat


# 카테고리별로 그룹핑 (등장 순서 유지)
groups: dict[str, list] = {}

# Home (대시보드)은 항상 맨 위
home_pages = [p for p in visible_entries if p.category == "Home"]
if home_pages:
    home_group_label = _category_label("Home")
    groups[home_group_label] = [
        st.Page(str(p.absolute_entry), title=p.name, icon=p.icon, default=(p.key == "launcher"))
        for p in home_pages
    ]

# 그 외 카테고리들
for cat in all_categories_in_use():
    if cat in ("Home", "Admin"):
        continue
    cat_pages = [p for p in visible_entries if p.category == cat]
    if not cat_pages:
        continue
    label = _category_label(cat)
    groups[label] = [
        st.Page(str(p.absolute_entry), title=p.name, icon=p.icon)
        for p in cat_pages
    ]

# Admin은 항상 맨 아래
admin_pages = [p for p in visible_entries if p.category == "Admin"]
if admin_pages:
    admin_group_label = _category_label("Admin")
    groups[admin_group_label] = [
        st.Page(str(p.absolute_entry), title=p.name, icon=p.icon)
        for p in admin_pages
    ]


# 안전망: 그룹이 비면 launcher만이라도
if not groups:
    launcher_entry = PAGES_BY_KEY.get("launcher")
    if launcher_entry:
        groups["🏠 Home"] = [
            st.Page(str(launcher_entry.absolute_entry), title=launcher_entry.name,
                    icon=launcher_entry.icon, default=True)
        ]


# ─────────────────────────────────────────────────────────────
# 10) 네비게이션 실행
# ─────────────────────────────────────────────────────────────
nav = st.navigation(groups, position="sidebar", expanded=True)
nav.run()
