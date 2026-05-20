"""
대시보드 (Launcher) — 로그인된 모든 사용자에게 보여지는 홈 페이지.
카테고리 칩 + 권한 있는 앱 카드 그리드.
"""
import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가 → auth, pages_registry, app_utils import
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

import streamlit as st
import auth
import app_utils
from pages_registry import (
    PAGES,
    PAGES_BY_KEY,
    CATEGORY_ICONS,
    launcher_pages,
    all_categories_in_use,
)


# ─────────────────────────────────────────────────────────────
# 권한 체크 — 로그인만 되면 대시보드 진입 가능
# ─────────────────────────────────────────────────────────────
email = auth.get_current_email()
if not email:
    st.error("🔒 로그인이 필요합니다.")
    st.stop()


# ─────────────────────────────────────────────────────────────
# 헤더
# ─────────────────────────────────────────────────────────────
_logo = app_utils.get_logo_html(56)
if _logo:
    st.markdown(
        f"""
        <div style="display:flex; align-items:center; gap:18px; margin:0 0 4px 0;">
          {_logo}
          <div style="line-height:1.1;">
            <div style="font-size:1.05rem; font-weight:600; color:#0F172A;">Internal Tool Launcher</div>
            <div style="font-size:0.85rem; color:rgba(15,23,42,0.6); margin-top:2px;">
              카테고리로 필터링하고, 카드의 <b>열기</b>를 누르면 앱이 열립니다.
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
else:
    st.title("Mandata Data Intelligence")
    st.caption("카테고리로 필터링하고, 카드의 '열기'를 누르면 앱이 열립니다.")


# ─────────────────────────────────────────────────────────────
# 권한 있는 앱만 필터 (admin은 전체)
# ─────────────────────────────────────────────────────────────
is_admin = auth.is_admin(email)
launcher_all = launcher_pages()
if is_admin:
    visible = list(launcher_all)
else:
    visible = [p for p in launcher_all if auth.has_access(p.key, email)]


# ─────────────────────────────────────────────────────────────
# 카테고리 칩
# ─────────────────────────────────────────────────────────────
cats_in_use: list[str] = []
seen: set = set()
for cat in all_categories_in_use():
    if cat in ("Home", "Admin"):
        continue
    if cat not in seen and any(p.category == cat for p in visible):
        seen.add(cat)
        cats_in_use.append(cat)


def _chip_label(cat: str) -> str:
    icon = CATEGORY_ICONS.get(cat, "🏷️")
    n = sum(1 for p in visible if p.category == cat)
    return f"{icon} {cat} ({n})"


chip_labels = [f"🌐 All ({len(visible)})"] + [_chip_label(c) for c in cats_in_use]
chip_to_cat: dict = {f"🌐 All ({len(visible)})": None}
for c in cats_in_use:
    chip_to_cat[_chip_label(c)] = c

default_label = st.session_state.get("_launcher_chip", chip_labels[0])
if default_label not in chip_labels:
    default_label = chip_labels[0]


def _show_filter():
    if hasattr(st, "pills"):
        return st.pills(
            "카테고리 필터",
            options=chip_labels,
            default=default_label,
            label_visibility="collapsed",
            key="_launcher_chip",
        )
    if hasattr(st, "segmented_control"):
        return st.segmented_control(
            "카테고리 필터",
            options=chip_labels,
            default=default_label,
            label_visibility="collapsed",
            key="_launcher_chip",
        )
    return st.selectbox("카테고리", chip_labels, key="_launcher_chip")


selected_label = _show_filter() or chip_labels[0]
selected_category = chip_to_cat.get(selected_label, None)

filtered = visible if selected_category is None else [p for p in visible if p.category == selected_category]

st.divider()


# ─────────────────────────────────────────────────────────────
# 카드 그리드 (3열 컴팩트)
# ─────────────────────────────────────────────────────────────
if not filtered:
    st.info(f"이 카테고리에는 아직 앱이 없어요. (선택: {selected_category or 'All'})")
else:
    N_COLS = 3
    for i in range(0, len(filtered), N_COLS):
        cols = st.columns(N_COLS, gap="small")
        for j, col in enumerate(cols):
            idx = i + j
            if idx >= len(filtered):
                with col:
                    st.empty()
                continue
            app = filtered[idx]
            cat_icon = CATEGORY_ICONS.get(app.category, "🏷️")

            with col:
                with st.container(border=True):
                    # 1행: 이름
                    st.markdown(f"**{app.icon}  {app.name}**")
                    # 2행: 카테고리
                    st.caption(f"{cat_icon} {app.category}")
                    # 3행: 설명 (2~3줄 자동 줄바꿈, 카드 높이 통일)
                    st.markdown(
                        f"<div style='font-size:0.85em; line-height:1.45; "
                        f"color:rgba(49,51,63,0.7); margin:0.25rem 0 0.5rem 0; "
                        f"min-height:3.6em;'>{app.description}</div>",
                        unsafe_allow_html=True,
                    )

                    # 열기 버튼 — st.switch_page로 해당 페이지로 이동
                    if st.button("열기 →", key=f"open_{app.key}", type="primary", use_container_width=True):
                        st.switch_page(app.entry_file)


# ─────────────────────────────────────────────────────────────
# 푸터
# ─────────────────────────────────────────────────────────────
st.divider()
n_total = len(launcher_all)
n_visible = len(visible)
if is_admin:
    st.caption(f"🛡 관리자 모드 — 전체 앱 {n_total}개 표시")
else:
    st.caption(f"📦 접근 가능한 앱 {n_visible}개 / 전체 {n_total}개")
