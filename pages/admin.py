"""
관리자 페이지 — ACL 편집.
admin만 접근 가능 (auth.gate에서 통제).
"""
import json as _json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st
import pandas as pd
import auth
import app_utils
import pages_registry
from pages_registry import (
    PAGES,
    PAGES_BY_KEY,
    CATEGORY_ICONS,
    CATEGORIES,
    _BASE_PAGES,
)


# ─────────────────────────────────────────────────────────────
# 권한 — admin만
# ─────────────────────────────────────────────────────────────
email = auth.gate("admin", "관리자 페이지")
if not auth.is_admin(email):
    auth.render_access_denied("관리자 페이지")
    st.stop()


_on_cloud = app_utils.is_streamlit_cloud()


# ─────────────────────────────────────────────────────────────
# Header — 디스크 동기화 / 요약
# ─────────────────────────────────────────────────────────────
top1, top2 = st.columns([1, 5])
with top1:
    if st.button("🔄 디스크 재로드", use_container_width=True,
                 help="acl.json + page_overrides.json에서 다시 읽어 변경사항 폐기"):
        st.session_state["_admin_acl_draft"] = auth.get_acl()
        st.session_state["_admin_overrides_draft"] = pages_registry.load_overrides()
        # 카테고리 편집 selectbox 키도 초기화
        for k in list(st.session_state.keys()):
            if k.startswith("_admin_edit_cat_"):
                del st.session_state[k]
        st.rerun()

st.title("🛡 관리자 페이지")

# Draft 초기화 — ACL
if "_admin_acl_draft" not in st.session_state:
    st.session_state["_admin_acl_draft"] = auth.get_acl()
acl = st.session_state["_admin_acl_draft"]

acl.setdefault("admins", [])
acl.setdefault("default_access", [])
acl.setdefault("page_access", {})
for p in PAGES:
    if not p.admin_only and p.key != "launcher":
        acl["page_access"].setdefault(p.key, [])

# Draft 초기화 — page_overrides (카테고리 등)
if "_admin_overrides_draft" not in st.session_state:
    st.session_state["_admin_overrides_draft"] = pages_registry.load_overrides()
overrides_draft = st.session_state["_admin_overrides_draft"]
overrides_draft.setdefault("category_overrides", {})

# 요약
with top2:
    n_admins = len(acl["admins"])
    n_defaults = len(acl["default_access"])
    n_page_invites = sum(len(v) for v in acl["page_access"].values())
    n_cat_overrides = len(overrides_draft.get("category_overrides", {}))
    st.caption(
        f"📊 관리자 {n_admins}명 · 기본권한 규칙 {n_defaults}개 · "
        f"앱별 추가초대 {n_page_invites}건 · 카테고리 변경 {n_cat_overrides}건  ·  "
        f"현재 사용자: `{email}`"
    )

if _on_cloud:
    st.warning(
        "⚠️ **Streamlit Cloud는 파일이 영구 저장되지 않아요.** "
        "변경 후 반드시 **💾 저장/Export** 탭에서 JSON을 받아 GitHub에 commit해주세요. "
        "(GitHub 변경 감지 시 Cloud가 자동 재배포 → 변경사항 영구 반영)"
    )

st.divider()

# ─────────────────────────────────────────────────────────────
# 탭
# ─────────────────────────────────────────────────────────────
tab_admins, tab_default, tab_pages, tab_categories, tab_matrix, tab_save = st.tabs([
    "🛡 관리자",
    "🌐 기본 권한",
    "📦 앱별 권한",
    "📂 카테고리",
    "👥 사용자 목록",
    "💾 저장 / Export",
])

# ─── Tab 1: 관리자 ────────────────────────────────────────────
with tab_admins:
    st.subheader("관리자 (전체 권한 + Admin 페이지 접근)")
    st.caption(
        "이 명단에 있는 이메일은 모든 페이지에 접근 가능하고 Admin 페이지에도 들어올 수 있어요. "
        "본인을 명단에서 빼면 다음 새로고침부터 들어올 수 없으니 주의."
    )
    df_admins = pd.DataFrame({"이메일": acl["admins"]})
    edited = st.data_editor(
        df_admins,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        key="_admin_edit_admins",
        column_config={
            "이메일": st.column_config.TextColumn("이메일", required=False, help="예: hong@mandata.kr"),
        },
    )
    new_admins = [
        str(e).strip().lower()
        for e in edited["이메일"].tolist()
        if isinstance(e, str) and e.strip()
    ]
    acl["admins"] = list(dict.fromkeys(new_admins))

# ─── Tab 2: 기본 권한 ────────────────────────────────────────
with tab_default:
    st.subheader("기본 권한 (Default Access)")
    st.caption(
        "여기 매칭되는 사용자는 **모든 앱**에 자동 접근. "
        "도메인 와일드카드 지원: `*@mandata.kr` = 그 도메인 전체. "
        "`*` 하나만 적으면 모든 사용자에게 공개 (주의)."
    )
    df_default = pd.DataFrame({"이메일 또는 와일드카드 규칙": acl["default_access"]})
    edited = st.data_editor(
        df_default,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        key="_admin_edit_default",
    )
    new_defaults = [
        str(e).strip().lower()
        for e in edited["이메일 또는 와일드카드 규칙"].tolist()
        if isinstance(e, str) and e.strip()
    ]
    acl["default_access"] = list(dict.fromkeys(new_defaults))

# ─── Tab 3: 앱별 권한 ────────────────────────────────────────
with tab_pages:
    st.subheader("앱별 추가 초대")
    st.caption(
        "기본 권한 외에 **특정 앱만 접근시킬 외부 사용자**를 등록. "
        "와일드카드 도메인도 지원."
    )
    # 레지스트리 순서대로 표시 (launcher/admin 제외)
    for p in PAGES:
        if p.admin_only or p.key == "launcher":
            continue
        current = acl["page_access"].get(p.key, [])
        cat_icon = CATEGORY_ICONS.get(p.category, "🏷️")
        with st.expander(
            f"{p.icon}  {p.name}  ·  {cat_icon} {p.category}  ·  현재 {len(current)}명",
            expanded=(len(current) > 0),
        ):
            st.caption(f"page key: `{p.key}`")
            df_page = pd.DataFrame({"이메일": current})
            edited = st.data_editor(
                df_page,
                num_rows="dynamic",
                use_container_width=True,
                hide_index=True,
                key=f"_admin_edit_page_{p.key}",
            )
            new_list = [
                str(e).strip().lower()
                for e in edited["이메일"].tolist()
                if isinstance(e, str) and e.strip()
            ]
            acl["page_access"][p.key] = list(dict.fromkeys(new_list))

# ─── Tab 4: 카테고리 편집 ────────────────────────────────────
with tab_categories:
    st.subheader("앱별 카테고리 편집")
    st.caption(
        "사이드바 그룹과 대시보드 칩에 사용되는 카테고리를 앱별로 변경. "
        "변경사항은 `page_overrides.json`에 저장되며, 코드(`pages_registry.py`)는 손대지 않아요. "
        "변경 후 **💾 저장/Export** 탭에서 일괄 저장."
    )

    cat_overrides = overrides_draft.setdefault("category_overrides", {})

    # _BASE_PAGES 기준으로 표시 (launcher/admin 제외)
    editable_pages = [
        p for p in _BASE_PAGES
        if not p.admin_only and p.key != "launcher"
    ]

    if not editable_pages:
        st.info("편집할 페이지가 없어요.")
    else:
        # 헤더
        h1, h2, h3, h4 = st.columns([2, 1.5, 1.5, 1])
        h1.markdown("**앱**")
        h2.markdown("**기본 카테고리**")
        h3.markdown("**현재 카테고리 (편집)**")
        h4.markdown("**상태**")
        st.divider()

        for p in editable_pages:
            base_cat = p.category  # _BASE_PAGES의 원본
            current_cat = cat_overrides.get(p.key, base_cat)

            # CATEGORIES에 없는 카테고리도 dropdown에 포함 (base가 비표준일 경우)
            options = list(CATEGORIES)
            if base_cat and base_cat not in options:
                options.append(base_cat)
            if current_cat and current_cat not in options:
                options.append(current_cat)

            try:
                default_idx = options.index(current_cat)
            except ValueError:
                default_idx = 0

            c1, c2, c3, c4 = st.columns([2, 1.5, 1.5, 1])
            with c1:
                st.markdown(f"{p.icon}  **{p.name}**")
                st.caption(f"`{p.key}`")
            with c2:
                base_icon = CATEGORY_ICONS.get(base_cat, "🏷️")
                st.markdown(f"{base_icon} {base_cat}")
            with c3:
                selected = st.selectbox(
                    label=f"카테고리 — {p.key}",
                    options=options,
                    index=default_idx,
                    key=f"_admin_edit_cat_{p.key}",
                    label_visibility="collapsed",
                )
            with c4:
                if selected == base_cat:
                    st.caption("기본값")
                    # 깔끔하게 — 기본값과 같으면 overrides에서 제거
                    cat_overrides.pop(p.key, None)
                else:
                    st.caption("⭐ 변경됨")
                    cat_overrides[p.key] = selected

        st.divider()

        # 사용 가능한 카테고리 안내
        with st.expander("ℹ️ 사용 가능한 카테고리 / 새 카테고리 추가 방법"):
            st.markdown(
                "**현재 정의된 카테고리** (`pages_registry.CATEGORIES`):\n"
                + "\n".join(
                    f"- {CATEGORY_ICONS.get(c, '🏷️')} **{c}**"
                    for c in CATEGORIES
                )
            )
            st.caption(
                "새 카테고리를 추가하려면 `pages_registry.py` 의 `CATEGORIES` 리스트와 "
                "`CATEGORY_ICONS` 딕셔너리를 수정해주세요. 코드 변경이 필요해요."
            )

# ─── Tab 5: 사용자 목록 (권한 매트릭스) ─────────────────────
with tab_matrix:
    st.subheader("전체 사용자 권한 매트릭스")
    st.caption("등록된 모든 이메일과 각 앱에 대한 접근 권한.")

    all_emails: set = set()
    for e in acl["admins"]:
        if not e.startswith("*"):
            all_emails.add(e)
    for e in acl["default_access"]:
        if not e.startswith("*"):
            all_emails.add(e)
    for lst in acl["page_access"].values():
        for e in lst:
            if not e.startswith("*"):
                all_emails.add(e)

    def _draft_is_admin(em: str) -> bool:
        return auth._matches_any(em, acl.get("admins", []))

    def _draft_has_access(em: str, page_key: str) -> bool:
        if _draft_is_admin(em):
            return True
        if auth._matches_any(em, acl.get("default_access", [])):
            return True
        return auth._matches_any(em, acl.get("page_access", {}).get(page_key, []))

    if not all_emails:
        st.info("아직 등록된 사용자가 없어요. 위 탭에서 이메일을 추가하세요.")
    else:
        # 매트릭스 (launcher/admin 제외하고 일반 앱만)
        rows = []
        app_pages = [p for p in PAGES if not p.admin_only and p.key != "launcher"]
        for em in sorted(all_emails):
            row = {"이메일": em, "🛡 admin": "✓" if _draft_is_admin(em) else ""}
            for p in app_pages:
                row[f"{p.icon} {p.name}"] = "✓" if _draft_has_access(em, p.key) else ""
            rows.append(row)
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    wildcards = (
        [r for r in acl["default_access"] if "*" in r]
        + [r for r in acl["admins"] if "*" in r]
    )
    if wildcards:
        st.caption(
            f"※ 와일드카드 규칙({', '.join(sorted(set(wildcards)))})으로 "
            f"자동 접근하는 사용자는 위 표에 안 나와요."
        )

# ─── Tab 6: 저장 (단순화 — DB 자동 저장) ───────────────────
with tab_save:
    # 현재 변경사항 계산 (다른 탭에서 변경한 session_state vs DB/디스크 저장본)
    disk_acl = auth.get_acl()
    acl_dirty = (
        disk_acl.get("admins") != acl["admins"]
        or disk_acl.get("default_access") != acl["default_access"]
        or disk_acl.get("page_access") != acl["page_access"]
    )
    disk_overrides = pages_registry.load_overrides()
    overrides_dirty = (
        disk_overrides.get("category_overrides", {})
        != overrides_draft.get("category_overrides", {})
    )
    any_dirty = acl_dirty or overrides_dirty

    def _build_acl_to_save():
        to_save = {**disk_acl}
        to_save["admins"] = acl["admins"]
        to_save["default_access"] = acl["default_access"]
        to_save["page_access"] = acl["page_access"]
        return to_save

    def _build_overrides_to_save():
        to_save = {**disk_overrides}
        to_save["category_overrides"] = overrides_draft.get("category_overrides", {})
        return to_save

    # DB 활성 여부
    db_enabled = False
    try:
        from ar_app import db_store as _ds
        db_enabled = _ds.enabled()
    except Exception:
        pass

    # ── 1) 저장 상태 안내 (한 줄) ─────────────────────────
    if db_enabled:
        st.success(
            "✅ **자동 영구 저장 활성** — 다른 탭에서 변경 후 아래 **[💾 적용]** 한 번만 누르면 "
            "Neon DB에 영구 저장됩니다. 컨테이너 재시작이나 재배포에도 유지돼요."
        )
    else:
        st.warning(
            "⚠️ **DB 연결 없음** — JSON 파일에만 저장됩니다. Cloud 환경은 재배포 시 사라지니, "
            "변경 후 백업 JSON을 받아 GitHub commit 해주세요."
        )

    st.markdown("")

    # ── 2) 변경사항 요약 + 적용 버튼 (단일 액션) ──────────
    if not any_dirty:
        st.info("🟢 현재 변경사항 없음 — 모두 저장된 상태입니다.")
    else:
        st.markdown("##### 변경사항 요약")
        bullets = []
        if acl_dirty:
            bullets.append("- 권한 (admin·기본·앱별)")
        if overrides_dirty:
            bullets.append("- 카테고리 매핑")
        st.markdown("\n".join(bullets))

        if st.button(
            "💾 변경사항 적용 (한 번에 저장)",
            type="primary",
            use_container_width=True,
        ):
            if acl_dirty:
                auth.save_acl(_build_acl_to_save())
            if overrides_dirty:
                pages_registry.save_overrides(_build_overrides_to_save())
            st.success(
                "✅ 저장 완료" + (" (Neon DB)" if db_enabled else " (JSON 파일)")
                + " — 새로고침하면 사이드바·카테고리에 반영됩니다."
            )
            st.rerun()

    # ── 3) 백업 (필요할 때만 — expander) ──────────────────
    with st.expander("📦 백업 JSON 다운로드 (GitHub commit · 디버깅용)", expanded=False):
        st.caption(
            "Neon DB가 truth라 평소엔 받지 않아도 됩니다. 백업 보관 / "
            "GitHub 커밋용으로 가져가실 때만."
        )
        c1, c2 = st.columns(2)
        with c1:
            st.download_button(
                "📥 acl.json",
                data=_json.dumps(_build_acl_to_save(), ensure_ascii=False, indent=2).encode("utf-8"),
                file_name="acl.json",
                mime="application/json",
                use_container_width=True,
            )
        with c2:
            st.download_button(
                "📥 page_overrides.json",
                data=_json.dumps(_build_overrides_to_save(), ensure_ascii=False, indent=2).encode("utf-8"),
                file_name="page_overrides.json",
                mime="application/json",
                use_container_width=True,
            )

    # ── 4) 현재 데이터 미리보기 (expander) ───────────────
    with st.expander("👀 현재 데이터 확인 (JSON 미리보기)", expanded=False):
        st.markdown("**권한 (acl)**")
        st.code(
            _json.dumps({
                "admins": acl["admins"],
                "default_access": acl["default_access"],
                "page_access": acl["page_access"],
            }, ensure_ascii=False, indent=2),
            language="json",
        )
        st.markdown("**카테고리 매핑**")
        st.code(
            _json.dumps(
                {"category_overrides": overrides_draft.get("category_overrides", {})},
                ensure_ascii=False, indent=2,
            ),
            language="json",
        )
