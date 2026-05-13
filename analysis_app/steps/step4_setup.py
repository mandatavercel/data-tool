"""Step 4 — Analysis Setup (Capability Map + Module Selection + Run)."""
from __future__ import annotations

import streamlit as st

from modules.common.foundation import _eval_caps
from analysis_app.navigation import go_to
from analysis_app.setup_ui    import render_param_block
from analysis_app.config      import ANALYSIS_OPTIONS, RUNNERS


# ── 스타일 상수 ───────────────────────────────────────────────────────────────
LAYER_STYLE = {
    "Intelligence": ("🧠", "Intelligence Hub", "#3b82f6", "#eff6ff"),
    "Signal":       ("📡", "Signal Layer",     "#8b5cf6", "#f5f3ff"),
    "Factor":       ("🧪", "Factor Layer",     "#0ea5e9", "#ecfeff"),
}
STATUS_DOT = {
    "executable":              ("#16a34a", "실행 가능"),
    "executable_with_warning": ("#f59e0b", "제한 실행"),
    "failed_requirement":      ("#9ca3af", "데이터 부족"),
}
HAS_PARAMS = {"growth", "demand", "anomaly", "earnings_intel", "factor_research"}


def render() -> None:
    st.subheader("Step 4 — Analysis Setup")

    df       = st.session_state.get("raw_df")
    role_map = st.session_state.get("role_map", {})
    if not role_map:
        go_to(2)

    all_caps_list = _eval_caps(role_map)
    st.session_state["capability_map"] = all_caps_list
    all_caps = {c["key"]: c for c in all_caps_list}

    results = st.session_state.setdefault("results", {})

    n_ok   = sum(1 for c in all_caps_list if c["cap_status"] == "executable")
    n_warn = sum(1 for c in all_caps_list if c["cap_status"] == "executable_with_warning")
    n_fail = sum(1 for c in all_caps_list if c["cap_status"] == "failed_requirement")
    runnable_keys = [c["key"] for c in all_caps_list if c["cap_status"] != "failed_requirement"]

    # ── Top toolbar ──────────────────────────────────────────────────────────
    status_html = (
        "<div style='display:flex;align-items:center;gap:14px;font-size:12px;"
        "color:#475569;line-height:1'>"
        f"<span>● <b style='color:#16a34a'>{n_ok}</b> 실행</span>"
        f"<span>● <b style='color:#f59e0b'>{n_warn}</b> 제한</span>"
        f"<span>● <b style='color:#9ca3af'>{n_fail}</b> 부족</span>"
        f"<span style='color:#cbd5e1'>·</span>"
        f"<span style='color:#94a3b8'>총 {len(all_caps_list)}개</span>"
        "</div>"
    )
    tcol1, tcol2, tcol3, tcol4 = st.columns([3.0, 0.85, 0.85, 0.7])
    with tcol1:
        st.markdown(status_html, unsafe_allow_html=True)
    with tcol2:
        if st.button("✓ 전체 선택", use_container_width=True, key="btn_all_sel"):
            for k in runnable_keys:
                st.session_state[f"sel_{k}"] = True
            st.rerun()
    with tcol3:
        if st.button("✕ 전체 해제", use_container_width=True, key="btn_all_clear"):
            for k in runnable_keys:
                st.session_state[f"sel_{k}"] = False
            st.rerun()
    with tcol4:
        if hasattr(st, "popover"):
            with st.popover("📖 가이드", use_container_width=True):
                from modules.analysis.guides import _guide_step4 as _g4
                _g4()
        else:
            with st.expander("📖", expanded=False):
                from modules.analysis.guides import _guide_step4 as _g4
                _g4()

    st.write("")

    selected_modules: list[str] = []
    params_map: dict[str, dict] = {}

    # ── Layer + 모듈 카드 ────────────────────────────────────────────────────
    for layer_key in ("Intelligence", "Signal", "Factor"):
        layer_caps = [c for c in all_caps_list if c["layer"] == layer_key]
        if not layer_caps:
            continue
        icon, label, accent, bg = LAYER_STYLE[layer_key]

        st.markdown(
            f"<div style='background:{bg};border-left:3px solid {accent};"
            f"padding:5px 12px;margin:14px 0 8px 0;border-radius:4px;"
            f"display:flex;align-items:center;justify-content:space-between'>"
            f"<span style='font-size:13px;font-weight:700;color:#0f172a'>"
            f"{icon} {label}</span>"
            f"<span style='font-size:10px;color:#64748b'>{len(layer_caps)}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )

        for cap in layer_caps:
            key       = cap["key"]
            cap_st    = cap.get("cap_status", "executable")
            done      = key in results
            disabled  = cap_st == "failed_requirement"
            dot_color, status_text = STATUS_DOT.get(cap_st)

            done_badge = (
                "<span style='background:#dbeafe;color:#1e40af;border-radius:3px;"
                "padding:1px 5px;font-size:10px;font-weight:600;margin-left:5px'>완료</span>"
                if done else ""
            )
            warn_tags = ""
            if cap_st == "failed_requirement":
                for r in cap["missing"]:
                    warn_tags += (
                        f"<span style='background:#fee2e2;color:#dc2626;border-radius:3px;"
                        f"padding:1px 5px;font-size:10px;margin-left:4px'>필요 {r}</span>"
                    )
            elif cap_st == "executable_with_warning":
                for r in cap["warn_missing"]:
                    warn_tags += (
                        f"<span style='background:#fef3c7;color:#92400e;border-radius:3px;"
                        f"padding:1px 5px;font-size:10px;margin-left:4px'>{r}</span>"
                    )

            with st.container(border=True):
                cc1, cc2 = st.columns([0.25, 9])
                with cc1:
                    sel = st.checkbox(
                        cap["name"],
                        value=st.session_state.get(f"sel_{key}", False),
                        key=f"sel_{key}",
                        disabled=disabled,
                        label_visibility="collapsed",
                    )
                with cc2:
                    st.markdown(
                        f"<div style='line-height:1.35;padding-top:1px'>"
                        f"<span style='display:inline-block;width:7px;height:7px;background:{dot_color};"
                        f"border-radius:50%;margin-right:6px;vertical-align:middle'></span>"
                        f"<span style='font-size:13px;font-weight:700;color:#0f172a'>{cap['name']}</span>"
                        f"<span style='font-size:11px;color:#94a3b8;margin-left:6px'>{status_text}</span>"
                        f"{done_badge}{warn_tags}"
                        f"<span style='display:block;font-size:11.5px;color:#64748b;margin-top:1px'>"
                        f"{cap['desc']}</span></div>",
                        unsafe_allow_html=True,
                    )

                if sel:
                    selected_modules.append(key)

                # 인라인 파라미터 — 선택 시에만, 같은 카드 안에서 펼침
                if sel and key in HAS_PARAMS:
                    st.markdown(
                        "<hr style='margin:8px 0 6px;border:none;"
                        "border-top:1px dashed #e2e8f0'>",
                        unsafe_allow_html=True,
                    )
                    st.markdown(
                        "<div style='font-size:10px;font-weight:700;color:#64748b;"
                        "letter-spacing:0.5px;margin-bottom:4px'>⚙ 파라미터</div>",
                        unsafe_allow_html=True,
                    )
                    params_map[key] = render_param_block(key)
                else:
                    params_map[key] = {}

    # ── 실행 버튼 ─────────────────────────────────────────────────────────────
    c_run, c_prev = st.columns([3, 1])
    with c_prev:
        if st.button("← Data Validation"):
            go_to(3)
    with c_run:
        if selected_modules:
            if st.button(f"▶ {len(selected_modules)}개 분석 실행", type="primary"):
                progress = st.progress(0, text="분석 준비 중...")
                for i, key in enumerate(selected_modules):
                    progress.progress(
                        i / len(selected_modules),
                        text=f"실행 중: {ANALYSIS_OPTIONS.get(key, key)}",
                    )
                    p = params_map.get(key, {})
                    if key == "alpha_validation":
                        p = {**p, "all_results": dict(results)}
                    try:
                        res = RUNNERS[key](df, role_map, p)
                    except Exception as exc:
                        err_str = str(exc)
                        if any(k in err_str for k in ("ConnectionError", "Max retries",
                                                       "timed out", "URLError", "getaddrinfo")):
                            msg = "네트워크 연결 실패 — 외부 API(DART/yfinance) 접근 불가"
                        elif "PIT" in err_str or "panel" in err_str.lower():
                            msg = f"데이터 부족 — {err_str[:120]}"
                        else:
                            msg = f"{type(exc).__name__}: {err_str[:200]}"
                        res = {"status": "failed", "message": msg,
                               "data": None, "metrics": {}}
                    results[key] = res
                progress.progress(1.0, text="완료!")
                st.session_state["results"] = results
                go_to(5)
        else:
            st.button("▶ 분석 실행", disabled=True,
                      help="실행할 모듈을 하나 이상 선택하세요.")
