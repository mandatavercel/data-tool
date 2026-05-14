"""
스텝 네비게이션 — go_to(step), render_stepper(), require_state().
"""
from __future__ import annotations

import streamlit as st

from analysis_app.config import STEPS


def go_to(step: int) -> None:
    """현재 step을 변경하고 즉시 rerun."""
    st.session_state["step"] = step
    st.rerun()


def require_state(keys: list[str], redirect_step: int,
                  hint: str = "이전 단계를 완료하세요.") -> None:
    """필요한 session_state 키 존재 여부 확인 후 없으면 안전하게 redirect.

    사용:
        from analysis_app.navigation import require_state
        require_state(["raw_df", "role_map"], redirect_step=2)

    동작:
        - 모든 key가 session_state에 있고 truthy → 통과
        - 하나라도 없거나 falsy → warning + go_to(redirect_step)
        - go_to가 st.rerun()을 호출하므로 호출 측 함수는 자동 중단
    """
    missing = [k for k in keys if not st.session_state.get(k)]
    if missing:
        st.warning(f"⚠️ {hint} (필요: {', '.join(missing)})")
        go_to(redirect_step)


def render_stepper() -> None:
    """상단 진행 표시기 — 현재 step 강조."""
    current = st.session_state.get("step", 1)
    n = len(STEPS)
    items = ""
    for i, label in STEPS.items():
        if i < current:
            state, icon = "done", "✓"
        elif i == current:
            state, icon = "active", str(i)
        else:
            state, icon = "future", str(i)
        items += f'<div class="step {state}"><div class="circle">{icon}</div><div class="label">{label}</div></div>'
        if i < n:
            items += "<div class='connector'></div>"

    st.markdown(f"""
    <style>
    .stepper {{display:flex;align-items:center;padding:20px 0 28px;gap:0;}}
    .step {{display:flex;flex-direction:column;align-items:center;flex:0 0 auto;}}
    .connector {{flex:1;height:2px;background:#d1d5db;margin:0 4px;margin-bottom:20px;}}
    .circle {{width:30px;height:30px;border-radius:50%;display:flex;align-items:center;
               justify-content:center;font-size:12px;font-weight:700;}}
    .label {{font-size:10px;margin-top:4px;white-space:nowrap;color:#6b7280;}}
    .step.done   .circle {{background:#1e40af;color:#fff;}}
    .step.active .circle {{background:#3b82f6;color:#fff;box-shadow:0 0 0 3px #bfdbfe;}}
    .step.future .circle {{background:#e5e7eb;color:#9ca3af;}}
    .step.active .label  {{color:#1e40af;font-weight:600;}}
    .step.done   .label  {{color:#1e40af;}}
    </style>
    <div class="stepper">{items}</div>
    """, unsafe_allow_html=True)
