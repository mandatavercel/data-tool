"""
Alternative Data Intelligence Platform — 데이터 분석 앱 (entry point)

📂 위치: analysis_app/analysis_app.py
📋 역할: thin router — 6단계 워크플로우 dispatch
🚀 실행: `📊 데이터분석 실행.command` 더블클릭 (포트 8501)

🏗 구조 (2026.05 리팩토링):
    analysis_app/
    ├── analysis_app.py    ← 이 파일 (router + 헤더)
    ├── config.py          ← STEPS, ANALYSIS_OPTIONS, RUNNERS, RENDERERS
    ├── navigation.py      ← go_to, render_stepper
    ├── secrets_store.py   ← DART API Key 등 영구 저장
    ├── export.py          ← Excel 멀티시트 export
    ├── dashboard.py       ← Investor Dashboard
    ├── setup_ui.py        ← Step 4 모듈별 파라미터 UI
    └── steps/
        ├── step1_upload.py
        ├── step2_schema.py
        ├── step3_validation.py
        ├── step4_setup.py
        ├── step5_results.py
        └── step6_dashboard.py
"""
import sys
from pathlib import Path

# 프로젝트 루트(data-tool/)를 sys.path에 추가해 modules.* / analysis_app.* import 가능하게 한다.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

from analysis_app.secrets_store import seed_session_state
from analysis_app.navigation    import render_stepper
from analysis_app.steps import (
    step1_upload, step2_schema, step3_validation,
    step4_setup,  step5_results, step6_dashboard,
)


# ══════════════════════════════════════════════════════════════════════════════
# 페이지 설정 + 영구 secrets 시드
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="📊 데이터 분석 — Mandata",
    page_icon="📊",
    layout="wide",
)
seed_session_state()   # ~/.mandata_analysis/config.json → session_state


# ══════════════════════════════════════════════════════════════════════════════
# 세션 초기화 + 헤더 + 진행 표시기
# ══════════════════════════════════════════════════════════════════════════════
if "step" not in st.session_state:
    st.session_state["step"] = 1

st.title("📊 데이터 분석 — Alt-Data Intelligence")
st.caption(
    "POS 데이터 + 주가/공시로 매출 선행성·상관·알파를 검증하는 6단계 워크플로우. "
    "8501 포트에서 실행 중입니다."
)
render_stepper()


# ══════════════════════════════════════════════════════════════════════════════
# Step Dispatch — step 번호 → render 함수
# ══════════════════════════════════════════════════════════════════════════════
STEP_RENDERERS = {
    1: step1_upload.render,
    2: step2_schema.render,
    3: step3_validation.render,
    4: step4_setup.render,
    5: step5_results.render,
    6: step6_dashboard.render,
}

current_step = st.session_state["step"]
renderer = STEP_RENDERERS.get(current_step)
if renderer:
    renderer()
else:
    st.error(f"알 수 없는 step: {current_step}. step 1로 돌아갑니다.")
    st.session_state["step"] = 1
    st.rerun()
