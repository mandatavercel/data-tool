"""
analysis_app — Alternative Data Intelligence Platform (분석 앱)

이 폴더는 Python 패키지입니다. 내부 모듈들이 `analysis_app.*` 경로로
import 되도록 합니다 (예: `from analysis_app.secrets_store import ...`).

📂 구성:
    analysis_app.py    — Streamlit entry point + step routing
    secrets_store.py   — DART API Key 등 영구 저장
    export.py          — Excel 멀티시트 export
    dashboard.py       — Step 5 Investor Dashboard 렌더링
    setup_ui.py        — Step 4 모듈별 파라미터 UI
"""
