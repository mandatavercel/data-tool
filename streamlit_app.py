"""
Streamlit Community Cloud entry point.

Streamlit Cloud는 default로 'streamlit_app.py'를 entry point로 찾습니다.
실제 분석 앱은 `analysis_app/analysis_app.py`에 있고, 이 파일은
working directory와 sys.path를 정확히 설정한 뒤 그 파일을 실행합니다.

로컬에서는 그대로 `analysis_app/analysis_app.py`를 실행해도 OK.
Cloud에서는 이 파일이 entry point.
"""
import sys
import os
from pathlib import Path

# 1) 프로젝트 루트를 sys.path에 추가
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

# 2) 작업 디렉토리도 루트로 통일 (relative path 안전성)
try:
    os.chdir(ROOT)
except Exception:
    pass

# 3) 실제 분석 앱 실행
import runpy
runpy.run_path(
    str(ROOT / "analysis_app" / "analysis_app.py"),
    run_name="__main__",
)
