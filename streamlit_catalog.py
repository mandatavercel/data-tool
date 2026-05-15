"""Streamlit Cloud entry point for Data Catalog app.

Streamlit Cloud Settings에서 Main file path를 `streamlit_catalog.py`로 지정하면
이 wrapper가 sys.path를 설정한 뒤 catalog_app/catalog_app.py를 실행합니다.
"""
import sys
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
try:
    os.chdir(ROOT)
except Exception:
    pass

import runpy
runpy.run_path(
    str(ROOT / "catalog_app" / "catalog_app.py"),
    run_name="__main__",
)
