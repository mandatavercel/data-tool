"""데이터 카탈로그 — catalog_app/ 위임 wrapper (root의 streamlit_catalog.py 경유)."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import auth

auth.gate("catalog", "데이터 카탈로그")
# catalog는 root의 streamlit_catalog.py가 wrapper 역할 (catalog_app/catalog_app.py 실행)
auth.run_legacy_app("", "streamlit_catalog.py")
