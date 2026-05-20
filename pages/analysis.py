"""데이터 분석 — analysis_app/ 위임 wrapper."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import auth

auth.gate("analysis", "데이터 분석")
auth.run_legacy_app("analysis_app", "analysis_app.py")
