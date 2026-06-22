"""마켓 데이터 — marketdata_app/ 위임 wrapper."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import auth

auth.gate("marketdata", "마켓 데이터")
auth.run_legacy_app("marketdata_app", "app.py")
