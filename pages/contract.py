"""계약서 생성기 — contract_app/ 위임 wrapper."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import auth

auth.gate("contract", "계약서 생성기")
auth.run_legacy_app("contract_app", "app.py")
