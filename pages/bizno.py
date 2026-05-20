"""사업자조회 — bizno_app/ 위임 wrapper."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import auth

auth.gate("bizno", "사업자조회")
auth.run_legacy_app("bizno_app", "app.py")
