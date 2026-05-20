"""종목 식별 — security_id_app/ 위임 wrapper."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import auth

auth.gate("security_id", "종목 식별")
auth.run_legacy_app("security_id_app", "app.py")
