"""데이터 매핑 — mapping_app/ 위임 wrapper."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import auth

auth.gate("mapping", "데이터 매핑")
auth.run_legacy_app("mapping_app", "app.py")
