"""수익배분 산정 — revshare_app/ 위임 wrapper."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import auth

auth.gate("revshare", "수익배분 산정")
auth.run_legacy_app("revshare_app", "app.py")
