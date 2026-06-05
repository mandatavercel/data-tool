"""AR Management — ar_app/ 위임 wrapper."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import auth

auth.gate("ar", "AR Management")
auth.run_legacy_app("ar_app", "app.py")
