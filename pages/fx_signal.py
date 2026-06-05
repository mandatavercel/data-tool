"""FX 환율 신호 — fx_signal_app/ 위임 wrapper."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import auth

auth.gate("fx_signal", "FX 환율 신호")
auth.run_legacy_app("fx_signal_app", "app.py")
