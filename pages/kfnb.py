"""K-F&B 데이터 상품 — kfnb_app/ 위임 wrapper."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import auth

auth.gate("kfnb", "K-F&B 데이터 상품")
auth.run_legacy_app("kfnb_app", "app.py")
