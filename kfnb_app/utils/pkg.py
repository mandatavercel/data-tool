"""
kfnb_app/utils/pkg.py — 런타임 pip 설치 헬퍼.

homebrew/externally-managed Python(예: 3.12+)에서는 일반 pip 가 거부되므로
실패 시 --break-system-packages 로 자동 재시도한다.
"""
from __future__ import annotations

import subprocess
import sys


def pip_install(packages: list[str]) -> tuple[bool, str]:
    """packages 설치. (성공여부, 로그). 1차 실패 시 --break-system-packages 재시도."""
    base = [sys.executable, "-m", "pip", "install", "--quiet", *packages]
    r = subprocess.run(base, capture_output=True, text=True)
    if r.returncode == 0:
        return True, r.stdout + r.stderr
    r2 = subprocess.run(base + ["--break-system-packages"],
                        capture_output=True, text=True)
    return r2.returncode == 0, (r.stderr + "\n--- retry(break-system) ---\n"
                                + r2.stdout + r2.stderr)
