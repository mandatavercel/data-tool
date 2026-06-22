"""
AR Management — 1인 편집 락 (동시 편집 충돌 방지).

여러 명이 같은 서버(같은 PC에서 실행)에 접속해도 한 번에 한 명만 사용하도록
파일 기반 락을 둔다. 보유자가 일정 시간(TTL) 활동이 없으면 자동 해제된다.

  data/lock.json = {"holder": "<세션ID>", "email": "<표시용>", "ts": <unix초>}
"""
from __future__ import annotations

import json
import time

from .models import DATA_DIR

LOCK_PATH = DATA_DIR / "lock.json"
TTL_SECONDS = 150  # 보유자가 이 시간 동안 활동(클릭) 없으면 락 자동 해제


def _read() -> dict:
    try:
        return json.loads(LOCK_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write(d: dict) -> None:
    try:
        LOCK_PATH.write_text(json.dumps(d, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def current() -> "dict | None":
    """유효한(만료 안 된) 현재 보유자 정보. 없으면 None."""
    d = _read()
    if not d:
        return None
    if time.time() - float(d.get("ts", 0)) > TTL_SECONDS:
        return None
    return d


def acquire(holder_id: str, email: str) -> tuple:
    """락 획득 시도. (성공여부, 현재보유자정보) 반환.
    비어있거나 내가 보유 중이면 갱신(heartbeat)하고 True."""
    cur = current()
    if cur is None or cur.get("holder") == holder_id:
        _write({"holder": holder_id, "email": email, "ts": time.time()})
        return True, None
    return False, cur


def release(holder_id: str) -> None:
    if _read().get("holder") == holder_id:
        _write({})


def force_release() -> None:
    _write({})


def remaining(cur: dict) -> int:
    """현재 보유자 락이 자동 해제되기까지 남은 초."""
    return max(0, int(TTL_SECONDS - (time.time() - float(cur.get("ts", 0)))))
