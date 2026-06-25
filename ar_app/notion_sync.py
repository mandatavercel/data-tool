"""
노션 동기화 — AR 정산 현황(배분사 × 계약별 분기 정산)을 Notion 데이터베이스로 push.

원본(Neon DB / 로컬 JSON)에서 계산한 정산 현황을 노션의 공유용 DB에 upsert 한다.
공유용이므로 단방향(앱 → 노션). 노션에서 직접 편집한 값은 다음 동기화 때 덮어쓰인다.

필요 설정 (Streamlit secrets 또는 환경변수):
  NOTION_TOKEN        : 노션 내부 통합(integration) 토큰 (secret_xxx 또는 ntn_xxx)
  NOTION_DATABASE_ID  : 대상 데이터베이스 ID (생략 시 아래 기본값)

노션 DB 속성(컬럼)명은 다음과 정확히 일치해야 한다:
  고객사·계약(title), 배분사(select), 연도(number), 배분율(number),
  1/4분기, 2/4분기, 3/4분기, 4/4분기, 연 합계(number)
"""
from __future__ import annotations

import os
from datetime import date
from typing import Optional

import requests

from . import models as ar_models
from .schedule import expected_collections

# 기본 대상 DB (이미 생성된 "AR 정산 현황 (자동 동기화)")
DEFAULT_DATABASE_ID = "747e0897-e2ba-4c6d-8333-c1a02da73ea2"
NOTION_VERSION = "2022-06-28"
API = "https://api.notion.com/v1"

# 컬럼명 상수
P_TITLE = "고객사·계약"
P_OWNER = "배분사"
P_YEAR = "연도"
P_RATIO = "배분율"
P_Q = ["1/4분기", "2/4분기", "3/4분기", "4/4분기"]
P_TOTAL = "연 합계"


# ─────────────────────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────────────────────
def _secret(name: str, default: str = "") -> str:
    # Streamlit secrets 우선, 없으면 환경변수
    try:
        import streamlit as st
        v = st.secrets.get(name)
        if v:
            return str(v).strip()
    except Exception:
        pass
    return os.environ.get(name, default).strip()


def get_token() -> str:
    return _secret("NOTION_TOKEN")


def get_database_id() -> str:
    return _secret("NOTION_DATABASE_ID") or DEFAULT_DATABASE_ID


def enabled() -> bool:
    return bool(get_token())


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {get_token()}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


# ─────────────────────────────────────────────────────────────
# 정산 현황 계산 (앱 대시보드 '정산 현황표'와 동일 로직)
# ─────────────────────────────────────────────────────────────
def _is_running(ct, today: date) -> bool:
    se = ar_models.parse_iso(ct.subscription_end_date)
    if ct.status != "active":
        return False
    return ct.auto_renewal or se is None or se >= today


def build_rows(today: Optional[date] = None) -> list[dict]:
    """배분사 × 계약 × 연도 단위 행 리스트를 계산해 반환."""
    today = today or date.today()
    contracts = ar_models.load_contracts()
    customers = {c.id: c for c in ar_models.load_customers()}
    settings = ar_models.load_settings()
    rate = float(settings.get("usd_krw") or ar_models.DEFAULT_USD_KRW)

    def to_krw(amount: float, currency: str) -> float:
        return ar_models.to_base(amount, currency, rate)

    rows: list[dict] = []
    for ct in contracts:
        if not _is_running(ct, today):
            continue
        cust = customers.get(ct.customer_id)
        label = f"{cust.name if cust else '?'} · {ct.order_form_name or '(제목없음)'}"
        for rs in ct.revenue_shares:
            if not rs.is_active():
                continue
            eff = rs.effective_ratio(ct.yearly_fee)
            by_year: dict[int, list] = {}
            for p in expected_collections(ct, today):
                due = ar_models.parse_iso(p.due_date) or today
                # 정산(지급) 분기 = 수금 분기의 다음 분기 (연말 → 다음 해 Q1 롤오버)
                pidx = (due.year * 4 + (due.month - 1) // 3) + 1
                py, qi = divmod(pidx, 4)
                q = by_year.setdefault(py, [0.0, 0.0, 0.0, 0.0])
                q[qi] += to_krw(p.amount * eff, p.currency)
            for y, q in by_year.items():
                rows.append({
                    "year": y, "owner": rs.owner, "contract": label, "ratio": float(eff),
                    "q": [round(x) for x in q], "total": round(sum(q)),
                })
    return rows


def _key(year, owner, contract) -> str:
    return f"{int(year)}|{owner}|{contract}"


# ─────────────────────────────────────────────────────────────
# 노션 API
# ─────────────────────────────────────────────────────────────
def _props_payload(r: dict) -> dict:
    return {
        P_TITLE: {"title": [{"text": {"content": r["contract"]}}]},
        P_OWNER: {"select": {"name": r["owner"]}},
        P_YEAR: {"number": int(r["year"])},
        P_RATIO: {"number": float(r["ratio"])},
        P_Q[0]: {"number": r["q"][0]},
        P_Q[1]: {"number": r["q"][1]},
        P_Q[2]: {"number": r["q"][2]},
        P_Q[3]: {"number": r["q"][3]},
        P_TOTAL: {"number": r["total"]},
    }


def _plain_title(prop: dict) -> str:
    try:
        return "".join(t.get("plain_text", "") for t in prop.get("title", []))
    except Exception:
        return ""


def _query_existing(db_id: str) -> dict:
    """기존 노션 행을 {key: page_id} 로 반환."""
    out: dict[str, str] = {}
    cursor = None
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        resp = requests.post(f"{API}/databases/{db_id}/query", headers=_headers(),
                             json=body, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        for pg in data.get("results", []):
            props = pg.get("properties", {})
            year = (props.get(P_YEAR, {}) or {}).get("number")
            owner_sel = (props.get(P_OWNER, {}) or {}).get("select") or {}
            owner = owner_sel.get("name", "")
            contract = _plain_title(props.get(P_TITLE, {}) or {})
            if year is None:
                continue
            out[_key(year, owner, contract)] = pg["id"]
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return out


def sync(today: Optional[date] = None) -> dict:
    """앱 데이터 → 노션 upsert. 결과 카운트 반환."""
    if not enabled():
        raise RuntimeError("NOTION_TOKEN 이 설정되지 않았습니다.")
    db_id = get_database_id()
    rows = build_rows(today)
    desired = {_key(r["year"], r["owner"], r["contract"]): r for r in rows}
    existing = _query_existing(db_id)

    created = updated = archived = 0

    # upsert
    for k, r in desired.items():
        payload = {"properties": _props_payload(r)}
        if k in existing:
            resp = requests.patch(f"{API}/pages/{existing[k]}", headers=_headers(),
                                  json=payload, timeout=30)
            resp.raise_for_status()
            updated += 1
        else:
            payload["parent"] = {"database_id": db_id}
            resp = requests.post(f"{API}/pages", headers=_headers(),
                                 json=payload, timeout=30)
            resp.raise_for_status()
            created += 1

    # 더 이상 존재하지 않는 행 보관처리(archive)
    for k, pid in existing.items():
        if k not in desired:
            resp = requests.patch(f"{API}/pages/{pid}", headers=_headers(),
                                  json={"archived": True}, timeout=30)
            resp.raise_for_status()
            archived += 1

    return {"created": created, "updated": updated, "archived": archived,
            "total": len(desired)}
