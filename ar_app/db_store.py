"""
Postgres 저장 백엔드 — 클라우드 배포 시 영구 보존 + 여러 명 사용.

활성화 조건: Streamlit secrets 에 `DATABASE_URL` (Postgres 연결 문자열)이 있을 때만 동작.
  예: postgresql://USER:PASSWORD@HOST/DBNAME?sslmode=require   (Neon / Supabase 등)

데이터는 키-값 테이블 한 곳에 종류별 JSON blob 으로 저장한다.
  ar_store(name text primary key, data jsonb)
  name ∈ {customers, contracts, staff, settings, collections}

secrets 에 DATABASE_URL 이 없으면 enabled()=False → models.py 가 JSON 파일로 폴백한다.
모든 함수는 예외를 던질 수 있으며 호출부(models.py)에서 try/except 로 감싸 안전하게 폴백한다.
"""
from __future__ import annotations

import json

import streamlit as st


def enabled() -> bool:
    try:
        return bool(st.secrets.get("DATABASE_URL"))
    except Exception:
        return False


@st.cache_resource(show_spinner=False)
def _conn():
    import psycopg2
    conn = psycopg2.connect(st.secrets["DATABASE_URL"], connect_timeout=10)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS ar_store (name text PRIMARY KEY, data jsonb)")
    return conn


def _reconnect():
    try:
        _conn.clear()
    except Exception:
        pass
    return _conn()


def _coerce(v):
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, str):
        try:
            return json.loads(v)
        except (ValueError, TypeError):
            return None
    return None


@st.cache_data(ttl=3, show_spinner=False)
def _read_all_cached() -> dict:
    sql = "SELECT name, data FROM ar_store"
    try:
        conn = _conn()
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception:
        conn = _reconnect()
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return {r[0]: _coerce(r[1]) for r in rows}


def read(name: str):
    return _read_all_cached().get(name)


def write(name: str, data) -> None:
    payload = json.dumps(data, ensure_ascii=False)
    sql = ("INSERT INTO ar_store(name, data) VALUES(%s, %s::jsonb) "
           "ON CONFLICT(name) DO UPDATE SET data = EXCLUDED.data")
    try:
        conn = _conn()
        with conn.cursor() as cur:
            cur.execute(sql, (name, payload))
    except Exception:
        conn = _reconnect()
        with conn.cursor() as cur:
            cur.execute(sql, (name, payload))
    try:
        _read_all_cached.clear()
    except Exception:
        pass
