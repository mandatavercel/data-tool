"""
카탈로그 로더 — catalog/*.parquet 자동 스캔 + 최신 사용.

데이터 소스 우선순위:
    1. 사용자 직접 업로드 (st.file_uploader) — runtime
    2. catalog/ 폴더의 최신 .parquet — repo 동봉
    3. 데모 데이터 (fallback) — 카탈로그 미존재 시 샘플 제공

스키마 (필수 컬럼):
    company:        str   회사명
    ticker:         str   KRX 6자리 종목코드 (없으면 빈 문자열)
    sector:         str   섹터/카테고리
    signal_score:   float 0.0~1.0 — 매출-주가 시그널 강도
    mom_growth:     float % — 최근 매출 MoM
    coverage_months int   데이터 커버 기간 (월)
    has_dart:       bool  DART 공시 연동 여부
    has_stock:      bool  주가 데이터 매칭 여부
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional
import pandas as pd
import streamlit as st


_CATALOG_DIR = Path(__file__).parent.parent / "catalog"


def list_catalogs() -> list[Path]:
    """catalog/ 폴더의 .parquet 파일 목록 — 최신 순."""
    if not _CATALOG_DIR.exists():
        return []
    files = sorted(_CATALOG_DIR.glob("*.parquet"),
                   key=lambda p: p.stat().st_mtime, reverse=True)
    return files


def load_latest_catalog() -> Optional[pd.DataFrame]:
    """가장 최근 catalog.parquet 로드. 없으면 None."""
    files = list_catalogs()
    if not files:
        return None
    try:
        return pd.read_parquet(files[0])
    except Exception:
        return None


def load_from_upload(uploaded) -> Optional[pd.DataFrame]:
    """업로드된 parquet/xlsx 파일을 DataFrame으로."""
    try:
        if uploaded.name.endswith(".parquet"):
            return pd.read_parquet(uploaded)
        if uploaded.name.endswith(".xlsx"):
            return pd.read_excel(uploaded)
        if uploaded.name.endswith(".csv"):
            return pd.read_csv(uploaded)
    except Exception as e:
        st.error(f"파일 읽기 실패: {type(e).__name__}: {e}")
    return None


def demo_catalog() -> pd.DataFrame:
    """데모용 가짜 카탈로그 — 카탈로그 미존재 시 화면이 빈 채로 떠 있지 않게."""
    import numpy as np
    rng = np.random.default_rng(42)
    sectors = ["음식료", "생활용품", "패션", "뷰티", "전자", "유통", "통신"]
    n = 60
    data = {
        "company":         [f"DemoCompany_{i:03d}" for i in range(n)],
        "ticker":          [f"{rng.integers(1000, 999999):06d}" if rng.random() > 0.2 else "" for _ in range(n)],
        "sector":          [sectors[rng.integers(0, len(sectors))] for _ in range(n)],
        "signal_score":    rng.uniform(0.1, 0.95, n).round(2),
        "mom_growth":      rng.uniform(-30, 50, n).round(1),
        "coverage_months": rng.integers(6, 36, n),
        "has_dart":        rng.random(n) > 0.3,
        "has_stock":       rng.random(n) > 0.25,
    }
    return pd.DataFrame(data)


def normalize_catalog(df: pd.DataFrame) -> pd.DataFrame:
    """필수 컬럼 보장 + 누락은 합리적 기본값."""
    out = df.copy()
    defaults = {
        "company":         "",
        "ticker":          "",
        "sector":          "기타",
        "signal_score":    0.0,
        "mom_growth":      0.0,
        "coverage_months": 0,
        "has_dart":        False,
        "has_stock":       False,
    }
    for c, default in defaults.items():
        if c not in out.columns:
            out[c] = default
    # 타입 보정
    out["company"]         = out["company"].astype(str)
    out["ticker"]          = out["ticker"].astype(str).str.zfill(6).replace("000000", "")
    out["sector"]          = out["sector"].astype(str)
    out["signal_score"]    = pd.to_numeric(out["signal_score"], errors="coerce").fillna(0.0)
    out["mom_growth"]      = pd.to_numeric(out["mom_growth"], errors="coerce").fillna(0.0)
    out["coverage_months"] = pd.to_numeric(out["coverage_months"], errors="coerce").fillna(0).astype(int)
    out["has_dart"]        = out["has_dart"].astype(bool)
    out["has_stock"]       = out["has_stock"].astype(bool)
    return out.reset_index(drop=True)
