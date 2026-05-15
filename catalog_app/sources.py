"""
Mandata Data Catalog — 데이터 소스 (원천) 정의 + 회사별 커버리지 계산.

설계:
    • 데이터 소스 = 데이터의 *원천* (Card / POS / Satellite / ...)
    • 회사별로 어떤 소스를 보유하는지 + 각 소스별 커버리지% 가 다름
    • 사용자가 글로벌하게 어떤 소스(들)을 구매할지 선택 → 그것에 따라:
        - 카탈로그 필터링 (AND/OR)
        - 회사별 합산 커버리지 계산
        - 단가 조정 (커버리지 ↑ → 가격 ↑)

스키마 (회사별 컬럼):
    src_{key}_coverage : float (0~100) — 해당 소스의 회사 커버리지%
    src_{key}          : bool — coverage > 0 이면 True (편의)
"""
from __future__ import annotations

from typing import Iterable, Optional
import pandas as pd


# ── Canonical 데이터 소스 (원천) ─────────────────────────────────────────
# (key, 한글 라벨, 영문, 설명, 기본 가용성 가중치 0~1)
CANONICAL_SOURCES: list[tuple[str, str, str, str, float]] = [
    ("card",          "💳 Card",         "Credit/Debit Card",    "신용·체크카드 결제 트랜잭션",       0.90),
    ("pos",           "🧾 POS",          "Point-of-Sale",         "오프라인 매장 POS 데이터",          0.55),
    ("satellite",     "🛰 Satellite",    "Satellite Imagery",     "위성 이미지 (주차장·창고 등)",     0.18),
    ("foot_traffic",  "👣 Foot Traffic", "Foot Traffic",          "방문객·유동인구 (모바일 GPS)",      0.55),
    ("web",           "🌐 Web",          "Web Traffic",           "웹사이트 트래픽·세션",              0.85),
    ("app",           "📱 App",          "Mobile App Usage",      "앱 다운로드·세션·DAU",              0.60),
    ("ecommerce",     "🛍 E-commerce",   "E-commerce Panel",      "온라인 쇼핑 거래·장바구니",         0.45),
    ("reviews",       "⭐ Reviews",      "Online Reviews",        "리뷰·평점 (제품·서비스)",           0.40),
    ("jobs",          "💼 Jobs",         "Job Postings",          "채용 공고·재직자 LinkedIn",         0.50),
    ("geo",           "📍 Geolocation",  "Geolocation",           "위치 데이터 (체크인 등)",           0.30),
    ("social",        "💬 Social",       "Social Media",          "SNS 멘션·해시태그·감성",            0.65),
]

SOURCE_KEYS: list[str] = [k for k, *_ in CANONICAL_SOURCES]


def source_label(key: str) -> str:
    for k, lbl, *_ in CANONICAL_SOURCES:
        if k == key:
            return lbl
    return key


def coverage_col(key: str) -> str:
    return f"src_{key}_coverage"


def has_col(key: str) -> str:
    return f"src_{key}"


# ── 회사별 합산 커버리지 ──────────────────────────────────────────────────
def combined_coverage(df: pd.DataFrame, selected: Iterable[str]) -> pd.Series:
    """선택된 소스들의 합산 커버리지 (cap 100%).

    회사별로 sum(src_{k}_coverage for k in selected if column exists).
    """
    keys = [k for k in selected if coverage_col(k) in df.columns]
    if not keys:
        return pd.Series([0.0] * len(df), index=df.index, dtype="float64")
    summed = sum(
        pd.to_numeric(df[coverage_col(k)], errors="coerce").fillna(0.0)
        for k in keys
    )
    return summed.clip(upper=100.0)


def matched_count(df: pd.DataFrame, selected: Iterable[str]) -> pd.Series:
    """선택된 소스 중 회사가 보유한 소스의 개수."""
    sel = [k for k in selected if has_col(k) in df.columns]
    if not sel:
        return pd.Series([0] * len(df), index=df.index, dtype="int64")
    return sum(df[has_col(k)].astype(bool).astype(int) for k in sel)


def available_sources(row: pd.Series) -> list[str]:
    """한 행에서 회사가 보유한 소스 키 리스트."""
    out: list[str] = []
    for k in SOURCE_KEYS:
        col = has_col(k)
        if col in row.index and bool(row[col]):
            out.append(k)
    return out


def default_selection() -> list[str]:
    """디폴트 — 가용성 높은 4개 소스."""
    return ["card", "pos", "foot_traffic", "web"]
