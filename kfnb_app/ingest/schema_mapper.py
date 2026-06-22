"""
kfnb_app/ingest/schema_mapper.py — 멀티 오너 스키마 매핑.

데이터 오너마다 컬럼명이 달라도 configs/owner_schema_mapping.yaml 의 규칙으로
표준(canonical) 컬럼명으로 통일한다. 오너 자동 감지 + 명시 지정 모두 지원.
"""
from __future__ import annotations

from kfnb_app import config


def detect_owner(columns: list[str]) -> str:
    """원천 헤더로 오너를 추정. 가장 많이 매칭되는 오너. 동률이면 default."""
    cols = {str(c).strip().lstrip("﻿") for c in columns}
    best, best_score = config.DEFAULT_OWNER, -1
    for owner, sch in config.OWNER_SCHEMAS.items():
        headers = {str(h) for hs in sch.values() for h in (hs or [])}
        score = len(cols & headers)
        if score > best_score:
            best, best_score = owner, score
    return best


def rename_map(columns: list[str], owner: str | None = None) -> dict[str, str]:
    """원천 헤더 → canonical. owner 미지정 시 자동 감지."""
    owner = owner or detect_owner(columns)
    return config.rename_map(columns, owner)


def missing_required(columns: list[str], owner: str | None = None) -> list[str]:
    """매핑 후에도 빠진 필수 canonical 컬럼."""
    present = set(rename_map(columns, owner).values())
    return [c for c in config.REQUIRED_CANON if c not in present]


def capabilities(columns: list[str], owner: str | None = None) -> dict:
    """가용 표준 컬럼으로 분석 입자도/기능 판정."""
    p = set(rename_map(columns, owner).values())
    has_sku = "barcode" in p or "sku_name_kr" in p
    has_brand = "brand_kr" in p
    grain = "sku" if has_sku else ("brand" if has_brand else "company")
    return {
        "present": sorted(p),
        "missing_recommended": [c for c in config.RECOMMENDED_CANON if c not in p],
        "grain": grain,
        "has_brand": has_brand,
        "has_sku": has_sku,
        "has_category": "cat_l2" in p,
        "has_qty": "sales_qty" in p,
        "has_region": "region" in p,
    }
