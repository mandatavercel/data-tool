"""
FX Signal — 매크로 이벤트 캘린더.

events.json 을 로드해 다가오는 매크로 이벤트(FOMC, BOK MPC, US CPI, NFP 등)를
대시보드에 표시. 부정확한 일정을 코드에 박는 대신, 사용자가 events.json 을
직접 편집해 정확한 일정만 보이게 함.

events.json 스키마:
[
  {
    "date":     "2026-06-18",          # ISO 날짜 (필수)
    "title":    "FOMC 6월 회의",          # 필수
    "category": "Fed",                  # Fed / BOK / US Data / KR Data / Other
    "impact":   "high",                 # high / medium / low (USD/KRW 영향도)
    "note":     "점도표 업데이트"           # 선택 (자유 메모)
  },
  ...
]

빈 파일이면 대시보드에 안내만 표시.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional


HERE = Path(__file__).resolve().parent
EVENTS_PATH = HERE / "events.json"


CATEGORY_ICONS = {
    "Fed":     "🇺🇸",
    "BOK":     "🇰🇷",
    "US Data": "📊",
    "KR Data": "📈",
    "ECB":     "🇪🇺",
    "BOJ":     "🇯🇵",
    "Other":   "📌",
}


IMPACT_COLORS = {
    "high":   "#EF4444",
    "medium": "#F59E0B",
    "low":    "#94A3B8",
}


@dataclass
class MacroEvent:
    date: date
    title: str
    category: str = "Other"
    impact: str = "medium"   # high/medium/low
    note: str = ""

    @property
    def icon(self) -> str:
        return CATEGORY_ICONS.get(self.category, "📌")

    @property
    def color(self) -> str:
        return IMPACT_COLORS.get(self.impact, "#94A3B8")

    @property
    def days_until(self) -> int:
        return (self.date - date.today()).days


def _parse_date(raw: str) -> Optional[date]:
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def load_events() -> list[MacroEvent]:
    """events.json 로드. 손상되거나 없으면 빈 리스트."""
    if not EVENTS_PATH.exists():
        return []
    try:
        data = json.loads(EVENTS_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []

    out: list[MacroEvent] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        d = _parse_date(item.get("date", ""))
        if d is None:
            continue
        title = str(item.get("title", "")).strip()
        if not title:
            continue
        out.append(MacroEvent(
            date=d,
            title=title,
            category=str(item.get("category", "Other")),
            impact=str(item.get("impact", "medium")).lower(),
            note=str(item.get("note", "")).strip(),
        ))
    out.sort(key=lambda e: e.date)
    return out


def upcoming(days: int = 30) -> list[MacroEvent]:
    """오늘 ~ days일 이내 이벤트 (오늘 포함)."""
    today = date.today()
    return [e for e in load_events() if 0 <= (e.date - today).days <= days]


def save_events(events: list[MacroEvent]) -> None:
    """events 리스트를 events.json 으로 저장. (관리자 UI 용)"""
    out = [
        {
            "date":     e.date.isoformat(),
            "title":    e.title,
            "category": e.category,
            "impact":   e.impact,
            "note":     e.note,
        }
        for e in events
    ]
    EVENTS_PATH.write_text(
        json.dumps(out, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
