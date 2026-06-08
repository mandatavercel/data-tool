"""
FX Signal — 매크로 이벤트별 환율 영향 분석.

각 이벤트 카테고리(FOMC, BOK, 미국 CPI, NFP, PCE, ECB 등)에 대해
시나리오별 USD/KRW 방향 + 환전자 관점 권고를 미리 작성한 정적 분석.

새 이벤트 카테고리/패턴이 등장하면 _analyze_*() 함수만 추가하면 됨.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class EventAnalysis:
    overview: str                 # 이벤트가 무엇을 발표하는지 한 줄
    release_time_kst: str         # 한국시간 발표 시각
    key_variables: list[str]      # 시장이 주목하는 변수들

    hawkish_label: str            # 매파적/높음/강함 시나리오 라벨
    hawkish_path: str             # 매파적 결과 → 어떤 경로로 USD/KRW에 작용
    hawkish_direction: str        # USD/KRW 방향 ("↑ 상승" / "↓ 하락" 등)

    dovish_label: str             # 비둘기파적/낮음/약함 시나리오 라벨
    dovish_path: str
    dovish_direction: str

    actionable: str               # 환전자 관점 한 줄 권고
    volatility: str               # 발표 직후 예상 변동성


# ─────────────────────────────────────────────────────────────
# 카테고리별 분석
# ─────────────────────────────────────────────────────────────
def _analyze_fomc(title: str) -> EventAnalysis:
    has_sep = "점도표" in title or "SEP" in title or "dot" in title.lower()
    return EventAnalysis(
        overview=(
            "미국 연방준비제도(Fed) FOMC 통화정책 결정. "
            + ("**분기별 점도표(SEP)도 함께 업데이트**되어 향후 1~2년 금리 경로가 드러납니다." if has_sep else "기준금리 결정과 성명, 파월 의장 기자회견이 핵심.")
        ),
        release_time_kst="03:00 KST(다음날) 성명 · 03:30 KST 파월 기자회견",
        key_variables=[
            "기준금리 결정 (동결/인상/인하)",
            "점도표(SEP)" if has_sep else "성명문 내 인플레/고용 평가 변화",
            "파월 기자회견 톤 — 매파/비둘기 신호",
        ],
        hawkish_label="🦅 매파적 — 금리 동결 시사 또는 점도표 상향",
        hawkish_path=(
            "Fed가 '인플레 아직 안 잡힘, 금리 더 오래 유지' 시그널 → "
            "미국 국채금리 상승 → 달러 매력 ↑ → 글로벌 자금 미국 유입 → "
            "한국에서 자금 유출 → **원화 약세 → USD/KRW 상승**"
        ),
        hawkish_direction="↑ USD/KRW 상승 (환전 대기 유리)",
        dovish_label="🕊 비둘기파적 — 금리 인하 시사 또는 점도표 하향",
        dovish_path=(
            "Fed가 '경기 둔화 우려, 곧 인하' 시그널 → "
            "미국 금리 하락 → 달러 매력 ↓ → 신흥국 위험자산으로 자금 이동 → "
            "한국 자산 매수 → **원화 강세 → USD/KRW 하락**"
        ),
        dovish_direction="↓ USD/KRW 하락 (지금 환전 유리)",
        actionable=(
            "FOMC 직전 12시간 환전은 신중. 결과 확인 후 행동 권장. "
            "점도표 발표 시 변동성 특히 큼 — 회견 30분 이후 가격이 안정되는 경향."
        ),
        volatility="발표 후 1시간 내 USD/KRW ±0.5~1.5% 변동 가능 (매우 높음)",
    )


def _analyze_bok(title: str) -> EventAnalysis:
    return EventAnalysis(
        overview=(
            "한국은행 금융통화위원회 통화정책방향 결정. "
            "기준금리 변경 여부와 총재 기자회견이 핵심."
        ),
        release_time_kst="10:00 KST 회의 시작 · 11:10 KST 총재 기자회견",
        key_variables=[
            "기준금리 결정 (동결/인상/인하)",
            "통방 결정문의 인플레/경기 평가",
            "총재 기자회견 톤",
            "Fed와의 금리차 (현재 격차에 대한 인식)",
        ],
        hawkish_label="🦅 매파적 — 금리 동결 또는 인상",
        hawkish_path=(
            "BOK가 '인플레 우려 / 외환 안정 위해 긴축 유지' 시그널 → "
            "한국 자산 매력 ↑ → 외국인 자금 유입 → "
            "**원화 강세 → USD/KRW 하락**"
        ),
        hawkish_direction="↓ USD/KRW 하락 (환전 불리)",
        dovish_label="🕊 비둘기파적 — 금리 인하",
        dovish_path=(
            "BOK가 '경기 부양 우선, 인하 시작' 시그널 → "
            "한국 자산 매력 ↓ → Fed와 금리차 확대 → "
            "**원화 약세 → USD/KRW 상승**"
        ),
        dovish_direction="↑ USD/KRW 상승 (환전 유리)",
        actionable=(
            "KRW에 직접 영향. 결과 발표 후 1~2시간 변동성 큼. "
            "BOK는 통상 Fed에 후행하는 경향 — Fed가 인하 시작했으면 BOK도 곧."
        ),
        volatility="발표 후 1시간 내 USD/KRW ±0.3~0.8% 변동 (높음)",
    )


def _analyze_us_cpi(title: str) -> EventAnalysis:
    return EventAnalysis(
        overview=(
            "미국 소비자물가지수(CPI). Fed의 인플레 목표(2%) 달성 진척도를 보는 핵심 지표. "
            "Headline + Core(식품·에너지 제외) 둘 다 중요."
        ),
        release_time_kst="21:30 KST (서머타임 시) / 22:30 KST (표준시)",
        key_variables=[
            "Headline CPI MoM (전월 대비)",
            "Headline CPI YoY (전년 대비)",
            "Core CPI MoM / YoY — Fed가 더 주목",
            "컨센서스 대비 surprise (+/- 0.1%p 이상이 시장 흔듦)",
        ],
        hawkish_label="🔥 예상보다 높음 — 인플레 끈적",
        hawkish_path=(
            "물가가 안 잡히는 신호 → Fed 더 오래 매파적 → "
            "미국 금리 상승 → 달러 강세 → **USD/KRW 상승**"
        ),
        hawkish_direction="↑ USD/KRW 상승 (환전 대기 유리)",
        dovish_label="❄️ 예상보다 낮음 — 인플레 완화",
        dovish_path=(
            "물가 둔화 신호 → Fed 인하 기대 ↑ → "
            "미국 금리 하락 → 달러 약세 → **USD/KRW 하락**"
        ),
        dovish_direction="↓ USD/KRW 하락 (지금 환전 유리)",
        actionable=(
            "다음 FOMC 결정의 가장 큰 인풋. 발표 직후 30분 내 큰 변동. "
            "Core CPI MoM이 +0.3%를 넘느냐가 통상 분기점."
        ),
        volatility="발표 후 30분 내 USD/KRW ±0.4~1% 변동 (매우 높음)",
    )


def _analyze_us_nfp(title: str) -> EventAnalysis:
    return EventAnalysis(
        overview=(
            "미국 비농업 부문 고용지표(NFP) — 매월 첫 금요일 발표. "
            "고용 변화(NFP) + 실업률 + 평균 시간당 임금이 핵심."
        ),
        release_time_kst="21:30 KST (서머타임 시) / 22:30 KST (표준시)",
        key_variables=[
            "신규 고용 (NFP) — 컨센서스 대비 ±5만 이상이 큰 surprise",
            "실업률 (U-3)",
            "평균 시간당 임금 (Average Hourly Earnings) — 인플레 신호",
            "노동참여율 / 전월 수치 수정 폭",
        ],
        hawkish_label="💪 강한 고용 — 예상 대폭 상회 + 임금 ↑",
        hawkish_path=(
            "경제 견고 + 임금발 인플레 우려 → Fed 매파적 유지 → "
            "미국 금리 ↑ → 달러 강세 → **USD/KRW 상승**"
        ),
        hawkish_direction="↑ USD/KRW 상승",
        dovish_label="📉 약한 고용 — 예상 대폭 하회 + 실업률 ↑",
        dovish_path=(
            "경제 둔화 우려 → Fed 인하 기대 ↑ → "
            "미국 금리 ↓ → 달러 약세 → **USD/KRW 하락**"
        ),
        dovish_direction="↓ USD/KRW 하락",
        actionable=(
            "매월 첫 금요일 정기 이벤트라 단기 트레이딩 노이즈가 큼. "
            "NFP 자체보다 임금 상승률이 Fed 정책에 더 영향. "
            "발표 직후 1~2시간은 환전 자제 권장."
        ),
        volatility="발표 후 1시간 내 USD/KRW ±0.3~0.7% 변동 (높음)",
    )


def _analyze_us_pce(title: str) -> EventAnalysis:
    return EventAnalysis(
        overview=(
            "미국 PCE 물가지수 — Fed가 공식 인플레 타깃(2%)으로 사용하는 지표. "
            "CPI보다 약 한 달 뒤 발표되지만 정책 결정엔 더 큰 가중치."
        ),
        release_time_kst="21:30 KST (서머타임 시) / 22:30 KST (표준시)",
        key_variables=[
            "Core PCE YoY — Fed가 가장 주목 (목표 2%)",
            "Core PCE MoM",
            "Headline PCE",
            "소비/소득 데이터 (가계 소비 견고도)",
        ],
        hawkish_label="🔥 Core PCE 예상 상회",
        hawkish_path=(
            "Fed 선호 지표가 안 식음 → 인하 더 지연 → "
            "달러 강세 → **USD/KRW 상승**"
        ),
        hawkish_direction="↑ USD/KRW 상승",
        dovish_label="❄️ Core PCE 예상 하회",
        dovish_path=(
            "Fed 목표에 가까워짐 → 인하 명분 ↑ → "
            "달러 약세 → **USD/KRW 하락**"
        ),
        dovish_direction="↓ USD/KRW 하락",
        actionable=(
            "CPI보다 시장 반응 약하지만 다음 FOMC의 결정 인풋. "
            "월말 발표라 분기말 리밸런싱 효과와 겹칠 수 있음."
        ),
        volatility="발표 후 30분 내 USD/KRW ±0.2~0.5% 변동 (중간)",
    )


def _analyze_ecb(title: str) -> EventAnalysis:
    return EventAnalysis(
        overview=(
            "ECB 통화정책 결정. EUR이 DXY 가중치의 약 58%를 차지해, "
            "ECB의 매파/비둘기는 DXY를 통해 USD/KRW에 간접 영향."
        ),
        release_time_kst="21:15 KST 성명 (서머타임) · 21:45 KST 라가르드 기자회견",
        key_variables=[
            "예금금리(DFR) 결정 — ECB의 정책 금리",
            "MRO (Main Refinancing Rate)",
            "라가르드 기자회견 톤",
            "Fed-ECB 금리차 인식",
        ],
        hawkish_label="🦅 매파적 — 동결 또는 인상",
        hawkish_path=(
            "ECB가 긴축 유지 → EUR 강세 → DXY 하락 → "
            "달러 약세 → **USD/KRW 하방 압력** (간접)"
        ),
        hawkish_direction="↓ USD/KRW 하락 (간접, 약함)",
        dovish_label="🕊 비둘기파적 — 인하 또는 연속 인하 시사",
        dovish_path=(
            "ECB가 완화 → EUR 약세 → DXY 상승 → "
            "달러 강세 → **USD/KRW 상방 압력** (간접)"
        ),
        dovish_direction="↑ USD/KRW 상승 (간접, 약함)",
        actionable=(
            "USD/KRW에 직접 영향은 약함. 다만 ECB가 Fed보다 먼저 인하 시작하면 "
            "DXY가 구조적 강세로 가서 원화도 지속 약세 압력."
        ),
        volatility="USD/KRW 직접 영향은 ±0.1~0.3% 정도 (낮음)",
    )


def _analyze_kr_cpi(title: str) -> EventAnalysis:
    return EventAnalysis(
        overview=(
            "한국 소비자물가지수(KOSTAT). BOK의 인플레 목표(2%) 달성도를 보는 지표. "
            "BOK 다음 금통위의 인풋."
        ),
        release_time_kst="08:00 KST",
        key_variables=[
            "Headline CPI YoY (전년 대비)",
            "Core CPI (식품·에너지 제외)",
            "Service CPI — BOK가 주목",
        ],
        hawkish_label="🔥 예상 상회 — 물가 안 잡힘",
        hawkish_path=(
            "BOK 인하 지연 압력 → KRW 강세 → **USD/KRW 하락**"
        ),
        hawkish_direction="↓ USD/KRW 하락 (환전 불리)",
        dovish_label="❄️ 예상 하회 — 물가 둔화",
        dovish_path=(
            "BOK 인하 명분 ↑ → KRW 약세 → **USD/KRW 상승**"
        ),
        dovish_direction="↑ USD/KRW 상승 (환전 유리)",
        actionable="시장 반응은 미국 CPI 대비 작음. BOK 회의 직전 발표라면 영향 ↑.",
        volatility="발표 후 USD/KRW ±0.1~0.3% (낮음~중간)",
    )


def _analyze_generic(title: str, category: str) -> EventAnalysis:
    return EventAnalysis(
        overview=f"{category} 카테고리의 매크로 이벤트. 자세한 분석은 미등록 — events_analysis.py에 추가 가능.",
        release_time_kst="—",
        key_variables=["발표 자체 결과", "컨센서스 대비 surprise"],
        hawkish_label="🔥 예상보다 강한 결과",
        hawkish_path="달러 강세 시나리오에 우호적이면 USD/KRW 상방 압력",
        hawkish_direction="↑ 가능성",
        dovish_label="❄️ 예상보다 약한 결과",
        dovish_path="달러 약세 시나리오에 우호적이면 USD/KRW 하방 압력",
        dovish_direction="↓ 가능성",
        actionable="이벤트 직전·직후 변동성 가능. 결과 확인 후 행동 권장.",
        volatility="중간",
    )


# ─────────────────────────────────────────────────────────────
# 라우터 — title/category 기반으로 분석 선택
# ─────────────────────────────────────────────────────────────
def analyze_event(category: str, title: str) -> EventAnalysis:
    """이벤트 카테고리 + 제목 키워드로 적절한 분석 반환."""
    t = title or ""
    c = (category or "").strip()

    if c == "Fed" or "FOMC" in t:
        return _analyze_fomc(t)
    if c == "BOK" or "금통위" in t or "한국은행" in t:
        return _analyze_bok(t)
    if c == "ECB":
        return _analyze_ecb(t)

    # 키워드 기반
    if "CPI" in t:
        if "미국" in t or c == "US Data":
            return _analyze_us_cpi(t)
        if "한국" in t or c == "KR Data":
            return _analyze_kr_cpi(t)
        return _analyze_us_cpi(t)  # default to US (더 큰 영향)
    if "NFP" in t or "고용지표" in t or "고용" in t:
        return _analyze_us_nfp(t)
    if "PCE" in t:
        return _analyze_us_pce(t)

    return _analyze_generic(t, c)
