"""
Mandata Data Intelligence — 페이지 레지스트리
==============================================
모든 앱(페이지)의 메타데이터를 한 곳에서 관리. 확장성의 핵심.

새 앱 추가 절차 (5분):
  1. 이 파일의 PAGES 리스트에 PageEntry 하나 추가
  2. pages/<key>.py 에 thin wrapper 생성 (4~5줄). 템플릿:

         # pages/<key>.py
         import sys; from pathlib import Path
         sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
         import auth
         auth.gate("<key>", "<display name>")
         auth.run_legacy_app("<folder>", "<entry.py>")

  3. (선택) acl.json 의 page_access 에 "<key>": [] 추가 — 안 해도 admin/도메인 와일드카드는 자동 접근

레지스트리 필드 의미:
  - key:           ACL과 매칭되는 식별자 (acl.json 의 page_access 키)
  - name:          사이드바·카드에 표시될 한글 이름
  - icon:          이모지
  - category:      카테고리 (CATEGORIES 중 하나 권장. 새 값 쓰면 자동 추가됨)
  - description:   2~3줄 설명 (대시보드 카드용)
  - entry_file:    Streamlit이 페이지로 로드할 파일 경로 (ROOT 기준 상대)
  - show_in_launcher: 대시보드 카드로 노출할지 (launcher/admin은 False)
  - admin_only:    admin만 진입 가능한 페이지
"""
from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path


ROOT = Path(__file__).resolve().parent
_OVERRIDES_PATH = ROOT / "page_overrides.json"


@dataclass(frozen=True)
class PageEntry:
    key: str
    name: str
    icon: str
    category: str
    description: str = ""
    entry_file: str = ""
    show_in_launcher: bool = True
    admin_only: bool = False

    @property
    def absolute_entry(self) -> Path:
        return ROOT / self.entry_file


# ─────────────────────────────────────────────────────────────
# 카테고리 (사이드바 그룹핑 + 칩 필터)
# 새 카테고리 쓰면 자동 추가되지만, 아이콘은 여기서 등록해줘야 예쁘게 표시됨
# ─────────────────────────────────────────────────────────────
CATEGORIES: list[str] = [
    "Data Analysis",
    "Partnership",
    "Sales",
    "Operations",
    "Marketing",
]

CATEGORY_ICONS: dict[str, str] = {
    "Data Analysis": "📊",
    "Partnership": "🤝",
    "Sales": "💰",
    "Operations": "⚙️",
    "Marketing": "📣",
    "Admin": "🛡",
    "Home": "🏠",
}


# ─────────────────────────────────────────────────────────────
# 페이지 목록 (등장 순서 = 사이드바 표시 순서)
# _BASE_PAGES = 코드로 정의된 기본값
# PAGES = _BASE_PAGES에 page_overrides.json 적용한 최종 결과 (외부 import용)
# 관리자 페이지에서 카테고리 등을 바꾸면 page_overrides.json만 변경됨 (이 파일은 안 건드림)
# ─────────────────────────────────────────────────────────────
_BASE_PAGES: list[PageEntry] = [
    # ──── Home (대시보드) ──────────────────────────────────
    PageEntry(
        key="launcher",
        name="대시보드",
        icon="🏠",
        category="Home",
        description="모든 앱 한눈에 보기",
        entry_file="pages/launcher.py",
        show_in_launcher=False,
    ),

    # ──── 📊 Data Analysis ─────────────────────────────────
    PageEntry(
        key="analysis",
        name="데이터 분석",
        icon="📊",
        category="Data Analysis",
        description=(
            "Factor 리서치와 POS 데이터 분석을 한 곳에서. "
            "KRX 종목·산업 데이터와 매출 흐름을 결합해 통계를 뽑고, "
            "결과를 PPT·PDF 리포트로 바로 export."
        ),
        entry_file="pages/analysis.py",
    ),
    PageEntry(
        key="mapping",
        name="데이터 매핑",
        icon="🗂",
        category="Data Analysis",
        description=(
            "POS·카드 거래 raw 데이터의 컬럼명·코드체계를 우리 표준에 맞게 매핑. "
            "원본별 다른 스키마를 통일된 마스터 형태로 변환해 분석 단계로 넘김."
        ),
        entry_file="pages/mapping.py",
    ),
    PageEntry(
        key="kfnb",
        name="K-F&B 데이터 상품",
        icon="🍜",
        category="Data Analysis",
        description=(
            "F&B POS(편의점 등) 원천 데이터를 글로벌 투자기관용 '투자등급' 데이터 "
            "상품으로 자동 변환. ①프로파일링 → ②정규화(SKU 파싱) → ③투자 테마 태깅 "
            "→ ④티커 매핑 → ⑤패널 집계 → ⑥xlsx 상품 생성까지 단계마다 검증."
        ),
        entry_file="pages/kfnb.py",
    ),
    PageEntry(
        key="security_id",
        name="종목 식별",
        icon="🔎",
        category="Data Analysis",
        description=(
            "한국 주식 식별자(이름·약어·로컬코드·ISIN·Bloomberg·RIC·DART 코드) "
            "어떤 표기로 들어와도 단일 종목 레코드로 매칭. 파생 underlying name 변환, "
            "보통주↔우선주 연결, 지수 멤버 조회, CSV 일괄 변환까지."
        ),
        entry_file="pages/security_id.py",
    ),

    # 마켓 데이터 페이지는 Next.js (hangang/) 로 이동됨 — 2026-05 데시전.
    # marketdata_app/ 의 data.py·brief.py 는 FastAPI 백엔드 포팅 참조용으로 보존.

    # ──── 💰 Sales / Finance ──────────────────────────────
    PageEntry(
        key="fx_signal",
        name="FX 환율 신호",
        icon="💱",
        category="Sales",
        description=(
            "글로벌 매출(USD) → KRW 환전 타이밍을 신호 점수로 제시. "
            "단기(1~2주) + 중기(1~3개월) 두 호라이즌으로 USD/KRW·DXY·UST·KOSPI·원유·CNY를 "
            "종합해 '지금 환전 / 대기 / 중립' 판정. 매크로 이벤트 캘린더 포함."
        ),
        entry_file="pages/fx_signal.py",
    ),
    PageEntry(
        key="ar",
        name="AR Management",
        icon="💰",
        category="Sales",
        description=(
            "고객사 계약·인보이스 자동 일정·수금 추적·데이터 오너 배분 관리. "
            "Billing frequency(월/분기/연/일회성)에 맞춰 인보이스 일정 자동 생성. "
            "임박/연체 인보이스 한눈에. (Phase 2에서 이메일 알림·배분 송금서·월별 리포트 추가)"
        ),
        entry_file="pages/ar.py",
    ),

    # ──── ⚙️ Operations ────────────────────────────────────
    PageEntry(
        key="catalog",
        name="데이터 카탈로그",
        icon="🛒",
        category="Operations",
        description=(
            "상품·브랜드 카탈로그를 정리하고 영문화 번역까지 일괄 처리. "
            "Claude API로 한글 브랜드명을 공식 영문 표기로 변환해 글로벌 분석에 사용."
        ),
        entry_file="pages/catalog.py",
    ),

    # ──── 🤝 Partnership ───────────────────────────────────
    PageEntry(
        key="bizno",
        name="사업자조회",
        icon="🏢",
        category="Partnership",
        description=(
            "사업자번호로 업체명·상태를 자동 조회하고 마스터 파일과 매칭. "
            "bizno.net 스크래핑, 국세청 API, 영문 머천트 코드 → 한글명(Claude), "
            "VLOOKUP 매칭까지 3가지 모드 지원."
        ),
        entry_file="pages/bizno.py",
    ),
    PageEntry(
        key="revshare",
        name="수익배분 산정",
        icon="💼",
        category="Partnership",
        description=(
            "데이터 파트너 수익배분율을 정량 평가로 자동 산정. "
            "원천사 유형 + 7개 평가 항목 점수 입력 → Tier별 배분율, 협상 문구, "
            "산정 근거 즉시 출력. 산정 이력 저장 및 PDF 인쇄 지원."
        ),
        entry_file="pages/revshare.py",
    ),
    PageEntry(
        key="contract",
        name="계약서 생성기",
        icon="📜",
        category="Partnership",
        description=(
            "데이터 공급 계약서·NDA·DDQ 등 표준 양식을 질문 답변만으로 자동 생성. "
            "맨데이터 정통 법무 양식(국내 DSA v1.0 기본 포함)에 진척도·미치환 변수 감지·"
            "입력값 백업까지 한 화면에서 처리. 새 양식은 templates/<key>/ 폴더만 추가하면 즉시 인식."
        ),
        entry_file="pages/contract.py",
    ),

    # ──── 🛡 Admin (관리자 전용) ───────────────────────────
    PageEntry(
        key="admin",
        name="관리자",
        icon="🛡",
        category="Admin",
        description="권한·사용자 관리",
        entry_file="pages/admin.py",
        show_in_launcher=False,
        admin_only=True,
    ),
]


# ─────────────────────────────────────────────────────────────
# Overrides — 코드 수정 없이 카테고리 등을 바꿀 수 있는 데이터 레이어
# 관리자 페이지에서 편집 → page_overrides.json 저장 → 다음 streamlit run 시 반영
# ─────────────────────────────────────────────────────────────
def _empty_overrides() -> dict:
    return {"category_overrides": {}}


# Neon Postgres (db_store)을 시도하고, 실패/비활성 시 JSON 파일 fallback.
# AR 앱의 데이터 저장 패턴과 동일.
def _db_read(name: str):
    try:
        from ar_app import db_store
        if db_store.enabled():
            return db_store.read(name)
    except Exception:
        pass
    return None


def _db_write(name: str, data) -> bool:
    try:
        from ar_app import db_store
        if db_store.enabled():
            db_store.write(name, data)
            return True
    except Exception:
        pass
    return False


def load_overrides() -> dict:
    """page_overrides 로드. 우선순위: Neon DB → JSON 파일 → 빈 overrides."""
    # 1) Neon DB
    db_data = _db_read("page_overrides")
    if isinstance(db_data, dict):
        db_data.setdefault("category_overrides", {})
        return db_data
    # 2) JSON 파일 fallback
    if not _OVERRIDES_PATH.exists():
        return _empty_overrides()
    try:
        data = json.loads(_OVERRIDES_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return _empty_overrides()
    data.setdefault("category_overrides", {})
    return data


def save_overrides(data: dict) -> None:
    """page_overrides 저장. Neon DB에 우선 저장, JSON 파일에도 best-effort 동기화."""
    out = {k: v for k, v in data.items()}
    # 1) Neon DB (영구 저장)
    db_ok = _db_write("page_overrides", out)
    # 2) JSON 파일 (로컬 / 백업 / DB 비활성 시 단일 source)
    try:
        _OVERRIDES_PATH.write_text(
            json.dumps(out, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        if not db_ok:
            raise


def apply_overrides(base: list[PageEntry], overrides: dict | None = None) -> list[PageEntry]:
    """_BASE_PAGES에 overrides 적용한 새 리스트 반환 (원본 불변)."""
    if overrides is None:
        overrides = load_overrides()
    cat_map: dict = overrides.get("category_overrides", {}) or {}

    result: list[PageEntry] = []
    for p in base:
        new_cat = cat_map.get(p.key)
        if new_cat and new_cat != p.category:
            result.append(replace(p, category=new_cat))
        else:
            result.append(p)
    return result


# 최종 PAGES — overrides 적용된 리스트 (모듈 import 시 한 번 평가, reload()로 갱신)
PAGES: list[PageEntry] = apply_overrides(_BASE_PAGES)


# ─────────────────────────────────────────────────────────────
# 빠른 lookup
# ─────────────────────────────────────────────────────────────
PAGES_BY_KEY: dict[str, PageEntry] = {p.key: p for p in PAGES}


def reload() -> None:
    """
    PAGES / PAGES_BY_KEY 를 최신 overrides 로 재계산.

    모듈 import 시점에 한 번만 평가된 PAGES 는 admin 페이지에서 카테고리를 바꿔도
    바로 안 보임. streamlit_app.py / pages/launcher.py 의 매 rerun 시작부에서
    호출해야 최신 상태로 사이드바·대시보드가 그려진다.
    """
    global PAGES, PAGES_BY_KEY
    PAGES = apply_overrides(_BASE_PAGES)
    PAGES_BY_KEY = {p.key: p for p in PAGES}


def all_categories_in_use() -> list[str]:
    """레지스트리에 실제로 등장하는 카테고리 목록 (등장 순서). CATEGORIES + 새 값 자동 통합."""
    seen: set = set()
    out: list[str] = []
    # 기본 정의된 카테고리 먼저
    for c in CATEGORIES:
        if c not in seen:
            seen.add(c)
            out.append(c)
    # 레지스트리에 등장하는 카테고리 (Home/Admin 같은 특수 카테고리 포함)
    for p in PAGES:
        if p.category and p.category not in seen:
            seen.add(p.category)
            out.append(p.category)
    return out


def launcher_pages() -> list[PageEntry]:
    """대시보드 카드 영역에 보여줄 페이지들 (show_in_launcher=True)."""
    return [p for p in PAGES if p.show_in_launcher]
