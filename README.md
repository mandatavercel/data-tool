# 📊 Mandata Data Intelligence

거래·카드·통신 등 alt-data 운영에 필요한 내부 도구들을 **한 화면에서 권한 기반으로** 제공하는 통합 플랫폼.

> 데이터 분석 · 데이터 매핑 · 데이터 카탈로그 · 사업자 조회 · 수익배분 산정 — 앱별로 초대 명단을 따로 관리하면서 한 URL로 사용 가능.

---

## 🚀 빠른 시작 (로컬, macOS)

1. **통합 런처**: `🚀 통합 런처 실행.command` 더블클릭
   - 첫 실행 시 필요한 패키지 자동 설치 (5~10분, 인터넷 필요)
   - 브라우저에 `http://localhost:8500` 자동 열림
2. **자동 로그인 설정 (선택)**:
   ```bash
   echo 'export MANDATA_DEV_EMAIL=yonghan@mandata.kr' >> ~/.zshrc
   source ~/.zshrc
   ```
3. **개별 앱 단독 실행** (디버깅 용): 각 앱별 `.command` 파일 더블클릭

---

## ☁️ Streamlit Cloud 배포

### 사전 준비
- GitHub 계정 + **private repo** (acl.json에 이메일이 들어있어 public 부적절)
- (선택) Anthropic / DART / NTS API key

### 배포 절차

```bash
# 1) Git 초기화 + push
cd ~/Desktop/data-tool
git init && git add . && git commit -m "Initial multi-page deploy"
git remote add origin git@github.com:<org>/<repo>.git
git push -u origin main
```

1. [share.streamlit.io](https://share.streamlit.io) 접속 → GitHub 연동
2. **New app** 클릭 → 정보 입력:
   - Repository: `<org>/<repo>`
   - Branch: `main`
   - **Main file path: `streamlit_app.py`** (multi-page 진입점)
   - App URL: `mandata-data-intelligence` 등 원하는 subdomain
3. **Advanced settings → Secrets** 에 다음 붙여넣기 (`secrets.toml.example` 참고):
   ```toml
   ANTHROPIC_API_KEY = "sk-ant-..."
   DART_API_KEY = "..."
   NTS_API_KEY = "..."
   ```
4. **Deploy!** → 첫 빌드 5~10분, 이후 GitHub push마다 자동 재배포

### 초대제 운영 방식 비교

| 방식 | 장점 | 단점 |
|---|---|---|
| **Community Cloud (무료)** + acl.json | $0, 즉시 시작 | URL 알면 누구나 접근 가능 (이메일 입력 후 권한 체크는 됨) |
| **Cloud Connect (유료)** + viewer auth | 진짜 초대제, Google SSO | 시트당 $25/월 |

> **권장**: 일단 Community Cloud로 시작 → 정식 운영 단계에서 Cloud Connect로 업그레이드. 어느 쪽이든 `acl.json`이 진짜 권한 통제를 담당하고, viewer auth는 "URL 접근 가능 명단의 superset" 역할.

---

## 🧩 새 앱 추가하기 (5분 가이드)

핵심: **레지스트리에 한 줄, wrapper 5줄.** 사이드바·카드·카테고리·권한·관리자 페이지가 모두 자동 반영됩니다.

### 1. `pages_registry.py`에 entry 추가

```python
PAGES = [
    # ... 기존 항목 ...
    PageEntry(
        key="my_new_app",
        name="새 앱 이름",
        icon="🆕",
        category="Sales",          # CATEGORIES 중 하나 또는 새 값
        description="이 앱이 뭐 하는지 2~3줄로...",
        entry_file="pages/my_new_app.py",
    ),
]
```

### 2. `pages/my_new_app.py` wrapper 생성

기존 폴더에 코드가 이미 있으면 위임 모드:

```python
"""새 앱 — my_new_app/ 위임 wrapper."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import auth
auth.gate("my_new_app", "새 앱 이름")
auth.run_legacy_app("my_new_app", "app.py")
```

또는 코드를 직접 작성:

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st
import auth

auth.gate("my_new_app", "새 앱 이름")
# 여기서부터 Streamlit 코드 자유롭게
st.title("새 앱")
```

### 3. (선택) `acl.json`에 page key 추가

`page_access`에 `"my_new_app": []` 추가하면 관리자 페이지에 항목으로 보임. 안 해도 admin/도메인 와일드카드는 자동 접근.

### 4. 끝!

브라우저 새로고침 → 사이드바·대시보드 카드·관리자 페이지에 즉시 등장.

---

## 🏗 아키텍처

```
data-tool/
├── streamlit_app.py            # 메인 엔트리 (st.navigation 기반, ~200줄)
├── auth.py                     # 권한 체크 (gate / has_access / accessible_pages)
├── app_utils.py                # 공용 헬퍼 (Cloud 감지, 로고 로딩)
├── acl.json                    # ACL 데이터 (관리자 페이지에서 편집)
├── pages_registry.py           # 모든 앱 메타데이터 (단일 진실)
├── pages/                      # 페이지 wrapper (각 5줄)
│   ├── launcher.py             # 대시보드 (카드 그리드)
│   ├── admin.py                # 관리자 페이지 (5 탭)
│   ├── analysis.py → analysis_app/
│   ├── mapping.py  → mapping_app/
│   ├── catalog.py  → streamlit_catalog.py
│   ├── bizno.py    → bizno_app/
│   └── revshare.py → revshare_app/
├── analysis_app/  mapping_app/  catalog_app/  bizno_app/  revshare_app/
├── modules/                    # 분석 공통 모듈
├── assets/                     # 로고 파일들
├── requirements.txt
├── .gitignore
├── .streamlit/
│   ├── config.toml
│   └── secrets.toml.example
└── README.md
```

**확장성의 핵심**: `pages_registry.PAGES` 가 단일 진실 source. 새 앱 추가 시 이 파일만 손대면 모든 UI가 자동 갱신.

---

## 🔐 권한 시스템

### ACL 구조 (`acl.json`)

```jsonc
{
  "admins": ["yonghan@mandata.kr"],           // 모든 권한 + 관리자 페이지
  "default_access": ["*@mandata.kr"],          // 도메인 와일드카드 — 회사 직원 자동 접근
  "page_access": {                             // 외부 사용자 앱별 초대
    "analysis": ["customer@example.com"],
    "bizno": ["partner@goodwater.com"],
    "...": []
  }
}
```

### 권한 결정 로직 (OR)
1. `admins`에 매칭 → 전체 권한
2. `default_access`에 매칭 → 전체 권한 (관리자 페이지 제외)
3. `page_access[<key>]`에 매칭 → 해당 페이지만

### 관리 방법
- 🛡 **관리자 페이지** (앱 안에서) — UI로 이메일 추가/삭제
- **Cloud 환경**: 변경 후 **Export** → `acl.json`을 GitHub commit → 자동 재배포
- **로컬**: 저장 버튼이 acl.json 파일에 직접 write

### 도메인 와일드카드
- `*@mandata.kr` → mandata.kr 도메인 전체 자동 접근
- `*` → 모든 사용자 (주의: 사실상 공개)

---

## 🔑 Secrets 관리

| Key | 용도 | 발급처 |
|---|---|---|
| `ANTHROPIC_API_KEY` | 사업자조회 영문코드→한글명, 카탈로그 영문화 | [console.anthropic.com](https://console.anthropic.com) |
| `DART_API_KEY` | 데이터분석 Earnings·Factor Research | [opendart.fss.or.kr](https://opendart.fss.or.kr) (무료) |
| `NTS_API_KEY` | 사업자조회 국세청 상태 조회 | [data.go.kr](https://data.go.kr) (무료) |

- 로컬: `.streamlit/secrets.toml` (gitignore됨)
- Cloud: Streamlit Cloud → App settings → Secrets에 붙여넣기

---

## ⚠️ 보안

- **acl.json은 private repo에만** (이메일 노출 방지)
- **secrets.toml은 절대 commit 금지** (gitignore에 등록 완료)
- **거래/매출 데이터 (xlsx, csv 등)는 commit 금지** (gitignore에 등록 완료)
- **API 키는 hard-code 금지** — 항상 `st.secrets[...]` 또는 환경변수
- **Cloud 배포 시**: private repo + viewer auth(Cloud Connect) 권장

---

## 📂 카테고리 (사이드바 그룹)

| 카테고리 | 아이콘 | 현재 앱 |
|---|---|---|
| Data Analysis | 📊 | 데이터 분석, 데이터 매핑 |
| Partnership | 🤝 | 사업자조회, 수익배분 산정 |
| Operations | ⚙️ | 데이터 카탈로그 |
| Sales | 💰 | (예약) |
| Marketing | 📣 | (예약) |

새 카테고리는 `pages_registry.CATEGORIES`에 추가 + `CATEGORY_ICONS`에 아이콘 등록.

---

## 🛠 개발 / 디버깅 팁

- **개별 앱 단독 실행** (의존성 격리 필요할 때): 각 앱의 `.command` 파일
- **다른 사용자 권한 시뮬레이션**: 사이드바 `🚪 로그아웃 (dev)` → 다른 이메일로 재로그인
- **권한 매트릭스 확인**: 관리자 페이지 → 👥 사용자 목록 탭
- **로그 위치**:
  - 통합 런처: 터미널 + `.launcher_logs/`
  - Streamlit Cloud: App 페이지 우상단 "Manage app" → Logs

---

## 📄 라이센스 / 데이터 출처

- **거래/매출 데이터**: 사용자 업로드 (개인 데이터, 외부 반출 금지)
- **DART**: 금융감독원 전자공시 — 무료 API
- **주가**: yfinance — 무료
- **KRX 산업 분류**: pykrx — 무료
- **bizno.net**: 사업자 정보 스크래핑 (공개 정보 한도 내 사용)
