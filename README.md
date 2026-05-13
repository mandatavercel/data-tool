# 📊 Mandata Alt-Data Intelligence Platform

POS · 카드 · 통신사 거래 데이터로 매출 선행성·주가 상관·DART 공시 정합성을 검증하는 6단계 분석 도구.

> 글로벌 기관투자자가 alt data 가치 평가할 때 보는 KPI (Tracking Quality · CS Rank IC · L/S Sharpe) 를 한국 소비재 데이터에 적용.

---

## 🚀 빠른 시작 — 로컬 (macOS)

1. 폴더 다운로드 → 더블클릭으로 실행:
   - `📊 데이터분석 실행.command`  ← 분석 앱 (포트 8501)
   - `🗂 데이터매핑 실행.command`  ← 매핑 앱 (포트 8502)
2. 최초 실행 시 자동으로 Python 패키지 설치 (1~3분, 인터넷 필요)
3. 브라우저에서 `http://localhost:8501` 자동 열림

---

## ☁️ 팀과 공유 — Streamlit Community Cloud (무료)

### 사전 준비
- GitHub 계정 (없으면 [github.com/signup](https://github.com/signup))
- DART API Key (선택, [opendart.fss.or.kr](https://opendart.fss.or.kr) 무료 발급)

### 배포 단계

#### 1. GitHub에 코드 올리기

```bash
cd ~/Desktop/data-tool
git init
git add .
git commit -m "Initial commit"
# GitHub에서 새 repo 생성 후
git remote add origin https://github.com/<your-id>/data-tool.git
git branch -M main
git push -u origin main
```

#### 2. Streamlit Cloud 가입 + 앱 배포

1. [share.streamlit.io](https://share.streamlit.io) 접속 → GitHub로 로그인
2. **New app** 클릭
3. 다음 정보 입력:
   - **Repository**: `<your-id>/data-tool`
   - **Branch**: `main`
   - **Main file path**: `analysis_app/analysis_app.py`
   - **App URL**: 원하는 subdomain (예: `mandata-alt-data`)
4. **Advanced settings** → **Secrets** 에 다음 내용 입력:
   ```toml
   # DART API Key (선택)
   DART_API_KEY = "여기에_DART_API_키_붙여넣기"
   ```
5. **Deploy!** 클릭 → 1~2분 후 라이브
6. 팀에 URL 공유: `https://mandata-alt-data.streamlit.app`

### 비용
- **$0/월** — 무료 공개 cloud
- **5GB RAM · 1GB storage** — 분석앱 충분
- **자동 sleep** — 7일 미사용 시 sleep, 다시 접속하면 재시작

---

## 🏗 프로젝트 구조

```
data-tool/
├── analysis_app/                ← 분석 앱 (entry point)
│   ├── analysis_app.py          # thin router (85줄)
│   ├── config.py                # STEPS, ANALYSIS_OPTIONS
│   ├── navigation.py            # go_to, stepper
│   ├── secrets_store.py         # DART API Key 영구 저장
│   ├── export.py                # Excel 멀티시트 export
│   ├── report_export.py         # PPT/PDF 한·영 export
│   ├── dashboard.py             # Investor Dashboard
│   ├── setup_ui.py              # Step 4 파라미터 UI
│   └── steps/                   # 6단계 step 파일
│       ├── step1_upload.py
│       ├── step2_schema.py
│       ├── step3_validation.py
│       ├── step4_setup.py
│       ├── step5_results.py
│       └── step6_dashboard.py
├── mapping_app/                 ← 매핑 앱
│   ├── app.py
│   └── ...
├── modules/                     ← 공통 모듈
│   ├── common/                  # foundation (schema), dashboard
│   ├── analysis/                # 10개 분석 모듈
│   │   ├── intelligence/        # growth · demand · brand · sku · category
│   │   ├── signal/              # anomaly · market · earnings · alpha · factor
│   │   └── factor/              # PIT panel · IC · backtest
│   └── mapping/                 # DART · ISIN · 영문화
├── requirements.txt
├── README.md
└── .gitignore
```

---

## 🛠 분석 모듈 (10개)

### Intelligence Hub
- 📈 **Growth Analytics** — MoM/QoQ/YoY 성장률
- 🔥 **Demand Intelligence** — P/Q 분해, ATV 추이
- 🏷 **Brand Intelligence** — 브랜드별 점유율·HHI
- 📦 **SKU Intelligence** — Pareto 80/20, lifecycle
- 🗂 **Category Intelligence** — 카테고리 계층 분석

### Signal Layer
- 🚨 **Anomaly Detection** — Z-score / IQR / 구조 변화점
- 📉 **Market Signal** — POS vs 주가 시차 상관
- 📊 **Earnings Intelligence** — POS vs DART 공시 정합성
- 🎯 **Alpha Validation** — 종합 알파 신호

### Factor Layer
- 🧪 **Factor Research** — Cross-Sectional Rank IC + Quintile Backtest

---

## 📄 라이센스 / 데이터 출처

- **POS 데이터**: 사용자 업로드 (개인 데이터, 외부 반출 금지)
- **DART**: 금융감독원 전자공시 (`opendart.fss.or.kr`) — 무료 API
- **주가**: yfinance (Yahoo Finance) — 무료
- **KRX 산업 분류**: pykrx — 무료

---

## ⚠️ 보안 주의

- **DART API Key**: `.streamlit/secrets.toml` 에만 저장. **절대 코드에 hard-code 금지**
- **POS 데이터**: 업로드 후 분석 종료 시 자동 삭제 (서버에 영구 저장 안 함)
- **공개 cloud 배포 시**: Streamlit Cloud secrets 또는 환경변수 사용
