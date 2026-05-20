#!/bin/bash
# ════════════════════════════════════════════════════════════════════════
# Mandata Data Catalog — 고객용 데이터 마켓플레이스
# streamlit_catalog.py 실행 바로가기 (macOS)
# 사용법: Finder에서 더블클릭
# ════════════════════════════════════════════════════════════════════════

cd "$(dirname "$0")"

clear
cat <<'EOF'
╔════════════════════════════════════════════════════════════════════╗
║                                                                    ║
║    🛒  Mandata Data Catalog                                        ║
║        고객용 데이터 마켓플레이스 (필터 · 카트 · 다운로드)             ║
║                                                                    ║
╚════════════════════════════════════════════════════════════════════╝
EOF
echo ""
echo "📂 실행 폴더: $(pwd)"
echo ""

# ─── Python3 확인 ──────────────────────────────────────────────────
if ! command -v python3 &> /dev/null; then
    echo "❌ python3가 설치되어 있지 않습니다."
    echo "👉 https://www.python.org/downloads/ 에서 Python 3.10+ 설치 후 다시 실행하세요."
    echo ""
    read -n 1 -s -r -p "아무 키나 누르면 창이 닫힙니다..."
    exit 1
fi

PY_VER=$(python3 --version 2>&1)
echo "🐍 $PY_VER 감지"
echo ""

# ─── Streamlit + pandas 확인 ───────────────────────────────────────
if ! python3 -c "import streamlit, pandas" &> /dev/null 2>&1; then
    echo "📦 필수 패키지가 설치되어 있지 않습니다. 최초 1회 설치를 진행합니다..."
    echo "   (약 30초~2분 소요, 인터넷 연결 필요)"
    echo ""
    # 카탈로그 앱은 분석 앱보다 가벼움 — 핵심만 설치
    if [ -f "requirements.txt" ]; then
        echo "📋 requirements.txt 기반 설치 중..."
        python3 -m pip install -r requirements.txt --break-system-packages 2>/dev/null \
            || python3 -m pip install -r requirements.txt
    else
        echo "📋 핵심 패키지 설치 중..."
        python3 -m pip install --break-system-packages \
            streamlit pandas openpyxl xlsxwriter pyarrow 2>/dev/null \
            || python3 -m pip install \
                streamlit pandas openpyxl xlsxwriter pyarrow
    fi
    echo ""
    echo "✅ 패키지 설치 완료"
    echo ""
fi

# ─── parquet 지원 확인 (pyarrow) ──────────────────────────────────
if ! python3 -c "import pyarrow" &> /dev/null 2>&1; then
    echo "📦 parquet 지원(pyarrow) 설치 중..."
    python3 -m pip install --break-system-packages pyarrow 2>/dev/null \
        || python3 -m pip install pyarrow
    echo "✅ parquet 준비됨"
    echo ""
fi

# ─── 실행 ──────────────────────────────────────────────────────────
echo "🚀 카탈로그 앱 시작 중..."
echo "   • 브라우저가 자동으로 열립니다 (http://localhost:8502)"
echo "   • 종료하려면 이 창에서 Ctrl+C 를 누르세요"
echo ""
echo "💡 데이터 소스 선택:"
echo "   - 자동:    catalog/ 폴더의 최신 parquet (분석앱 Step 6에서 생성)"
echo "   - 업로드:  catalog.parquet 직접 업로드"
echo "   - 데모:    가짜 60개 회사로 UI 체험"
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo ""

# 분석 앱과 다른 포트(8502) — 두 앱 동시 실행 가능
python3 -m streamlit run streamlit_catalog.py \
    --server.headless false \
    --server.port 8502 \
    --server.maxUploadSize 512

# ─── 종료 후 창 유지 ───────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "📴 카탈로그 앱이 종료되었습니다."
read -n 1 -s -r -p "아무 키나 누르면 창이 닫힙니다..."
