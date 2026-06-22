#!/bin/bash
# ════════════════════════════════════════════════════════════════════════
# Mandata Alt-Data Intelligence Platform — K-F&B 데이터 상품 에이전트
# kfnb_app/app.py 실행 바로가기 (macOS)
# 사용법: Finder에서 더블클릭
# ════════════════════════════════════════════════════════════════════════

cd "$(dirname "$0")"

clear
cat <<'EOF'
╔════════════════════════════════════════════════════════════════════╗
║                                                                    ║
║    🍜  Mandata Alt-Data Intelligence Platform                      ║
║        K-F&B 데이터 상품 에이전트 (kfnb_app)                        ║
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

# ─── Streamlit/duckdb/yaml/pykrx 설치 확인 ─────────────────────────
if ! python3 -c "import streamlit, duckdb, yaml, pykrx" &> /dev/null 2>&1; then
    echo "📦 필수 패키지가 설치되어 있지 않습니다. 최초 1회 설치를 진행합니다..."
    echo "   (약 1~3분 소요, 인터넷 연결 필요)"
    echo ""
    if [ -f "requirements.txt" ]; then
        echo "📋 requirements.txt 기반 설치 중..."
        python3 -m pip install -r requirements.txt --break-system-packages 2>/dev/null \
            || python3 -m pip install -r requirements.txt
    else
        echo "📋 핵심 패키지 설치 중..."
        python3 -m pip install --break-system-packages \
            streamlit pandas numpy openpyxl duckdb 2>/dev/null \
            || python3 -m pip install streamlit pandas numpy openpyxl duckdb
    fi
    echo ""
    echo "✅ 패키지 설치 완료"
    echo ""
fi

# ─── Streamlit 실행 (포트 8508 — 다른 앱과 충돌 회피) ──────────────
echo "🚀 K-F&B 데이터 상품 에이전트 시작 중..."
echo "   • 브라우저가 자동으로 열립니다 (http://localhost:8508)"
echo "   • 다른 앱이 동시에 실행 중이어도 OK (포트 분리)"
echo "   • 종료하려면 이 창에서 Ctrl+C 를 누르세요"
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo ""

# kfnb_app/ 으로 이동해서 실행 (entry 안에서 sys.path를 루트로 잡아 kfnb_app.* 임포트 정상)
cd kfnb_app
python3 -m streamlit run app.py --server.headless false --server.port 8508 --server.maxUploadSize 5120

# ─── 종료 후 창 유지 ───────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "📴 프로그램이 종료되었습니다."
read -n 1 -s -r -p "아무 키나 누르면 창이 닫힙니다..."
