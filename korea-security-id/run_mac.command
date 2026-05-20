#!/usr/bin/env bash
# Double-click to launch an interactive Mandata Korea Security ID prompt on macOS.
set -u
cd "$(dirname "$0")" || exit 1

echo "=================================================================="
echo " Mandata Korea Security ID — interactive prompt"
echo "=================================================================="
echo ""

PY=""
for c in python3 python; do
  if command -v "$c" >/dev/null 2>&1; then
    if "$c" -c "import sys; sys.exit(0 if sys.version_info>=(3,9) else 1)" 2>/dev/null; then
      PY="$c"; break
    fi
  fi
done
if [ -z "$PY" ]; then
  echo "ERROR: Python 3.9+ not found. Install from https://www.python.org/downloads/"
  read -n 1 -s -r -p "Press any key to close..."; exit 1
fi
echo "Using: $($PY --version)"
echo ""
echo "Commands:"
echo "  Type any identifier (Korean name, English, code, ISIN, Bloomberg, RIC, DART code)"
echo "  /search <q>     – substring search"
echo "  /members KOSPI200 | KOSDAQ150 | KRX300"
echo "  /validate <ISIN>"
echo "  /quit"
echo ""

while true; do
  printf "mandata> "
  read -r line || break
  [ -z "$line" ] && continue
  case "$line" in
    /quit|/q|/exit) break ;;
    /search\ *)    "$PY" -m mandata_kr search "${line#/search }" ;;
    /members\ *)   "$PY" -m mandata_kr members "${line#/members }" ;;
    /validate\ *)  "$PY" -m mandata_kr validate "${line#/validate }" ;;
    *)             "$PY" -m mandata_kr lookup "$line" ;;
  esac
done
