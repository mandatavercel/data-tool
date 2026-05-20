@echo off
setlocal enabledelayedexpansion
pushd "%~dp0"

echo ==================================================================
echo  Mandata Korea Security ID -- interactive prompt
echo ==================================================================
echo.

set "PY="
for %%C in (py python python3) do (
    if not defined PY (
        where %%C >nul 2>nul && (
            %%C -c "import sys; sys.exit(0 if sys.version_info>=(3,9) else 1)" >nul 2>nul && set "PY=%%C"
        )
    )
)
if not defined PY (
    echo ERROR: Python 3.9+ not found. Install from https://www.python.org/downloads/
    pause & popd & exit /b 1
)

echo Using: %PY%
echo.
echo Commands:
echo   Type any identifier (Korean, English, code, ISIN, Bloomberg, RIC, DART)
echo   /search ^<q^>         substring search
echo   /members KOSPI200 ^| KOSDAQ150 ^| KRX300
echo   /validate ^<ISIN^>
echo   /quit
echo.

:loop
set "line="
set /p line=mandata^>
if "%line%"=="" goto loop
if /i "%line%"=="/quit" goto end
if /i "%line%"=="/q"    goto end
if /i "%line:~0,8%"=="/search "    %PY% -m mandata_kr search "%line:~8%" & goto loop
if /i "%line:~0,9%"=="/members "   %PY% -m mandata_kr members "%line:~9%" & goto loop
if /i "%line:~0,10%"=="/validate " %PY% -m mandata_kr validate "%line:~10%" & goto loop
%PY% -m mandata_kr lookup "%line%"
goto loop

:end
popd
