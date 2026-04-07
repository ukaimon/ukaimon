@echo off
setlocal
cd /d "%~dp0"

set "BOOTSTRAP=%~dp0bootstrap_launcher.py"
set "EXITCODE=0"

where py >nul 2>nul
if not errorlevel 1 (
  py -3 "%BOOTSTRAP%"
  set "EXITCODE=%ERRORLEVEL%"
  goto :finish
)

where python >nul 2>nul
if not errorlevel 1 (
  python "%BOOTSTRAP%"
  set "EXITCODE=%ERRORLEVEL%"
  goto :finish
)

if exist "%~dp0.venv\Scripts\python.exe" (
  "%~dp0.venv\Scripts\python.exe" "%BOOTSTRAP%"
  set "EXITCODE=%ERRORLEVEL%"
  goto :finish
)

echo Python 3.11 以上が見つかりません。別 PC で使うには Python を先に入れてください。
set "EXITCODE=1"

:finish
if not "%EXITCODE%"=="0" (
  echo.
  echo Enter キーで閉じます。
  pause >nul
)
exit /b %EXITCODE%
