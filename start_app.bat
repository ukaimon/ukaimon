@echo off
setlocal
cd /d "%SystemRoot%"

set "BOOTSTRAP=%~dp0bootstrap_launcher.py"
set "EXITCODE=1"

where python >nul 2>nul
if not errorlevel 1 (
  python "%BOOTSTRAP%"
  set "EXITCODE=%ERRORLEVEL%"
  if "%EXITCODE%"=="0" goto :finish
)

where py >nul 2>nul
if not errorlevel 1 (
  py -3 "%BOOTSTRAP%"
  set "EXITCODE=%ERRORLEVEL%"
  if "%EXITCODE%"=="0" goto :finish
)

if exist "%~dp0.venv\Scripts\python.exe" (
  "%~dp0.venv\Scripts\python.exe" "%BOOTSTRAP%"
  set "EXITCODE=%ERRORLEVEL%"
  goto :finish
)

echo Python 3.11 or newer was not found. Install Python first.
set "EXITCODE=1"

:finish
if not "%EXITCODE%"=="0" (
  echo.
  echo Press Enter to close.
  pause >nul
)
exit /b %EXITCODE%
