@echo off
setlocal

REM Optional: set workflow; leave empty to use MEOW_WORKFLOW_NAME env or default
REM set MEOW_WORKFLOW_NAME=DAILY_ELT
REM set MEOW_TARGET_LANGUAGES=

cd /d "%~dp0.."
if exist .venv\Scripts\activate.bat (
    call .venv\Scripts\activate.bat
) else (
    echo Virtualenv not found at .venv\Scripts\activate.bat
    exit /b 1
)

python main.py
endlocal
