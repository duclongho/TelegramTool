@echo off
chcp 65001 > nul
title Binance Futures Signal Bot

cd /d "%~dp0"

echo.
echo ================================================
echo    Binance Futures Signal Bot
echo ================================================
echo.

:: Kiem tra Python
python --version > nul 2>&1
if errorlevel 1 (
    echo [LOI] Khong tim thay Python.
    echo Tai va cai dat Python 3.10+ tai: https://www.python.org/downloads/
    echo Nho check "Add Python to PATH" khi cai.
    echo.
    pause
    exit /b 1
)

:: Kich hoat virtual environment neu co
if exist "venv\Scripts\activate.bat" (
    echo [INFO] Kich hoat virtual environment...
    call venv\Scripts\activate.bat
) else (
    echo [INFO] Dung Python he thong...
)

:: Cai thu vien neu thieu
echo [INFO] Kiem tra thu vien...
pip install -r requirements.txt -q
echo.

:: Vong lap tu dong khoi dong lai neu crash
:run
echo [%time%] Khoi dong bot...
echo.
python songkiem_bot.py
echo.
echo [%time%] Bot da dung (exit code: %errorlevel%)

if %errorlevel% == 0 (
    echo Thoat binh thuong.
    pause
    exit /b 0
)

echo Khoi dong lai sau 10 giay... (Nhan Ctrl+C de thoat)
timeout /t 10 /nobreak > nul
goto run
