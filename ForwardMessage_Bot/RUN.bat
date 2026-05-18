@echo off
title Telegram Forwarder Bot
cls

if exist requirements.txt (
    pip install -r requirements.txt --quiet --upgrade
) else (
    echo Dang cai dat thu vien mac dinh...
    pip install telethon PySocks --quiet --upgrade
)

echo.
echo ================================================
echo    DANG KHOI DONG BOT CHUYEN TIEP...
echo ================================================
echo.

:: Chay file bot
if exist ForwardMessage_Bot.py (
    python ForwardMessage_Bot.py
) else (
    echo [LOI] Khong tim thay file ForwardMessage_Bot.py trong thu muc nay.
)

echo.
echo Bot da dung lai. Kiem tra loi phia tren (neu co).
pause