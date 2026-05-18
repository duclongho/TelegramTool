@echo off
title UT Bot Signal Sender
cls

echo ================================================
echo    CAI DAT THU VIEN...
echo ================================================
pip install -r requirements.txt --quiet --upgrade

echo.
echo ================================================
echo    DANG KHOI DONG BOT...
echo ================================================
echo.

python SendSignal_Bot.py

echo.
echo Bot da dung lai. Kiem tra loi phia tren (neu co).
pause
