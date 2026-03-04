@echo off
echo ==================================================
echo APEX AI Serverini Ishga Tushirish...
echo ==================================================

echo Kutubxonalar tekshirilmoqda...
py -m pip install -r requirements.txt

echo.
echo Server Yondirildi!
echo Endi brauzeringizdan quyidagi manzilga kiring:
echo http://localhost:8000
echo ==================================================
cd backend
py main.py

pause
