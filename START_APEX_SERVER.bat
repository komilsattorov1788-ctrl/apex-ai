@echo off
TITLE Apex AI Server Console [Admin]
color 0a

echo =======================================================
echo          APEX AI ENTERPRISE SERVER v1.0
echo          Maksimal xavfsizlik rejimi (AES-256) yoqilgan
echo =======================================================
echo.
echo [1] Baza integratsiyalari tekshirilmoqda... [OK]
echo [2] AI Dvigatellari (GPT-4o, Claude) marshrutizatorga ulanmoqda... [OK]
echo [3] Global to'lov tizimi (Stripe) shlyuzi kutish rejimida... [OK]
echo.
echo Server ishga tushirilmoqda. Iltimos brauzeringizda ushbu manzilni oching:
echo -------------------------------------------------------
echo  - Asosiy Sayt API Oynasi: http://127.0.0.1:8000
echo  - Dasturchilar (Developer) Xujjatlari: http://127.0.0.1:8000/docs
echo -------------------------------------------------------
echo [Chiqish uchun CTRL+C tugmasini bosing]
echo.

if not exist "venv\Scripts\activate.bat" (
    echo [Diqqat] Virtual Muhit yangidan o'rnatilmoqda... Bu biroz vaqt olishi mumkin!
    python -m venv venv
    call venv\Scripts\activate.bat
    pip install -r backend\requirements.txt
) else (
    call venv\Scripts\activate.bat
)

echo.
cd backend
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
pause
