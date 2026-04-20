@echo off
cd /d "%~dp0"

echo.
echo  ========================================
echo   Power BI Copilot - Iniciando...
echo  ========================================
echo.
echo  Acesse: http://localhost:8000
echo  Pressione Ctrl+C para parar
echo.

.venv\Scripts\python.exe -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
