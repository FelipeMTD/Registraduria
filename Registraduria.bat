@echo off
title REGISTRADURIA PIPELINE

echo ======================================
echo INICIANDO REGISTRADURIA
echo ======================================

REM === Ir al directorio del proyecto ===
cd /d "C:\Users\FELIPE SISTEMAS\Desktop\PROYECTOS SISTEMAS\Fosyga & Registraduria"

REM === Activar entorno virtual ===
call venv\Scripts\activate

REM === Verificar Python activo ===
python --version

REM === ESPERAR DNS GOOGLE (Sheets + OAuth) ===
echo Verificando conectividad Google...

:CHECK_NET
ping -n 1 oauth2.googleapis.com >nul
if errorlevel 1 (
    echo Esperando DNS Google...
    timeout /t 5 >nul
    goto CHECK_NET
)

echo DNS OK

REM === Ejecutar Supervisor Registraduria ===
echo ======================================
echo EJECUTANDO SUPERVISOR REGISTRADURIA
echo ======================================
python SupervisorRegistraduria.py

echo ======================================
echo REGISTRADURIA FINALIZADO
echo ======================================

pause
