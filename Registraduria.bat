@echo off
title REGISTRADURIA PIPELINE

echo ======================================
echo INICIANDO REGISTRADURIA
echo ======================================

REM === Ir al directorio donde esta este archivo (.bat) ===
cd /d "%~dp0"

REM === Crear entorno si no existe (Opcional, pero util) ===
if not exist venv (
    echo Creando entorno virtual...
    python -m venv venv
)

REM === Activar entorno virtual ===
call venv\Scripts\activate

REM === Instalar dependencias si faltan (Opcional) ===
REM pip install -r requirements.txt

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