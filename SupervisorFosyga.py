# =========================
# SupervisorFosyga.py
# =========================
# Orquestador CONTINUO:
# 1) Ejecuta SheetFosyga.py hasta que salga OK (rc=0).
#    - Si rc=2 (abort reintentable por portal/UI), espera 60s y relanza.
#    - Si rc!=0, espera 60s y relanza.
# 2) Cuando Sheet queda OK, ejecuta GestionaFosyga.py hasta que salga OK (rc=0).
#    - Si falla, espera 60s y relanza.
# 3) Cuando ambos quedan OK, espera 120s y repite el ciclo completo.

import os
import sys
import time
import subprocess
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SHEET_SCRIPT = os.path.join(BASE_DIR, "SheetFosyga.py")
GESTIONA_SCRIPT = os.path.join(BASE_DIR, "GestionaFosyga.py")

VENV_PY = os.path.join(BASE_DIR, "venv", "Scripts", "python.exe")
PYTHON_EXE = VENV_PY if os.path.exists(VENV_PY) else sys.executable

WAIT_RESTART_SECONDS = 60
SLEEP_OK_SECONDS = 120

def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def sleep_with_log(seconds: int, reason: str) -> None:
    print(f"{now_str()} | [SLEEP] {seconds}s | {reason}", flush=True)
    time.sleep(seconds)

def run_script(label: str, script_path: str) -> int:
    print(f"{now_str()} | === EJECUTANDO {label} ===", flush=True)
    r = subprocess.run([PYTHON_EXE, script_path])
    return r.returncode

def run_until_ok(label: str, script_path: str) -> None:
    while True:
        rc = run_script(label, script_path)

        if rc == 0:
            print(f"{now_str()} | [OK] {label} terminó OK", flush=True)
            return

        if rc == 2:
            print(
                f"{now_str()} | [RESTART] {label} rc=2 (abort reintentable) -> reinicio en {WAIT_RESTART_SECONDS}s",
                flush=True,
            )
            sleep_with_log(WAIT_RESTART_SECONDS, f"{label}: rc=2 (portal/UI).")
            continue

        print(
            f"{now_str()} | [WARN] {label} falló rc={rc} -> reintento en {WAIT_RESTART_SECONDS}s",
            flush=True,
        )
        sleep_with_log(WAIT_RESTART_SECONDS, f"{label}: rc={rc} (error general).")

def main():
    print("======================================", flush=True)
    print("SUPERVISOR FOSYGA (SHEET -> GESTIONA) | REINTENTO INFINITO", flush=True)
    print(f"PYTHON_EXE = {PYTHON_EXE}", flush=True)
    print("======================================", flush=True)

    ciclo = 0
    while True:
        ciclo += 1
        print("", flush=True)
        print("############################################################", flush=True)
        print(f"{now_str()} | CICLO #{ciclo}", flush=True)
        print("############################################################", flush=True)

        run_until_ok("SHEET FOSYGA", SHEET_SCRIPT)
        run_until_ok("GESTIONA FOSYGA", GESTIONA_SCRIPT)

        print(f"{now_str()} | [OK] CICLO COMPLETO OK (Sheet + Gestiona)", flush=True)
        sleep_with_log(SLEEP_OK_SECONDS, "Ciclo OK. Espera normal antes del siguiente ciclo.")

if __name__ == "__main__":
    main()
