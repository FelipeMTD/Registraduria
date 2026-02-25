import subprocess
import sys
import os
from datetime import datetime, timedelta

# ================= CONFIG =================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))



SHEET_SCRIPT = os.path.join(BASE_DIR, "SheetRegistraduria.py")
GESTIONA_SCRIPT = os.path.join(BASE_DIR, "GestionaRegistraduria.py")


# ================= CONTROL TIEMPO =================

def debe_ejecutar(path, dias):
    try:
        # Si no existe → ejecutar
        if not os.path.exists(path):
            return True

        with open(path, "r", encoding="utf-8") as f:
            contenido = f.read().strip()

        # Si está vacío → ejecutar
        if not contenido:
            return True

        ultima = datetime.fromisoformat(contenido)

        return datetime.now() - ultima >= timedelta(days=dias)

    except Exception as e:
        print("ERROR leyendo LAST_RUN — forzando ejecución:", e)
        return True

def marcar_ejecucion(path):
    with open(path, "w", encoding="utf-8") as f:
        f.write(datetime.now().isoformat())


# ================= LOCK =================

def crear_lock(path):
    if os.path.exists(path):
        print("REGISTRADURIA YA ESTA EN EJECUCION — CANCELANDO")
        sys.exit(0)

    with open(path, "w"):
        pass


def liberar_lock(path):
    if os.path.exists(path):
        os.remove(path)


# ================= MAIN =================

if __name__ == "__main__":


        print("=== EJECUTANDO SHEET REGISTRADURIA ===")

        r1 = subprocess.run([sys.executable, SHEET_SCRIPT])

        if r1.returncode != 0:
            print("ERROR EN SHEET REGISTRADURIA")
            sys.exit(1)

        print("=== EJECUTANDO GESTIONA REGISTRADURIA ===")

        r2 = subprocess.run([sys.executable, GESTIONA_SCRIPT])

        if r2.returncode != 0:
            print("ERROR EN GESTIONA REGISTRADURIA")
            sys.exit(1)


        print("REGISTRADURIA COMPLETADO OK")

