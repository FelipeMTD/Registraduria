import subprocess
import sys
import os
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta

# ================= CONFIG =================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SHEET_SCRIPT = os.path.join(BASE_DIR, "SheetRegistraduria.py")
GESTIONA_SCRIPT = os.path.join(BASE_DIR, "GestionaRegistraduria.py")
LOCK_FILE = os.path.join(BASE_DIR, "registraduria.lock")
LAST_RUN_FILE = os.path.join(BASE_DIR, "last_run.txt")

# Cada cuántos días se permite correr de nuevo (0 = siempre)
DIAS_FRECUENCIA = 0 


def limpiar_pacientes_vivos():
    """Borra el estado de los pacientes vivos para que sean consultados nuevamente."""
    print("🧹 Iniciando fase de limpieza de pacientes VIGENTES...")
    
    # Reutilizamos tu configuración de Sheets
    creds = Credentials.from_service_account_file("service-account.json", scopes=["https://www.googleapis.com/auth/spreadsheets"])
    client = gspread.authorize(creds)
    ws = client.open_by_key("1wY9sRQ_KbaCiVUb4UHX50NzHweZnMh-YmATtY8UA-mQ").worksheet("REGISTRADURIA")
    
    data = ws.get_all_values()
    headers = data[0]
    
    # Localizamos columnas
    idx_reg = headers.index("ESTADO_REGISTRADURIA")
    idx_gest = headers.index("ESTADO_GESTIONA")
    
    updates = []
    for i, row in enumerate(data[1:], start=2):
        # Si el paciente está vivo, borramos sus estados para que el robot lo tome de nuevo
        if row[idx_reg] == "VIGENTE (VIVO)":
            # Agregamos a la lista de limpieza (Celda de Registro y Celda de Gestiona)
            updates.append({'range': gspread.utils.rowcol_to_a1(i, idx_reg + 1), 'values': [['']]})
            updates.append({'range': gspread.utils.rowcol_to_a1(i, idx_gest + 1), 'values': [['']]})

    if updates:
        # Usamos batch_update para no agotar la cuota de Google
        ws.batch_update(updates)
        print(f"✅ Se han reseteado {len(updates)//2} pacientes vigentes para nueva consulta.")
    else:
        print("ℹ️ No hay pacientes vigentes para limpiar.")
# ================= CONTROL TIEMPO =================

def debe_ejecutar(path, dias):
    try:
        if not os.path.exists(path):
            return True
        
        with open(path, "r", encoding="utf-8") as f:
            contenido = f.read().strip()
            
        if not contenido:
            return True
            
        ultima = datetime.fromisoformat(contenido)
        # Si la diferencia es mayor a X dias
        return (datetime.now() - ultima) >= timedelta(days=dias)

    except Exception as e:
        print(f"Advertencia: No se pudo leer last_run ({e}). Se forzará ejecución.")
        return True

def marcar_ejecucion(path):
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(datetime.now().isoformat())
    except Exception as e:
        print(f"Error guardando fecha de ejecucion: {e}")

# ================= LOCK (BLOQUEO) =================

def crear_lock(path):
    if os.path.exists(path):
        print("!!! ALERTA: REGISTRADURIA YA ESTA EN EJECUCION (Lock file existe) !!!")
        print("Si cree que es un error, borre el archivo 'registraduria.lock' manualmente.")
        sys.exit(0)

    with open(path, "w") as f:
        f.write(str(os.getpid()))

def liberar_lock(path):
    if os.path.exists(path):
        try:
            os.remove(path)
        except Exception:
            pass

# ================= MAIN =================

if __name__ == "__main__":

    # 1. Verificar Frecuencia
    if not debe_ejecutar(LAST_RUN_FILE, DIAS_FRECUENCIA):
        print("No es necesario ejecutar hoy (Frecuencia configurada).")
        sys.exit(0)

    # 2. Crear Bloqueo
    crear_lock(LOCK_FILE)

    try:
        limpiar_pacientes_vivos()
        print("\n=== [1/2] EJECUTANDO CONSULTA REGISTRADURIA (SHEETS) ===")
        r1 = subprocess.run([sys.executable, SHEET_SCRIPT])

        if r1.returncode != 0:
            print("❌ ERROR CRÍTICO EN SHEET REGISTRADURIA. DETENIENDO PROCESO.")
            sys.exit(1)

        print("\n=== [2/2] EJECUTANDO ACTUALIZACION CRM (GESTIONA) ===")
        r2 = subprocess.run([sys.executable, GESTIONA_SCRIPT])

        if r2.returncode != 0:
            print("❌ ERROR CRÍTICO EN GESTIONA REGISTRADURIA.")
            sys.exit(1)

        # Si todo salió bien, marcamos la fecha
        marcar_ejecucion(LAST_RUN_FILE)
        print("\n✅ REGISTRADURIA COMPLETADO CORRECTAMENTE.")

    except KeyboardInterrupt:
        print("\nCANCELADO POR EL USUARIO.")

    finally:
        # Siempre liberar el lock, incluso si falla
        liberar_lock(LOCK_FILE)