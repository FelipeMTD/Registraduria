import asyncio
import time
from google.oauth2.service_account import Credentials
import gspread
from playwright.async_api import async_playwright

# Importamos módulos locales
from IniciarSesion import create_pool
from AccionesRegistraduria import (
    abrir_edicion_paciente,
    marcar_fallecido,
    guardar,
    ya_muerto_inactivo,
    PacienteNoEncontrado
)

# ================= CONFIGURACIÓN DE CUOTA EXTREMA =================
SERVICE_ACCOUNT_FILE = "service-account.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = "1wY9sRQ_KbaCiVUb4UHX50NzHweZnMh-YmATtY8UA-mQ"
HOJA = "REGISTRADURIA"

COL_DOCUMENTO = "DOCUMENTO"
COL_ESTADO_REG = "ESTADO_REGISTRADURIA"
COL_ESTADO_GEST = "ESTADO_GESTIONA"

# --- ESTRATEGIA DE AHORRO ---
WORKERS = 4               
BATCH_SIZE = 1000         
PAUSA_ENTRE_LOTES = 5     
# ----------------------------

URL_PACIENTES = "https://saludgestiona.com/business/patients-list"

# ================= UTILS DE CONEXIÓN =================

def conectar_sheet():
    for intento in range(1, 6):
        try:
            creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
            client = gspread.authorize(creds)
            return client.open_by_key(SHEET_ID).worksheet(HOJA)
        except Exception as e:
            print(f"⚠️ Error conexión Sheets ({intento}/5): {e}")
            time.sleep(10)
    raise ConnectionError("No se pudo conectar a Google Sheets.")

def col_letra(n):
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

# ================= ESCRITURA MASIVA =================

async def batch_writer(ws, result_queue):
    buffer = []
    
    async def flush(data):
        if not data: return
        for i in range(7):
            try:
                ws.spreadsheet.values_batch_update({
                    "valueInputOption": "RAW",
                    "data": data
                })
                print(f"💾 [CUOTA] Lote de {len(data)} enviado. (1 petición consumida)")
                return
            except Exception as e:
                wait_time = (i + 1) * 10
                if "429" in str(e) or "Quota" in str(e):
                    print(f"⏳ [LIMITE API] Cuota agotada. Esperando {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    print(f"❌ [ERROR ESCRITURA] {e}")
                    await asyncio.sleep(5)
        print("🚨 [PERDIDA] No se pudo guardar un lote de datos.")

    while True:
        try:
            item = await asyncio.wait_for(result_queue.get(), timeout=10.0)
            if item is None:
                if buffer: await flush(buffer)
                result_queue.task_done()
                break
            
            buffer.append(item)
            result_queue.task_done()
            
            if len(buffer) >= BATCH_SIZE:
                await flush(buffer.copy())
                buffer.clear()
                await asyncio.sleep(PAUSA_ENTRE_LOTES)

        except asyncio.TimeoutError:
            if buffer:
                await flush(buffer.copy())
                buffer.clear()

# ================= PRODUCER Y WORKER =================

async def producer(ws, job_queue, result_queue, col_gest_letter):
    print("⏳ Leyendo base de datos completa (1 sola petición de lectura)...")
    values = ws.get_all_values() 
    
    headers = values[0]
    try:
        idx_doc = headers.index(COL_DOCUMENTO)
        idx_reg = headers.index(COL_ESTADO_REG)
        idx_gest = headers.index(COL_ESTADO_GEST)
    except ValueError as e:
        print(f"Falta columna: {e}")
        return 0

    count = 0
    for i, row in enumerate(values[1:], start=2):
        doc = row[idx_doc].strip() if len(row) > idx_doc else ""
        reg = row[idx_reg].strip() if len(row) > idx_reg else ""
        gest = row[idx_gest].strip() if len(row) > idx_gest else ""

        if doc and reg in ("CANCELADA POR MUERTE", "YA_MUERTO") and not gest:
            await job_queue.put((i, doc))
            await result_queue.put({
                "range": f"'{HOJA}'!{col_gest_letter}{i}",
                "values": [["PROCESANDO"]]
            })
            count += 1
    return count

async def worker(wid, page, job_queue, result_queue, col_gest_letter):
    while True:
        item = await job_queue.get()
        if item is None:
            job_queue.task_done()
            return
        
        fila, doc = item
        try:
            await abrir_edicion_paciente(page, doc, url_pacientes=URL_PACIENTES)
            
            # --- LÓGICA CORREGIDA ---
            if await ya_muerto_inactivo(page):
                obs = "YA_MUERTO"
            else:
                # El robot prepara el formulario
                await marcar_fallecido(page)
                # El robot guarda y verifica
                ok, msg = await guardar(page)
                obs = "MUERTE_OK" if ok else f"ERR: {msg[:20]}"
                
        except PacienteNoEncontrado:
            obs = "NO_EXISTE"
        except Exception as e:
            obs = f"ERROR"
            try: await page.goto("about:blank")
            except: pass

        await result_queue.put({
            "range": f"'{HOJA}'!{col_gest_letter}{fila}",
            "values": [[obs]]
        })
        job_queue.task_done()

# ================= MAIN =================

async def main():
    ws = conectar_sheet()
    col_gest_letter = col_letra(ws.row_values(1).index(COL_ESTADO_GEST) + 1)

    job_q = asyncio.Queue()
    res_q = asyncio.Queue()

    async with async_playwright() as p:
        # ATENCIÓN: Lo he dejado en headless=True para producción masiva
        pool = await create_pool(p, workers=WORKERS, headless=False)
        
        writer_t = asyncio.create_task(batch_writer(ws, res_q))
        workers = [asyncio.create_task(worker(i, pool.pages[i], job_q, res_q, col_gest_letter)) for i in range(WORKERS)]

        total = await producer(ws, job_q, res_q, col_gest_letter)
        print(f"📊 {total} fallecidos detectados para procesar.")

        for _ in range(WORKERS): await job_q.put(None)
        await job_q.join()
        await asyncio.gather(*workers)
        
        await res_q.put(None)
        await writer_t
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())