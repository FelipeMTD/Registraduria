import asyncio
import time
from google.oauth2.service_account import Credentials
import gspread
from playwright.async_api import async_playwright
from Logger import get_logger
logger = get_logger("CRM_GESTIONA")

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
COL_FECHA_FALLECIDO = "FECHA_FALLECIDO" # Nueva Columna E

# --- ESTRATEGIA DE AHORRO ---
WORKERS = 10               
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
            logger.warning(f"Error conexión Sheets ({intento}/5): {e}")
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
                logger.info(f"[CUOTA] Lote de {len(data)} actualizaciones enviado a Sheets.")
                print(f"💾 [CUOTA] Lote de {len(data)} actualizaciones enviado.")
                return
            except Exception as e:
                wait_time = (i + 1) * 10
                if "429" in str(e) or "Quota" in str(e):
                    logger.warning(f"[LIMITE API] Cuota agotada. Esperando {wait_time}s...")
                    print(f"⏳ [LIMITE API] Cuota agotada. Esperando {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"[ERROR ESCRITURA] {e}")
                    print(f"❌ [ERROR ESCRITURA] {e}")
                    await asyncio.sleep(5)

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

async def producer(ws, job_queue, result_queue, col_gest_letter, col_fecha_letter):
    logger.info("Leyendo base de datos completa de Sheets...")
    print("⏳ Leyendo base de datos completa...")
    values = ws.get_all_values() 
    headers = values[0]
    
    try:
        idx_doc = headers.index(COL_DOCUMENTO)
        idx_reg = headers.index(COL_ESTADO_REG)
        idx_gest = headers.index(COL_ESTADO_GEST)
    except ValueError as e:
        logger.error(f"Falta columna: {e}")
        print(f"Falta columna: {e}")
        return 0

    count = 0
    for i, row in enumerate(values[1:], start=2):
        doc = row[idx_doc].strip() if len(row) > idx_doc else ""
        reg = row[idx_reg].strip() if len(row) > idx_reg else ""
        gest = row[idx_gest].strip() if len(row) > idx_gest else ""

        if doc and reg in ("CANCELADA POR MUERTE", "YA_MUERTO") and not gest:
            await job_queue.put((i, doc))
            # Marcamos como procesando
            await result_queue.put({
                "range": f"'{HOJA}'!{col_gest_letter}{i}",
                "values": [["PROCESANDO"]]
            })
            count += 1
    return count

async def worker(wid, page, job_queue, result_queue, col_gest_letter, idx_fecha):
    col_fecha_letter = col_letra(idx_fecha + 1) 
    
    while True:
        item = await job_queue.get()
        if item is None:
            job_queue.task_done()
            return
        
        fila, doc = item
        try:
            await abrir_edicion_paciente(page, doc, url_pacientes=URL_PACIENTES)
            
            # Capturamos la tupla (accion, fecha)
            accion, fecha_grabada = await marcar_fallecido(page)
            
            if accion in ("DO", "SKIP"):
                # 1. Guardar resultado en el CRM
                if accion == "DO":
                    ok, msg = await guardar(page)
                    obs = "MUERTE_OK" if ok else f"ERR: {msg[:20]}"
                else:
                    obs = "YA_MUERTO"

                # 2. Enviar actualización de ESTADO a la cola
                await result_queue.put({
                    "range": f"'{HOJA}'!{col_gest_letter}{fila}",
                    "values": [[obs]]
                })

                # 3. Enviar actualización de FECHA (Columna E) a la cola
                if obs in ("MUERTE_OK", "YA_MUERTO"):
                    await result_queue.put({
                        "range": f"'{HOJA}'!{col_fecha_letter}{fila}",
                        "values": [[fecha_grabada]]
                    })
                
        except PacienteNoEncontrado:
            obs = "NO_EXISTE"
            await result_queue.put({"range": f"'{HOJA}'!{col_gest_letter}{fila}", "values": [[obs]]})
        except Exception as e:
            obs = "ERROR"
            await result_queue.put({"range": f"'{HOJA}'!{col_gest_letter}{fila}", "values": [[obs]]})
            try: await page.goto("about:blank")
            except: pass

        job_queue.task_done()

# ================= MAIN =================

async def main():
    ws = conectar_sheet()
    headers = ws.row_values(1)
    
    # Resolvemos letras de columnas
    col_gest_letter = col_letra(headers.index(COL_ESTADO_GEST) + 1)
    idx_fecha = headers.index(COL_FECHA_FALLECIDO)
    col_fecha_letter = col_letra(idx_fecha + 1)

    job_q = asyncio.Queue()
    res_q = asyncio.Queue()

    async with async_playwright() as p:
        pool = await create_pool(p, workers=WORKERS, headless=True)
        
        writer_t = asyncio.create_task(batch_writer(ws, res_q))
        
        # PASAMOS EL idx_fecha A LOS WORKERS
        workers = [
            asyncio.create_task(worker(i, pool.pages[i], job_q, res_q, col_gest_letter, idx_fecha)) 
            for i in range(WORKERS)
        ]

        total = await producer(ws, job_q, res_q, col_gest_letter, col_fecha_letter)
        logger.info(f"{total} pacientes fallecidos detectados listos para procesar en CRM.")
        print(f"📊 {total} fallecidos detectados para procesar.")

        for _ in range(WORKERS): await job_q.put(None)
        await job_q.join()
        await asyncio.gather(*workers)
        
        await res_q.put(None)
        await writer_t
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())