import asyncio
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials
import os
import time
import random
from Logger import get_logger
logger = get_logger("CONSULTA_REG")
# ================= CONFIGURACIÓN STEALTH (SIGILOSA) =================
SERVICE_ACCOUNT_FILE = "service-account.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# REEMPLAZA CON TU ID DE HOJA REAL
SHEET_ID = "1wY9sRQ_KbaCiVUb4UHX50NzHweZnMh-YmATtY8UA-mQ" 
HOJA = "REGISTRADURIA"

COL_DOCUMENTO = "DOCUMENTO"
COL_ESTADO = "ESTADO_REGISTRADURIA"
COL_TIPO_DOC = "TIPO_DOCUMENTO"

URL_REG = "https://defunciones.registraduria.gov.co"
SEL_DOC = "#nuip"
SEL_BTN = "button.btn.btn-primary[type='submit']"

# --- AJUSTES DE RENDIMIENTO ---
NUM_WORKERS = 10           # Un poco más conservador para evitar bloqueos
PAUSA_ENTRE_CONSULTAS = 0.5 
BATCH_SIZE = 500         

# ================= HELPERS =================

def conectar_sheet():
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError(f"No se encuentra {SERVICE_ACCOUNT_FILE}")
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).worksheet(HOJA)

def col_letra(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def resolver_columnas(ws):
    headers = ws.row_values(1)
    def idx(col):
        if col not in headers: raise ValueError(f"Falta columna: {col}")
        return headers.index(col) + 1
    return idx(COL_DOCUMENTO), idx(COL_ESTADO), idx(COL_TIPO_DOC)

# ================= SCRAPING OPTIMIZADO & SIGILOSO =================

async def obtener_estado(page, documento: str) -> str:
    try:
        # 1. Navegación
        try:
            await page.goto(URL_REG, wait_until="domcontentloaded", timeout=45000)
        except Exception:
            pass 
        
        # 2. Esperamos el input
        try:
            input_el = await page.wait_for_selector(SEL_DOC, state="visible", timeout=20000)
        except Exception:
            return "ERROR_CARGA_PAGINA"

        # 3. Llenado con pausas humanas mínimas
        await input_el.fill(documento)
        await asyncio.sleep(random.uniform(0.1, 0.3)) # Pequeña pausa humana
        await page.click(SEL_BTN)

        # 4. Espera de respuesta
        try:
            await page.wait_for_function(
                "() => document.body.innerText.includes('VIGENTE') || document.body.innerText.includes('CANCELADA') || document.body.innerText.includes('NO SE ENCONTRO')",
                timeout=8000 
            )
        except:
            pass 

        texto = (await page.inner_text("body")).lower()

        if "vigente (vivo)" in texto: return "VIGENTE (VIVO)"
        if "cancelada por muerte" in texto: return "CANCELADA POR MUERTE"
        if "no se encontró" in texto or "no existe" in texto: return "NO SE ENCONTRO ESTADO"
        
        return "ERROR: RESPUESTA INESPERADA"

    except Exception as e:
        return f"RETRY_ERROR: {str(e)[:30]}"

# ================= PIPELINE MASIVO =================

async def job_producer(ws, job_queue, idx_doc, idx_est, idx_tipo):
    logger.info("Leyendo hoja (modo robusto)...")
    print("⏳ Leyendo hoja (modo robusto)...")
    values = []
    for intento in range(3):
        try:
            values = ws.get_all_values()
            break
        except Exception as e:
            logger.warning(f"Error leyendo hoja (Intento {intento+1}): {e}")
            print(f"⚠️ Error leyendo hoja (Intento {intento+1}): {e}")
            time.sleep(5)
    
    if not values:
        logger.error("No se pudo leer la hoja.")
        print("❌ No se pudo leer la hoja.")
        return 0
    logger.info(f"Hoja leída. Total filas: {len(values)}. Filtrando pendientes...")
    print(f"✅ Hoja leída. Total filas: {len(values)}. Filtrando pendientes...")
    
    encolados = 0
    for i in range(1, len(values)):
        row = values[i]
        fila = i + 1

        doc = row[idx_doc - 1] if len(row) >= idx_doc else ""
        est = row[idx_est - 1] if len(row) >= idx_est else ""
        tipo = (row[idx_tipo - 1] if len(row) >= idx_tipo else "").strip().upper()

        if doc and not est and tipo == "CC":
            await job_queue.put((fila, doc))
            encolados += 1
            
    return encolados

async def worker(pid, page, job_queue, result_queue, hoja, letra_est):
    while True:
        item = await job_queue.get()
        if item is None:
            job_queue.task_done()
            break

        fila, doc = item
        est = await obtener_estado(page, doc)
        
        await result_queue.put({
            "range": f"'{hoja}'!{letra_est}{fila}",
            "values": [[est]]
        })
        
        if "VIGENTE" not in est:
             print(f"[{pid}] 🔍 {doc} -> {est}")
        
        job_queue.task_done()
        await asyncio.sleep(PAUSA_ENTRE_CONSULTAS)

async def batch_writer(ws, result_queue):
    buffer = []
    print(f"[WRITER] Iniciado. Buffer: {BATCH_SIZE}.")
    
    while True:
        try:
            item = await asyncio.wait_for(result_queue.get(), timeout=5.0)
            if item is None:
                if buffer: await flush(ws, buffer)
                result_queue.task_done()
                break
            buffer.append(item)
            result_queue.task_done()
            if len(buffer) >= BATCH_SIZE:
                await flush(ws, buffer)
                buffer.clear()
        except asyncio.TimeoutError:
            if buffer:
                await flush(ws, buffer)
                buffer.clear()

async def flush(ws, data):
    for intento in range(5):
        try:
            ws.spreadsheet.values_batch_update({"valueInputOption": "RAW", "data": data})
            logger.info(f"[GUARDADO] {len(data)} registros en Sheets.")
            print(f"💾 [GUARDADO] {len(data)} registros.")
            return
        except Exception as e:
            wait = (intento + 1) * 5
            logger.warning(f"[API BUSY] Esperando {wait}s... {e}")
            print(f"⚠️ [API BUSY] Esperando {wait}s... {e}")
            await asyncio.sleep(wait)

# ================= ENTRY POINT =================

async def main():
    try:
        ws = conectar_sheet()
    except Exception as e:
        logger.error(f"Error conectando a Sheets: {e}")
        print(f"❌ Error conectando a Sheets: {e}")
        return

    idx_doc, idx_est, idx_tipo = resolver_columnas(ws)
    letra_est = col_letra(idx_est)
    job_q = asyncio.Queue()
    res_q = asyncio.Queue()

    async with async_playwright() as p:
        logger.info("Iniciando Motor en MODO STEALTH (Sigiloso)...")
        print("🚀 Iniciando Motor en MODO STEALTH (Sigiloso)...")
        
        # --- TRUCO ANTIBLOQUEO ---
        # Lanzamos Chrome con argumentos para desactivar la detección de automatización
        browser = await p.chromium.launch(
            headless=True,  # Sigue siendo fantasma
            args=[
                "--disable-blink-features=AutomationControlled", # Clave para que no sepan que es robot
                "--no-sandbox",
                "--disable-setuid-sandbox"
            ]
        )
        
        pages = []
        for i in range(NUM_WORKERS):
            # Creamos un contexto que finge ser un usuario real de Windows
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                viewport={'width': 1920, 'height': 1080},
                locale="es-CO",
                timezone_id="America/Bogota"
            )
            
            # Script extra para ocultar la propiedad 'webdriver' de javascript
            await ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # Bloquear imagenes para velocidad
            await ctx.route("**/*.{png,jpg,jpeg,svg,gif}", lambda route: route.abort())
            
            pages.append(await ctx.new_page())
        logger.info(f"{NUM_WORKERS} Workers Stealth listos.")
        print(f"✅ {NUM_WORKERS} Workers Stealth listos.")

        writer_task = asyncio.create_task(batch_writer(ws, res_q))
        
        workers = [
            asyncio.create_task(worker(i, pages[i], job_q, res_q, HOJA, letra_est))
            for i in range(NUM_WORKERS)
        ]

        total = await job_producer(ws, job_q, idx_doc, idx_est, idx_tipo)
        logger.info(f"Total pendientes a consultar: {total}")
        print(f"📊 Total pendientes: {total}")

        if total == 0:
            logger.info("Nada pendiente.")
            print("Nada pendiente.")
            for _ in range(NUM_WORKERS): await job_q.put(None)
            await res_q.put(None)
            await writer_task
            return

        for _ in range(NUM_WORKERS): await job_q.put(None)
        await job_q.join()
        await asyncio.gather(*workers)
        
        await res_q.put(None)
        await writer_task
        await browser.close()
    logger.info("Proceso de Consulta Finalizado.")
    print("🏁 Proceso Finalizado.")

if __name__ == "__main__":
    asyncio.run(main())