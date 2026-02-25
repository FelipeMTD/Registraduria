import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import gspread
from google.oauth2.service_account import Credentials

# =====================================================
# GOOGLE SHEETS
# =====================================================
SERVICE_ACCOUNT_FILE = "service-account.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SHEET_ID = "1wY9sRQ_KbaCiVUb4UHX50NzHweZnMh-YmATtY8UA-mQ"
HOJA = "REGISTRADURIA"

COL_DOCUMENTO = "DOCUMENTO"
COL_ESTADO = "ESTADO_REGISTRADURIA"
COL_TIPO_DOC = "TIPO_DOCUMENTO"   # <-- NUEVO

# =====================================================
# SCRAPINGñ
# =====================================================
URL = "https://defunciones.registraduria.gov.co"
SEL_DOC = "#nuip"
SEL_BTN = "button.btn.btn-primary[type='submit']"

NUM_WORKERS = 10
PAUSA_ENTRE_CONSULTAS = 0.6

# =====================================================
# CONTROL DE ESTALLIDOS (MEMORIA CONSTANTE)
# =====================================================
BATCH_SIZE = 5000
JOB_QUEUE_MAX = 6000
RESULT_QUEUE_MAX = BATCH_SIZE

# =====================================================
# HELPERS SHEETS
# =====================================================
def conectar_sheet():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).worksheet(HOJA)


def limpiar_rango_c_d(ws):
    # Borra desde la fila 2 para no dañar headers (fila 1)
    ws.batch_clear(["C2:D"])


def col_letra(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def resolver_columnas(ws):
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("Fila 1 vacía: no hay headers.")

    def idx(col):
        try:
            return headers.index(col) + 1
        except ValueError:
            raise RuntimeError(f"No existe header '{col}'.")

    return (
        idx(COL_DOCUMENTO),
        idx(COL_ESTADO),
        idx(COL_TIPO_DOC),
    )

def flush_batch(ws, batch):
    if not batch:
        return
    body = {"valueInputOption": "RAW", "data": batch}
    ws.spreadsheet.values_batch_update(body)

# =====================================================
# SCRAPING REGISTRADURÍA
# =====================================================
# async def obtener_estado(page, documento: str) -> str:
#     await page.goto(URL, wait_until="load")
#     await page.wait_for_selector(SEL_DOC, timeout=15000)

#     await page.fill(SEL_DOC, documento)
#     await page.click(SEL_BTN)
#     await page.wait_for_timeout(2000)

#     try:
#         texto = (await page.inner_text("body")).lower()
#     except PlaywrightTimeoutError:
#         texto = (await page.content()).lower()

#     if "vigente (vivo)" in texto:
#         return "VIGENTE (VIVO)"
#     if "cancelada por muerte" in texto:
#         return "CANCELADA POR MUERTE"
#     return "NO SE ENCONTRO ESTADO"

async def obtener_estado(page, documento: str) -> str:
    # 1. Esperamos a que la red esté totalmente quieta (esto carga el CSS y JS)
    await page.goto(URL, wait_until="networkidle", timeout=60000)
    
    # 2. Esperamos específicamente al input de la cédula
    # El selector #nuip es correcto, pero hay que darle tiempo a Angular
    await page.wait_for_selector(SEL_DOC, state="visible", timeout=20000)

    # 3. Llenamos y enviamos
    await page.fill(SEL_DOC, documento)
    await page.click(SEL_BTN)
    
    # 4. Esperamos a que aparezca la respuesta o cambie la URL
    # En lugar de un tiempo fijo, esperamos a que el texto de carga desaparezca
    try:
        # Esperamos a que el cuerpo de la página contenga la respuesta
        await page.wait_for_function(
            "() => document.body.innerText.includes('VIGENTE') || document.body.innerText.includes('CANCELADA') || document.body.innerText.includes('NO SE ENCONTRO')",
            timeout=10000
        )
    except:
        pass # Si falla la espera, intentamos leer lo que haya

    texto = (await page.inner_text("body")).lower()

    if "vigente (vivo)" in texto:
        return "VIGENTE (VIVO)"
    if "cancelada por muerte" in texto:
        return "CANCELADA POR MUERTE"
    if "no se encontró" in texto or "no existe" in texto:
        return "NO SE ENCONTRO ESTADO"
    
    return "ERROR: RESPUESTA INESPERADA"



# =====================================================
# PIPELINE
# =====================================================
async def job_producer(
    ws,
    job_queue: asyncio.Queue,
    col_doc_idx: int,
    col_estado_idx: int,
    col_tipo_idx: int,
):
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return 0

    encolados = 0

    for i in range(1, len(values)):
        fila = i + 1
        row = values[i]

        def cell(idx):
            return row[idx - 1].strip() if len(row) >= idx and row[idx - 1] else ""

        doc = cell(col_doc_idx)
        estado = cell(col_estado_idx)
        tipo = cell(col_tipo_idx).upper()

        # FILTRO CLARO Y DURO
        if doc and not estado and tipo == "CC":
            await job_queue.put((fila, doc))
            encolados += 1

    return encolados

async def worker(worker_id, page, job_queue, result_queue, hoja, col_estado_letter):
    while True:
        item = await job_queue.get()
        if item is None:
            job_queue.task_done()
            return

        fila, doc = item
        try:
            estado = await obtener_estado(page, doc)
        except Exception as e:
            estado = f"ERROR: {type(e).__name__}"

        await result_queue.put({
            "range": f"'{hoja}'!{col_estado_letter}{fila}",
            "values": [[estado]]
        })

        job_queue.task_done()
        await asyncio.sleep(PAUSA_ENTRE_CONSULTAS)

async def batch_writer(ws, result_queue):
    batch = []

    async def safe_flush(data):
        if not data:
            return
        for intento in range(3):
            try:
                ws.spreadsheet.values_batch_update({
                    "valueInputOption": "RAW",
                    "data": data
                })
                return
            except Exception as e:
                print(f"[WRITER] Retry {intento+1}/3 -> {e}")
                await asyncio.sleep(2)
        print("[WRITER] ERROR FATAL: batch perdido")

    try:
        while True:
            item = await result_queue.get()

            if item is None:
                result_queue.task_done()
                break

            batch.append(item)
            result_queue.task_done()

            if len(batch) >= BATCH_SIZE:
                await safe_flush(batch.copy())
                batch.clear()
                await asyncio.sleep(3)


    except asyncio.CancelledError:
        print("[WRITER] Cancelado → flush de emergencia")
        await safe_flush(batch)
        raise

    finally:
        if batch:
            print("[WRITER] Finalizando → flush final")
            await safe_flush(batch)


# =====================================================
# MAIN
# =====================================================
async def main():
    ws = conectar_sheet()
    
    # ===== LIMPIEZA PARA EMPEZAR EN CERO =====
    limpiar_rango_c_d(ws)

    col_doc_idx, col_estado_idx, col_tipo_idx = resolver_columnas(ws)
    col_estado_letter = col_letra(col_estado_idx)

    job_queue = asyncio.Queue(maxsize=JOB_QUEUE_MAX)
    result_queue = asyncio.Queue(maxsize=RESULT_QUEUE_MAX)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)

        writer_task = asyncio.create_task(batch_writer(ws, result_queue))

        # pages = [await browser.new_page() for _ in range(NUM_WORKERS)]
        pages = []
        for _ in range(NUM_WORKERS):
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                viewport={'width': 1280, 'height': 720}
            )
            pages.append(await context.new_page())
        workers = [
            asyncio.create_task(
                worker(f"W{i}", pages[i], job_queue, result_queue, HOJA, col_estado_letter)
            )
            for i in range(NUM_WORKERS)
        ]

        total = await job_producer(ws, job_queue, col_doc_idx, col_estado_idx, col_tipo_idx)
        print(f"Pendientes CC encolados: {total}")

        for _ in range(NUM_WORKERS):
            await job_queue.put(None)

        await job_queue.join()

        for w in workers:
            w.cancel()

        await asyncio.gather(*workers, return_exceptions=True)

        await result_queue.put(None)
        await result_queue.join()
        await writer_task

        for w in workers:
            w.cancel()

        await browser.close()

    print("Salida limpia.")

if __name__ == "__main__":
    asyncio.run(main())
