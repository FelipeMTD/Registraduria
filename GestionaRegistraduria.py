import asyncio
from google.oauth2.service_account import Credentials
import gspread
from playwright.async_api import async_playwright

from IniciarSesion import create_pool
from AccionesRegistraduria import (
    abrir_edicion_paciente,
    marcar_fallecido,
    guardar,
    ya_muerto_inactivo,
    PacienteNoEncontrado
)


# =====================================================
# GOOGLE SHEETS
# =====================================================

SERVICE_ACCOUNT_FILE = "service-account.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SHEET_ID = "1wY9sRQ_KbaCiVUb4UHX50NzHweZnMh-YmATtY8UA-mQ"
HOJA = "REGISTRADURIA"

COL_DOCUMENTO = "DOCUMENTO"
COL_ESTADO_REG = "ESTADO_REGISTRADURIA"
COL_ESTADO_GEST = "ESTADO_GESTIONA"

# =====================================================
# PIPELINE CONFIG
# =====================================================

WORKERS = 5
QUEUE_MAX = 2000

URL_PACIENTES = "https://saludgestiona.com/business/patients-list"

PAUSA_ACCION = 0.5
BATCH_SIZE = 900

# =====================================================
# SHEET UTILS
# =====================================================

def conectar_sheet():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES
    )
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).worksheet(HOJA)


def resolver_indices(headers):

    def idx(col):
        try:
            return headers.index(col) + 1
        except ValueError:
            raise RuntimeError(f"Falta columna: {col}")

    return (
        idx(COL_DOCUMENTO),
        idx(COL_ESTADO_REG),
        idx(COL_ESTADO_GEST),
    )


def col_letra(n):
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


async def batch_writer(ws, result_queue):
    batch = []

    async def safe_flush(data):
        if not data:
            return
        # Backoff real para 429 (Sheets write quota)
        wait_s = 2
        for intento in range(7):
            try:
                print(f"[WRITER] FLUSH -> {len(data)} registros")

                ws.spreadsheet.values_batch_update({
                    "valueInputOption": "RAW",
                    "data": data
                })
                return
            except Exception as e:
                msg = str(e)
                is_429 = ("429" in msg) or (type(e).__name__ == "APIError" and "Quota exceeded" in msg)
                print(f"[WRITER] Retry {intento+1}/7 -> {type(e).__name__} -> {e}")
                await asyncio.sleep(wait_s if is_429 else 2)
                wait_s = min(wait_s * 2, 60)
        print("[WRITER] ERROR FATAL: batch perdido")

    try:
        while True:
            item = await result_queue.get()

            if item is None:

                # ===== FLUSH FINAL OBLIGATORIO =====
                if batch:
                    try:
                        await safe_flush(batch.copy())
                    except Exception as e:
                        print("[WRITER FINAL ERROR]", type(e).__name__, e)

                    batch.clear()

                result_queue.task_done()
                break

            batch.append(item)
            result_queue.task_done()

            if len(batch) >= BATCH_SIZE:
                await safe_flush(batch.copy())
                batch.clear()

    except asyncio.CancelledError:
        print("[WRITER] Cancelado → flush en finally")
        return


# =====================================================
# SHEET PRODUCER
# =====================================================

async def producer(ws, job_queue, result_queue, col_gest_letter):

    values = ws.get_all_values()
    headers = values[0]
    rows = values[1:]

    col_doc, col_reg, col_gest = resolver_indices(headers)

    total = 0

    for i, row in enumerate(rows, start=2):

        def cell(idx):
            return row[idx - 1].strip() if len(row) >= idx and row[idx - 1] else ""

        documento = cell(col_doc)
        estado_reg = cell(col_reg)
        estado_gest = cell(col_gest)

        if (
            documento
            and estado_reg in ("CANCELADA POR MUERTE", "YA_MUERTO")
            and not estado_gest
        ):
            await job_queue.put((i, documento))

            # NO usar update_cell por fila (rompe por cuota 429). Enviar al writer batch.
            await result_queue.put({
                "range": f"'{HOJA}'!{col_gest_letter}{i}",
                "values": [["PROCESANDO"]]
            })

            total += 1

    return total


# =====================================================
# WORKER
# =====================================================

async def worker(worker_id, page, job_queue, result_queue, col_estado_letter, col_gest_letter):

    while True:

        item = await job_queue.get()

        if item is None:
            job_queue.task_done()
            return

        fila, documento = item

        try:

            # Buscar paciente -> abrir edición -> marcar fallecido -> guardar
            await abrir_edicion_paciente(page, documento, url_pacientes=URL_PACIENTES)

            if await ya_muerto_inactivo(page):
                obs = "YA_MUERTO"
            else:
                res = await marcar_fallecido(page)

                if res == "SKIP":
                    obs = "YA_MUERTO"
                else:
                    ok, msg = await guardar(page)

                    if not ok:
                        raise RuntimeError(msg)

                    obs = "MUERTE_OK"

            await result_queue.put({
                "range": f"'{HOJA}'!{col_gest_letter}{fila}",
                "values": [[obs]]
            })

            print(f"[W{worker_id}] OK -> {documento}")

        except PacienteNoEncontrado:
            obs = "NO_EXISTE"

            await result_queue.put({
                "range": f"'{HOJA}'!{col_gest_letter}{fila}",
                "values": [[obs]]
            })

            print(f"[W{worker_id}] NO EXISTE -> {documento}")

        except Exception as e:

            await result_queue.put({
                "range": f"'{HOJA}'!{col_gest_letter}{fila}",
                "values": [[f"ERROR: {type(e).__name__} | {str(e)}"]]
            })

            print(f"[W{worker_id}] ERROR -> {documento} -> {type(e).__name__} -> {str(e)}")

        finally:
            job_queue.task_done()

            # Limpieza defensiva: si el navegador/contexto ya cerró, no reventar el worker.
            try:
                await page.goto("about:blank")
            except Exception as e:
                # Playwright viejo: TargetClosedError no está expuesto
                t = type(e).__name__
                s = str(e)
                if ("TargetClosed" in t) or ("has been closed" in s) or ("ERR_ABORTED" in s) or ("frame was detached" in s):
                    return
                # cualquier otro error, lo ignoras en limpieza
                pass


            await asyncio.sleep(PAUSA_ACCION)


# =====================================================
# MAIN
# =====================================================

async def main():

    ws = conectar_sheet()
    col_doc, col_reg, col_gest = resolver_indices(ws.row_values(1))
    col_estado_letter = col_letra(col_reg)
    col_gest_letter = col_letra(col_gest)

    job_queue = asyncio.Queue(maxsize=QUEUE_MAX)
    result_queue = asyncio.Queue()

    async with async_playwright() as p:

        pool = None

        try:

            pool = await create_pool(p, workers=WORKERS, headless=False)

            writer_task = asyncio.create_task(batch_writer(ws, result_queue))

            workers = [
                asyncio.create_task(
                    worker(i + 1, pool.pages[i], job_queue, result_queue, col_estado_letter, col_gest_letter)
                )
                for i in range(WORKERS)
            ]

            total = await producer(ws, job_queue, result_queue, col_gest_letter)
            print(f"[QUEUE] Total a procesar: {total}")

            for _ in range(WORKERS):
                await job_queue.put(None)

            await job_queue.join()

            # Recuperar excepciones y evitar "Task exception was never retrieved"
            await asyncio.gather(*workers, return_exceptions=True)

            await result_queue.put(None)
            await result_queue.join()

            await writer_task

        finally:

            if pool:
                await pool.close()

    print("[FIN] Proceso terminado limpio.")


if __name__ == "__main__":
    asyncio.run(main())
