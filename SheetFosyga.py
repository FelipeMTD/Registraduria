# SheetFosyga.py  (REAL + FIX TIMEOUT RETRY + DEDUPE)
# ------------------------------------------------------------
# - Consulta ADRES BDUA (Playwright) y escribe resultados en Google Sheets.
# - NO contiene lógica de supervisor.
# - Si detecta portal inusable (ventana deslizante ERROR_UI), hace FLUSH y sale con code 2.
# - Reintento interno: SOLO reintenta los TIMEOUT_CONSULTA reales (sin releer Sheet).
# - Dedup: evita repetir el mismo (tipo_doc, documento) en pendientes.
# ------------------------------------------------------------

import os
import sys
import asyncio
import logging
import unicodedata
from contextlib import suppress
from datetime import datetime
from collections import deque
from typing import Any, Dict, List, Tuple, Union, Optional

import gspread
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


# ==============================
# CONFIG
# ==============================
URL_CONSULTA = "https://servicios.adres.gov.co/BDUA/Consulta-Afiliados-BDUA"

SHEET_ID = "1wY9sRQ_KbaCiVUb4UHX50NzHweZnMh-YmATtY8UA-mQ"
WORKSHEET_NAME = "PACIENTES"

SERVICE_ACCOUNT_JSON = "service-accountFosyga.json"

COL_TIPO_DOC = "TIPO_DOCUMENTO"
COL_DOCUMENTO = "DOCUMENTO"
COL_ESTADO = "ESTADO_FOSYGA"

COLS_EXTRA = [
    "ENTIDAD",
    "REGIMEN",
    "FECHA_AFILIACION",
    "FECHA_FIN_AFILIACION",
    "TIPO_AFILIADO",
]

NUM_WORKERS = 5
HEADLESS = False

PAUSA_ENTRE_CONSULTAS = 5  # segundos
TIMEOUT_NAV_MS = 30_000
TIMEOUT_POSTBACK_MS = 20_000
TIMEOUT_POPUP_MS = 15_000

WRITE_BATCH_SIZE = 50

# Reintento interno SOLO de TIMEOUT_CONSULTA reales
MAX_TIMEOUT_RETRIES_PER_DOC = 1

# Corte real (tester): ventana deslizante en writer
ERROR_UI_WINDOW_SIZE = 8
ERROR_UI_THRESHOLD = 4


# ==============================
# LOGGING
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("fosyga.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)


# ==============================
# ABORT
# ==============================
class FatalAbort(Exception):
    """Abort reintentable: el supervisor debe reiniciar (exit code 2)."""
    pass


ABORT_EVENT = asyncio.Event()


# ==============================
# GOOGLE SHEETS
# ==============================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def get_gsheet(sheet_id: str, worksheet_name: str) -> gspread.Worksheet:
    if not os.path.exists(SERVICE_ACCOUNT_JSON):
        raise RuntimeError(f"NO EXISTE {SERVICE_ACCOUNT_JSON} en el directorio: {os.getcwd()}")
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=SCOPES)
    client = gspread.authorize(creds)
    ss = client.open_by_key(sheet_id)
    return ss.worksheet(worksheet_name)

def asegurar_columnas_sheet(ws: gspread.Worksheet, columnas_requeridas: List[str]) -> None:
    headers = ws.row_values(1)
    faltantes = [c for c in columnas_requeridas if c not in headers]
    if not faltantes:
        return
    nuevas = headers + faltantes
    ws.update("1:1", [nuevas])

def col_letter(n: int) -> str:
    return gspread.utils.rowcol_to_a1(1, n).replace("1", "")


# ==============================
# NORMALIZACIÓN / TIPO DOC
# ==============================
MAP_TIPO_DOC = {
    "CC": "CC", "CEDULA": "CC", "CEDULA DE CIUDADANIA": "CC",
    "TI": "TI", "TARJETA DE IDENTIDAD": "TI",
    "CE": "CE", "CEDULA DE EXTRANJERIA": "CE",
    "PA": "PA", "PASAPORTE": "PA",
    "RC": "RC", "REGISTRO CIVIL": "RC",
    "NU": "NU",
    "AS": "AS",
    "MS": "MS",
    "CD": "CD",
    "CN": "CN",
    "SC": "SC",
    "PE": "PE",
    "PT": "PT",
}

def normalizar(txt: str) -> str:
    if not txt:
        return ""
    txt = txt.strip().upper()
    txt = unicodedata.normalize("NFD", txt)
    return "".join(c for c in txt if unicodedata.category(c) != "Mn")

def homologar_tipo_doc(valor: str) -> str:
    key = normalizar(valor)
    if key in MAP_TIPO_DOC:
        return MAP_TIPO_DOC[key]
    raise ValueError("TIPO_DOC_NO_VALIDO")

def normalizar_fecha(valor: str) -> str:
    if not valor:
        return ""
    valor = str(valor).strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(valor, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return valor


# ==============================
# CARGA PENDIENTES (CON DEDUPE)
# Pendiente = ESTADO_FOSYGA vacío o TIMEOUT_CONSULTA
# ==============================
def cargar_pendientes(ws: gspread.Worksheet) -> List[Tuple[int, str, str]]:
    headers = ws.row_values(1)
    for col in [COL_TIPO_DOC, COL_DOCUMENTO, COL_ESTADO]:
        if col not in headers:
            raise RuntimeError(f"Falta columna obligatoria: {col}")

    c_tipo = headers.index(COL_TIPO_DOC) + 1
    c_doc = headers.index(COL_DOCUMENTO) + 1
    c_est = headers.index(COL_ESTADO) + 1

    docs = ws.col_values(c_doc)
    last_row = len(docs)
    if last_row < 2:
        return []

    r_tipo = f"{col_letter(c_tipo)}2:{col_letter(c_tipo)}{last_row}"
    r_est  = f"{col_letter(c_est)}2:{col_letter(c_est)}{last_row}"

    tipos = ws.get(r_tipo, value_render_option="UNFORMATTED_VALUE")
    estados = ws.get(r_est, value_render_option="UNFORMATTED_VALUE")

    pendientes: List[Tuple[int, str, str]] = []
    seen: set[Tuple[str, str]] = set()

    for i in range(len(docs) - 1):
        idx = i
        tipo_raw = tipos[i][0] if i < len(tipos) and tipos[i] else ""
        doc_raw = docs[i + 1]
        estado_raw = estados[i][0] if i < len(estados) and estados[i] else ""

        documento = str(doc_raw).strip()
        if not documento:
            continue

        estado = str(estado_raw).strip()
        if estado and estado != "TIMEOUT_CONSULTA":
            continue

        try:
            tipo = homologar_tipo_doc(str(tipo_raw))
        except Exception:
            continue

        key = (tipo, documento)
        if key in seen:
            continue
        seen.add(key)

        pendientes.append((idx, tipo, documento))

    return pendientes


# ==============================
# PLAYWRIGHT: CONSULTA
# ==============================
async def set_select_if_needed(iframe, selector: str, value: str) -> bool:
    current = await iframe.evaluate(f"document.querySelector('{selector}')?.value || ''")
    if current == value:
        return False
    await iframe.select_option(selector, value=value)
    return True

async def consultar_paciente(context, tipo_doc: str, documento: str) -> Union[str, Dict[str, str]]:
    page = None
    popup = None
    try:
        page = await context.new_page()
        page.set_default_timeout(45_000)

        await page.goto(URL_CONSULTA, timeout=TIMEOUT_NAV_MS)
        await page.wait_for_selector("#iframeBDUA", timeout=TIMEOUT_NAV_MS)

        iframe_el = await page.query_selector("#iframeBDUA")
        iframe = await iframe_el.content_frame() if iframe_el else None
        if not iframe:
            return "ERROR_IFRAME"

        changed = await set_select_if_needed(iframe, "#tipoDoc", tipo_doc)
        if changed:
            try:
                vs_before = await iframe.evaluate("document.getElementById('__VIEWSTATE').value")
                await iframe.wait_for_function(
                    f"document.getElementById('__VIEWSTATE').value !== '{vs_before}'",
                    timeout=TIMEOUT_POSTBACK_MS,
                )
            except Exception:
                logging.warning(f"Postback no confirmado tipoDoc={tipo_doc}, continuo.")

        await iframe.fill("#txtNumDoc", documento)

        btn = await iframe.query_selector("#btnConsultar")
        if not btn:
            return "ERROR_UI"

        disabled = await btn.get_attribute("disabled")
        if disabled is not None:
            return "ERROR_UI"

        try:
            async with context.expect_page(timeout=TIMEOUT_POPUP_MS) as popup_info:
                await iframe.click("#btnConsultar")
        except PlaywrightTimeoutError:
            return "ERROR_UI"

        popup = await popup_info.value
        await popup.wait_for_load_state("domcontentloaded")
        await popup.wait_for_timeout(500)

        html = await popup.content()
        soup = BeautifulSoup(html, "html.parser")

        lower = html.lower()
        if "captcha" in lower or "bloqueado" in lower:
            raise FatalAbort("BLOQUEO_ADRES")

        panel_no = soup.find(id="PanelNoAfiliado")
        if panel_no:
            lbl = panel_no.find(id="lblError")
            if lbl:
                texto = lbl.get_text(strip=True).lower()
                if "no se encuentra" in texto and "bdua" in texto:
                    return "NO_ENCONTRADO"

        tabla = soup.find("table", id="GridViewAfiliacion")
        if tabla:
            filas = tabla.find_all("tr", class_="DataGrid_Item")
            if filas:
                celdas = filas[0].find_all("td")
                if len(celdas) >= 6:
                    datos = {
                        "ESTADO": celdas[0].get_text(strip=True),
                        "ENTIDAD": celdas[1].get_text(strip=True),
                        "REGIMEN": celdas[2].get_text(strip=True),
                        "FECHA_AFILIACION": celdas[3].get_text(strip=True),
                        "FECHA_FIN_AFILIACION": celdas[4].get_text(strip=True),
                        "TIPO_AFILIADO": celdas[5].get_text(strip=True),
                    }
                    if datos["ESTADO"] and datos["ENTIDAD"]:
                        return datos

        return "ERROR_RESPUESTA"

    except PlaywrightTimeoutError:
        return "TIMEOUT_CONSULTA"
    except FatalAbort:
        raise
    except Exception as e:
        logging.exception(f"ERROR_INTERNO {tipo_doc} {documento}: {e}")
        return "ERROR_RESPUESTA"
    finally:
        with suppress(Exception):
            if popup and not popup.is_closed():
                await popup.close()
        with suppress(Exception):
            if page and not page.is_closed():
                await page.close()


# ==============================
# WORKERS
# ==============================
async def worker_fosyga(worker_id: int, browser, trabajos: List[Tuple[int, str, str]], queue: asyncio.Queue):
    context = await browser.new_context()
    try:
        for idx, tipo_doc, documento in trabajos:
            if ABORT_EVENT.is_set():
                logging.warning(f"[W{worker_id}] ABORT_EVENT activo -> saliendo worker")
                break

            logging.info(f"[SHEET | W{worker_id}] {tipo_doc} {documento}")
            estado = await consultar_paciente(context, tipo_doc, documento)

            if ABORT_EVENT.is_set():
                break

            await queue.put((idx, estado, tipo_doc, documento))

            if ABORT_EVENT.is_set():
                break

            await asyncio.sleep(PAUSA_ENTRE_CONSULTAS)

    finally:
        with suppress(Exception):
            await context.close()

async def lanzar_workers(browser, queue: asyncio.Queue, pendientes: List[Tuple[int, str, str]]):
    grupos = [pendientes[i::NUM_WORKERS] for i in range(NUM_WORKERS)]
    tasks = []
    for wid in range(NUM_WORKERS):
        if grupos[wid]:
            tasks.append(asyncio.create_task(worker_fosyga(wid, browser, grupos[wid], queue)))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


# ==============================
# WRITER
# - Ventana deslizante ERROR_UI
# - Guarda lista REAL de TIMEOUT para reintento interno (sin releer Sheet)
# ==============================
async def writer(queue: asyncio.Queue, shared: Dict[str, Any]):
    logging.info("[WRITER] Iniciando writer...")

    ws = get_gsheet(SHEET_ID, WORKSHEET_NAME)
    asegurar_columnas_sheet(ws, [COL_ESTADO] + COLS_EXTRA)

    headers = ws.row_values(1)
    col_map = {h: i + 1 for i, h in enumerate(headers)}
    if COL_ESTADO not in col_map:
        raise RuntimeError(f"No existe columna {COL_ESTADO} tras asegurar headers.")

    batch: List[Dict[str, Any]] = []
    batch_count = 0

    ventana = deque(maxlen=ERROR_UI_WINDOW_SIZE)

    async def flush(reason: str):
        nonlocal batch, batch_count
        if not batch:
            return
        logging.critical(f"[WRITER] FLUSH ({reason}) | items={batch_count} celdas={len(batch)}")
        try:
            await asyncio.to_thread(ws.batch_update, batch)
        finally:
            batch.clear()
            batch_count = 0

    def append_string_estado(idx: int, estado: str):
        nonlocal batch, batch_count
        row = idx + 2
        cell = gspread.utils.rowcol_to_a1(row, col_map[COL_ESTADO])
        batch.append({"range": cell, "values": [[estado]]})
        batch_count += 1

    def append_dict_estado(idx: int, estado_dict: Dict[str, str]):
        nonlocal batch, batch_count
        row = idx + 2
        for campo, valor in estado_dict.items():
            dest = COL_ESTADO if campo == "ESTADO" else campo
            if dest in col_map:
                if "FECHA" in campo:
                    valor = normalizar_fecha(valor)
                cell = gspread.utils.rowcol_to_a1(row, col_map[dest])
                batch.append({"range": cell, "values": [[valor]]})
        batch_count += 1

    while True:
        item = await queue.get()

        if item is None:
            queue.task_done()
            break

        idx, estado, tipo_doc, documento = item

        estado_str: Optional[str] = None
        if not isinstance(estado, dict):
            estado_str = str(estado).strip()

        # ---- registrar TIMEOUT real para reintento interno ----
        if estado_str == "TIMEOUT_CONSULTA":
            shared["timeouts"].append((idx, tipo_doc, documento))

        # ---- ventana deslizante SOLO ERROR_UI ----
        ventana.append(estado_str == "ERROR_UI")
        if sum(1 for x in ventana if x) >= ERROR_UI_THRESHOLD:
            ABORT_EVENT.set()
            await flush("VENTANA_ERROR_UI_THRESHOLD")
            queue.task_done()
            raise FatalAbort("VENTANA_ERROR_UI_THRESHOLD")

        # ---- escritura normal ----
        if isinstance(estado, dict):
            append_dict_estado(idx, estado)
        else:
            append_string_estado(idx, estado_str or "")

        if batch_count >= WRITE_BATCH_SIZE:
            await flush("BATCH_SIZE")

        queue.task_done()

    await flush("FINAL")


# ==============================
# MAIN
# ==============================
def _dedupe_trabajos(trabajos: List[Tuple[int, str, str]]) -> List[Tuple[int, str, str]]:
    seen = set()
    out = []
    for idx, tipo, doc in trabajos:
        k = (tipo, doc)
        if k in seen:
            continue
        seen.add(k)
        out.append((idx, tipo, doc))
    return out

async def main():
    ABORT_EVENT.clear()

    ws = get_gsheet(SHEET_ID, WORKSHEET_NAME)
    pendientes = cargar_pendientes(ws)
    pendientes = _dedupe_trabajos(pendientes)

    logging.info(f"[SHEET] Pendientes detectados: {len(pendientes)}")

    if not pendientes:
        logging.info("[SHEET] No hay trabajo. Salida limpia.")
        return

    queue: asyncio.Queue = asyncio.Queue()
    shared: Dict[str, Any] = {"timeouts": []}  # <- aquí guardamos TIMEOUT reales

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)

        writer_task = asyncio.create_task(writer(queue, shared))
        exit_code = 0

        try:
            # -------- corrida 1 --------
            await lanzar_workers(browser, queue, pendientes)
            await queue.join()  # writer consumió todo lo encolado (aunque no haya flush)

            # -------- reintento interno SOLO TIMEOUT reales --------
            if not ABORT_EVENT.is_set() and MAX_TIMEOUT_RETRIES_PER_DOC > 0:
                timeouts = shared.get("timeouts", [])
                if timeouts:
                    # Dedup y reconstrucción de trabajos (solo por tipo/doc)
                    # OJO: idx se conserva para escribir en la fila correcta
                    retry = _dedupe_trabajos([(idx, tipo, doc) for (idx, tipo, doc) in timeouts])
                    shared["timeouts"] = []  # limpia para no reusar

                    logging.info(f"[SHEET] Reintento interno TIMEOUT reales: {len(retry)}")
                    await lanzar_workers(browser, queue, retry)
                    await queue.join()

        except FatalAbort as e:
            exit_code = 2
            logging.critical(f"[SHEET] ABORT (reintentable): {e}")

        finally:
            with suppress(Exception):
                await queue.put(None)
            with suppress(Exception):
                await writer_task
            with suppress(Exception):
                await browser.close()

        if exit_code == 2:
            raise SystemExit(2)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except SystemExit:
        raise
    except KeyboardInterrupt:
        logging.warning("INTERRUPCION MANUAL (CTRL+C)")
    except Exception as e:
        logging.exception(f"ERROR_FATAL: {e}")
        raise
