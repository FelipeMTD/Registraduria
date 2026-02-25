import asyncio
import random
import pandas as pd
import unicodedata
import re
import time
from datetime import datetime


from playwright.async_api import async_playwright, TimeoutError
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import os
from dotenv import load_dotenv
from AccionesRegistraduria import (
    abrir_edicion_paciente,
    PacienteNoEncontrado,
    marcar_fallecido,
    guardar as guardar_fallecido,
    guardar_generico as guardar_form
)

# Sesión única (login) + pool de páginas ya autenticadas
from IniciarSesion import create_pool, do_login

load_dotenv()  # <-- ESTA LÍNEA ES LA CLAVE

# ================= CONFIG =================
URL_PACIENTES = "https://saludgestiona.com/business/patients-list"

COL_DOC = "DOCUMENTO"
OBS_COL = "OBSERVACIONES"
BATCH_SIZE = 100

SERVICE_ACCOUNT_FILE = "service-accountFosyga.json"
SPREADSHEET_ID = "1wY9sRQ_KbaCiVUb4UHX50NzHweZnMh-YmATtY8UA-mQ"
SHEET_NAME = "PACIENTES"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
NUM_VENTANAS = 10
# ================= UTILS =================
def norm(txt):
    if not txt:
        return ""
    txt = str(txt).replace("_x000d_", " ")
    txt = unicodedata.normalize("NFKD", txt)
    txt = "".join(c for c in txt if not unicodedata.combining(c))
    return txt.upper().strip()

def limpiar_doc(v):
    if pd.isna(v):
        return ""
    return "".join(c for c in str(v) if c.isdigit())

def normalizar_fecha_excel(valor):
    if not valor:
        return None

    valor = str(valor).strip()

    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(valor, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None
# =========================
# NORMALIZACIÓN EPS (IDENTIDAD) + REGLAS DE NEGOCIO
# =========================
EPS_MAP = {
    "COMFENALCO": "EPS012",
    "SALUD TOTAL": "EPS002",
    "SANITAS": "EPS005",
    "COMPENSAR": "EPS008",
    "NUEVA EPS": "EPS037",
    "COOSALUD": "EPS042",
    "PARTICULAR": "EPSP001",
}

def resolver_codigo_eps(txt_excel):
    t = norm(txt_excel)

    # Si ya viene código
    m = re.search(r"EPS\d{3}", t)
    if m:
        return m.group(0)

    # Resolver por palabra clave
    for k, v in EPS_MAP.items():
        if k in t:
            return v

    return None

def esperado_3374(regimen):
    r = norm(regimen)
    if r == "CONTRIBUTIVO":
        return "CONTRIBUTIVO"
    if r == "SUBSIDIADO":
        return "SUBSIDIADO"
    if r == "PARTICULAR":
        return "PARTICULAR"
    return None

def esperado_2275(regimen, tipo):
    r = norm(regimen)
    t = norm(tipo)

    if r == "SUBSIDIADO":
        return "SUBSIDIADO"

    if r == "CONTRIBUTIVO":
        if t == "COTIZANTE":
            return "CONTRIBUTIVO COTIZANTE"
        if t == "BENEFICIARIO":
            return "CONTRIBUTIVO BENEFICIARIO"
        if t == "ADICIONAL":
            return "CONTRIBUTIVO ADICIONAL"

    return None

# ================= SHEETS =================
def sheets_service():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

def leer_google_sheet(service):

    res = service.spreadsheets().values().batchGet(
        spreadsheetId=SPREADSHEET_ID,
        ranges=[f"{SHEET_NAME}!A1:I"]
    ).execute()

    values = res["valueRanges"][0].get("values", [])

    if not values:
        raise RuntimeError("Sheet vacío o sin encabezados")

    headers = values[0]
    rows = values[1:]

    rows = [r + [""] * (len(headers) - len(r)) for r in rows]

    return pd.DataFrame(rows, columns=headers)

def col_to_a1(n):
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def asegurar_col_obs(service):
    res = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_NAME}!1:1"
    ).execute()
    headers = res.get("values", [[]])[0]
    if OBS_COL in headers:
        return headers.index(OBS_COL) + 1
    headers.append(OBS_COL)
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!1:1",
        valueInputOption="RAW",
        body={"values": [headers]},
    ).execute()
    return len(headers)

def escribir_obs(service, col_obs, fila, texto):
    col = col_to_a1(col_obs)
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!{col}{fila}",
        valueInputOption="RAW",
        body={"values": [[texto]]},
    ).execute()


# ======== CAMPOS (FLUJO REAL) ========
async def leer_erp(page):
    el = page.locator(
        "select[name='spc'] + span span.select2-selection__rendered"
    )

    await el.wait_for(timeout=8000)
    txt = await el.first.inner_text()

    txt = norm(txt)
    for token in txt.split():
        if token.startswith("EPS"):
            return token
    return txt

async def set_erp(page, valor_excel):
    codigo = resolver_codigo_eps(valor_excel)
    if not codigo:
        return False

    # Abrir Select2
    await page.locator(
        "select[name='spc'] + span .select2-selection--single"
    ).click()

    await asyncio.sleep(0.4)

    inp = await page.wait_for_selector(
        "input.select2-search__field", timeout=5000
    )

    # LIMPIAR + TECLEAR HUMANO (igual al sync)
    await inp.click()
    await inp.fill("")
    await inp.type(codigo, delay=80)

    await asyncio.sleep(0.7)

    opciones = page.locator(".select2-results__option")
    n = await opciones.count()

    for i in range(n):
        txt = norm(await opciones.nth(i).inner_text())
        if codigo in txt:
            await opciones.nth(i).click()
            await asyncio.sleep(0.5)

            # VALIDACION REAL (como en sync)
            final = await leer_erp(page)
            return final == codigo

    return False

async def leer_select(page, name):
    sel = page.locator(f"#form_update_patient select[name='{name}']")
    val = await sel.input_value()

    opt = sel.locator(f"option[value='{val}']")
    txt = await opt.inner_text()

    return norm(txt)

async def set_select_por_texto(page, name, texto_objetivo):
    sel = page.locator(f"#form_update_patient select[name='{name}']")
    opciones = sel.locator("option")

    n = await opciones.count()
    for i in range(n):
        txt = norm(await opciones.nth(i).inner_text())
        if texto_objetivo in txt:
            val = await opciones.nth(i).get_attribute("value")

            await sel.select_option(val)
            await asyncio.sleep(0.3)

            # Validación real como ERP
            actual = await leer_select(page, name)
            return texto_objetivo in actual

    return False

async def asegurar_campos_basicos(page):
    changed = False
    if not await page.locator("select[name='country_origin']").evaluate("el => el.value"):
        changed = True
        await page.select_option("select[name='country_origin']", value="170")

    if not await page.locator("select[name='center']").evaluate("el => el.value"):
        changed = True
        await page.select_option("select[name='center']", value="1")

    if not await page.locator("select[name='zone']").evaluate("el => el.value"):
        changed = True
        await page.select_option("select[name='zone']", value="1")

    if not await page.locator("input[name='phone_primary']").evaluate("el => el.value"):
        changed = True
        await page.fill("input[name='phone_primary']", "1000000000")

    if not await page.locator("#address_field1").evaluate("el => el.value"):
        changed = True
        await page.fill("#address_field1", "1")

    if not await page.locator("input[name='email']").evaluate("el => el.value"):
        changed = True
        await page.fill("input[name='email']", "sincorreo@mtd.net.co")

    return changed

# ================= WORKER =================
async def worker(wid, pool, page, queue, result_q, relogin_lock):
    print(f"[W{wid}] WORKER INICIADO")

    async def _relogin_si_redirigio(log_prefix: str) -> bool:
        # Si SaludGestiona te manda a /signin, no inventes: re-login en la page_login del pool.
        if "signin" not in (page.url or "").lower():
            return False

        print(f"{log_prefix} SESION PERDIDA -> RELOGIN")
        async with relogin_lock:
            await do_login(pool.page_login)

        # Limpia la pestaña del worker antes de reintentar
        await page.goto(URL_PACIENTES)
        await asyncio.sleep(0.6)
        return True


    while True:
        item = await queue.get()
        if item is None:
            await page.goto("about:blank")

            queue.task_done()
            break

        doc, row, fila = item
        obs = ""
        log_prefix = f"[W{wid}][ROW:{fila}][DOC:{doc}]"
        print(f"{log_prefix} INICIO PROCESO")


        # =========================
        # ABRIR PACIENTE (re-login si redirige a /signin)
        # =========================

        opened = False
        for intento in range(2):
            try:
                await abrir_edicion_paciente(
                    page,
                    doc,
                    url_pacientes=URL_PACIENTES
                )
                opened = True
                break

            except PacienteNoEncontrado:
                obs = "PACIENTE_NO_ENCONTRADO"
                break

            except Exception as e:
                # Si SaludGestiona te mandó a /signin, re-login 1 vez y reintenta
                if intento == 0 and await _relogin_si_redirigio(log_prefix):
                    continue
                obs = f"ERROR_ABRIR_EDICION: {type(e).__name__}"
                break

        if not opened:
            await result_q.put((fila, obs))
            await page.goto("about:blank")
            queue.task_done()
            continue

        estado_fosyga = norm(row.get("ESTADO_FOSYGA", "")).replace("_", " ")
        # =========================
        # INIT FLAGS + ERP (SIEMPRE)
        # =========================

        hubo_muerte = False
        hubo_cambios = False

        erp_actual = await leer_erp(page)

        if not erp_actual:
            obs = "ERP_NO_LEIDO"
            await result_q.put((fila, obs))
            await page.goto("about:blank")
            queue.task_done()
            continue

        

        if estado_fosyga == "AFILIADO FALLECIDO":

            fecha_excel = row.get("FECHA_FIN_AFILIACION", "")
            fecha_muerte = normalizar_fecha_excel(fecha_excel)

            try:
                res = await marcar_fallecido(page, fecha_muerte)

                if res != "SKIP":
                    hubo_muerte = True   # <-- ESTA ERA LA FALLA

            except Exception as e:
                obs = f"ERROR_MUERTE:{type(e).__name__}"
                await result_q.put((fila, obs))
                await page.goto("about:blank")
                queue.task_done()
                continue



        codigo_esperado = resolver_codigo_eps(row.get("ENTIDAD", ""))
        # ===== DEBUG ERP =====
        print(f"{log_prefix} ERP ACTUAL = {erp_actual}")
        print(f"{log_prefix} ERP ESPERADO = {codigo_esperado}")

        # =====================

        if codigo_esperado and codigo_esperado != erp_actual:
            if await set_erp(page, row.get("ENTIDAD", "")):
                hubo_cambios = True
                print(f"{log_prefix} ERP CAMBIO OK") 

            else:
                print(f"{log_prefix} ERP CAMBIO FALLÓ")

                obs = "ERROR_ERP_NO_ENCONTRADO"

                await result_q.put((fila, obs))
                await page.goto("about:blank")

                queue.task_done()
                continue
            
        else:
           print(f"{log_prefix} ERP SIN CAMBIO")


        # =========================
        # RESOL 3374
        # =========================
        esp_3374 = esperado_3374(row.get("REGIMEN", ""))
        act_3374 = await leer_select(page, "plan")

        print(f"{log_prefix} 3374 ACTUAL = {act_3374}")
        print(f"{log_prefix} 3374 ESPERADO = {esp_3374}")


        if esp_3374 and esp_3374 not in act_3374:
            if await set_select_por_texto(page, "plan", esp_3374):
                hubo_cambios = True
                erp_actual = codigo_esperado

                print(f"{log_prefix} 3374 CAMBIO OK")

            else:
                print(f"{log_prefix} 3374 CAMBIO FALLÓ")
                obs = "ERROR_3374"

                await result_q.put((fila, obs))
                await page.goto("about:blank")
                queue.task_done()
                continue



        else:
            print(f"{log_prefix} 3374 SIN CAMBIO")


        # =========================
        # SINCRONIZAR DOM TRAS 3374
        # =========================
        if  hubo_cambios or hubo_muerte:

            await asyncio.sleep(0.6)

        # =========================
        # RESOL 2275
        # =========================
        esp_2275 = esperado_2275(row.get("REGIMEN", ""), row.get("TIPO_AFILIADO", ""))
        act_2275 = await leer_select(page, "type")

        print(f"{log_prefix} 2275 ACTUAL = {act_2275}")
        print(f"{log_prefix} 2275 ESPERADO = {esp_2275}")


        if esp_2275 and esp_2275 not in act_2275:
            if await set_select_por_texto(page, "type", esp_2275):
                hubo_cambios = True
                print(f"{log_prefix} 2275 CAMBIO OK")
            else:
                print(f"{log_prefix} 2275 CAMBIO FALLÓ")
                obs = "ERROR_2275"

                await result_q.put((fila, obs))
                await page.goto("about:blank")
                queue.task_done()
                continue

        else:
            print(f"{log_prefix} 2275 SIN CAMBIO")

        
        # =========================
        # OBLIGATORIOS BASE (si están vacíos, se llenan y eso cuenta como cambio)
        # =========================
        basicos_cambiados = await asegurar_campos_basicos(page)
        if basicos_cambiados:
            hubo_cambios = True
            print(f"{log_prefix} OBLIGATORIOS COMPLETADOS")


        # =========================
        # GUARDAR (MUERTE O CAMBIOS)
        # =========================

        if hubo_cambios or hubo_muerte:

            if hubo_muerte:
                ok, motivo = await guardar_fallecido(page)
            else:
                ok, motivo = await guardar_form(page)

            if not ok:
                obs = f"ERROR_GUARDADO:{motivo}"
            else:
                obs = "MUERTE_OK" if hubo_muerte else "OK"


                # Post-validar ERP solo si NO es muerte
                if not hubo_muerte and codigo_esperado:
                    erp_post = await leer_erp(page)

                    if not erp_post:
                        obs = "ERROR_POST_ERP_NO_LEIDO"

                    elif erp_post != codigo_esperado:
                        obs = "ERROR_POST_ERP"



        else:
            obs = "OK"

        await result_q.put((fila, obs))
        await page.goto("about:blank")
        print(f"{log_prefix} FIN PROCESO\n")

        queue.task_done()


print("INICIO SCRIPT")

# ================= MAIN =================
async def main():
    print("ENTRO A MAIN")

    service = sheets_service()
    print("CONECTANDO SHEETS...")

    df = leer_google_sheet(service)
    print("FILAS LEIDAS:", len(df))

    df["_DOC"] = df[COL_DOC].apply(limpiar_doc)

    print("VERIFICANDO COLUMNA OBSERVACIONES...")
    col_obs_num = df.columns.get_loc("OBSERVACIONES") + 1

    print("COLUMNA OBSERVACIONES OK:", col_obs_num)
    col_obs = col_to_a1(col_obs_num)


    queue = asyncio.Queue()
    result_q = asyncio.Queue()

    total_encolados = 0

    for i, row in df.iterrows():

        estado = norm(row.get("ESTADO_FOSYGA", ""))
        obs = norm(row.get("OBSERVACIONES", ""))

        # ===== BLOQUEO SOLO ERRORES TECNICOS =====
        estado_limpio = estado.replace("_", " ")

        if estado_limpio in ("", "TIMEOUT_CONSULTA", "ERROR_FINAL", "NO ENCONTRADO"):
            continue

        # ===== NO reprocesar filas ya trabajadas =====
        if obs:
            continue

        doc = row["_DOC"]
        if not doc:
            continue

        await queue.put((doc, row, i + 2))
        total_encolados += 1


        if i % 100 == 0:
            await asyncio.sleep(0)
    
    print("REGISTROS ENCOLADOS:", total_encolados)

    if total_encolados == 0:
        print("NO HAY REGISTROS PARA PROCESAR — SALIENDO")
        return


    async with async_playwright() as p:
        pool = await create_pool(p, workers=NUM_VENTANAS, headless=False)

        try:
            print("LOGIN OK (POOL) — INICIANDO WORKERS")
            await asyncio.sleep(0)

            writer = asyncio.create_task(writer_obs(result_q, service, col_obs))

            relogin_lock = asyncio.Lock()

            workers = []
            for i, page in enumerate(pool.pages):
                t = asyncio.create_task(worker(i, pool, page, queue, result_q, relogin_lock))
                workers.append(t)

            print("WORKERS ACTIVOS")

            print("ESPERANDO COLA PRINCIPAL...")
            await queue.join()
            print("COLA PRINCIPAL LISTA")

            # Apagar workers
            for _ in workers:
                await queue.put(None)

            await asyncio.gather(*workers)
            print("WORKERS CERRADOS")

            print("ESPERANDO WRITER...")

            # Esperar que writer consuma todo lo pendiente
            await result_q.join()
            # Luego cerrar writer
            await result_q.put(None)
            await writer
            print("WRITER CERRADO")

        finally:
            try:
                await pool.close()
            except Exception:
                pass

async def writer_obs(q, service, col_obs):
    buffer = []
    ultimo_flush = time.time()

    while True:
        item = await q.get()

        if item is None:
            # FLUSH FINAL
            if buffer:
                try:
                    await flush_batch(service, col_obs, buffer)
                except Exception as e:
                    print("[WRITER ERROR][FINAL]", type(e).__name__, e)

                buffer.clear()

            q.task_done()
            break

        fila, texto = item
        buffer.append((fila, texto))

        # ===== FLUSH POR TAMAÑO =====
        if len(buffer) >= BATCH_SIZE:
            try:
                await flush_batch(service, col_obs, buffer)
            except Exception as e:
                print("[WRITER ERROR][BATCH]", type(e).__name__, e)

            buffer.clear()
            ultimo_flush = time.time()

        # ===== FLUSH POR TIEMPO =====
        if time.time() - ultimo_flush > 600 and buffer:
            try:
                await flush_batch(service, col_obs, buffer)
            except Exception as e:
                print("[WRITER ERROR][TIME]", type(e).__name__, e)

            buffer.clear()
            ultimo_flush = time.time()

        q.task_done()

async def flush_batch(service, col_obs, batch):
    print(f"[WRITER] Escribiendo lote de {len(batch)} registros")

    data = []
    for fila, texto in batch:
        data.append({
            "range": f"{SHEET_NAME}!{col_obs}{fila}",
            "values": [[texto]]
        })

    body = {
        "valueInputOption": "RAW",
        "data": data
    }

    def _do():
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body=body
        ).execute()

    await asyncio.to_thread(_do)



if __name__ == "__main__":
    print("SCRIPT ARRANCANDO")

    try:
        asyncio.run(main())
    except TimeoutError as e:
        print(f"ERROR FATAL: TimeoutError {e}")
        raise SystemExit(2)

    except Exception as e:
        print(f"ERROR FATAL: {type(e).__name__} {e}")
        raise SystemExit(2)
