import asyncio
from datetime import date
from playwright.async_api import TimeoutError, Page

# =====================================================
# COMMON UI FLOW: Buscar paciente -> Abrir edición
# =====================================================

class PacienteNoEncontrado(RuntimeError):
    pass


async def abrir_edicion_paciente(
    page: Page,
    documento: str,
    *,
    url_pacientes: str,
    delay_type_ms: int = 60,
    pausa_post_type_s: float = 0.4,
    timeout_list_ms: int = 10000,
    timeout_input_ms: int = 5000,
    timeout_edit_ms: int = 15000,
    form_selector: str = "#form_update_patient",
) -> None:
    """Abre la página de edición del paciente buscándolo por DOCUMENTO en patients-list.
    Deja el browser en /edit-patient/... y con el form visible.

    Lanza PacienteNoEncontrado si no hay resultados.
    """

    # Ir al listado
    await page.goto(url_pacientes, wait_until="domcontentloaded")

    # Abrir select2
    await page.wait_for_selector(".select2-selection--single", timeout=timeout_list_ms)
    await page.click(".select2-selection--single")

    inp = await page.wait_for_selector("input.select2-search__field", timeout=timeout_input_ms)

    # Buscar documento
    await inp.fill("")
    await inp.type(documento, delay=delay_type_ms)
    await asyncio.sleep(pausa_post_type_s)
    await inp.press("Enter")

    # Esperar botón editar (resultado)
    try:
        await page.wait_for_selector("a[href^='edit-patient/']", timeout=timeout_list_ms)
    except TimeoutError as e:
        raise PacienteNoEncontrado("Paciente no encontrado") from e

    edit = page.locator("a[href^='edit-patient/'][title^='Editar ']").first
    await edit.wait_for(timeout=timeout_edit_ms)
    await edit.click()

    # Form en edición
    await page.wait_for_selector(form_selector, timeout=timeout_edit_ms)


# =====================================================
# REGISTRADURÍA: Acciones en el formulario de edición
# =====================================================

async def ya_muerto_inactivo(page: Page) -> bool:
    alive = page.locator("input[name='alive']")
    status = page.locator("input[name='status']")
    death = page.locator("#death_date")

    alive_checked = await alive.is_checked()
    status_checked = await status.is_checked()
    fecha = (await death.evaluate("el => el.value")).strip()

    return (not alive_checked) and (not status_checked) and bool(fecha)


async def marcar_fallecido(page: Page, fecha_muerte: str | None = None) -> str:
    """Marca paciente como fallecido + inactivo, setea fecha muerte = hoy.
    Retorna:
      - 'SKIP' si ya está muerto e inactivo (idempotencia)
      - 'DO' si aplicó cambios
    """

    if await ya_muerto_inactivo(page):
        return "SKIP"

    # Obligatorios base (idéntico a tu script)
    if not await page.locator("select[name='country_origin']").evaluate("el => el.value"):
        await page.select_option("select[name='country_origin']", value="170")

    if not await page.locator("select[name='center']").evaluate("el => el.value"):
        await page.select_option("select[name='center']", value="1")

    if not await page.locator("select[name='zone']").evaluate("el => el.value"):
        await page.select_option("select[name='zone']", value="1")

    if not await page.locator("input[name='phone_primary']").evaluate("el => el.value"):
        await page.fill("input[name='phone_primary']", "1000000000")

    if not await page.locator("#address_field1").evaluate("el => el.value"):
        await page.fill("#address_field1", "1")

    if not await page.locator("input[name='email']").evaluate("el => el.value"):
        await page.fill("input[name='email']", "sincorreo@mtd.net.co")

    # Activar fallecido (toggle alive)
    await page.evaluate(
        """
        () => {
            const chk = document.querySelector("input[name='alive']");
            if (chk && chk.checked) chk.click();
        }
        """
    )
    await page.wait_for_timeout(300)

    # Fecha muerte = hoy (disparando eventos)
    if fecha_muerte:
        fecha_final = fecha_muerte
    else:
        fecha_final = date.today().strftime("%Y-%m-%d")
    await page.evaluate(
        f"""
        () => {{
            const d = document.querySelector('#death_date');
            if (!d) return;
            d.value = '{fecha_final}';
            d.dispatchEvent(new Event('input', {{ bubbles: true }}));
            d.dispatchEvent(new Event('change', {{ bubbles: true }}));
        }}
        """
    )

    # Status off
    await page.evaluate(
        """
        () => {
            const st = document.querySelector("input[name='status']");
            if (st && st.checked) st.click();
        }
        """
    )

    return "DO"


async def guardar(page: Page) -> tuple[bool, str]:
    """Guarda el form y valida confirmación por SweetAlert y persistencia de death_date."""

    form = page.locator("#form_update_patient")
    await form.wait_for(state="visible", timeout=10000)

    # submit REAL
    await page.evaluate(
        """
        () => {
            const f = document.querySelector('#form_update_patient');
            if (!f) throw 'FORM_NOT_FOUND';
            f.requestSubmit();
        }
        """
    )

    await page.wait_for_timeout(200)

    popup = page.locator("div.swal2-popup")

    try:
        await popup.wait_for(state="visible", timeout=18000)
        texto = (await popup.inner_text()).lower().replace("\n", " ").strip()

        if "modificó correctamente" in texto:
            btn = page.locator("button.swal2-confirm").first
            if await btn.count():
                try:
                    await btn.click()
                except Exception:
                    pass

            # Confirmación backend: death_date persistió
            await page.wait_for_timeout(300)
            death_val = await page.locator("#death_date").evaluate("el => el.value")
            if not death_val:
                return False, "BACKEND_NO_PERSISTIO_FECHA"
            return True, "OK"

        return False, texto.strip()

    except TimeoutError:
        pass

    # Fallback errores form
    errores = page.locator(
        ".alert-danger, .toast-error, "
        ".is-invalid, .invalid-feedback, .text-danger, small.error"
    )

    msgs: list[str] = []
    n = await errores.count()
    for i in range(n):
        t = (await errores.nth(i).inner_text()).strip()
        if t:
            msgs.append(t)

    if msgs:
        # dedupe estable
        seen = set()
        dedup = []
        for m in msgs:
            if m not in seen:
                dedup.append(m)
                seen.add(m)
        return False, " | ".join(dedup)

    return False, "NO_GUARDO_SIN_RESPUESTA"


async def guardar_generico(page: Page) -> tuple[bool, str]:
    """Guarda el form y valida confirmación por SweetAlert.

    NOTA: Esto es genérico (no valida campos específicos como #death_date).
    Úsalo en flujos tipo FOSYGA (ERP/3374/2275) donde death_date normalmente está vacío.
    """

    form = page.locator("#form_update_patient")
    await form.wait_for(state="visible", timeout=10000)

    # submit REAL
    await page.evaluate(
        """
        () => {
            const f = document.querySelector('#form_update_patient');
            if (!f) throw 'FORM_NOT_FOUND';
            f.requestSubmit();
        }
        """
    )

    await page.wait_for_timeout(200)

    popup = page.locator("div.swal2-popup")

    try:
        await popup.wait_for(state="visible", timeout=18000)
        texto = (await popup.inner_text()).lower().replace("\n", " ").strip()

        if "modificó correctamente" in texto:
            btn = page.locator("button.swal2-confirm").first
            if await btn.count():
                try:
                    await btn.click()
                except Exception:
                    pass
            return True, "OK"

        return False, texto.strip()

    except TimeoutError:
        pass

    # Fallback errores form
    errores = page.locator(
        ".alert-danger, .toast-error, "
        ".is-invalid, .invalid-feedback, .text-danger, small.error"
    )

    msgs: list[str] = []
    n = await errores.count()
    for i in range(n):
        t = (await errores.nth(i).inner_text()).strip()
        if t:
            msgs.append(t)

    if msgs:
        seen = set()
        dedup = []
        for m in msgs:
            if m not in seen:
                dedup.append(m)
                seen.add(m)
        return False, " | ".join(dedup)

    return False, "NO_GUARDO_SIN_RESPUESTA"



async def procesar_muerte_registraduria(page: Page, documento: str, *, url_pacientes: str) -> str:
    """Orquesta: buscar->abrir edición->marcar fallecido->guardar (si aplica)."""
    await abrir_edicion_paciente(page, documento, url_pacientes=url_pacientes)
    res = await marcar_fallecido(page)
    if res == "SKIP":
        return "SKIP"
    ok, msg = await guardar(page)
    if not ok:
        raise RuntimeError(msg)
    return "OK"
