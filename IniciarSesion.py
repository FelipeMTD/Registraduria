import asyncio
import os
import random
from dataclasses import dataclass
from typing import List, Optional

import os
from dotenv import load_dotenv

load_dotenv()  # <-- ESTA LÍNEA ES LA CLAVE

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


# =====================================================
# CONFIG SALUDGESTIONA
# =====================================================
URL_LOGIN = "https://saludgestiona.com/business/signin/MTAwNQ=="
URL_PACIENTES = "https://saludgestiona.com/business/patients-list"

# Si no existen env vars, usa los hardcode (como tu script original).
SG_USER = os.getenv("SG_USER")
SG_PASS = os.getenv("SG_PASS")

# Concurrencia realista en portal web (no te pases).
WORKERS = int(os.getenv("SG_WORKERS", "4"))

# Timeouts y reintentos
NAV_TIMEOUT_MS = 30000
LOGIN_TIMEOUT_MS = 90000
HEALTHCHECK_RETRIES = 2
RELOGIN_RETRIES = 2

# Pausas humanas (mínimas, sin pendejadas)
TYPE_DELAY_MIN = 70
TYPE_DELAY_MAX = 140
PAUSE_MIN = 0.2
PAUSE_MAX = 0.6


# =====================================================
# HELPERS
# =====================================================
async def human_type(page, selector: str, text: str, timeout_ms: int = 15000) -> None:
    inp = await page.wait_for_selector(selector, timeout=timeout_ms)
    await inp.click()
    # limpia antes de escribir
    await inp.fill("")
    for ch in text:
        await inp.type(ch, delay=random.uniform(TYPE_DELAY_MIN, TYPE_DELAY_MAX))
    await asyncio.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))


async def safe_goto(page, url: str, timeout_ms: int = NAV_TIMEOUT_MS):
    for intento in range(3):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            return
        except Exception as e:
            if intento >= 2:
                raise
            await asyncio.sleep(1)


async def is_on_login(page) -> bool:
    # Señales típicas de login: inputs document/password o url de signin.
    url = page.url.lower()
    if "signin" in url:
        return True
    try:
        # Si existen esos inputs, estás en login (o te redirigieron).
        await page.wait_for_selector("input[name='document']", timeout=1500)
        await page.wait_for_selector("input[name='password']", timeout=1500)
        return True
    except PlaywrightTimeoutError:
        return False


async def assert_logged_in_by_patients(page) -> None:
    await safe_goto(page, URL_PACIENTES)

    # deja estabilizar SPA interna
    await asyncio.sleep(1)

    if await is_on_login(page):
        raise RuntimeError("Sesión inválida: redirigido a login")

    try:
        await page.wait_for_selector(".select2-selection--single", timeout=12000)
    except PlaywrightTimeoutError:
        raise RuntimeError("patients-list cargó sin UI esperada")


# =====================================================
# SESIÓN + POOL
# =====================================================
@dataclass
class GestionaPool:
    browser: object
    context: object
    page_login: object
    pages: List[object]

    async def close(self):
        # Cierra limpio.
        try:
            for p in self.pages:
                try:
                    await p.close()
                except Exception:
                    pass
            try:
                await self.page_login.close()
            except Exception:
                pass
            await self.context.close()
        finally:
            await self.browser.close()


async def do_login(page) -> None:
    await safe_goto(page, URL_LOGIN, timeout_ms=LOGIN_TIMEOUT_MS)

    # Si por alguna razón ya estás logueado y te manda directo, igual validamos luego.
    if not await is_on_login(page):
        return

    await human_type(page, "input[name='document']", SG_USER, timeout_ms=15000)
    await human_type(page, "input[name='password']", SG_PASS, timeout_ms=15000)
    await asyncio.sleep(4)

    btn_selector = "button[type='submit']"
    await page.wait_for_selector(btn_selector, state="visible", timeout=LOGIN_TIMEOUT_MS)
    await asyncio.sleep(2.5)  # si quieres pausa humana, deja esto

    async with page.expect_navigation(wait_until="domcontentloaded", timeout=LOGIN_TIMEOUT_MS):
        await page.click(btn_selector, timeout=LOGIN_TIMEOUT_MS)


    await asyncio.sleep(1.5)
    await assert_logged_in_by_patients(page)

async def ensure_logged_in(pool: GestionaPool) -> None:
    """
    Si la sesión se murió, re-login en page_login y luego revalida todas las pestañas.
    """
    for intento in range(RELOGIN_RETRIES + 1):
        try:
            await do_login(pool.page_login)
            # Revalida pestañas (por si alguna quedó en login).
            for p in pool.pages:
                await assert_logged_in_by_patients(p)
            return
        except Exception as e:
            if intento >= RELOGIN_RETRIES:
                raise
            # reset suave
            try:
                await safe_goto(pool.page_login, "about:blank")
            except Exception:
                pass
            await asyncio.sleep(1.5)


async def create_pool(playwright, workers: int = WORKERS, headless: bool = False) -> GestionaPool:
    browser = await playwright.chromium.launch(headless=headless)
    context = await browser.new_context()

    # 1) page de login (única)
    page_login = await context.new_page()
    await do_login(page_login)

    # 2) pool de páginas ya autenticadas
    pages = [await context.new_page() for _ in range(workers)]

    # 3) healthcheck: asegurar que todas pueden abrir patients-list
    for p in pages:
        ok = False
        last_err: Optional[Exception] = None
        for _ in range(HEALTHCHECK_RETRIES + 1):
            try:
                await assert_logged_in_by_patients(p)
                ok = True
                break
            except Exception as e:
                last_err = e
                await asyncio.sleep(1)
        if not ok:
            raise RuntimeError(f"Healthcheck falló en una pestaña: {last_err}")

    return GestionaPool(browser=browser, context=context, page_login=page_login, pages=pages)


# =====================================================
# DEMO / PRUEBA: login único + N pestañas válidas
# =====================================================
async def main():
    async with async_playwright() as p:
        pool = await create_pool(p, workers=WORKERS, headless=False)
        print(f"[OK] Login único listo. Pestañas activas: {len(pool.pages)}")

        # Prueba mínima: cada pestaña abre patients-list (sin hacer updates).
        async def ping(i: int, page):
            try:
                await assert_logged_in_by_patients(page)
                print(f"[OK] Page {i}: patients-list accesible")
            except Exception as e:
                print(f"[FAIL] Page {i}: {e}")

        await asyncio.gather(*[ping(i, pg) for i, pg in enumerate(pool.pages, start=1)])

        # Mantén abierto para que veas que sí quedó sesión estable.
        print("[INFO] Pool listo. Cierra manualmente cuando quieras (CTRL+C).")
        try:
            while True:
                await asyncio.sleep(5)
        except KeyboardInterrupt:
            pass
        finally:
            await pool.close()
            print("[OK] Cerrado limpio.")


if __name__ == "__main__":
    asyncio.run(main())
