import asyncio
import os
import random
from dataclasses import dataclass
from typing import List, Optional
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# 1. Cargar variables de entorno
load_dotenv()

# 2. DEFINICIÓN DE VARIABLES GLOBALES (Debe ir antes de las funciones)
URL_LOGIN = "https://saludgestiona.com/business/signin/MTAwNQ=="
URL_PACIENTES = "https://saludgestiona.com/business/patients-list"

SG_USER = os.getenv("SG_USER")
SG_PASS = os.getenv("SG_PASS")

# Definimos WORKERS aquí para que create_pool lo reconozca
WORKERS = int(os.getenv("SG_WORKERS", "4"))

# Configuraciones de tiempo
NAV_TIMEOUT_MS = 30000
LOGIN_TIMEOUT_MS = 90000

@dataclass
class GestionaPool:
    browser: object
    context: object
    page_login: object
    pages: List[object]

    async def close(self):
        try:
            for p in self.pages:
                try: await p.close()
                except: pass
            await self.context.close()
        finally:
            await self.browser.close()

# --- FUNCIONES DE APOYO ---

async def human_type(page, selector: str, text: str) -> None:
    inp = await page.wait_for_selector(selector, timeout=15000)
    await inp.click()
    await inp.fill("")
    for ch in text:
        await inp.type(ch, delay=random.uniform(70, 140))

# Modifica la función do_login en IniciarSesion.py
async def do_login(page) -> None:
    print(f"Buscando página de inicio de sesión...")
    # Cambiamos wait_until a 'load' y luego forzamos 'networkidle'
    await page.goto(URL_LOGIN, wait_until="load", timeout=60000)
    await page.wait_for_load_state("networkidle") 
    
    # Si ya estamos dentro, no hacemos nada
    if "patients-list" in page.url:
        return

    # Esperamos explícitamente a que el input sea visible y estable
    await page.wait_for_selector("input[name='document']", state="visible", timeout=15000)
    
    print(f"Ingresando credenciales para: {SG_USER}")
    await page.fill("input[name='document']", SG_USER)
    await page.fill("input[name='password']", SG_PASS)
    
    await asyncio.sleep(1) # Pausa táctica

    print("Haciendo clic en ingresar...")
    await asyncio.gather(
        page.click("button[type='submit']"),
        page.wait_for_url("**/patients-list", timeout=60000)
    )
# --- FUNCIÓN PRINCIPAL DEL POOL (CORREGIDA) ---

async def create_pool(playwright, workers: int = WORKERS, headless: bool = False) -> GestionaPool:
    """Crea un grupo de navegadores autenticados con modo sigiloso."""
    
    # Lanzar navegador con argumentos anti-detección
    browser = await playwright.chromium.launch(
        headless=headless,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-setuid-sandbox"
        ]
    )
    
    # Crear contexto simulando un usuario real
    context = await browser.new_context(
        viewport={'width': 1366, 'height': 768},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    )
    
    # Ocultar rastro de automatización en JS
    await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    # 1) Realizar Login
    page_login = await context.new_page()
    await do_login(page_login)

    # 2) Crear las demás pestañas que heredarán la sesión
    pages = []
    for _ in range(workers):
        p = await context.new_page()
        await p.goto(URL_PACIENTES, wait_until="domcontentloaded")
        pages.append(p)

    return GestionaPool(browser=browser, context=context, page_login=page_login, pages=pages)