import logging
import os
from datetime import datetime

# Crear carpeta de logs si no existe
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Nombre del archivo basado en el mes actual (ej. log_2024_05.log)
mes_actual = datetime.now().strftime("%Y_%m")
log_file = os.path.join(log_dir, f"robot_{mes_actual}.log")

# Configuración básica del logger
logging.basicConfig(
    level=logging.INFO, # Solo guardar INFO, WARNING y ERROR
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler() # También lo muestra en la consola
    ]
)

def get_logger(nombre_modulo):
    return logging.getLogger(nombre_modulo)