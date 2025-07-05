import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

class APIConfig:
    """
    Configuración para la API OPC UA
    """
    # URL base de la API (puede ser configurada por variable de entorno)
    API_BASE_URL = os.getenv("OPC_API_BASE_URL", "http://localhost:8000")
    
    # Endpoint para obtener variables
    API_ENDPOINT = "/get-variable/{variable_name}"
    
    # Timeout para las peticiones HTTP (en segundos)
    REQUEST_TIMEOUT = int(os.getenv("OPC_API_TIMEOUT", "10"))
    
    # Intervalo entre peticiones para no sobrecargar la API (en segundos)
    REQUEST_DELAY = float(os.getenv("OPC_API_DELAY", "0.1"))
    
    # Intervalo de recolección continua (en segundos)
    COLLECTION_INTERVAL = int(os.getenv("OPC_COLLECTION_INTERVAL", "60"))
    
    # Habilitar logging detallado
    ENABLE_LOGGING = os.getenv("OPC_ENABLE_LOGGING", "true").lower() == "true"
    
    # Configuración de reintentos
    MAX_RETRIES = int(os.getenv("OPC_MAX_RETRIES", "3"))
    RETRY_DELAY = int(os.getenv("OPC_RETRY_DELAY", "5"))

# Instancia global de configuración
api_config = APIConfig()

def get_api_url(variable_name):
    """
    Construye la URL completa para obtener una variable
    """
    return f"{api_config.API_BASE_URL}{api_config.API_ENDPOINT.format(variable_name=variable_name)}"

def print_config():
    """
    Imprime la configuración actual
    """
    print("=== Configuración de la API OPC UA ===")
    print(f"URL Base: {api_config.API_BASE_URL}")
    print(f"Timeout: {api_config.REQUEST_TIMEOUT} segundos")
    print(f"Delay entre peticiones: {api_config.REQUEST_DELAY} segundos")
    print(f"Intervalo de recolección: {api_config.COLLECTION_INTERVAL} segundos")
    print(f"Logging habilitado: {api_config.ENABLE_LOGGING}")
    print(f"Máximo de reintentos: {api_config.MAX_RETRIES}")
    print(f"Delay de reintento: {api_config.RETRY_DELAY} segundos")
    print() 