import requests
import time
from datetime import datetime
from database import sql_connection
from pg8000.exceptions import DatabaseError
import json
from api_config import api_config, get_api_url

# Definición de los símbolos con sus tipos de datos
SYMBOLS_CONFIG = [
    {"symbol": "Inicio", "address": "%I1.0", "data_type": "Bool"},
    {"symbol": "Tolva1", "address": "%MD1", "data_type": "Real"},
    {"symbol": "molido", "address": "%I2.0", "data_type": "Bool"},
    {"symbol": "tolvaintermediacafe", "address": "%MD28", "data_type": "Real"},
    {"symbol": "total molido", "address": "%MD5", "data_type": "Real"},
    {"symbol": "tolva2", "address": "%MD24", "data_type": "Real"},
    {"symbol": "tolvaempaque", "address": "%MD12", "data_type": "Real"},
    {"symbol": "tipodecafe", "address": "%MW15", "data_type": "Int"},
    {"symbol": "tipodebaso", "address": "%MW34", "data_type": "Int"},
    {"symbol": "cafefinal", "address": "%MD62", "data_type": "Real"},
    {"symbol": "aguafinal", "address": "%MD66", "data_type": "Real"},
    {"symbol": "lechefinal", "address": "%MD70", "data_type": "Real"},
    {"symbol": "iniciocafe", "address": "%I74.1", "data_type": "Bool"},
    {"symbol": "botoniniciocafe", "address": "%I82.0", "data_type": "Bool"},
    {"symbol": "americano", "address": "%I82.1", "data_type": "Bool"},
    {"symbol": "cafe", "address": "%I82.2", "data_type": "Bool"},
    {"symbol": "tinto", "address": "%I82.3", "data_type": "Bool"},
    {"symbol": "100ml", "address": "%I82.4", "data_type": "Bool"},
    {"symbol": "150ml", "address": "%I82.5", "data_type": "Bool"},
    {"symbol": "250ml", "address": "%I82.6", "data_type": "Bool"},
    {"symbol": "sacarvaso", "address": "%I82.7", "data_type": "Bool"}
]

def get_variable_from_api(variable_name):
    """
    Obtiene el valor de una variable desde la API OPC UA
    """
    try:
        url = get_api_url(variable_name)
        response = requests.get(url, timeout=api_config.REQUEST_TIMEOUT)
        
        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                return {
                    "success": True,
                    "value": data.get("value"),
                    "data_type": data.get("data_type")
                }
            else:
                print(f"Error en la respuesta de la API para {variable_name}: {data}")
                return {"success": False, "error": "API response error"}
        else:
            print(f"Error HTTP {response.status_code} para {variable_name}: {response.text}")
            return {"success": False, "error": f"HTTP {response.status_code}"}
            
    except requests.exceptions.RequestException as e:
        print(f"Error de conexión para {variable_name}: {e}")
        return {"success": False, "error": str(e)}
    except Exception as e:
        print(f"Error inesperado para {variable_name}: {e}")
        return {"success": False, "error": str(e)}

def convert_value_to_appropriate_type(value, data_type):
    """
    Convierte el valor al tipo de dato apropiado
    """
    if value is None:
        return None
    
    try:
        if data_type == "Bool":
            return bool(value)
        elif data_type == "Byte":
            return int(value)
        elif data_type == "Int":
            return int(value)
        elif data_type == "Real":
            return float(value)
        else:
            return str(value)
    except (ValueError, TypeError):
        return str(value)

def upload_symbols_to_sql(symbols_data):
    """
    Sube los datos de los símbolos a la base de datos SQL
    """
    if not sql_connection or not sql_connection.connection:
        print("Error: No hay conexión a la base de datos.")
        return

    table_name = "chocolatin_variables_history"
    
    insert_query = f"""
    INSERT INTO {table_name} (module, address, symbol, data_type, comment, value, "timestamp")
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    
    cursor = None
    try:
        cursor = sql_connection.connection.cursor()
        
        print("Insertando datos en la base de datos...")
        for symbol_data in symbols_data:
            if symbol_data.get("success") and symbol_data.get("value") is not None:
                # Determinar el módulo basado en la dirección
                address = symbol_data.get("address", "")
                if address.startswith("%I"):
                    module = "Digital_Inputs"
                elif address.startswith("%Q"):
                    module = "Digital_Outputs"
                elif address.startswith("%M"):
                    module = "Memory_Bits"
                elif address.startswith("%MD"):
                    module = "Memory_Double"
                elif address.startswith("%MW"):
                    module = "Memory_Word"
                elif address.startswith("%MB"):
                    module = "Memory_Byte"
                else:
                    module = "OPC_UA_Data"
                
                # Convertir el valor al tipo apropiado
                converted_value = convert_value_to_appropriate_type(
                    symbol_data.get("value"), 
                    symbol_data.get("data_type")
                )
                
                cursor.execute(insert_query, (
                    module,
                    symbol_data.get("address"),
                    symbol_data.get("symbol"),
                    symbol_data.get("data_type"),
                    "",  # Comment vacío
                    str(converted_value) if converted_value is not None else "",
                    symbol_data.get("timestamp")
                ))
        
        sql_connection.connection.commit()
        print("¡Datos insertados correctamente en Cloud SQL!")

    except DatabaseError as e:
        print(f"Error de base de datos: {e}")
        if sql_connection.connection:
            sql_connection.connection.rollback()
    except Exception as e:
        print(f"Ha ocurrido un error inesperado: {e}")
        if sql_connection.connection:
            sql_connection.connection.rollback()
    finally:
        if cursor:
            cursor.close()

def collect_all_variables():
    """
    Recolecta todos los valores de las variables desde la API
    """
    print("Iniciando recolección de datos desde la API OPC UA...")
    
    symbols_data = []
    current_timestamp = datetime.now().isoformat()
    
    for symbol_config in SYMBOLS_CONFIG:
        symbol_name = symbol_config["symbol"]
        print(f"Obteniendo valor para: {symbol_name}")
        
        # Obtener el valor desde la API
        api_result = get_variable_from_api(symbol_name)
        
        if api_result["success"]:
            symbol_data = {
                "success": True,
                "symbol": symbol_name,
                "address": symbol_config["address"],
                "data_type": symbol_config["data_type"],
                "value": api_result["value"],
                "timestamp": current_timestamp
            }
            symbols_data.append(symbol_data)
            print(f"  ✓ {symbol_name}: {api_result['value']} ({api_result['data_type']})")
        else:
            print(f"  ✗ {symbol_name}: Error - {api_result.get('error', 'Unknown error')}")
            # Agregar entrada con valor vacío para mantener consistencia
            symbol_data = {
                "success": False,
                "symbol": symbol_name,
                "address": symbol_config["address"],
                "data_type": symbol_config["data_type"],
                "value": None,
                "timestamp": current_timestamp
            }
            symbols_data.append(symbol_data)
        
        # Pequeña pausa para no sobrecargar la API
        time.sleep(api_config.REQUEST_DELAY)
    
    return symbols_data

def main():
    """
    Función principal para recolectar datos desde la API y subirlos a la base de datos
    Se ejecuta cada 30 segundos de forma continua
    """
    print("=== Recolector de Datos OPC UA ===")
    print(f"URL de la API: {api_config.API_BASE_URL}")
    print(f"Total de símbolos a consultar: {len(SYMBOLS_CONFIG)}")
    print("El programa se ejecutará cada 30 segundos...")
    print("Presiona Ctrl+C para detener el programa.")
    print()
    
    try:
        while True:
            print(f"\n{'='*50}")
            print(f"Ejecutando recolección de datos - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*50}")
            
            # Recolectar todos los valores
            symbols_data = collect_all_variables()
            
            # Mostrar resumen
            successful_collections = sum(1 for s in symbols_data if s["success"])
            print(f"\nResumen:")
            print(f"  - Total de símbolos: {len(symbols_data)}")
            print(f"  - Consultas exitosas: {successful_collections}")
            print(f"  - Consultas fallidas: {len(symbols_data) - successful_collections}")
            
            # Subir a la base de datos
            if symbols_data:
                print("\nSubiendo datos a la base de datos...")
                upload_symbols_to_sql(symbols_data)
                print("Proceso completado.")
            else:
                print("No hay datos para subir a la base de datos.")
            
            print(f"\nEsperando 30 segundos para la próxima ejecución...")
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\n\nPrograma detenido por el usuario.")
        print("¡Hasta luego!")

if __name__ == "__main__":
    main() 