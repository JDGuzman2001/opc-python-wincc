import json
import os
from database import sql_connection
from pg8000.exceptions import DatabaseError

def read_symbols_from_json(file_path='data.json'):
    """
    Lee los símbolos desde un archivo JSON.
    """
    if not os.path.exists(file_path):
        print(f"Error: El archivo {file_path} no fue encontrado.")
        return None
        
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError:
        print(f"Error: El archivo {file_path} no es un JSON válido.")
        return None
    except Exception as e:
        print(f"Ha ocurrido un error inesperado al leer el archivo: {e}")
        return None

def upload_symbols_to_sql(symbols_data):
    """
    Crea la tabla si no existe y sube los datos de los símbolos a Cloud SQL.
    """
    if not sql_connection or not sql_connection.connection:
        print("Error: No hay conexión a la base de datos.")
        return

    table_name = "chocolatin_variables_history"
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        module VARCHAR(255) NOT NULL,
        address VARCHAR(255) NOT NULL,
        symbol VARCHAR(255),
        data_type VARCHAR(50),
        comment TEXT,
        value TEXT,
        "timestamp" TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    """

    insert_query = f"""
    INSERT INTO {table_name} (module, address, symbol, data_type, comment, value, "timestamp")
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    
    cursor = None
    try:
        cursor = sql_connection.connection.cursor()
        print(f"Verificando y/o creando la tabla '{table_name}'...")
        cursor.execute(create_table_query)
        print("Tabla lista.")

        print("Insertando datos en la base de datos...")
        for module, symbol_list in symbols_data.items():
            for symbol in symbol_list:
                if symbol.get('Symbol'):
                    cursor.execute(insert_query, (
                        module,
                        symbol.get('Address'),
                        symbol.get('Symbol'),
                        symbol.get('Data type'),
                        symbol.get('Comment'),
                        str(symbol.get('value')), # Convertir valor a string
                        symbol.get('timestamp')
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

def main():
    """
    Función principal para cargar símbolos y subirlos a la base de datos.
    """
    symbols = read_symbols_from_json()

    if symbols:
        print("Símbolos cargados correctamente desde el archivo JSON:")
        for module, symbol_list in symbols.items():
            print(f"\nMódulo: {module}")
            for symbol in symbol_list:
                if symbol.get('Symbol'):
                    print(f"  - Símbolo: {symbol['Symbol']}, Dirección: {symbol['Address']}, Tipo: {symbol['Data type']}, Valor: {symbol.get('value')}, Timestamp: {symbol.get('timestamp')}")

        print("\n--- Lectura de datos finalizada ---")
        for module, symbol_list in symbols.items():
            for symbol in symbol_list:
                if symbol.get('Symbol'):
                    print(f"Leyendo valor para el símbolo: {symbol['Symbol']}. Valor actual: {symbol.get('value')}")

        upload_symbols_to_sql(symbols)
if __name__ == "__main__":
    main()
