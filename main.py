import csv
import os
import chardet
from datetime import datetime
from database import sql_connection
from pg8000.exceptions import DatabaseError

def detect_encoding(file_path):
    """
    Detecta la codificación del archivo CSV.
    """
    with open(file_path, 'rb') as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        return result['encoding']

def parse_timestamp(timestamp_str):
    """
    Convierte el timestamp del formato CSV al formato ISO para PostgreSQL.
    """
    try:
        # Parsear el formato "26/06/2025 15:01:52"
        dt = datetime.strptime(timestamp_str, "%d/%m/%Y %H:%M:%S")
        return dt.isoformat()
    except ValueError:
        # Si falla, intentar otros formatos
        try:
            dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            return dt.isoformat()
        except ValueError:
            print(f"Warning: No se pudo parsear el timestamp: {timestamp_str}")
            return timestamp_str

def get_module_for_symbol(symbol_name):
    """
    Asigna un módulo basado en el nombre del símbolo.
    """
    # Mapeo de símbolos a módulos basado en la estructura proporcionada
    symbol_to_module = {
        # DI16xDC24V - Entradas digitales
        'Start': 'DI16xDC24V',
        'Emergencia': 'DI16xDC24V',
        'BandaVI': 'DI16xDC24V',
        'BandaRI': 'DI16xDC24V',
        'BandaMI': 'DI16xDC24V',
        'TarrosListos': 'DI16xDC24V',
        'TarrosVM': 'DI16xDC24V',
        'TarrosRM': 'DI16xDC24V',
        'OnRotator': 'DI16xDC24V',
        
        # DO8xDC24V_2A - Salidas digitales
        'Rotador': 'DO8xDC24V_2A',
        'FinRojoVerde': 'DO8xDC24V_2A',
        'MotorVI': 'DO8xDC24V_2A',
        'MotorRI': 'DO8xDC24V_2A',
        'MotorMI': 'DO8xDC24V_2A',
        'MotorVO': 'DO8xDC24V_2A',
        'MotorRO': 'DO8xDC24V_2A',
        'MotorMO': 'DO8xDC24V_2A',
        
        # AI8x13Bit - Entradas analógicas
        'Repeticiones': 'AI8x13Bit',
        
        # Mapeo para los nombres del CSV
        'MotorVerdesIn': 'DO8xDC24V_2A',  # MotorVI
        'MotorRojosIn': 'DO8xDC24V_2A',   # MotorRI
        'MotorMoradosIn': 'DO8xDC24V_2A', # MotorMI
        'MotorMoradosOut': 'DO8xDC24V_2A', # MotorMO
    }
    
    return symbol_to_module.get(symbol_name, 'CSV_Data')

def get_address_for_symbol(symbol_name):
    """
    Asigna una dirección basada en el nombre del símbolo.
    """
    # Mapeo de símbolos a direcciones basado en la estructura proporcionada
    symbol_to_address = {
        # DI16xDC24V - Entradas digitales
        'Start': 'I 0.0',
        'Emergencia': 'I 0.1',
        'BandaVI': 'I 0.2',
        'BandaRI': 'I 0.3',
        'BandaMI': 'I 0.4',
        'TarrosListos': 'I 1.0',
        'TarrosVM': 'I 1.1',
        'TarrosRM': 'I 1.2',
        'OnRotator': 'I 1.3',
        
        # DO8xDC24V_2A - Salidas digitales
        'Rotador': 'Q 0.0',
        'FinRojoVerde': 'Q 0.1',
        'MotorVI': 'Q 0.2',
        'MotorRI': 'Q 0.3',
        'MotorMI': 'Q 0.4',
        'MotorVO': 'Q 0.5',
        'MotorRO': 'Q 0.6',
        'MotorMO': 'Q 0.7',
        
        # AI8x13Bit - Entradas analógicas
        'Repeticiones': 'MW 512',
        
        # Mapeo para los nombres del CSV
        'MotorVerdesIn': 'Q 0.2',    # MotorVI
        'MotorRojosIn': 'Q 0.3',     # MotorRI
        'MotorMoradosIn': 'Q 0.4',   # MotorMI
        'MotorMoradosOut': 'Q 0.7',  # MotorMO
    }
    
    return symbol_to_address.get(symbol_name, '')

def get_data_type_for_symbol(symbol_name):
    """
    Asigna el tipo de dato basado en el nombre del símbolo.
    """
    if symbol_name == 'Repeticiones' or symbol_name == 'CiclosTerminados':
        return 'WORD'
    else:
        return 'BOOL'

def read_symbols_from_csv(file_path):
    """
    Lee los símbolos desde un archivo CSV.
    """
    if not os.path.exists(file_path):
        print(f"Error: El archivo {file_path} no fue encontrado.")
        return None
        
    try:
        # Detectar la codificación del archivo
        encoding = detect_encoding(file_path)
        print(f"Detectada codificación: {encoding}")
        
        # Si la detección falla, usar una codificación por defecto
        if not encoding:
            encoding = 'latin-1'  # Codificación común para archivos CSV en Windows
        
        symbols_data = {}
        
        with open(file_path, 'r', encoding=encoding, errors='replace') as f:
            csv_reader = csv.reader(f, delimiter=';')
            
            # Leer la primera fila para obtener los nombres de las variables
            headers = next(csv_reader)
            print(f"Headers encontrados: {headers}")
            
            # Crear un mapeo de pares (timestamp, valor) para cada variable
            # Los nombres de variables están en índices pares (0, 2, 4, 6, 8, 10)
            variable_mapping = {}
            for i in range(0, len(headers), 2):
                if i + 1 < len(headers):
                    variable_name = headers[i].strip('"')
                    timestamp_index = i
                    value_index = i + 1
                    variable_mapping[variable_name] = (timestamp_index, value_index)
            
            print(f"Mapeo de variables: {variable_mapping}")
            
            # Procesar cada fila de datos
            for row_num, row in enumerate(csv_reader, 1):
                if len(row) < 2:  # Saltar filas vacías
                    continue
                
                # Procesar cada variable y su par (timestamp, valor)
                for variable_name, (timestamp_index, value_index) in variable_mapping.items():
                    if timestamp_index < len(row) and value_index < len(row):
                        timestamp_str = row[timestamp_index].strip('"')
                        value = row[value_index].strip('"')
                        
                        # Parsear el timestamp
                        parsed_timestamp = parse_timestamp(timestamp_str)
                        
                        # Determinar el módulo y dirección basado en el símbolo
                        module = get_module_for_symbol(variable_name)
                        address = get_address_for_symbol(variable_name)
                        
                        # Crear entrada para la variable
                        symbol_entry = {
                            'Address': address,
                            'Symbol': variable_name,
                            'Data type': get_data_type_for_symbol(variable_name),
                            'Comment': '',
                            'value': value,
                            'timestamp': parsed_timestamp
                        }
                        
                        # Agrupar por módulo
                        if module not in symbols_data:
                            symbols_data[module] = []
                        
                        symbols_data[module].append(symbol_entry)
        
        return symbols_data
        
    except UnicodeDecodeError as e:
        print(f"Error de codificación: {e}")
        print("Intentando con diferentes codificaciones...")
        
        # Intentar con diferentes codificaciones comunes
        encodings_to_try = ['latin-1', 'cp1252', 'iso-8859-1', 'utf-16', 'utf-8-sig']
        
        for enc in encodings_to_try:
            try:
                print(f"Intentando con codificación: {enc}")
                symbols_data = {}
                
                with open(file_path, 'r', encoding=enc, errors='replace') as f:
                    csv_reader = csv.reader(f, delimiter=';')
                    
                    # Leer la primera fila para obtener los nombres de las variables
                    headers = next(csv_reader)
                    print(f"Headers encontrados: {headers}")
                    
                    # Crear un mapeo de pares (timestamp, valor) para cada variable
                    # Los nombres de variables están en índices pares (0, 2, 4, 6, 8, 10)
                    variable_mapping = {}
                    for i in range(0, len(headers), 2):
                        if i + 1 < len(headers):
                            variable_name = headers[i].strip('"')
                            timestamp_index = i
                            value_index = i + 1
                            variable_mapping[variable_name] = (timestamp_index, value_index)
                    
                    print(f"Mapeo de variables: {variable_mapping}")
                    
                    # Procesar cada fila de datos
                    for row_num, row in enumerate(csv_reader, 1):
                        if len(row) < 2:  # Saltar filas vacías
                            continue
                        
                        # Procesar cada variable y su par (timestamp, valor)
                        for variable_name, (timestamp_index, value_index) in variable_mapping.items():
                            if timestamp_index < len(row) and value_index < len(row):
                                timestamp_str = row[timestamp_index].strip('"')
                                value = row[value_index].strip('"')
                                
                                # Parsear el timestamp
                                parsed_timestamp = parse_timestamp(timestamp_str)
                                
                                # Determinar el módulo y dirección basado en el símbolo
                                module = get_module_for_symbol(variable_name)
                                address = get_address_for_symbol(variable_name)
                                
                                # Crear entrada para la variable
                                symbol_entry = {
                                    'Address': address,
                                    'Symbol': variable_name,
                                    'Data type': 'BOOL' if variable_name != 'Repeticiones' else 'WORD',
                                    'Comment': '',
                                    'value': value,
                                    'timestamp': parsed_timestamp
                                }
                                
                                # Agrupar por módulo
                                if module not in symbols_data:
                                    symbols_data[module] = []
                                
                                symbols_data[module].append(symbol_entry)
                
                print(f"Archivo leído exitosamente con codificación: {enc}")
                return symbols_data
                
            except Exception as inner_e:
                print(f"Falló con codificación {enc}: {inner_e}")
                continue
        
        print("No se pudo leer el archivo con ninguna codificación conocida.")
        return None
        
    except Exception as e:
        print(f"Ha ocurrido un error inesperado al leer el archivo CSV: {e}")
        return None
    
def convert_value_to_boolean_or_word(value, symbol):
    if symbol.get('Symbol') != 'Repeticiones' or symbol.get('Symbol') != 'CiclosTerminados':
        return True if value == '1' else False
    else:
        return int(value)

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
                    value = str(symbol.get('value'))
                    cursor.execute(insert_query, (
                        module,
                        symbol.get('Address'),
                        symbol.get('Symbol'),
                        symbol.get('Data type'),
                        symbol.get('Comment'),
                        convert_value_to_boolean_or_word(value, symbol),
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
    symbols = read_symbols_from_csv('C:/Users/Juan/Desktop/Projects/Universidad/automatica-dsc/opc-python-wincc/EXPORT.csv')

    if symbols:
        print("Símbolos cargados correctamente desde el archivo CSV:")
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
