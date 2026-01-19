import os
import pandas as pd
import pyodbc
import json
import uuid
import numpy as np
from datetime import datetime
from django.conf import settings

class ExcelProcessor:
    """
    Clase para manejar la lectura y procesamiento de archivos Excel
    """
    def __init__(self, file_path):
        self.file_path = file_path
        self.excel_file = None
        
    def load_file(self):
        """Carga el archivo Excel en memoria"""
        try:
            self.excel_file = pd.ExcelFile(self.file_path)
            return True
        except Exception as e:
            print(f"Error al cargar el archivo Excel: {str(e)}")
            return False
            
    def get_sheet_names(self):
        """Retorna la lista de nombres de hojas en el Excel"""
        if self.excel_file is None and not self.load_file():
            return []
        
        return self.excel_file.sheet_names
    
    def _clean_dataframe(self, df):
        """
        Limpia el DataFrame: renombra columnas Unnamed y reemplaza valores NaN
        """
        # Limpiar nombres de columnas
        new_columns = []
        unnamed_counter = 1
        
        for col in df.columns:
            col_str = str(col)
            if col_str.startswith('Unnamed'):
                # Renombrar columnas Unnamed con un nombre más descriptivo
                new_name = f'Columna_{unnamed_counter}'
                unnamed_counter += 1
            elif pd.isna(col) or col_str.lower() in ['nan', 'null', '']:
                # Manejar columnas con nombres nulos o vacíos
                new_name = f'Columna_{unnamed_counter}'
                unnamed_counter += 1
            else:
                new_name = col_str
            
            new_columns.append(new_name)
        
        # Aplicar nuevos nombres de columnas
        df.columns = new_columns
        
        # Reemplazar valores NaN, None, y variantes de 'nan'
        df = df.fillna('')  # Reemplazar NaN con cadena vacía
        
        # Reemplazar valores de texto que son 'nan', 'null', etc.
        for col in df.columns:
            if df[col].dtype == 'object':  # Columnas de texto
                df[col] = df[col].astype(str)
                df[col] = df[col].replace({
                    'nan': '',
                    'NaN': '',
                    'null': '',
                    'NULL': '',
                    'None': '',
                    '<NA>': ''
                })
        
        return df
        
    def get_sheet_preview(self, sheet_name, max_rows=10):
        """Obtiene una vista previa de una hoja específica"""
        if self.excel_file is None and not self.load_file():
            return None
            
        try:
            df = pd.read_excel(self.file_path, sheet_name=sheet_name, nrows=max_rows)
            df = self._clean_dataframe(df)  # Limpiar datos
            
            return {
                'columns': list(df.columns),
                'sample_data': df.head(max_rows).values.tolist(),  # Convertir a lista de listas
                'data': df.head(max_rows).to_dict('records'),
                'total_rows': len(pd.read_excel(self.file_path, sheet_name=sheet_name)),
            }
        except Exception as e:
            print(f"Error al leer la hoja {sheet_name}: {str(e)}")
            return None
            
    def get_sheet_columns(self, sheet_name):
        """Obtiene las columnas de una hoja específica con tipos de datos"""
        if self.excel_file is None and not self.load_file():
            return []
            
        try:
            df = pd.read_excel(self.file_path, sheet_name=sheet_name, nrows=10)
            df = self._clean_dataframe(df)  # Limpiar datos
            columns = []
            
            for col in df.columns:
                # Detectar tipo de datos
                dtype_name = df[col].dtype.name
                sql_type = self._map_pandas_type_to_sql(dtype_name)
                
                columns.append({
                    'name': col,
                    'type': dtype_name,
                    'sql_type': sql_type,
                })
                
            return columns
        except Exception as e:
            print(f"Error al obtener columnas de la hoja {sheet_name}: {str(e)}")
            return []
            
    def _map_pandas_type_to_sql(self, pandas_type):
        """Mapea tipos de pandas a tipos SQL comunes"""
        type_mapping = {
            'int64': 'INT',
            'int32': 'INT',
            'float64': 'FLOAT',
            'float32': 'FLOAT',
            'object': 'NVARCHAR(255)',
            'bool': 'BIT',
            'datetime64': 'DATETIME',
        }
        
        return type_mapping.get(pandas_type, 'NVARCHAR(255)')
            
    def read_sheet_data(self, sheet_name, selected_columns=None):
        """Lee todos los datos de una hoja, opcionalmente filtrando columnas"""
        if self.excel_file is None and not self.load_file():
            return None
            
        try:
            if selected_columns:
                df = pd.read_excel(self.file_path, sheet_name=sheet_name, usecols=selected_columns)
            else:
                df = pd.read_excel(self.file_path, sheet_name=sheet_name)
            
            df = self._clean_dataframe(df)  # Limpiar datos
            return df
        except Exception as e:
            print(f"Error al leer datos de la hoja {sheet_name}: {str(e)}")
            return None

class CSVProcessor:
    """
    Clase para manejar la lectura y procesamiento de archivos CSV
    """
    def __init__(self, file_path):
        self.file_path = file_path
    
    def _clean_dataframe(self, df):
        """
        Limpia el DataFrame: renombra columnas Unnamed y reemplaza valores NaN
        """
        # Limpiar nombres de columnas
        new_columns = []
        unnamed_counter = 1
        
        for col in df.columns:
            col_str = str(col)
            if col_str.startswith('Unnamed'):
                # Renombrar columnas Unnamed con un nombre más descriptivo
                new_name = f'Columna_{unnamed_counter}'
                unnamed_counter += 1
            elif pd.isna(col) or col_str.lower() in ['nan', 'null', '']:
                # Manejar columnas con nombres nulos o vacíos
                new_name = f'Columna_{unnamed_counter}'
                unnamed_counter += 1
            else:
                new_name = col_str
            
            new_columns.append(new_name)
        
        # Aplicar nuevos nombres de columnas
        df.columns = new_columns
        
        # Reemplazar valores NaN, None, y variantes de 'nan'
        df = df.fillna('')  # Reemplazar NaN con cadena vacía
        
        # Reemplazar valores de texto que son 'nan', 'null', etc.
        for col in df.columns:
            if df[col].dtype == 'object':  # Columnas de texto
                df[col] = df[col].astype(str)
                df[col] = df[col].replace({
                    'nan': '',
                    'NaN': '',
                    'null': '',
                    'NULL': '',
                    'None': '',
                    '<NA>': ''
                })
        
        return df
        
    def get_preview(self, max_rows=10):
        """Obtiene una vista previa del archivo CSV"""
        try:
            df = pd.read_csv(self.file_path, nrows=max_rows)
            df = self._clean_dataframe(df)  # Limpiar datos
            return {
                'columns': list(df.columns),
                'data': df.head(max_rows).to_dict('records'),
                'total_rows': sum(1 for line in open(self.file_path, 'r')),
            }
        except Exception as e:
            print(f"Error al leer el archivo CSV: {str(e)}")
            return None
            
    def get_columns(self):
        """Obtiene las columnas del CSV con tipos de datos"""
        try:
            df = pd.read_csv(self.file_path, nrows=10)
            df = self._clean_dataframe(df)  # Limpiar datos
            columns = []
            
            for col in df.columns:
                # Detectar tipo de datos
                dtype_name = df[col].dtype.name
                sql_type = self._map_pandas_type_to_sql(dtype_name)
                
                columns.append({
                    'name': col,
                    'type': dtype_name,
                    'sql_type': sql_type,
                })
                
            return columns
        except Exception as e:
            print(f"Error al obtener columnas del CSV: {str(e)}")
            return []
            
    def _map_pandas_type_to_sql(self, pandas_type):
        """Mapea tipos de pandas a tipos SQL comunes"""
        type_mapping = {
            'int64': 'INT',
            'int32': 'INT',
            'float64': 'FLOAT',
            'float32': 'FLOAT',
            'object': 'NVARCHAR(255)',
            'bool': 'BIT',
            'datetime64': 'DATETIME',
        }
        
        return type_mapping.get(pandas_type, 'NVARCHAR(255)')
            
    def read_data(self, selected_columns=None):
        """Lee todos los datos del CSV, opcionalmente filtrando columnas"""
        try:
            if selected_columns:
                df = pd.read_csv(self.file_path, usecols=selected_columns)
            else:
                df = pd.read_csv(self.file_path)
            
            df = self._clean_dataframe(df)  # Limpiar datos
            return df
        except Exception as e:
            print(f"Error al leer datos del CSV: {str(e)}")
            return None

class SQLServerConnector:
    """
    Clase para manejar conexiones y operaciones con SQL Server
    """
    def __init__(self, server, username, password, port=1433, database=None):
        self.server = server
        self.username = username
        self.password = password
        self.port = port
        self.database = database
        self.conn = None
    
    def connect(self, database=None):
        """
        Establece conexión con el servidor SQL Server.
        Si se proporciona una base de datos, se conecta a ella; de lo contrario,
        se conecta al servidor sin especificar una base de datos.
        """
        # Verificar drivers ODBC disponibles
        try:
            available_drivers = pyodbc.drivers()
            print(f"Drivers ODBC disponibles: {available_drivers}")
            
            # Verificar si el driver necesario está disponible
            sql_drivers = [d for d in available_drivers if 'SQL Server' in d]
            print(f"Drivers SQL Server disponibles: {sql_drivers}")
            
            if not sql_drivers:
                print("ERROR: No se encontraron drivers de SQL Server")
                return False
                
        except Exception as e:
            print(f"Error al verificar drivers ODBC: {str(e)}")
            return False
        try:
            # Si se proporciona una base de datos en la llamada, usarla; de lo contrario, usar la del objeto
            db_to_use = database if database is not None else self.database
            
            if db_to_use:
                connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server},{self.port};DATABASE={db_to_use};UID={self.username};PWD={self.password}"
            else:
                # Conectar solo al servidor sin especificar base de datos
                connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server},{self.port};UID={self.username};PWD={self.password}"
            
            print(f"Intentando conectar con: SERVER={self.server},{self.port}, USER={self.username}")
            self.conn = pyodbc.connect(connection_string)
            
            # Actualizar la base de datos actual si la conexión es exitosa y se proporcionó una
            if database is not None:
                self.database = database
                
            print("Conexión exitosa!")
            return True
        except pyodbc.Error as e:
            error_msg = f"Error ODBC al conectar a SQL Server: {str(e)}"
            if len(e.args) > 1:
                error_msg += f" - Código: {e.args[0]}, Mensaje: {e.args[1]}"
            print(error_msg)
            return False
        except Exception as e:
            print(f"Error general al conectar a SQL Server: {str(e)} - Tipo: {type(e).__name__}")
            return False
            
    def get_databases(self):
        """
        Lista todas las bases de datos disponibles en el servidor
        """
        if not self.conn and not self.connect():
            return []
        
        try:
            cursor = self.conn.cursor()
            databases = []
            
            # Consultar las bases de datos del servidor
            cursor.execute("""
                SELECT name 
                FROM sys.databases 
                WHERE database_id > 4  -- Excluir bases del sistema (master, tempdb, model, msdb)
                ORDER BY name
            """)
            
            for row in cursor.fetchall():
                databases.append(row[0])
                
            return databases
        except Exception as e:
            print(f"Error al obtener bases de datos: {str(e)}")
            return []
        finally:
            if self.conn:
                self.disconnect()
                
    def select_database(self, database_name):
        """
        Selecciona una base de datos específica para trabajar con ella
        """
        # Cerrar cualquier conexión existente
        self.disconnect()
        
        # Conectar a la base de datos específica
        success = self.connect(database=database_name)
        
        if success:
            self.database = database_name
            return True
        
        return False
    
    def disconnect(self):
        """Cierra la conexión"""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def test_connection(self):
        """Prueba la conexión y devuelve True si es exitosa"""
        success = self.connect()
        if success:
            self.disconnect()
        return success
    
    def get_tables(self):
        """Obtiene la lista de tablas en la base de datos"""
        if not self.conn and not self.connect():
            return []
        
        try:
            cursor = self.conn.cursor()
            tables = []
            
            # Obtener tablas regulares (no vistas)
            cursor.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE' 
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)
            
            for schema, table in cursor.fetchall():
                # Asegurarse de que schema y table no sean None ni estén vacíos
                schema_safe = schema if schema else 'dbo'
                table_safe = table if table else ''
                
                if table_safe:  # Solo añadir tablas con nombre válido
                    tables.append({
                        'schema': schema_safe,
                        'name': table_safe,
                        'full_name': f"{schema_safe}.{table_safe}"
                    })
                
            # En caso de que no se hayan encontrado tablas
            if not tables:
                print("No se encontraron tablas en la base de datos.")
                
            return tables
        except Exception as e:
            print(f"Error al obtener tablas: {str(e)}")
            return []
        finally:
            if self.conn:
                self.disconnect()
    
    def get_table_columns(self, schema, table):
        """Obtiene las columnas de una tabla específica"""
        if not self.conn and not self.connect():
            return []
        
        try:
            cursor = self.conn.cursor()
            columns = []
            
            # Obtener información de columnas
            cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """, (schema, table))
            
            for name, data_type, max_length, is_nullable in cursor.fetchall():
                type_info = data_type
                if max_length and max_length > 0:
                    type_info = f"{data_type}({max_length})"
                
                columns.append({
                    'name': name,
                    'type': type_info,
                    'nullable': is_nullable == 'YES',
                })
                
            return columns
        except Exception as e:
            print(f"Error al obtener columnas: {str(e)}")
            return []
        finally:
            if self.conn:
                self.disconnect()
    
    def get_table_preview(self, schema, table, max_rows=10):
        """Obtiene una vista previa de una tabla"""
        if not self.conn and not self.connect():
            return None
        
        try:
            # Obtener todas las columnas primero
            columns = self.get_table_columns(schema, table)
            column_names = [col['name'] for col in columns]
            
            cursor = self.conn.cursor()
            
            # Contar filas totales
            cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
            total_rows = cursor.fetchone()[0]
            
            # Obtener muestra de datos
            cursor.execute(f"SELECT TOP {max_rows} * FROM [{schema}].[{table}]")
            
            # Crear diccionario de resultados
            data = []
            for row in cursor.fetchall():
                row_dict = {}
                for i, value in enumerate(row):
                    # Convertir tipos de datos no serializables
                    if isinstance(value, (datetime, bytes, bytearray)):
                        value = str(value)
                    row_dict[column_names[i]] = value
                data.append(row_dict)
            
            return {
                'columns': column_names,
                'data': data,
                'total_rows': total_rows,
            }
        except Exception as e:
            print(f"Error al obtener vista previa: {str(e)}")
            return None
        finally:
            if self.conn:
                self.disconnect()
    
    def read_table_data(self, schema, table, selected_columns=None):
        """Lee datos de una tabla, opcionalmente filtrando columnas"""
        if not self.conn and not self.connect():
            return None
        
        try:
            cursor = self.conn.cursor()
            
            if selected_columns:
                columns_str = ', '.join([f"[{col}]" for col in selected_columns])
                cursor.execute(f"SELECT {columns_str} FROM [{schema}].[{table}]")
            else:
                cursor.execute(f"SELECT * FROM [{schema}].[{table}]")
            
            # Obtener nombres de columnas
            column_names = [column[0] for column in cursor.description]
            
            # Leer todas las filas en un DataFrame
            rows = cursor.fetchall()
            
            # Crear DataFrame desde los resultados
            df = pd.DataFrame.from_records(rows, columns=column_names)
            
            return df
        except Exception as e:
            print(f"Error al leer datos de la tabla: {str(e)}")
            return None
        finally:
            if self.conn:
                self.disconnect()

class TargetDBManager:
    """
    Clase para gestionar operaciones en la base de datos de destino
    """
    def __init__(self, target_db=None):
        """Inicializa el gestor de base de datos destino"""
        self.target_db = target_db or 'destino'  # Usar la BD DestinoAutomatizacion por defecto si no se especifica
    
    def create_table_if_not_exists(self, table_name, column_definitions):
        """
        Crea una tabla en la base de datos destino si no existe
        - table_name: nombre de la tabla a crear
        - column_definitions: lista de diccionarios con 'name', 'type', 'nullable'
        """
        # Aquí se implementaría la lógica para crear tablas en la BD destino
        pass
    
    def insert_data(self, table_name, dataframe):
        """
        Inserta datos de un DataFrame en una tabla destino
        - table_name: nombre de la tabla donde insertar datos
        - dataframe: pandas DataFrame con los datos a insertar
        """
        # Aquí se implementaría la lógica para insertar datos en la BD destino
        pass
    
    def truncate_table(self, table_name):
        """Vacía una tabla antes de insertar nuevos datos"""
        # Aquí se implementaría la lógica para truncar tablas
        pass
