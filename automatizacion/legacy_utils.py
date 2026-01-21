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
    Soporta archivos locales y desde OneDrive
    """
    def __init__(self, file_path, source=None, is_cloud=False, cloud_url=None):
        """
        Inicializar ExcelProcessor
        
        Args:
            file_path: Ruta del archivo local (si es local)
            source: Objeto DataSource (opcional)
            is_cloud: Boolean indicando si es archivo de nube
            cloud_url: URL del archivo en OneDrive (si es cloud)
        """
        self.file_path = file_path
        self.source = source
        self.is_cloud = is_cloud or (source and source.is_cloud())
        self.cloud_url = cloud_url or (source and source.onedrive_url)
        self.excel_file = None
        
        # 🔧 DEBUG: Verificar que tenemos datos válidos
        if self.is_cloud and not self.cloud_url:
            print(f"⚠️ ADVERTENCIA: Detectado como cloud pero sin URL. source.onedrive_url = {source.onedrive_url if source else 'N/A'}")
        if not self.is_cloud and not self.file_path:
            print(f"⚠️ ADVERTENCIA: Detectado como local pero sin file_path. source.file_path = {source.file_path if source else 'N/A'}")
        
    def load_file(self):
        """Carga el archivo Excel en memoria"""
        try:
            if self.is_cloud:
                if not self.cloud_url:
                    print(f"❌ Error: Detectado como cloud pero cloud_url está vacío")
                    print(f"   source.onedrive_url = {self.source.onedrive_url if self.source else 'source es None'}")
                    print(f"   source.storage_type = {self.source.storage_type if self.source else 'source es None'}")
                    return False
                # Descargar desde OneDrive
                return self._load_from_cloud()
            else:
                # Cargar desde archivo local
                if not self.file_path:
                    print(f"❌ Error: Detectado como local pero file_path está vacío")
                    print(f"   source.file_path = {self.source.file_path if self.source else 'source es None'}")
                    return False
                self.excel_file = pd.ExcelFile(self.file_path)
                return True
        except Exception as e:
            print(f"Error al cargar el archivo Excel: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def _load_from_cloud(self):
        """
        Carga archivo desde OneDrive
        
        Returns:
            bool: True si carga exitosamente, False si falla
        """
        try:
            from .onedrive_service import get_onedrive_service
            
            print(f"📥 Descargando archivo de OneDrive: {self.cloud_url}")
            service = get_onedrive_service()
            
            # Descargar archivo como BytesIO
            file_content = service.download_file_from_url(self.cloud_url)
            
            # Cargar Excel desde el contenido en memoria
            self.excel_file = pd.ExcelFile(file_content)
            
            print("✅ Archivo de OneDrive cargado en memoria")
            return True
            
        except Exception as e:
            print(f"❌ Error cargando archivo de OneDrive: {str(e)}")
            return False
            
    def get_sheet_names(self):
        """Retorna la lista de nombres de hojas en el Excel"""
        if self.excel_file is None and not self.load_file():
            return []
        
        return self.excel_file.sheet_names
    
    def _clean_dataframe(self, df):
        """
        Limpia el DataFrame: renombra columnas Unnamed y reemplaza valores NaN/NaT
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
        
        # 🔧 NUEVO: Convertir columnas de fecha a string antes de fillna
        # para evitar que NaT se muestre como texto
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                # Convertir fechas a string, reemplazando NaT con vacío
                df[col] = df[col].apply(lambda x: '' if pd.isna(x) else x.strftime('%Y-%m-%d %H:%M:%S') if hasattr(x, 'strftime') else str(x))
        
        # Reemplazar valores NaN, None, y variantes de 'nan'
        df = df.fillna('')  # Reemplazar NaN con cadena vacía
        
        # Reemplazar valores de texto que son 'nan', 'null', 'NaT', etc.
        for col in df.columns:
            if df[col].dtype == 'object':  # Columnas de texto
                df[col] = df[col].astype(str)
                df[col] = df[col].replace({
                    'nan': '',
                    'NaN': '',
                    'null': '',
                    'NULL': '',
                    'None': '',
                    '<NA>': '',
                    'NaT': '',  # 🆕 Agregar limpieza de NaT
                    'nat': ''
                })
        
        return df
        
    def get_sheet_preview(self, sheet_name, max_rows=10):
        """Obtiene una vista previa de una hoja específica"""
        if self.excel_file is None and not self.load_file():
            return None
            
        try:
            # 🔧 IMPORTANTE: Usar excel_file en lugar de file_path (funciona para local y OneDrive)
            df = pd.read_excel(self.excel_file, sheet_name=sheet_name, nrows=max_rows)
            df = self._clean_dataframe(df)  # Limpiar datos
            
            # 🔧 Para contar total de filas, también usar excel_file
            df_full = pd.read_excel(self.excel_file, sheet_name=sheet_name)
            
            return {
                'columns': list(df.columns),
                'sample_data': df.head(max_rows).values.tolist(),  # Convertir a lista de listas
                'data': df.head(max_rows).to_dict('records'),
                'total_rows': len(df_full),
            }
        except Exception as e:
            print(f"Error al leer la hoja {sheet_name}: {str(e)}")
            return None
            
    def get_sheet_columns(self, sheet_name):
        """Obtiene las columnas de una hoja específica con tipos de datos"""
        if self.excel_file is None and not self.load_file():
            return []
            
        try:
            # 🔧 IMPORTANTE: Usar excel_file en lugar de file_path (funciona para local y OneDrive)
            df = pd.read_excel(self.excel_file, sheet_name=sheet_name, nrows=50)  # Más filas para mejor detección
            
            # 🆕 IMPORTANTE: Detectar tipos ANTES de limpiar (para no perder la información de tipo)
            columns = []
            for col in df.columns:
                col_str = str(col)
                # Saltar columnas Unnamed o vacías para el nombre
                if col_str.startswith('Unnamed') or pd.isna(col) or col_str.lower() in ['nan', 'null', '']:
                    continue
                    
                # Detectar tipo de datos con análisis inteligente
                dtype_name = df[col].dtype.name
                sql_type = self._infer_sql_type_smart(df[col], dtype_name)
                
                columns.append({
                    'name': col_str,
                    'type': dtype_name,
                    'sql_type': sql_type,
                })
                
            return columns
        except Exception as e:
            print(f"Error al obtener columnas de la hoja {sheet_name}: {str(e)}")
            return []
    
    def _infer_sql_type_smart(self, column_series, dtype_name):
        """
        Infiere el tipo SQL de forma inteligente analizando los valores reales.
        Útil cuando pandas detecta 'object' pero los valores son fechas o números.
        """
        import re
        from datetime import datetime
        
        # Si pandas ya detectó un tipo específico, usarlo
        ptype = str(dtype_name).lower()
        
        if 'datetime' in ptype or 'timestamp' in ptype:
            return 'DATETIME'
        if 'int' in ptype:
            return 'INT'
        if 'float' in ptype:
            return 'FLOAT'
        if ptype in ['bool', 'boolean']:
            return 'BIT'
        
        # Si es 'object', analizar los valores reales
        if ptype == 'object':
            # Obtener valores no nulos
            non_null_values = column_series.dropna()
            
            if len(non_null_values) == 0:
                return 'NVARCHAR(255)'  # Si todo es nulo, usar texto por defecto
            
            # Tomar una muestra de valores para analizar
            sample_values = non_null_values.head(20).tolist()
            
            # Contadores para cada tipo
            date_count = 0
            int_count = 0
            float_count = 0
            
            for val in sample_values:
                val_str = str(val).strip()
                
                # Detectar fechas (varios formatos)
                if self._is_date_value(val):
                    date_count += 1
                # Detectar enteros
                elif self._is_integer_value(val_str):
                    int_count += 1
                # Detectar flotantes
                elif self._is_float_value(val_str):
                    float_count += 1
            
            total = len(sample_values)
            
            # Si más del 70% son fechas, es DATETIME
            if date_count / total > 0.7:
                return 'DATETIME'
            # Si más del 70% son enteros, es INT
            if int_count / total > 0.7:
                return 'INT'
            # Si más del 70% son flotantes (incluyendo enteros), es FLOAT
            if (float_count + int_count) / total > 0.7:
                return 'FLOAT'
        
        # Por defecto: texto
        return 'NVARCHAR(255)'
    
    def _is_date_value(self, val):
        """Detecta si un valor es una fecha"""
        from datetime import datetime
        import pandas as pd
        
        # Si ya es un objeto datetime de pandas o python
        if isinstance(val, (datetime, pd.Timestamp)):
            return True
        
        # Si es string, intentar parsear como fecha
        if isinstance(val, str):
            val_str = val.strip()
            # Patrones comunes de fecha
            date_patterns = [
                r'^\d{4}-\d{2}-\d{2}',  # 2024-01-15
                r'^\d{2}/\d{2}/\d{4}',  # 15/01/2024
                r'^\d{2}-\d{2}-\d{4}',  # 15-01-2024
                r'^\d{1,2}/\d{1,2}/\d{2,4}',  # 1/1/24
            ]
            import re
            for pattern in date_patterns:
                if re.match(pattern, val_str):
                    return True
        
        return False
    
    def _is_integer_value(self, val_str):
        """Detecta si un string es un entero"""
        try:
            # Remover espacios y comas de miles
            clean_val = val_str.replace(',', '').replace(' ', '')
            int(float(clean_val))
            return '.' not in clean_val
        except:
            return False
    
    def _is_float_value(self, val_str):
        """Detecta si un string es un número flotante"""
        try:
            clean_val = val_str.replace(',', '').replace(' ', '')
            float(clean_val)
            return True
        except:
            return False
            
    def _map_pandas_type_to_sql(self, pandas_type):
        """Mapea tipos de pandas a tipos SQL comunes - MEJORADO"""
        # Normalizar el tipo a minúsculas para comparación
        ptype = str(pandas_type).lower()
        
        # Tipos de fecha/hora (detectar datetime64[ns], datetime64, etc.)
        if 'datetime' in ptype or 'timestamp' in ptype:
            return 'DATETIME'
        
        # Tipos de fecha sin hora
        if ptype == 'date':
            return 'DATE'
        
        # Tipos de hora
        if 'timedelta' in ptype or ptype == 'time':
            return 'TIME'
        
        # Tipos enteros
        if ptype in ['int64', 'int32', 'int16', 'int8', 'int']:
            return 'INT'
        if 'int' in ptype and 'uint' not in ptype:
            return 'BIGINT'
        if 'uint' in ptype:
            return 'BIGINT'
        
        # Tipos decimales/flotantes
        if ptype in ['float64', 'float32', 'float16', 'float']:
            return 'FLOAT'
        if 'decimal' in ptype:
            return 'DECIMAL(18,2)'
        
        # Tipos booleanos
        if ptype in ['bool', 'boolean']:
            return 'BIT'
        
        # Tipos de texto (por defecto)
        if ptype == 'object' or 'str' in ptype or 'string' in ptype:
            return 'NVARCHAR(255)'
        
        # Categorías de pandas
        if 'category' in ptype:
            return 'NVARCHAR(255)'
        
        # Por defecto: texto
        return 'NVARCHAR(255)'
            
    def read_sheet_data(self, sheet_name, selected_columns=None):
        """Lee todos los datos de una hoja, opcionalmente filtrando columnas"""
        if self.excel_file is None and not self.load_file():
            return None
            
        try:
            # 🔧 IMPORTANTE: Usar excel_file en lugar de file_path (funciona para local y OneDrive)
            if selected_columns:
                df = pd.read_excel(self.excel_file, sheet_name=sheet_name, usecols=selected_columns)
            else:
                df = pd.read_excel(self.excel_file, sheet_name=sheet_name)
            
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
        """Mapea tipos de pandas a tipos SQL comunes - MEJORADO"""
        # Normalizar el tipo a minúsculas para comparación
        ptype = str(pandas_type).lower()
        
        # Tipos de fecha/hora (detectar datetime64[ns], datetime64, etc.)
        if 'datetime' in ptype or 'timestamp' in ptype:
            return 'DATETIME'
        
        # Tipos de fecha sin hora
        if ptype == 'date':
            return 'DATE'
        
        # Tipos de hora
        if 'timedelta' in ptype or ptype == 'time':
            return 'TIME'
        
        # Tipos enteros
        if ptype in ['int64', 'int32', 'int16', 'int8', 'int']:
            return 'INT'
        if 'int' in ptype and 'uint' not in ptype:
            return 'BIGINT'
        if 'uint' in ptype:
            return 'BIGINT'
        
        # Tipos decimales/flotantes
        if ptype in ['float64', 'float32', 'float16', 'float']:
            return 'FLOAT'
        if 'decimal' in ptype:
            return 'DECIMAL(18,2)'
        
        # Tipos booleanos
        if ptype in ['bool', 'boolean']:
            return 'BIT'
        
        # Tipos de texto (por defecto)
        if ptype == 'object' or 'str' in ptype or 'string' in ptype:
            return 'NVARCHAR(255)'
        
        # Categorías de pandas
        if 'category' in ptype:
            return 'NVARCHAR(255)'
        
        # Por defecto: texto
        return 'NVARCHAR(255)'
            
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
        try:
            # Si se proporciona una base de datos en la llamada, usarla; de lo contrario, usar la del objeto
            db_to_use = database if database is not None else self.database
            
            if db_to_use:
                connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server},{self.port};DATABASE={db_to_use};UID={self.username};PWD={self.password}"
            else:
                # Conectar solo al servidor sin especificar base de datos
                connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server},{self.port};UID={self.username};PWD={self.password}"
            
            self.conn = pyodbc.connect(connection_string)
            
            # Actualizar la base de datos actual si la conexión es exitosa y se proporcionó una
            if database is not None:
                self.database = database
                
            return True
        except Exception as e:
            print(f"Error al conectar a SQL Server: {str(e)}")
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
            return []
    
    def get_table_preview(self, schema, table, max_rows=10):
        """Obtiene una vista previa de una tabla"""
        if not self.conn and not self.connect():
            return None
        
        try:
            # Obtener todas las columnas primero
            columns = self.get_table_columns(schema, table)
            if not columns:
                return None
                
            column_names = [col['name'] for col in columns]
            
            cursor = self.conn.cursor()
            
            # Contar filas totales
            count_query = f"SELECT COUNT(*) FROM [{schema}].[{table}]"
            cursor.execute(count_query)
            total_rows = cursor.fetchone()[0]
            
            # Obtener muestra de datos
            data_query = f"SELECT TOP {max_rows} * FROM [{schema}].[{table}]"
            cursor.execute(data_query)
            
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
            
            result = {
                'columns': column_names,
                'data': data,
                'total_rows': total_rows,
            }
            
            return result
            
        except Exception as e:
            return None
    
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
