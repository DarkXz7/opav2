"""
Utilidad para validar tablas SQL en conexiones
"""

import pyodbc

def check_table_exists(connection, table_name):
    """
    Verifica si una tabla existe en la conexión SQL
    
    Args:
        connection: Objeto DatabaseConnection
        table_name: Nombre de la tabla (con o sin esquema)
    
    Returns:
        bool: True si la tabla existe, False si no
    """
    try:
        if not connection or not table_name:
            return False
        
        # Separar esquema y nombre
        if '.' in table_name:
            schema, name = table_name.split('.', 1)
        else:
            schema = 'dbo'  # Esquema por defecto
            name = table_name
        
        # Crear string de conexión
        conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={connection.server};DATABASE={connection.selected_database};UID={connection.username};PWD={connection.password}'
        
        # Conectar a la base de datos
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()
        
        # Verificar si la tabla existe
        query = """
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """
        
        cursor.execute(query, (schema, name))
        result = cursor.fetchone()
        
        exists = result[0] > 0
        
        cursor.close()
        conn.close()
        
        return exists
        
    except Exception as e:
        print(f"Error verificando tabla '{table_name}': {str(e)}")
        return False

def get_valid_tables(connection, table_list):
    """
    Filtra una lista de tablas y devuelve solo las que existen
    
    Args:
        connection: Objeto DatabaseConnection
        table_list: Lista de nombres de tablas
    
    Returns:
        list: Lista de nombres de tablas que existen
    """
    if not connection or not table_list:
        return []
    
    valid_tables = []
    
    for table in table_list:
        # Manejar tanto diccionarios como strings
        if isinstance(table, dict):
            table_name = table.get('full_name') or table.get('name', '')
        else:
            table_name = table
        
        if table_name and check_table_exists(connection, table_name):
            valid_tables.append(table)
    
    return valid_tables

def ensure_test_table(connection):
    """
    Asegura que exista la tabla de prueba en la base de datos
    
    Args:
        connection: Objeto DatabaseConnection
    
    Returns:
        str: Nombre de la tabla de prueba si se creó o existe, None si hubo error
    """
    TEST_TABLE_NAME = "AutomatizacionTestTable"
    
    try:
        # Verificar si la tabla ya existe
        if check_table_exists(connection, TEST_TABLE_NAME):
            return TEST_TABLE_NAME
        
        # Crear string de conexión
        conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={connection.server};DATABASE={connection.selected_database};UID={connection.username};PWD={connection.password}'
        
        # Conectar a la base de datos
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()
        
        # Crear tabla con estructura estándar
        create_table_query = f"""
        CREATE TABLE {TEST_TABLE_NAME} (
            ID INT PRIMARY KEY,
            Nombre NVARCHAR(100),
            Descripcion NVARCHAR(255),
            Cantidad INT,
            FechaCreacion DATETIME DEFAULT GETDATE()
        )
        """
        cursor.execute(create_table_query)
        conn.commit()
        
        # Insertar datos de prueba
        insert_data_query = f"""
        INSERT INTO {TEST_TABLE_NAME} (ID, Nombre, Descripcion, Cantidad)
        VALUES 
            (1, 'Producto A', 'Producto de prueba A', 100),
            (2, 'Producto B', 'Producto de prueba B', 200),
            (3, 'Producto C', 'Producto de prueba C', 300),
            (4, 'Producto D', 'Producto de prueba D', 400),
            (5, 'Producto E', 'Producto de prueba E', 500)
        """
        cursor.execute(insert_data_query)
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return TEST_TABLE_NAME
        
    except Exception as e:
        print(f"Error creando tabla de prueba: {str(e)}")
        return None