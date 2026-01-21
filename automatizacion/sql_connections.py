"""
Módulo para gestionar conexiones directas a SQL Server usando pyodbc
Sin depender de backends de Django que pueden no estar instalados
"""

import pyodbc
from typing import Optional, Dict, Any

# Configuración de conexiones SQL Server
SQL_SERVER_CONFIG = {
    'logs': {
        'server': 'localhost\\SQLEXPRESS',
        'database': 'LogsAutomatizacion',
        'username': 'miguel',
        'password': '16474791@',
        'driver': 'ODBC Driver 17 for SQL Server',
    },
    'destino': {
        'server': 'localhost\\SQLEXPRESS',
        'database': 'DestinoAutomatizacion',
        'username': 'miguel',
        'password': '16474791@',
        'driver': 'ODBC Driver 17 for SQL Server',
    },
    'sqlserver': {
        'server': 'localhost\\SQLEXPRESS',
        'database': 'DestinoAutomatizacion',
        'username': 'miguel',
        'password': '16474791@',
        'driver': 'ODBC Driver 17 for SQL Server',
    },
}

# Pool de conexiones en cache
_connections_cache: Dict[str, pyodbc.Connection] = {}


def get_sql_connection(alias: str = 'destino') -> Optional[pyodbc.Connection]:
    """
    Obtiene una conexión a SQL Server usando pyodbc
    
    Args:
        alias: Identificador de la conexión ('logs', 'destino', 'sqlserver')
    
    Returns:
        pyodbc.Connection o None si hay error
    """
    if alias not in SQL_SERVER_CONFIG:
        raise ValueError(f"Alias de conexión '{alias}' no configurado")
    
    # Reutilizar conexión en cache si existe y está activa
    if alias in _connections_cache:
        try:
            # Prueba la conexión haciendo una query simple
            cursor = _connections_cache[alias].cursor()
            cursor.execute("SELECT 1")
            return _connections_cache[alias]
        except pyodbc.OperationalError:
            # Conexión muerta, eliminar del cache
            del _connections_cache[alias]
    
    config = SQL_SERVER_CONFIG[alias]
    try:
        connection_string = (
            f"Driver={config['driver']};"
            f"Server={config['server']};"
            f"Database={config['database']};"
            f"Uid={config['username']};"
            f"Pwd={config['password']};"
        )
        
        conn = pyodbc.connect(connection_string)
        conn.autocommit = True  # Autocommit por defecto
        _connections_cache[alias] = conn
        return conn
    except pyodbc.Error as e:
        print(f"Error conectando a {alias}: {e}")
        return None


def execute_query(sql: str, alias: str = 'destino', params: tuple = None) -> list:
    """
    Ejecuta una query SELECT y retorna los resultados
    
    Args:
        sql: Query SQL a ejecutar
        alias: Identificador de la conexión
        params: Parámetros para la query
    
    Returns:
        Lista de tuplas con los resultados
    """
    conn = get_sql_connection(alias)
    if not conn:
        return []
    
    cursor = conn.cursor()
    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        return cursor.fetchall()
    except pyodbc.Error as e:
        print(f"Error ejecutando query: {e}")
        return []


def execute_update(sql: str, alias: str = 'destino', params: tuple = None) -> bool:
    """
    Ejecuta una query INSERT, UPDATE o DELETE
    
    Args:
        sql: Query SQL a ejecutar
        alias: Identificador de la conexión
        params: Parámetros para la query
    
    Returns:
        True si fue exitoso, False en caso contrario
    """
    conn = get_sql_connection(alias)
    if not conn:
        return False
    
    cursor = conn.cursor()
    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        conn.commit()
        return True
    except pyodbc.Error as e:
        print(f"Error ejecutando update: {e}")
        conn.rollback()
        return False


def close_connection(alias: str = None):
    """
    Cierra una conexión del cache
    
    Args:
        alias: Identificador de la conexión. Si es None, cierra todas
    """
    if alias:
        if alias in _connections_cache:
            try:
                _connections_cache[alias].close()
            except:
                pass
            del _connections_cache[alias]
    else:
        # Cerrar todas
        for conn in _connections_cache.values():
            try:
                conn.close()
            except:
                pass
        _connections_cache.clear()
