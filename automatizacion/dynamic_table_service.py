"""
Servicio de Gestión de Tablas Dinámicas
Permite crear y gestionar tablas independientes para cada proceso
en lugar de usar una tabla única ResultadosProcesados
"""

import re
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
from django.db import connections
import pyodbc
from contextlib import contextmanager

logger = logging.getLogger('dynamic_tables')

class DynamicTableError(Exception):
    """Excepción para errores de gestión de tablas dinámicas"""
    pass

class DynamicTableManager:
    """
    Gestor de tablas dinámicas para resultados de procesos
    """
    
    def __init__(self, database_alias='destino'):
        self.database_alias = database_alias
        self.max_table_name_length = 128  # Límite SQL Server
        self.reserved_words = {
            'table', 'select', 'insert', 'update', 'delete', 'create', 'drop', 
            'alter', 'index', 'database', 'schema', 'user', 'order', 'by', 
            'where', 'group', 'having', 'join', 'union', 'intersect', 'except'
        }
    
    def generate_table_name(self, process_name: str, prefix: str = "Proceso_") -> str:
        """
        Genera un nombre válido de tabla basado en el nombre del proceso
        
        Args:
            process_name: Nombre original del proceso
            prefix: Prefijo para la tabla (default: "Proceso_")
        
        Returns:
            str: Nombre válido de tabla SQL Server
        
        Raises:
            DynamicTableError: Si no se puede generar un nombre válido
        """
        try:
            # Validación inicial
            if not process_name or not process_name.strip():
                raise DynamicTableError("El nombre del proceso no puede estar vacío")
            
            # Limpiar el nombre: solo letras, números y guiones bajos
            clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', process_name.strip())
            
            # Eliminar múltiples guiones bajos consecutivos
            clean_name = re.sub(r'_+', '_', clean_name)
            
            # Quitar guiones bajos del inicio y final
            clean_name = clean_name.strip('_')
            
            # Si queda vacío después de la limpieza, usar un nombre genérico con timestamp
            if not clean_name:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                clean_name = f"SinNombre_{timestamp}"
            
            # Generar nombre completo con prefijo
            full_name = f"{prefix}{clean_name}"
            
            # Truncar si es muy largo, dejando espacio para un sufijo numérico si es necesario
            if len(full_name) > self.max_table_name_length - 10:
                full_name = full_name[:self.max_table_name_length - 10]
                logger.warning(f"Nombre de tabla truncado a: '{full_name}'")
            
            # Verificar que no sea una palabra reservada
            if full_name.lower() in self.reserved_words:
                full_name += "_Tabla"
                logger.info(f"Nombre era palabra reservada, ajustado a: '{full_name}'")
            
            # Asegurarse de que empiece con letra o guión bajo
            if not re.match(r'^[a-zA-Z_]', full_name):
                full_name = f"Tabla_{full_name}"
                logger.info(f"Nombre ajustado para empezar correctamente: '{full_name}'")
            
            # Validación final del nombre generado
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', full_name):
                raise DynamicTableError(f"Nombre de tabla generado no es válido: '{full_name}'")
            
            logger.info(f"Nombre de tabla generado: '{process_name}' -> '{full_name}'")
            return full_name
            
        except DynamicTableError:
            raise
        except Exception as e:
            raise DynamicTableError(f"Error inesperado generando nombre de tabla para '{process_name}': {str(e)}")
    

    
    def table_exists(self, table_name: str) -> bool:
        """
        Verifica si una tabla existe en la base de datos
        
        Args:
            table_name: Nombre de la tabla a verificar
        
        Returns:
            bool: True si la tabla existe, False si no
        """
        try:
            # Usar cursor de Django directamente
            from django.db import connections
            cursor = connections[self.database_alias].cursor()
            
            # Query para verificar existencia de tabla en SQL Server
            query = """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = %s AND TABLE_TYPE = 'BASE TABLE'
            """
            
            cursor.execute(query, [table_name])
            row = cursor.fetchone()
            count = row[0] if row else 0
            
            exists = count > 0
            logger.info(f"Verificación tabla '{table_name}': {'Existe' if exists else 'No existe'}")
            return exists
                
        except Exception as e:
            logger.error(f"Error verificando existencia de tabla '{table_name}': {str(e)}")
            raise DynamicTableError(f"Error verificando tabla: {str(e)}")
        finally:
            if 'cursor' in locals():
                try:
                    cursor.close()
                except:
                    pass
    
    def create_process_table(self, table_name: str) -> bool:
        """
        Crea una nueva tabla para almacenar resultados de proceso
        
        Args:
            table_name: Nombre de la tabla a crear
        
        Returns:
            bool: True si se creó exitosamente
        
        Raises:
            DynamicTableError: Si hay errores en la creación
        """
        try:
            from django.db import connections
            cursor = connections[self.database_alias].cursor()
            
            # SQL para crear tabla con estructura mejorada que incluye NombreProceso
            # Usar formato de parámetros compatible con Django (%s en lugar de ?)
            create_sql = f"""
            CREATE TABLE [{table_name}] (
                ResultadoID INT IDENTITY(1,1) PRIMARY KEY,
                ProcesoID NVARCHAR(36) NOT NULL,
                NombreProceso NVARCHAR(255) NOT NULL,
                FechaRegistro DATETIME2 DEFAULT GETDATE(),
                DatosProcesados NTEXT,
                UsuarioResponsable NVARCHAR(100),
                EstadoProceso NVARCHAR(50) DEFAULT 'COMPLETADO',
                TipoOperacion NVARCHAR(100),
                RegistrosAfectados INT DEFAULT 0,
                TiempoEjecucion DECIMAL(10,2),
                MetadatosProceso NTEXT
            )
            """
            
            logger.info(f"Creando tabla '{table_name}'...")
            cursor.execute(create_sql)
            
            # Crear índices por separado
            index_sql1 = f"CREATE INDEX IX_{table_name}_ProcesoID ON [{table_name}] (ProcesoID)"
            index_sql2 = f"CREATE INDEX IX_{table_name}_FechaRegistro ON [{table_name}] (FechaRegistro)"
            index_sql3 = f"CREATE INDEX IX_{table_name}_NombreProceso ON [{table_name}] (NombreProceso)"
            
            cursor.execute(index_sql1)
            cursor.execute(index_sql2)
            cursor.execute(index_sql3)
            
            logger.info(f"Tabla '{table_name}' creada exitosamente con 3 índices")
            return True
                
        except Exception as e:
            # Manejo de errores más general para Django
            error_str = str(e).lower()
            if "permission" in error_str or "denied" in error_str:
                error_msg = f"Sin permisos para crear tabla '{table_name}'. Verifique permisos de usuario en la BD."
            elif "already exists" in error_str or "exist" in error_str:
                error_msg = f"La tabla '{table_name}' ya existe y no pudo ser recreada."
            elif "invalid" in error_str and "name" in error_str:
                error_msg = f"Nombre de tabla inválido '{table_name}'. Caracteres no permitidos."
            else:
                error_msg = f"Error creando tabla '{table_name}': {str(e)}"
            
            logger.error(error_msg)
            raise DynamicTableError(error_msg)
        finally:
            if 'cursor' in locals():
                try:
                    cursor.close()
                except:
                    pass
    
    def truncate_table(self, table_name: str) -> bool:
        """
        Elimina todos los datos de una tabla existente
        
        Args:
            table_name: Nombre de la tabla a truncar
        
        Returns:
            bool: True si se truncó exitosamente
        """
        try:
            from django.db import connections
            cursor = connections[self.database_alias].cursor()
            
            logger.info(f"Truncando tabla '{table_name}'...")
            cursor.execute(f"TRUNCATE TABLE [{table_name}]")
            
            logger.info(f"Tabla '{table_name}' truncada exitosamente")
            return True
                
        except Exception as e:
            error_msg = f"Error truncando tabla '{table_name}': {str(e)}"
            logger.error(error_msg)
            raise DynamicTableError(error_msg)
        finally:
            if 'cursor' in locals():
                try:
                    cursor.close()
                except:
                    pass
    
    def ensure_process_table(self, process_name: str, recreate: bool = True) -> str:
        """
        Asegura que existe una tabla para el proceso, creándola o limpiándola según sea necesario
        
        Args:
            process_name: Nombre del proceso
            recreate: Si True, trunca la tabla si ya existe. Si False, la mantiene
        
        Returns:
            str: Nombre de la tabla generada/verificada
        
        Raises:
            DynamicTableError: Si hay errores en la gestión de la tabla
        """
        try:
            # Generar nombre válido de tabla
            table_name = self.generate_table_name(process_name)
            logger.info(f"Gestionando tabla para proceso '{process_name}' -> '{table_name}'")
            
            # Verificar si la tabla existe
            if self.table_exists(table_name):
                if recreate:
                    logger.info(f"Tabla '{table_name}' existe. Truncando contenido...")
                    self.truncate_table(table_name)
                else:
                    logger.info(f"Tabla '{table_name}' existe. Manteniendo contenido existente.")
            else:
                logger.info(f"Tabla '{table_name}' no existe. Creándola...")
                self.create_process_table(table_name)
            
            return table_name
            
        except Exception as e:
            error_msg = f"Error gestionando tabla para proceso '{process_name}': {str(e)}"
            logger.error(error_msg)
            raise DynamicTableError(error_msg)
    
    def insert_to_process_table(self, 
                               table_name: str, 
                               data: Dict[str, Any]) -> Optional[int]:
        """
        Inserta datos en la tabla específica del proceso
        
        Args:
            table_name: Nombre de la tabla donde insertar
            data: Diccionario con los datos a insertar
        
        Returns:
            Optional[int]: ID del registro insertado si es exitoso
        """
        try:
            from django.db import connections
            cursor = connections[self.database_alias].cursor()
            
            # Preparar datos para inserción (INCLUYE NombreProceso)
            columns = [
                'ProcesoID', 'NombreProceso', 'DatosProcesados', 'UsuarioResponsable',
                'EstadoProceso', 'TipoOperacion', 'RegistrosAfectados',
                'TiempoEjecucion', 'MetadatosProceso'
            ]
            
            values = [
                data.get('ProcesoID'),
                data.get('NombreProceso', 'Proceso sin nombre'),  # NUEVO CAMPO
                data.get('DatosProcesados'),
                data.get('UsuarioResponsable'),
                data.get('EstadoProceso', 'COMPLETADO'),
                data.get('TipoOperacion'),
                data.get('RegistrosAfectados', 0),
                data.get('TiempoEjecucion'),
                data.get('MetadatosProceso')
            ]
            
            # Construir SQL de inserción usando %s para Django
            placeholders = ', '.join(['%s' for _ in columns])
            column_list = ', '.join([f'[{col}]' for col in columns])
            
            # Para SQL Server con Django, no podemos usar OUTPUT con parámetros
            # Haremos la inserción y luego obtenemos el ID
            insert_sql = f"""
            INSERT INTO [{table_name}] ({column_list})
            VALUES ({placeholders})
            """
            
            logger.info(f"Insertando en tabla '{table_name}'...")
            cursor.execute(insert_sql, values)
            
            # Obtener el último ID insertado (corregido para funcionar con Django)
            cursor.execute("SELECT @@IDENTITY")
            row = cursor.fetchone()
            resultado_id = int(row[0]) if row and row[0] else None
            
            logger.info(f"Registro insertado en '{table_name}' con ID: {resultado_id}")
            return resultado_id
                
        except Exception as e:
            error_msg = f"Error insertando en tabla '{table_name}': {str(e)}"
            logger.error(error_msg)
            raise DynamicTableError(error_msg)
        finally:
            if 'cursor' in locals():
                try:
                    cursor.close()
                except:
                    pass

# Instancia global del manager
dynamic_table_manager = DynamicTableManager()