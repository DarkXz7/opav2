"""
Utilidades para sincronizar procesos Django (MigrationProcess) con SQL Server (ProcesosGuardados).

Este módulo proporciona funciones helper para mantener sincronizados los procesos
creados en Django con una tabla centralizada en SQL Server (DestinoAutomatizacion).

Autor: Sistema de Automatización
Fecha: 2025-10-20
"""

import re
from django.utils import timezone
from django.db import connections
import logging

logger = logging.getLogger(__name__)


def normalize_process_name(name):
    """
    Normaliza el nombre del proceso eliminando caracteres especiales.
    
    Args:
        name (str): Nombre original del proceso
        
    Returns:
        str: Nombre normalizado (solo alfanuméricos y guiones bajos)
        
    Examples:
        >>> normalize_process_name("Proceso de Empleados - 2024")
        "Proceso_de_Empleados_2024"
        >>> normalize_process_name("Migración/Actualización #1")
        "Migracion_Actualizacion_1"
    """
    if not name:
        return ""
    
    # Eliminar caracteres especiales excepto espacios, letras, números y guiones
    cleaned = re.sub(r'[^\w\s\-]', '', name)
    
    # Reemplazar espacios y guiones por guiones bajos
    cleaned = re.sub(r'[\s\-]+', '_', cleaned)
    
    # Eliminar guiones bajos múltiples consecutivos
    cleaned = re.sub(r'_+', '_', cleaned)
    
    # Eliminar guiones bajos al inicio y final
    cleaned = cleaned.strip('_')
    
    return cleaned


def sync_process_to_sqlserver(migration_process, usuario='sistema', observaciones=None):
    """
    Sincroniza un proceso de Django (MigrationProcess) hacia SQL Server (ProcesosGuardados).
    
    Esta función:
    - Inserta el proceso en ProcesosGuardados si no existe
    - Actualiza el proceso si ya existe (basado en nombre normalizado)
    - Mantiene trazabilidad con FechaActualizacion
    - Maneja errores de duplicados
    
    Args:
        migration_process (MigrationProcess): Instancia del proceso Django a sincronizar
        usuario (str): Nombre del usuario que realiza la operación (default: 'sistema')
        observaciones (str): Notas adicionales sobre el cambio (opcional)
        
    Returns:
        tuple: (exito: bool, mensaje: str, proceso_id: int or None)
        
    Raises:
        Exception: Si hay un error crítico en la sincronización
        
    Examples:
        >>> proceso = MigrationProcess.objects.get(name="Mi Proceso")
        >>> exito, mensaje, id_sql = sync_process_to_sqlserver(proceso, usuario='admin')
        >>> if exito:
        >>>     print(f"Proceso sincronizado con ID {id_sql}")
    """
    try:
        # 1. Normalizar nombre del proceso
        nombre_normalizado = normalize_process_name(migration_process.name)
        
        if not nombre_normalizado:
            return False, "Error: El nombre del proceso no puede estar vacío", None
        
        # 2. Extraer información del proceso
        tipo_fuente = migration_process.source.source_type.upper() if migration_process.source else 'UNKNOWN'
        
        # Fuente: ruta de archivo o nombre de conexión
        fuente = None
        if migration_process.source:
            if migration_process.source.source_type == 'excel':
                fuente = migration_process.source.file_path
            elif migration_process.source.source_type == 'sql':
                fuente = migration_process.source.connection.name if migration_process.source.connection else None
        
        # Hoja/Tabla: primera hoja o primera tabla seleccionada (para referencia)
        hoja_tabla = None
        if migration_process.selected_sheets and len(migration_process.selected_sheets) > 0:
            hoja_tabla = migration_process.selected_sheets[0]
        elif migration_process.selected_tables and len(migration_process.selected_tables) > 0:
            hoja_tabla = migration_process.selected_tables[0]
        
        # Destino
        destino = migration_process.target_db_name or 'DestinoAutomatizacion'
        
        # Descripción
        descripcion = migration_process.description or f"Proceso de tipo {tipo_fuente}"
        
        # Estado basado en el status de Django
        estado_map = {
            'draft': 'Borrador',
            'configured': 'Configurado',
            'ready': 'Listo',
            'running': 'En_Ejecucion',
            'completed': 'Completado',
            'failed': 'Fallido',
        }
        estado = estado_map.get(migration_process.status, 'Activo')
        
        # Última ejecución
        ultima_ejecucion = migration_process.last_run
        
        # 3. Conectar a SQL Server usando alias 'sqlserver'
        with connections['sqlserver'].cursor() as cursor:
            
            # 4. Verificar si el proceso ya existe (por nombre normalizado)
            cursor.execute("""
                SELECT Id, Version 
                FROM dbo.ProcesosGuardados 
                WHERE NombreProceso = %s
            """, [nombre_normalizado])
            
            existing = cursor.fetchone()
            
            if existing:
                # ======== ACTUALIZACIÓN ========
                proceso_id, version_actual = existing
                nueva_version = version_actual + 1
                
                logger.info(f"Actualizando proceso existente: {nombre_normalizado} (ID: {proceso_id})")
                
                cursor.execute("""
                    UPDATE dbo.ProcesosGuardados
                    SET 
                        TipoFuente = %s,
                        Fuente = %s,
                        HojaTabla = %s,
                        Destino = %s,
                        Estado = %s,
                        FechaActualizacion = GETDATE(),
                        Descripcion = %s,
                        UltimaEjecucion = %s,
                        Version = %s,
                        Observaciones = %s
                    WHERE Id = %s
                """, [
                    tipo_fuente,
                    fuente,
                    hoja_tabla,
                    destino,
                    estado,
                    descripcion,
                    ultima_ejecucion,
                    nueva_version,
                    observaciones or f"Actualizado automáticamente por {usuario}",
                    proceso_id
                ])
                
                mensaje = f"Proceso '{nombre_normalizado}' actualizado exitosamente (ID: {proceso_id}, Versión: {nueva_version})"
                logger.info(mensaje)
                
                return True, mensaje, proceso_id
                
            else:
                # ======== INSERCIÓN ========
                logger.info(f"Insertando nuevo proceso: {nombre_normalizado}")
                
                cursor.execute("""
                    INSERT INTO dbo.ProcesosGuardados (
                        NombreProceso,
                        TipoFuente,
                        Fuente,
                        HojaTabla,
                        Destino,
                        Estado,
                        FechaCreacion,
                        UsuarioCreador,
                        Descripcion,
                        UltimaEjecucion,
                        Version,
                        Observaciones
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, GETDATE(), %s, %s, %s, 1, %s);
                    
                    SELECT SCOPE_IDENTITY();
                """, [
                    nombre_normalizado,
                    tipo_fuente,
                    fuente,
                    hoja_tabla,
                    destino,
                    estado,
                    usuario,
                    descripcion,
                    ultima_ejecucion,
                    observaciones or f"Creado automáticamente desde Django por {usuario}"
                ])
                
                # Obtener el ID del proceso recién insertado
                proceso_id = cursor.fetchone()[0]
                
                mensaje = f"Proceso '{nombre_normalizado}' creado exitosamente en SQL Server (ID: {proceso_id})"
                logger.info(mensaje)
                
                return True, mensaje, int(proceso_id)
                
    except Exception as e:
        error_msg = f"Error sincronizando proceso '{migration_process.name}' a SQL Server: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, error_msg, None


def update_ultima_ejecucion(nombre_proceso, fecha_ejecucion=None):
    """
    Actualiza el campo UltimaEjecucion de un proceso en SQL Server.
    
    Esta función se llama cada vez que un proceso se ejecuta, para mantener
    el registro de la última vez que corrió.
    
    Args:
        nombre_proceso (str): Nombre del proceso (sin normalizar o normalizado)
        fecha_ejecucion (datetime): Timestamp de la ejecución (default: ahora)
        
    Returns:
        tuple: (exito: bool, mensaje: str)
        
    Examples:
        >>> from django.utils import timezone
        >>> exito, msg = update_ultima_ejecucion("Mi Proceso", timezone.now())
    """
    try:
        # Normalizar nombre
        nombre_normalizado = normalize_process_name(nombre_proceso)
        
        if not nombre_normalizado:
            return False, "Error: Nombre de proceso vacío"
        
        # Usar timestamp actual si no se proporciona
        if fecha_ejecucion is None:
            fecha_ejecucion = timezone.now()
        
        with connections['sqlserver'].cursor() as cursor:
            cursor.execute("""
                UPDATE dbo.ProcesosGuardados
                SET UltimaEjecucion = %s,
                    FechaActualizacion = GETDATE()
                WHERE NombreProceso = %s
            """, [fecha_ejecucion, nombre_normalizado])
            
            rows_affected = cursor.rowcount
            
            if rows_affected > 0:
                mensaje = f"Campo UltimaEjecucion actualizado para '{nombre_normalizado}'"
                logger.info(mensaje)
                return True, mensaje
            else:
                mensaje = f"Proceso '{nombre_normalizado}' no encontrado en ProcesosGuardados"
                logger.warning(mensaje)
                return False, mensaje
                
    except Exception as e:
        error_msg = f"Error actualizando UltimaEjecucion para '{nombre_proceso}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, error_msg


def delete_process_from_sqlserver(nombre_proceso, soft_delete=True):
    """
    Elimina (lógica o físicamente) un proceso de SQL Server.
    
    Args:
        nombre_proceso (str): Nombre del proceso a eliminar
        soft_delete (bool): Si es True, solo marca como "Eliminado" (default: True)
                           Si es False, elimina el registro físicamente
        
    Returns:
        tuple: (exito: bool, mensaje: str)
    """
    try:
        nombre_normalizado = normalize_process_name(nombre_proceso)
        
        if not nombre_normalizado:
            return False, "Error: Nombre de proceso vacío"
        
        with connections['sqlserver'].cursor() as cursor:
            if soft_delete:
                # Eliminación lógica (marcar como "Eliminado")
                cursor.execute("""
                    UPDATE dbo.ProcesosGuardados
                    SET Estado = 'Eliminado',
                        FechaActualizacion = GETDATE(),
                        Observaciones = CONCAT(
                            ISNULL(Observaciones, ''),
                            CHAR(13) + CHAR(10),
                            'Eliminado el ' + CONVERT(VARCHAR, GETDATE(), 120)
                        )
                    WHERE NombreProceso = %s
                """, [nombre_normalizado])
                
                mensaje = f"Proceso '{nombre_normalizado}' marcado como eliminado (soft delete)"
            else:
                # Eliminación física
                cursor.execute("""
                    DELETE FROM dbo.ProcesosGuardados
                    WHERE NombreProceso = %s
                """, [nombre_normalizado])
                
                mensaje = f"Proceso '{nombre_normalizado}' eliminado físicamente de SQL Server"
            
            rows_affected = cursor.rowcount
            
            if rows_affected > 0:
                logger.info(mensaje)
                return True, mensaje
            else:
                mensaje = f"Proceso '{nombre_normalizado}' no encontrado en ProcesosGuardados"
                logger.warning(mensaje)
                return False, mensaje
                
    except Exception as e:
        error_msg = f"Error eliminando proceso '{nombre_proceso}' de SQL Server: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, error_msg
