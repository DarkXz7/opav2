"""
Integración de vistas web con el sistema de logs en SQL Server
Este módulo proporciona funciones para registrar eventos y procesos desde las vistas web
"""

# Importamos directamente las utilidades desde el módulo local para evitar problemas circulares
from .logs.utils import ProcesoLogger, registrar_evento
from django.core.exceptions import ObjectDoesNotExist
import traceback
import json

def registrar_proceso_web(nombre_proceso, usuario=None, datos_adicionales=None):
    """
    Registra el inicio de un proceso web en SQL Server
    
    Args:
        nombre_proceso (str): Nombre del proceso web (ej. "Nueva migración", "Procesamiento Excel")
        usuario (User, optional): Usuario que inició el proceso
        datos_adicionales (dict, optional): Datos adicionales a registrar
        
    Returns:
        tuple: (logger, proceso_id) - Logger iniciado y ID del proceso
    """
    # Preparar los parámetros
    parametros = datos_adicionales or {}
    
    # Agregar información del usuario si está disponible
    if usuario and not usuario.is_anonymous:
        parametros['usuario'] = {
            'id': usuario.id,
            'username': usuario.username,
            'email': usuario.email if hasattr(usuario, 'email') else None
        }
    
    # Iniciar el logger
    try:
        logger = ProcesoLogger(nombre_proceso)
        proceso_id = logger.iniciar(parametros=parametros)
        return logger, proceso_id
    except Exception as e:
        # Si hay error al guardar el log, registramos en consola pero permitimos continuar
        print(f"ERROR al registrar proceso en SQL Server: {str(e)}")
        return None, None

def finalizar_proceso_web(logger, exito=True, detalles=None, error=None):
    """
    Finaliza un proceso web registrando su resultado en SQL Server
    
    Args:
        logger (ProcesoLogger): Logger obtenido de registrar_proceso_web
        exito (bool): Si el proceso terminó exitosamente o no
        detalles (str, optional): Detalles adicionales del resultado
        error (Exception, optional): Excepción si ocurrió algún error
    
    Returns:
        bool: True si se registró correctamente, False si hubo error
    """
    if not logger:
        return False
    
    try:
        if exito:
            logger.finalizar_exito(detalles)
        else:
            # Preparar mensaje de error con detalles del traceback
            error_msg = f"{str(error) if error else 'Error desconocido'}"
            if error:
                error_msg += f"\n{traceback.format_exc()}"
            logger.finalizar_error(error_msg)
        return True
    except Exception as e:
        # Si hay error al guardar el log, registramos en consola
        print(f"ERROR al finalizar proceso en SQL Server: {str(e)}")
        return False

def log_migration_event(migration_id, event_type, datos=None, error=None):
    """
    Registra un evento relacionado con una migración específica
    
    Args:
        migration_id (int): ID del proceso de migración en la BD principal
        event_type (str): Tipo de evento (iniciado, completado, error, etc)
        datos (dict, optional): Datos adicionales del evento
        error (str, optional): Mensaje de error si aplica
    """
    try:
        parametros = datos or {}
        parametros['migration_id'] = migration_id
        
        return registrar_evento(
            nombre_evento=f"Migración #{migration_id}", 
            estado=event_type, 
            parametros=parametros,
            error=error
        )
    except Exception as e:
        # Si hay error al guardar el log, registramos en consola pero permitimos continuar
        print(f"ERROR al registrar evento de migración en SQL Server: {str(e)}")
        return None
