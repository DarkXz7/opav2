"""
Integración de vistas web con el sistema optimizado de logs en SQL Server
Este módulo proporciona funciones para registrar eventos y procesos desde las vistas web
de manera eficiente y consolidada
"""

import traceback
import json
from django.core.exceptions import ObjectDoesNotExist
from .logs.process_tracker import ProcessTracker, registrar_evento_unificado

# Variable para compatibilidad con el código anterior
__all__ = ['registrar_proceso_web', 'finalizar_proceso_web', 'log_migration_event']

# Diccionario para mantener referencia a los process trackers activos por sesión
_active_trackers = {}

def registrar_proceso_web(nombre_proceso, usuario=None, datos_adicionales=None):
    """
    Registra el inicio de un proceso web en SQL Server utilizando el sistema unificado
    
    Args:
        nombre_proceso (str): Nombre del proceso web (ej. "Nueva migración", "Procesamiento Excel")
        usuario (User, optional): Usuario que inició el proceso
        datos_adicionales (dict, optional): Datos adicionales a registrar
        
    Returns:
        tuple: (tracker, proceso_id) - Tracker iniciado y ID del proceso
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
    
    # Iniciar el tracker
    try:
        print(f"DEBUG: Creando ProcessTracker para '{nombre_proceso}'")
        tracker = ProcessTracker(nombre_proceso)
        print(f"DEBUG: Iniciando tracker...")
        proceso_id = tracker.iniciar(parametros=parametros)
        print(f"DEBUG: Tracker iniciado exitosamente con ID: {proceso_id}")
        
        # Guardar referencia al tracker para su uso posterior
        session_key = usuario.id if usuario and not usuario.is_anonymous else 'anonymous'
        if session_key not in _active_trackers:
            _active_trackers[session_key] = {}
        _active_trackers[session_key][proceso_id] = tracker
        
        return tracker, proceso_id
    except Exception as e:
        # Si hay error al guardar el log, registramos en consola pero permitimos continuar
        print(f"ERROR al registrar proceso en SQL Server: {str(e)}")
        return None, None

def actualizar_estado_proceso_web(tracker_o_id, usuario, estado, detalles=None):
    """
    Actualiza el estado de un proceso web sin finalizarlo
    
    Args:
        tracker_o_id: Puede ser un ProcessTracker o un ID de proceso
        usuario: Usuario que está ejecutando el proceso
        estado (str): Nuevo estado del proceso
        detalles (str, optional): Detalles adicionales
    
    Returns:
        bool: True si se actualizó correctamente
    """
    if tracker_o_id is None:
        return False
    
    try:
        # Determinar si tenemos un tracker o un ID
        if isinstance(tracker_o_id, ProcessTracker):
            tracker = tracker_o_id
        else:
            # Buscar el tracker en los activos
            session_key = usuario.id if usuario and not usuario.is_anonymous else 'anonymous'
            if session_key in _active_trackers and tracker_o_id in _active_trackers[session_key]:
                tracker = _active_trackers[session_key][tracker_o_id]
            else:
                # Si no lo encontramos, crear uno nuevo con el ID proporcionado
                tracker = ProcessTracker("Proceso continuado")
                tracker.proceso_id = tracker_o_id
            
        # Actualizar el estado
        tracker.actualizar_estado(estado, detalles)
        return True
    except Exception as e:
        print(f"ERROR al actualizar estado del proceso: {str(e)}")
        return False

def finalizar_proceso_web(tracker_o_id, usuario=None, exito=True, detalles=None, error=None):
    """
    Finaliza un proceso web registrando su resultado en SQL Server
    
    Args:
        tracker_o_id: Puede ser un ProcessTracker o un ID de proceso
        usuario: Usuario asociado al proceso
        exito (bool): Si el proceso finalizó con éxito
        detalles (str, optional): Detalles adicionales
        error (Exception, optional): Error si ocurrió
        
    Returns:
        bool: True si se registró correctamente
    """
    if tracker_o_id is None:
        return False
    
    try:
        # Determinar si tenemos un tracker o un ID
        if isinstance(tracker_o_id, ProcessTracker):
            tracker = tracker_o_id
        else:
            # Buscar el tracker en los activos
            session_key = usuario.id if usuario and not usuario.is_anonymous else 'anonymous'
            if session_key in _active_trackers and tracker_o_id in _active_trackers[session_key]:
                tracker = _active_trackers[session_key][tracker_o_id]
                # Eliminar la referencia una vez finalizado
                del _active_trackers[session_key][tracker_o_id]
            else:
                # Si no lo encontramos, crear uno nuevo con el ID proporcionado
                tracker = ProcessTracker("Proceso finalizado")
                tracker.proceso_id = tracker_o_id
        
        # Finalizar según corresponda
        if exito:
            tracker.finalizar_exito(detalles)
        else:
            if error:
                tracker.finalizar_error(error)
            else:
                tracker.finalizar_error(detalles or "Error no especificado")
                
        return True
    except Exception as e:
        print(f"ERROR al finalizar proceso: {str(e)}")
        return False

def registrar_evento_web(nombre_evento, estado, usuario=None, parametros=None, error=None):
    """
    Registra un evento web simple
    
    Args:
        nombre_evento (str): Nombre del evento
        estado (str): Estado del evento (ej. "Completado", "Error")
        usuario (User, optional): Usuario asociado al evento
        parametros (dict, optional): Parámetros del evento
        error (Exception, optional): Error si existe
        
    Returns:
        str: ID del evento registrado
    """
    # Preparar parámetros
    params = parametros or {}
    
    # Agregar información del usuario si está disponible
    if usuario and not usuario.is_anonymous:
        params['usuario'] = {
            'id': usuario.id,
            'username': usuario.username,
            'email': usuario.email if hasattr(usuario, 'email') else None
        }
    
    try:
        # Registrar usando la función unificada
        return registrar_evento_unificado(
            nombre_evento=nombre_evento,
            estado=estado,
            parametros=params,
            error=str(error) if error else None
        )
    except Exception as e:
        print(f"ERROR al registrar evento: {str(e)}")
        return None

def log_migration_event(migration_id, event_type, datos=None, error=None):
    """
    Registra un evento relacionado con una migración específica usando el sistema unificado
    
    Args:
        migration_id (int): ID del proceso de migración en la BD principal
        event_type (str): Tipo de evento (iniciado, completado, error, etc)
        datos (dict, optional): Datos adicionales del evento
        error (str, optional): Mensaje de error si aplica
    """
    try:
        parametros = datos or {}
        parametros['migration_id'] = migration_id
        
        return registrar_evento_unificado(
            nombre_evento=f"Migración #{migration_id}", 
            estado=event_type, 
            parametros=parametros,
            error=error
        )
    except Exception as e:
        # Si hay error al guardar el log, registramos en consola pero permitimos continuar
        print(f"ERROR al registrar evento de migración en SQL Server: {str(e)}")
        return None
