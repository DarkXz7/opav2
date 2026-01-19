"""
Utilidades para optimización de parámetros de entrada en el sistema de logging
Genera JSON conciso y legible para ParametrosEntrada en ProcesoLog
"""
import json
from typing import Dict, Any, Optional
from datetime import datetime

def optimizar_parametros_entrada(datos_completos: Dict[str, Any]) -> str:
    """
    Optimiza los parámetros de entrada para que sean concisos y legibles
    
    Args:
        datos_completos: Diccionario completo con todos los datos
        
    Returns:
        str: JSON optimizado con solo información esencial
    """
    try:
        parametros_optimizados = {
            'timestamp': datetime.now().isoformat(),
            'proceso': {},
            'configuracion': {},
            'origen': {},
            'usuario': {}
        }
        
        # SECCIÓN PROCESO: Información básica del proceso
        if 'process_name' in datos_completos:
            parametros_optimizados['proceso']['nombre'] = datos_completos['process_name']
        if 'migration_process_id' in datos_completos:
            parametros_optimizados['proceso']['id'] = datos_completos['migration_process_id']
        if 'action' in datos_completos:
            parametros_optimizados['proceso']['accion'] = datos_completos['action']
        
        # SECCIÓN CONFIGURACIÓN: Settings relevantes
        config_keys = ['selected_tables', 'selected_sheets', 'selected_columns', 'selected_database', 'target_db_name']
        config_data = {}
        for key in config_keys:
            if key in datos_completos and datos_completos[key]:
                try:
                    # Si es JSON string, parsearlo para obtener resumen
                    if isinstance(datos_completos[key], str) and datos_completos[key].startswith('['):
                        parsed = json.loads(datos_completos[key])
                        if isinstance(parsed, list):
                            config_data[key] = {
                                'cantidad': len(parsed),
                                'primeros_elementos': parsed[:3] if parsed else []
                            }
                        else:
                            config_data[key] = parsed
                    else:
                        config_data[key] = datos_completos[key]
                except:
                    config_data[key] = str(datos_completos[key])[:100]  # Truncar si es muy largo
        
        if config_data:
            parametros_optimizados['configuracion'] = config_data
        
        # SECCIÓN ORIGEN: Información de la fuente de datos
        origen_keys = ['source_type', 'source_id', 'connection_id']
        origen_data = {}
        for key in origen_keys:
            if key in datos_completos and datos_completos[key]:
                origen_data[key] = datos_completos[key]
        
        if origen_data:
            parametros_optimizados['origen'] = origen_data
        
        # SECCIÓN USUARIO: Información del usuario (solo lo esencial)
        if 'usuario' in datos_completos and isinstance(datos_completos['usuario'], dict):
            usuario_info = {}
            usuario_data = datos_completos['usuario']
            
            # Solo campos esenciales del usuario
            essential_user_fields = ['id', 'username']
            for field in essential_user_fields:
                if field in usuario_data:
                    usuario_info[field] = usuario_data[field]
            
            if usuario_info:
                parametros_optimizados['usuario'] = usuario_info
        
        # INFORMACIÓN TÉCNICA (solo si es relevante)
        tech_keys = ['user_agent', 'remote_addr']
        tech_data = {}
        for key in tech_keys:
            if key in datos_completos and datos_completos[key] and datos_completos[key] != 'Unknown':
                # Truncar user_agent si es muy largo
                if key == 'user_agent':
                    tech_data[key] = datos_completos[key][:100]
                else:
                    tech_data[key] = datos_completos[key]
        
        if tech_data:
            parametros_optimizados['tecnico'] = tech_data
        
        # Limpiar secciones vacías
        parametros_optimizados = {k: v for k, v in parametros_optimizados.items() 
                                if v and v != {}}
        
        # Serializar con formato legible
        return json.dumps(parametros_optimizados, 
                         ensure_ascii=False, 
                         indent=None, 
                         separators=(',', ':'))
        
    except Exception as e:
        # Fallback: crear JSON básico con información del error
        fallback = {
            'error_optimizacion': str(e),
            'timestamp': datetime.now().isoformat(),
            'datos_originales_keys': list(datos_completos.keys()) if isinstance(datos_completos, dict) else 'no_dict'
        }
        return json.dumps(fallback)

def crear_parametros_proceso(process_name: str, 
                           process_id: Optional[int] = None,
                           source_type: Optional[str] = None,
                           usuario_id: Optional[int] = None,
                           configuracion_adicional: Optional[Dict] = None) -> str:
    """
    Crear parámetros de entrada optimizados para procesos de migración
    
    Args:
        process_name: Nombre del proceso
        process_id: ID del proceso de migración
        source_type: Tipo de fuente (excel, csv, sql)
        usuario_id: ID del usuario que ejecuta
        configuracion_adicional: Configuración adicional relevante
        
    Returns:
        str: JSON optimizado para ParametrosEntrada
    """
    parametros = {
        'timestamp': datetime.now().isoformat(),
        'proceso': {
            'nombre': process_name,
        },
        'contexto': 'ejecucion_proceso'
    }
    
    if process_id:
        parametros['proceso']['id'] = process_id
    
    if source_type:
        parametros['origen'] = {'tipo': source_type}
    
    if usuario_id:
        parametros['usuario'] = {'id': usuario_id}
    
    if configuracion_adicional:
        parametros['configuracion'] = configuracion_adicional
    
    return json.dumps(parametros, ensure_ascii=False, separators=(',', ':'))

def crear_parametros_web_action(action: str, 
                               datos_request: Dict[str, Any],
                               usuario_id: Optional[int] = None) -> str:
    """
    Crear parámetros optimizados para acciones web
    
    Args:
        action: Tipo de acción (save_process, connect_sql, etc.)
        datos_request: Datos relevantes del request
        usuario_id: ID del usuario
        
    Returns:
        str: JSON optimizado para ParametrosEntrada
    """
    parametros = {
        'timestamp': datetime.now().isoformat(),
        'accion': action,
        'contexto': 'web_interface'
    }
    
    if usuario_id:
        parametros['usuario'] = {'id': usuario_id}
    
    # Extraer solo campos relevantes de los datos del request
    campos_relevantes = ['process_name', 'source_id', 'connection_id', 'selected_database']
    datos_filtrados = {k: v for k, v in datos_request.items() 
                      if k in campos_relevantes and v}
    
    if datos_filtrados:
        parametros['datos'] = datos_filtrados
    
    return json.dumps(parametros, ensure_ascii=False, separators=(',', ':'))