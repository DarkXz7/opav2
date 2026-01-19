"""
Decoradores para logging automático de procesos del frontend
"""
import functools
from datetime import datetime
from django.http import JsonResponse
from .logs.process_tracker import ProcessTracker
from .web_logger import registrar_proceso_web, finalizar_proceso_web


def auto_log_frontend_process(process_name_template=None):
    """
    Decorador que automáticamente registra procesos iniciados desde el frontend
    
    Args:
        process_name_template (str): Template para el nombre del proceso. 
                                   Puede usar {view_name}, {method}, {args}, {kwargs}
    
    Usage:
        @auto_log_frontend_process("Conexión SQL - {view_name}")
        def my_view(request, connection_id):
            ...
    """
    def decorator(view_func):
        @functools.wraps(view_func)
        def wrapper(request, *args, **kwargs):
            # Generar nombre del proceso
            if process_name_template:
                try:
                    process_name = process_name_template.format(
                        view_name=view_func.__name__,
                        method=request.method,
                        args=args,
                        kwargs=kwargs,
                        path=request.path
                    )
                except (KeyError, ValueError):
                    process_name = f"{view_func.__name__} - {request.method}"
            else:
                process_name = f"{view_func.__name__} - {request.method}"
            
            # Iniciar logging del proceso
            logger, proceso_id = registrar_proceso_web(
                nombre_proceso=process_name,
                datos_adicionales={
                    'view_name': view_func.__name__,
                    'method': request.method,
                    'path': request.path,
                    'user_agent': request.META.get('HTTP_USER_AGENT', 'Unknown'),
                    'remote_addr': request.META.get('REMOTE_ADDR', 'Unknown'),
                    'args': str(args),
                    'kwargs': str(kwargs)
                }
            )
            
            try:
                # Ejecutar la vista original
                response = view_func(request, *args, **kwargs)
                
                # Finalizar con éxito
                if logger:
                    finalizar_proceso_web(
                        logger, 
                        exito=True, 
                        detalles=f"Vista {view_func.__name__} ejecutada exitosamente"
                    )
                
                return response
                
            except Exception as e:
                # Finalizar con error
                if logger:
                    finalizar_proceso_web(
                        logger, 
                        exito=False, 
                        detalles=f"Error en vista {view_func.__name__}: {str(e)}"
                    )
                
                # Re-lanzar la excepción
                raise
                
        return wrapper
    return decorator


def auto_log_api_process(success_status_codes=None):
    """
    Decorador específico para endpoints API que automáticamente registra procesos
    
    Args:
        success_status_codes (list): Lista de códigos de estado HTTP considerados exitosos
                                   Por defecto: [200, 201, 202]
    
    Usage:
        @auto_log_api_process([200, 201])
        def my_api_view(request):
            return JsonResponse({...})
    """
    if success_status_codes is None:
        success_status_codes = [200, 201, 202]
        
    def decorator(view_func):
        @functools.wraps(view_func)
        def wrapper(request, *args, **kwargs):
            # Generar nombre del proceso para API
            process_name = f"API {view_func.__name__} - {request.method}"
            
            # Iniciar logging del proceso
            logger, proceso_id = registrar_proceso_web(
                nombre_proceso=process_name,
                datos_adicionales={
                    'api_endpoint': True,
                    'view_name': view_func.__name__,
                    'method': request.method,
                    'path': request.path,
                    'content_type': request.META.get('CONTENT_TYPE', ''),
                    'args': str(args),
                    'kwargs': str(kwargs)
                }
            )
            
            try:
                # Ejecutar la vista original
                response = view_func(request, *args, **kwargs)
                
                # Determinar si fue exitoso basado en el código de estado
                is_success = hasattr(response, 'status_code') and response.status_code in success_status_codes
                
                # Finalizar el proceso
                if logger:
                    status_text = f"HTTP {response.status_code}" if hasattr(response, 'status_code') else "Response OK"
                    finalizar_proceso_web(
                        logger, 
                        exito=is_success, 
                        detalles=f"API {view_func.__name__} completada - {status_text}"
                    )
                
                return response
                
            except Exception as e:
                # Finalizar con error
                if logger:
                    finalizar_proceso_web(
                        logger, 
                        exito=False, 
                        detalles=f"Error en API {view_func.__name__}: {str(e)}"
                    )
                
                # Re-lanzar la excepción
                raise
                
        return wrapper
    return decorator


def log_data_transfer_process(table_name_param='table_name'):
    """
    Decorador específico para procesos de transferencia de datos
    
    Args:
        table_name_param (str): Nombre del parámetro que contiene el nombre de la tabla
    
    Usage:
        @log_data_transfer_process('table_name')
        def transfer_data_view(request, connection_id, table_name):
            ...
    """
    def decorator(view_func):
        @functools.wraps(view_func)
        def wrapper(request, *args, **kwargs):
            # Obtener nombre de tabla desde kwargs
            table_name = kwargs.get(table_name_param, 'tabla_desconocida')
            process_name = f"Transferencia de datos - {table_name}"
            
            # Iniciar logging del proceso con detalles específicos de transferencia
            logger, proceso_id = registrar_proceso_web(
                nombre_proceso=process_name,
                datos_adicionales={
                    'transfer_process': True,
                    'table_name': table_name,
                    'connection_id': kwargs.get('connection_id'),
                    'view_name': view_func.__name__,
                    'method': request.method,
                    'path': request.path,
                    'timestamp': str(datetime.now())
                }
            )
            
            try:
                # Ejecutar la vista original
                response = view_func(request, *args, **kwargs)
                
                # Finalizar con éxito
                if logger:
                    finalizar_proceso_web(
                        logger, 
                        exito=True, 
                        detalles=f"Transferencia a {table_name} completada exitosamente"
                    )
                
                # Agregar proceso_id a la respuesta si es JSON
                if hasattr(response, 'content') and response.get('Content-Type', '').startswith('application/json'):
                    try:
                        import json
                        content = json.loads(response.content)
                        content['proceso_id'] = proceso_id
                        response.content = json.dumps(content)
                    except (json.JSONDecodeError, AttributeError):
                        pass  # Si no se puede parsear como JSON, continuar sin modificar
                
                return response
                
            except Exception as e:
                # Finalizar con error
                if logger:
                    finalizar_proceso_web(
                        logger, 
                        exito=False, 
                        detalles=f"Error en transferencia a {table_name}: {str(e)}"
                    )
                
                # Re-lanzar la excepción
                raise
                
        return wrapper
    return decorator