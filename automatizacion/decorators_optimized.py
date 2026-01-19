"""
Decoradores optimizados para registro de operaciones en SQL Server
Implementa un sistema de registro que consolida múltiples entradas en una sola
"""

import functools
import traceback
from .web_logger_optimized import registrar_proceso_web, actualizar_estado_proceso_web, finalizar_proceso_web

def log_operation_unified(operation_name):
    """
    Decorador para registrar operaciones web en SQL Server de manera unificada,
    minimizando el número de entradas en la base de datos
    
    Args:
        operation_name (str): Nombre base de la operación (ej. "Migración de datos")
        
    Ejemplo:
        @log_operation_unified("Consulta de clientes")
        def vista_clientes(request):
            # Vista normal...
    """
    def decorator(view_func):
        @functools.wraps(view_func)
        def wrapper(request, *args, **kwargs):
            # Preparar nombre de operación con datos dinámicos si están disponibles
            nombre_operacion = operation_name
            
            # Si hay parámetros de URL, agregarlos al nombre de la operación
            if kwargs:
                params_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())
                nombre_operacion = f"{operation_name} [{params_str}]"
            
            # Datos adicionales básicos
            datos = {
                'method': request.method,
                'path': request.path,
                'params': dict(request.GET),
            }
            
            # Iniciar tracker
            tracker, proceso_id = registrar_proceso_web(
                nombre_proceso=nombre_operacion,
                usuario=request.user,
                datos_adicionales=datos
            )
            
            try:
                # Ejecutar la vista
                response = view_func(request, *args, **kwargs)
                
                # Registrar éxito
                finalizar_proceso_web(
                    tracker,
                    usuario=request.user,
                    exito=True,
                    detalles=f"Operación completada: {nombre_operacion}"
                )
                
                # Agregar el ID del proceso como header para depuración
                if hasattr(response, 'headers') and proceso_id:
                    response.headers['X-Process-ID'] = proceso_id
                
                return response
                
            except Exception as e:
                # Capturar el error
                error_detail = str(e)
                stack_trace = traceback.format_exc()
                
                # Registrar error
                finalizar_proceso_web(
                    tracker,
                    usuario=request.user,
                    exito=False,
                    error=f"{error_detail}\n{stack_trace}"
                )
                
                # Re-lanzar la excepción para que Django la maneje
                raise
                
        return wrapper
    
    return decorator
