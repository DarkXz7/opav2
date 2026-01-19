"""
Decoradores para registro de operaciones en SQL Server
Facilita la integración de las vistas Django con el sistema de logs
"""

import functools
import traceback
from .web_logger import registrar_proceso_web, finalizar_proceso_web

def log_operation(operation_name):
    """
    Decorador para registrar operaciones web en SQL Server
    
    Args:
        operation_name (str): Nombre base de la operación (ej. "Migración de datos")
        
    Ejemplo:
        @log_operation("Consulta de clientes")
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
            
            # Iniciar logger
            sql_logger, proceso_id = registrar_proceso_web(
                nombre_proceso=nombre_operacion,
                usuario=request.user,
                datos_adicionales=datos
            )
            
            try:
                # Ejecutar la vista
                response = view_func(request, *args, **kwargs)
                
                # Registrar éxito
                if sql_logger:
                    finalizar_proceso_web(
                        sql_logger, 
                        exito=True,
                        detalles=f"Operación completada. Status: {getattr(response, 'status_code', 'N/A')}"
                    )
                
                return response
                
            except Exception as e:
                # Registrar error
                if sql_logger:
                    finalizar_proceso_web(
                        sql_logger, 
                        exito=False,
                        error=e
                    )
                # Re-lanzar la excepción para que Django la maneje
                raise
                
        return wrapper
    return decorator
