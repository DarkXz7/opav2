"""
Vistas para la transferencia segura de datos a SQL Server DestinoAutomatizacion
Implementa el endpoint: /automatizacion/sql/connection/18/table/dbo.Usuarios/columns/
"""
import json
import logging
from datetime import datetime
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.contrib.auth.decorators import login_required
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from django.views import View
from django.db import connections
from django.core.exceptions import ValidationError

from .models import DatabaseConnection
from .models_destino import ResultadosProcesados, UsuariosDestino
from .data_transfer_service import data_transfer_service, DataTransferError
from .web_logger_optimized import registrar_proceso_web, finalizar_proceso_web
from .frontend_logging import log_data_transfer_process

# Configurar logging
logger = logging.getLogger('data_transfer_views')

class SecureDataTransferView(View):
    """
    Vista principal para transferencia segura de datos
    """
    
    @method_decorator(login_required)
    @method_decorator(csrf_exempt)
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)
    
    def post(self, request, connection_id, table_name):
        """
        Maneja la transferencia de datos POST - SISTEMA UNIFICADO DE LOGGING
        
        URL: /automatizacion/sql/connection/<connection_id>/table/<table_name>/columns/
        """
        # USAR SOLO UN SISTEMA DE LOGGING - ProcessTracker unificado
        proceso_nombre = f"Transferencia de datos a {table_name}"
        tracker, proceso_id = registrar_proceso_web(
            nombre_proceso=proceso_nombre,
            usuario=request.user,
            datos_adicionales={
                'connection_id': connection_id,
                'table_name': table_name,
                'user_agent': request.META.get('HTTP_USER_AGENT', 'Unknown'),
                'remote_addr': request.META.get('REMOTE_ADDR', 'Unknown')
            }
        )
        
        try:
            # Validar conexión
            connection = get_object_or_404(DatabaseConnection, pk=connection_id)
            logger.info(f"Iniciando transferencia de datos - Conexión: {connection.name}, Tabla: {table_name}")
            
            # Parsear datos de la petición
            try:
                request_data = json.loads(request.body.decode('utf-8'))
            except json.JSONDecodeError as e:
                raise ValidationError(f"JSON inválido: {str(e)}")
            
            # Validar estructura de datos
            if not isinstance(request_data, dict):
                raise ValidationError("Los datos deben ser un objeto JSON")
            
            # Extraer datos para transferencia
            transfer_data = self._extract_transfer_data(request_data, request.user, connection, table_name)
            
            # Ejecutar transferencia
            success, result_info = data_transfer_service.transfer_processed_data(
                proceso_id=proceso_id,
                datos_procesados=transfer_data['datos'],
                usuario_responsable=request.user.username,
                metadata={
                    'connection_id': connection_id,
                    'table_name': table_name,
                    'timestamp': datetime.now().isoformat(),
                    'source_info': {
                        'user_agent': request.META.get('HTTP_USER_AGENT'),
                        'remote_addr': request.META.get('REMOTE_ADDR')
                    }
                },
                estado_proceso='COMPLETADO',
                tipo_operacion=f'INSERCION_{table_name.upper()}',
                registros_afectados=transfer_data.get('count', 1)
            )
            
            if success:
                # Finalizar logging con éxito
                finalizar_proceso_web(
                    tracker,
                    usuario=request.user,
                    exito=True,
                    detalles=f"Transferencia exitosa a {table_name}. ResultadoID: {result_info['resultado_id']}"
                )
                
                return JsonResponse({
                    'success': True,
                    'message': 'Datos transferidos exitosamente',
                    'proceso_id': proceso_id,
                    'resultado_id': result_info['resultado_id'],
                    'tiempo_ejecucion': result_info['tiempo_ejecucion'],
                    'timestamp': datetime.now().isoformat()
                })
            else:
                # Transferencia falló
                raise DataTransferError(result_info.get('error', 'Error desconocido en transferencia'))
        
        except ValidationError as e:
            logger.warning(f"Error de validación: {str(e)}")
            finalizar_proceso_web(tracker, usuario=request.user, exito=False, error=e)
            return JsonResponse({
                'success': False,
                'error': f'Error de validación: {str(e)}',
                'proceso_id': proceso_id
            }, status=400)
        
        except DataTransferError as e:
            logger.error(f"Error de transferencia: {str(e)}")
            finalizar_proceso_web(tracker, usuario=request.user, exito=False, error=e)
            return JsonResponse({
                'success': False,
                'error': f'Error de transferencia: {str(e)}',
                'proceso_id': proceso_id
            }, status=500)
        
        except Exception as e:
            logger.error(f"Error inesperado: {str(e)}")
            finalizar_proceso_web(tracker, usuario=request.user, exito=False, error=e)
            return JsonResponse({
                'success': False,
                'error': f'Error interno del servidor: {str(e)}',
                'proceso_id': proceso_id
            }, status=500)
    
    def _extract_transfer_data(self, request_data, user, connection, table_name):
        """
        Extrae y prepara los datos para transferencia
        """
        # Datos básicos requeridos
        if 'data' not in request_data:
            raise ValidationError("Campo 'data' es obligatorio")
        
        # Validar datos específicos según la tabla
        if table_name.lower() == 'dbo.usuarios':
            return self._validate_usuarios_data(request_data['data'])
        elif table_name.lower() == 'resultadosprocesados':
            return self._validate_resultados_data(request_data['data'])
        else:
            # Validación genérica
            return {
                'datos': request_data['data'],
                'count': 1 if isinstance(request_data['data'], dict) else len(request_data['data'])
            }
    
    def _validate_usuarios_data(self, data):
        """
        Valida datos específicos para la tabla dbo.Usuarios
        """
        required_fields = ['NombreUsuario', 'Email', 'NombreCompleto']
        
        if isinstance(data, dict):
            # Un solo usuario
            missing_fields = [field for field in required_fields if field not in data]
            if missing_fields:
                raise ValidationError(f"Campos obligatorios faltantes para Usuario: {', '.join(missing_fields)}")
            return {'datos': data, 'count': 1}
        
        elif isinstance(data, list):
            # Múltiples usuarios
            for i, usuario in enumerate(data):
                missing_fields = [field for field in required_fields if field not in usuario]
                if missing_fields:
                    raise ValidationError(f"Usuario {i+1} - Campos faltantes: {', '.join(missing_fields)}")
            return {'datos': data, 'count': len(data)}
        
        else:
            raise ValidationError("Datos de usuarios deben ser un objeto o array")
    
    def _validate_resultados_data(self, data):
        """
        Valida datos específicos para la tabla ResultadosProcesados
        """
        required_fields = ['ProcesoID', 'DatosProcesados', 'UsuarioResponsable']
        
        if isinstance(data, dict):
            missing_fields = [field for field in required_fields if field not in data]
            if missing_fields:
                raise ValidationError(f"Campos obligatorios faltantes: {', '.join(missing_fields)}")
            return {'datos': data, 'count': 1}
        
        elif isinstance(data, list):
            for i, resultado in enumerate(data):
                missing_fields = [field for field in required_fields if field not in resultado]
                if missing_fields:
                    raise ValidationError(f"Resultado {i+1} - Campos faltantes: {', '.join(missing_fields)}")
            return {'datos': data, 'count': len(data)}
        
        else:
            raise ValidationError("Datos de resultados deben ser un objeto o array")

@login_required
@require_http_methods(["GET"])
def test_destination_connection(request, connection_id):
    """
    Prueba la conexión a la base de datos de destino
    """
    try:
        connection = get_object_or_404(DatabaseConnection, pk=connection_id)
        
        # Probar conexión a DestinoAutomatizacion
        with connections['destino'].cursor() as cursor:
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchone()
            
            if result:
                return JsonResponse({
                    'success': True,
                    'message': f'Conexión exitosa a {connection.name}',
                    'database': 'DestinoAutomatizacion',
                    'timestamp': datetime.now().isoformat()
                })
        
    except Exception as e:
        logger.error(f"Error probando conexión {connection_id}: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': f'Error de conexión: {str(e)}'
        }, status=500)

@login_required
@require_http_methods(["GET"])
def get_table_structure(request, connection_id, table_name):
    """
    Obtiene la estructura de una tabla específica
    """
    try:
        connection = get_object_or_404(DatabaseConnection, pk=connection_id)
        
        with connections['destino'].cursor() as cursor:
            # Query para obtener estructura de tabla
            structure_query = """
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = ? 
            ORDER BY ORDINAL_POSITION
            """
            
            cursor.execute(structure_query, [table_name.replace('dbo.', '')])
            columns = cursor.fetchall()
            
            column_info = []
            for col in columns:
                column_info.append({
                    'name': col[0],
                    'type': col[1],
                    'nullable': col[2] == 'YES',
                    'default': col[3],
                    'max_length': col[4]
                })
            
            return JsonResponse({
                'success': True,
                'table_name': table_name,
                'columns': column_info,
                'total_columns': len(column_info)
            })
    
    except Exception as e:
        logger.error(f"Error obteniendo estructura de tabla {table_name}: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': f'Error obteniendo estructura: {str(e)}'
        }, status=500)

@login_required
@require_http_methods(["GET"])
def list_transfer_results(request):
    """
    Lista los resultados de transferencias recientes
    """
    try:
        # Obtener últimos resultados
        resultados = ResultadosProcesados.objects.using('destino').all()[:50]
        
        results_data = []
        for resultado in resultados:
            results_data.append({
                'resultado_id': resultado.ResultadoID,
                'proceso_id': resultado.ProcesoID,
                'fecha_registro': resultado.FechaRegistro.isoformat() if resultado.FechaRegistro else None,
                'usuario_responsable': resultado.UsuarioResponsable,
                'estado_proceso': resultado.EstadoProceso,
                'tipo_operacion': resultado.TipoOperacion,
                'registros_afectados': resultado.RegistrosAfectados,
                'tiempo_ejecucion': float(resultado.TiempoEjecucion) if resultado.TiempoEjecucion else None
            })
        
        return JsonResponse({
            'success': True,
            'resultados': results_data,
            'total': len(results_data)
        })
    
    except Exception as e:
        logger.error(f"Error listando resultados: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': f'Error listando resultados: {str(e)}'
        }, status=500)
