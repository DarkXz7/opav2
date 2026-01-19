#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Vistas Simplificadas para el Sistema de Carga de Datos
"""
import json
import uuid
from datetime import datetime
from typing import Dict, Any

from django.http import JsonResponse
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.views.decorators.http import require_http_methods
from django.db import connections

from .data_load_service import data_load_service

@method_decorator(csrf_exempt, name='dispatch')
class DataLoadView(View):
    """
    Vista principal para manejar cargas de datos robustas
    """
    
    def post(self, request):
        """
        Ejecuta una carga de datos completa
        
        Expected JSON:
        {
            "source_database": "default",
            "source_table": "usuarios",
            "target_database": "destino",
            "validation_config": {
                "required_fields": ["nombre", "email"]
            }
        }
        """
        try:
            # Parse request data
            request_data = json.loads(request.body)
            
            # Validar parámetros requeridos
            required_params = ['source_database', 'source_table']
            missing_params = [param for param in required_params if param not in request_data]
            
            if missing_params:
                return JsonResponse({
                    'success': False,
                    'error': f'Parámetros faltantes: {", ".join(missing_params)}',
                    'required_params': required_params
                }, status=400)
            
            # Extraer parámetros
            source_database = request_data['source_database']
            source_table = request_data['source_table']
            target_database = request_data.get('target_database', 'destino')
            validation_config = request_data.get('validation_config', {})
            
            # Ejecutar carga de datos
            result = data_load_service.execute_data_load(
                source_database=source_database,
                source_table=source_table,
                target_database=target_database,
                validation_rules=validation_config
            )
            
            # Preparar respuesta
            response_data = {
                'success': result['success'],
                'proceso_id': result['proceso_id'],
                'timestamp': datetime.now().isoformat()
            }
            
            if result['success']:
                response_data.update({
                    'registros_procesados': result['registros_procesados'],
                    'duracion_segundos': result['duracion'],
                    'resumen_id': result.get('resumen_id'),
                    'metadata': result['metadata']
                })
                status_code = 200
            else:
                response_data.update({
                    'error': result['error'],
                    'detalles': result['detalles'],
                    'duracion_segundos': result['duracion']
                })
                status_code = 422  # Unprocessable Entity
            
            return JsonResponse(response_data, status=status_code)
            
        except json.JSONDecodeError:
            return JsonResponse({
                'success': False,
                'error': 'JSON inválido en el cuerpo de la petición'
            }, status=400)
        
        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': f'Error interno del servidor: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }, status=500)

@method_decorator(csrf_exempt, name='dispatch')
class DataValidationView(View):
    """
    Vista para validar datos sin transferirlos
    """
    
    def post(self, request):
        """
        Valida un conjunto de datos básico
        """
        try:
            request_data = json.loads(request.body)
            
            if 'data' not in request_data:
                return JsonResponse({
                    'success': False,
                    'error': 'Campo "data" requerido'
                }, status=400)
            
            data = request_data['data']
            
            # Validación básica
            validation_result = {
                'valid': True,
                'total_records': len(data),
                'valid_records': len(data),
                'invalid_records': 0,
                'errors': [],
                'warnings': []
            }
            
            # Verificar que data no esté vacío
            if not data:
                validation_result['valid'] = False
                validation_result['errors'].append('No hay datos para validar')
            
            return JsonResponse({
                'success': validation_result['valid'],
                'validation_result': validation_result,
                'timestamp': datetime.now().isoformat()
            })
            
        except json.JSONDecodeError:
            return JsonResponse({
                'success': False,
                'error': 'JSON inválido'
            }, status=400)
        
        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': f'Error en validación: {str(e)}'
            }, status=500)

@method_decorator(csrf_exempt, name='dispatch')
class LoadStatusView(View):
    """
    Vista para consultar el estado de un proceso de carga
    """
    
    def get(self, request, proceso_id):
        """
        Obtiene el estado de un proceso de carga específico
        """
        try:
            # Buscar en logs de proceso
            with connections['logs'].cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        ProcesoID, NombreProceso, Estado, FechaEjecucion,
                        DuracionSegundos, MensajeError, MetadatosProceso
                    FROM ProcesoLog 
                    WHERE ProcesoID = ?
                    ORDER BY FechaEjecucion DESC
                """, [proceso_id])
                
                row = cursor.fetchone()
                if not row:
                    return JsonResponse({
                        'success': False,
                        'error': 'Proceso no encontrado'
                    }, status=404)
                
                # Buscar resultados procesados
                with connections['destino'].cursor() as cursor_destino:
                    cursor_destino.execute("""
                        SELECT COUNT(*) FROM ResultadosProcesados 
                        WHERE ProcesoID = ? OR ProcesoID LIKE ?
                    """, [proceso_id, f"{proceso_id}_%"])
                    
                    registros_procesados = cursor_destino.fetchone()[0]
                
                # Parsear metadatos si existen
                metadatos = {}
                if row[6]:  # MetadatosProceso
                    try:
                        metadatos = json.loads(row[6])
                    except:
                        pass
                
                return JsonResponse({
                    'success': True,
                    'proceso': {
                        'proceso_id': row[0],
                        'nombre_proceso': row[1],
                        'estado': row[2],
                        'fecha_ejecucion': row[3].isoformat() if row[3] else None,
                        'duracion_segundos': float(row[4]) if row[4] else None,
                        'mensaje_error': row[5],
                        'registros_procesados': registros_procesados,
                        'metadatos': metadatos
                    },
                    'timestamp': datetime.now().isoformat()
                })
        
        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': f'Error consultando estado: {str(e)}'
            }, status=500)

@require_http_methods(["GET"])
def list_recent_loads(request):
    """
    Lista las cargas recientes de datos
    """
    try:
        limit = int(request.GET.get('limit', 10))
        hours = int(request.GET.get('hours', 24))
        
        with connections['logs'].cursor() as cursor:
            cursor.execute("""
                SELECT TOP (?)
                    ProcesoID, NombreProceso, Estado, FechaEjecucion,
                    DuracionSegundos, MensajeError
                FROM ProcesoLog 
                WHERE FechaEjecucion >= DATEADD(HOUR, -?, GETDATE())
                    AND NombreProceso LIKE 'CARGA_DATOS_%'
                ORDER BY FechaEjecucion DESC
            """, [limit, hours])
            
            loads = []
            for row in cursor.fetchall():
                loads.append({
                    'proceso_id': row[0],
                    'nombre_proceso': row[1],
                    'estado': row[2],
                    'fecha_ejecucion': row[3].isoformat() if row[3] else None,
                    'duracion_segundos': float(row[4]) if row[4] else None,
                    'mensaje_error': row[5]
                })
        
        return JsonResponse({
            'success': True,
            'loads': loads,
            'total': len(loads),
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': f'Error listando cargas: {str(e)}'
        }, status=500)

@require_http_methods(["GET"])
def load_statistics(request):
    """
    Obtiene estadísticas de las cargas de datos
    """
    try:
        hours = int(request.GET.get('hours', 24))
        
        with connections['logs'].cursor() as cursor:
            # Estadísticas generales
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_loads,
                    SUM(CASE WHEN Estado = 'COMPLETADO' THEN 1 ELSE 0 END) as successful_loads,
                    SUM(CASE WHEN Estado = 'FALLIDO' THEN 1 ELSE 0 END) as failed_loads,
                    AVG(DuracionSegundos) as avg_duration,
                    SUM(CASE WHEN Estado = 'PARCIAL' THEN 1 ELSE 0 END) as partial_loads
                FROM ProcesoLog 
                WHERE FechaEjecucion >= DATEADD(HOUR, -?, GETDATE())
                    AND NombreProceso LIKE 'CARGA_DATOS_%'
            """, [hours])
            
            stats_row = cursor.fetchone()
            
            # Conteo de registros procesados
            with connections['destino'].cursor() as cursor_destino:
                cursor_destino.execute("""
                    SELECT COUNT(*) FROM ResultadosProcesados 
                    WHERE FechaRegistro >= DATEADD(HOUR, -?, GETDATE())
                        AND TipoOperacion = 'CARGA_MASIVA'
                """, [hours])
                
                total_records = cursor_destino.fetchone()[0]
            
            statistics = {
                'period_hours': hours,
                'total_loads': stats_row[0] or 0,
                'successful_loads': stats_row[1] or 0,
                'failed_loads': stats_row[2] or 0,
                'partial_loads': stats_row[4] or 0,
                'success_rate': (stats_row[1] / stats_row[0] * 100) if stats_row[0] > 0 else 0,
                'average_duration_seconds': float(stats_row[3]) if stats_row[3] else 0,
                'total_records_processed': total_records
            }
        
        return JsonResponse({
            'success': True,
            'statistics': statistics,
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': f'Error obteniendo estadísticas: {str(e)}'
        }, status=500)
