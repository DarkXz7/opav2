#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Vistas para Carga Robusta de Datos
Endpoints REST para manejar la carga completa con validación y logging
"""
import json
from django.http import JsonResponse
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.db import connections
from django.core.exceptions import ValidationError

from .data_load_service import data_load_service
from .logs.process_tracker import ProcessTracker

@method_decorator(csrf_exempt, name='dispatch')
class DataLoadView(View):
    """
    Vista principal para manejo de carga de datos
    """
    
    def post(self, request):
        """
        Ejecuta una carga completa de datos
        
        Body JSON esperado:
        {
            "source_database": "nombre_bd_origen",
            "source_table": "nombre_tabla",
            "target_database": "destino", 
            "validation_type": "users|transactions|inventory|custom",
            "transformation_type": "users|transactions|inventory|none",
            "custom_validation_rules": {...}, // opcional
            "options": {
                "allow_partial_success": true,
                "max_error_rate": 0.05
            }
        }
        """
        try:
            # Parsear datos de entrada
            body = json.loads(request.body.decode('utf-8'))
            
            source_database = body.get('source_database', 'default')
            source_table = body.get('source_table')
            target_database = body.get('target_database', 'destino')
            validation_type = body.get('validation_type', 'custom')
            transformation_type = body.get('transformation_type', 'none')
            custom_rules = body.get('custom_validation_rules', {})
            options = body.get('options', {})
            
            # Validar parámetros requeridos
            if not source_table:
                return JsonResponse({
                    'success': False,
                    'error': 'source_table es requerido',
                    'code': 'MISSING_PARAMETER'
                }, status=400)
            
            # Obtener reglas de validación
            validation_rules = self._get_validation_rules(validation_type, custom_rules)
            
            # Obtener función de transformación
            transform_function = self._get_transform_function(transformation_type)
            
            # Ejecutar carga de datos
            result = data_load_service.execute_data_load(
                source_database=source_database,
                source_table=source_table,
                target_database=target_database,
                validation_rules=validation_rules,
                transform_function=transform_function
            )
            
            # Determinar código de estado HTTP
            status_code = 200 if result['success'] else 422
            
            return JsonResponse(result, status=status_code)
            
        except json.JSONDecodeError:
            return JsonResponse({
                'success': False,
                'error': 'JSON inválido en el body de la solicitud',
                'code': 'INVALID_JSON'
            }, status=400)
            
        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': f'Error interno del servidor: {str(e)}',
                'code': 'INTERNAL_ERROR'
            }, status=500)
    
    def get(self, request):
        """
        Obtiene información sobre cargas disponibles y estado del sistema
        """
        try:
            # Obtener estadísticas de cargas recientes
            with connections['logs'].cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_cargas,
                        SUM(CASE WHEN Estado = 'COMPLETADO' THEN 1 ELSE 0 END) as exitosas,
                        SUM(CASE WHEN Estado = 'FALLIDO' THEN 1 ELSE 0 END) as fallidas,
                        AVG(DuracionSegundos) as duracion_promedio
                    FROM ProcesoLog 
                    WHERE NombreProceso LIKE 'CARGA_DATOS_%'
                      AND FechaEjecucion >= DATEADD(day, -7, GETDATE())
                """)
                stats = cursor.fetchone()
            
            # Obtener tablas disponibles para carga
            available_tables = self._get_available_tables()
            
            return JsonResponse({
                'success': True,
                'estadisticas_ultimos_7_dias': {
                    'total_cargas': stats[0] or 0,
                    'cargas_exitosas': stats[1] or 0,
                    'cargas_fallidas': stats[2] or 0,
                    'duracion_promedio_segundos': round(stats[3] or 0, 2)
                },
                'tablas_disponibles': available_tables,
                'tipos_validacion_soportados': [
                    'users', 'transactions', 'inventory', 'custom'
                ],
                'tipos_transformacion_soportados': [
                    'users', 'transactions', 'inventory', 'none'
                ]
            })
            
        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': f'Error obteniendo información: {str(e)}',
                'code': 'INFO_ERROR'
            }, status=500)
    
    def _get_validation_rules(self, validation_type: str, custom_rules: dict) -> dict:
        """
        Obtiene las reglas de validación según el tipo
        """
        if validation_type == 'users':
            return DataValidators.create_user_validation_rules()
        elif validation_type == 'transactions':
            return DataValidators.create_transaction_validation_rules()
        elif validation_type == 'inventory':
            return DataValidators.create_inventory_validation_rules()
        elif validation_type == 'custom':
            return custom_rules
        else:
            return {}
    
    def _get_transform_function(self, transformation_type: str):
        """
        Obtiene la función de transformación según el tipo
        """
        if transformation_type == 'users':
            return DataTransformations.clean_user_data
        elif transformation_type == 'transactions':
            return DataTransformations.normalize_transaction_data
        elif transformation_type == 'inventory':
            return DataTransformations.standardize_inventory_data
        else:
            return None
    
    def _get_available_tables(self) -> list:
        """
        Obtiene lista de tablas disponibles para carga
        """
        try:
            tables = []
            
            # Tablas de la base de datos default (SQLite)
            with connections['default'].cursor() as cursor:
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name NOT LIKE 'django_%'
                    ORDER BY name
                """)
                default_tables = [f"default.{row[0]}" for row in cursor.fetchall()]
                tables.extend(default_tables)
            
            # Agregar tabla de usuarios destino como ejemplo
            tables.append("destino.dbo.Usuarios")
            
            return tables
            
        except Exception:
            return ["Error obteniendo tablas disponibles"]

@method_decorator(csrf_exempt, name='dispatch')
class LoadStatusView(View):
    """
    Vista para consultar el estado de cargas específicas
    """
    
    def get(self, request, proceso_id=None):
        """
        Obtiene el estado de una carga específica o las últimas cargas
        """
        try:
            if proceso_id:
                # Obtener información de un proceso específico
                with connections['logs'].cursor() as cursor:
                    cursor.execute("""
                        SELECT 
                            ProcesoID, NombreProceso, Estado, FechaEjecucion,
                            DuracionSegundos, MensajeError, MetadatosProceso
                        FROM ProcesoLog 
                        WHERE ProcesoID = ?
                    """, [proceso_id])
                    
                    row = cursor.fetchone()
                    if not row:
                        return JsonResponse({
                            'success': False,
                            'error': 'Proceso no encontrado',
                            'code': 'NOT_FOUND'
                        }, status=404)
                    
                    # Obtener registros transferidos
                    with connections['destino'].cursor() as dest_cursor:
                        dest_cursor.execute("""
                            SELECT COUNT(*) FROM ResultadosProcesados 
                            WHERE ProcesoID = ? OR ProcesoID LIKE ?
                        """, [proceso_id, f"{proceso_id}_%"])
                        registros_count = dest_cursor.fetchone()[0]
                    
                    return JsonResponse({
                        'success': True,
                        'proceso': {
                            'proceso_id': row[0],
                            'nombre_proceso': row[1],
                            'estado': row[2],
                            'fecha_ejecucion': row[3].isoformat() if row[3] else None,
                            'duracion_segundos': row[4],
                            'mensaje_error': row[5],
                            'metadatos': json.loads(row[6]) if row[6] else {},
                            'registros_transferidos': registros_count
                        }
                    })
            else:
                # Obtener últimas cargas
                with connections['logs'].cursor() as cursor:
                    cursor.execute("""
                        SELECT TOP 20
                            ProcesoID, NombreProceso, Estado, FechaEjecucion,
                            DuracionSegundos, MensajeError
                        FROM ProcesoLog 
                        WHERE NombreProceso LIKE %s
                        ORDER BY FechaEjecucion DESC
                    """, ['CARGA_DATOS_%'])
                    
                    cargas = []
                    for row in cursor.fetchall():
                        cargas.append({
                            'proceso_id': row[0],
                            'nombre_proceso': row[1],
                            'estado': row[2],
                            'fecha_ejecucion': row[3].isoformat() if row[3] else None,
                            'duracion_segundos': row[4],
                            'mensaje_error': row[5]
                        })
                    
                    return JsonResponse({
                        'success': True,
                        'cargas_recientes': cargas
                    })
                    
        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': f'Error consultando estado: {str(e)}',
                'code': 'STATUS_ERROR'
            }, status=500)

@method_decorator(csrf_exempt, name='dispatch')
class DataValidationView(View):
    """
    Vista para validar datos sin ejecutar la carga
    """
    
    def post(self, request):
        """
        Valida datos de una tabla sin ejecutar la transferencia
        """
        try:
            body = json.loads(request.body.decode('utf-8'))
            
            source_database = body.get('source_database', 'default')
            source_table = body.get('source_table')
            validation_type = body.get('validation_type', 'custom')
            custom_rules = body.get('custom_validation_rules', {})
            
            if not source_table:
                return JsonResponse({
                    'success': False,
                    'error': 'source_table es requerido'
                }, status=400)
            
            # Obtener reglas de validación
            validation_rules = DataValidationView._get_validation_rules(
                validation_type, custom_rules
            )
            
            # Ejecutar solo validación
            load_service = data_load_service
            validation_result = load_service._validate_source_data(
                source_database, source_table, validation_rules
            )
            
            return JsonResponse({
                'success': True,
                'validacion': validation_result,
                'recomendacion': self._get_load_recommendation(validation_result)
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
    
    @staticmethod
    def _get_validation_rules(validation_type: str, custom_rules: dict) -> dict:
        """
        Obtiene reglas de validación (mismo método que DataLoadView)
        """
        if validation_type == 'users':
            return DataValidators.create_user_validation_rules()
        elif validation_type == 'transactions':
            return DataValidators.create_transaction_validation_rules()
        elif validation_type == 'inventory':
            return DataValidators.create_inventory_validation_rules()
        elif validation_type == 'custom':
            return custom_rules
        else:
            return {}
    
    def _get_load_recommendation(self, validation_result: dict) -> str:
        """
        Genera recomendación basada en los resultados de validación
        """
        if not validation_result['valid']:
            return "NO RECOMENDADO - Corregir errores antes de proceder"
        
        error_rate = (validation_result.get('null_records', 0) + 
                     validation_result.get('duplicate_records', 0)) / max(validation_result.get('record_count', 1), 1)
        
        if error_rate > 0.1:
            return "PRECAUCIÓN - Alto porcentaje de datos problemáticos"
        elif error_rate > 0.05:
            return "REVISAR - Algunos datos requieren atención"
        else:
            return "RECOMENDADO - Datos listos para carga"
