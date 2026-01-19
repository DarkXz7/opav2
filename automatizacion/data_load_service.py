#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Servicio Robusto de Carga de Datos
Maneja la transferencia completa de datos con validación, logging y seguimiento
"""
import os
import django
import uuid
import json
import time
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional
from contextlib import contextmanager
from django.db import transaction, connections
from django.core.exceptions import ValidationError

# Configure Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proyecto_automatizacion.settings')
django.setup()

from automatizacion.models_destino import ResultadosProcesados
from automatizacion.logs.models_logs import ProcesoLog
from automatizacion.logs.process_tracker import ProcessTracker

class DataLoadService:
    """
    Servicio robusto para carga de datos con validación, transferencia y registro completo
    """
    
    def __init__(self):
        self.batch_size = 1000
        self.max_retries = 3
        
    def execute_data_load(self, 
                         source_database: str,
                         source_table: str,
                         target_database: str = 'destino',
                         validation_rules: Optional[Dict] = None,
                         transform_function: Optional[callable] = None) -> Dict[str, Any]:
        """
        Ejecuta una carga completa de datos con validación, transferencia y registro
        
        Args:
            source_database: Nombre de la base de datos origen
            source_table: Nombre de la tabla origen
            target_database: Base de datos destino (default: 'destino')
            validation_rules: Reglas de validación personalizadas
            transform_function: Función de transformación de datos opcional
            
        Returns:
            Dict con resultados detallados del proceso
        """
        proceso_id = str(uuid.uuid4())
        proceso_nombre = f"CARGA_DATOS_{source_table.upper()}"
        inicio_proceso = time.time()
        
        # Inicializar tracking del proceso
        process_tracker = ProcessTracker(proceso_nombre)
        process_tracker.iniciar_proceso(parametros={"source_database": source_database, "source_table": source_table})
        
        try:
            # 1. VALIDACIÓN DE DATOS DE ORIGEN
            print(f"=== INICIANDO CARGA DE DATOS ===")
            print(f"ProcesoID: {proceso_id}")
            print(f"Origen: {source_database}.{source_table}")
            print(f"Destino: {target_database}")
            
            validation_result = self._validate_source_data(
                source_database, source_table, validation_rules
            )
            
            if not validation_result['valid']:
                return self._handle_validation_failure(
                    proceso_id, proceso_nombre, validation_result, inicio_proceso
                )
            
            # 2. EXTRACCIÓN DE DATOS
            print(f"✓ Validación exitosa - {validation_result['record_count']} registros")
            source_data = self._extract_source_data(source_database, source_table)
            
            # 3. TRANSFORMACIÓN (si se especifica)
            if transform_function:
                print("🔄 Aplicando transformaciones...")
                source_data = self._apply_transformations(source_data, transform_function)
            
            # 4. TRANSFERENCIA DE DATOS
            print("📤 Iniciando transferencia...")
            transfer_result = self._transfer_data_to_destination(
                source_data, target_database, proceso_id
            )
            
            # 5. REGISTRO DE RESULTADOS
            duration = time.time() - inicio_proceso
            
            if transfer_result['success']:
                return self._handle_successful_load(
                    proceso_id, proceso_nombre, source_database, source_table,
                    target_database, transfer_result, validation_result, duration
                )
            else:
                return self._handle_failed_load(
                    proceso_id, proceso_nombre, transfer_result, duration
                )
                
        except Exception as e:
            duration = time.time() - inicio_proceso
            return self._handle_critical_error(
                proceso_id, proceso_nombre, str(e), duration
            )
    
    def _validate_source_data(self, database: str, table: str, 
                            rules: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Valida los datos de la tabla origen
        """
        try:
            print("🔍 Validando datos de origen...")
            
            # Validaciones básicas
            validations = {
                'valid': True,
                'errors': [],
                'warnings': [],
                'record_count': 0,
                'null_records': 0,
                'duplicate_records': 0
            }
            
            # Para este ejemplo, simulamos la validación de una tabla
            # En un caso real, aquí harías consultas a la base de datos origen
            with connections[database if database != 'origen' else 'default'].cursor() as cursor:
                # Contar registros totales
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                validations['record_count'] = cursor.fetchone()[0]
                
                if validations['record_count'] == 0:
                    validations['valid'] = False
                    validations['errors'].append("La tabla origen está vacía")
                    return validations
                
                # Validar registros nulos en campos críticos (ejemplo)
                if rules and 'required_fields' in rules:
                    for field in rules['required_fields']:
                        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {field} IS NULL")
                        null_count = cursor.fetchone()[0]
                        if null_count > 0:
                            validations['warnings'].append(
                                f"Campo {field} tiene {null_count} valores nulos"
                            )
                            validations['null_records'] += null_count
                
                # Validar duplicados si se especifica una clave única
                if rules and 'unique_field' in rules:
                    unique_field = rules['unique_field']
                    cursor.execute(f"""
                        SELECT COUNT(*) - COUNT(DISTINCT {unique_field}) 
                        FROM {table} WHERE {unique_field} IS NOT NULL
                    """)
                    duplicates = cursor.fetchone()[0]
                    if duplicates > 0:
                        validations['warnings'].append(
                            f"Se encontraron {duplicates} duplicados en {unique_field}"
                        )
                        validations['duplicate_records'] = duplicates
            
            print(f"   - Registros encontrados: {validations['record_count']}")
            print(f"   - Registros con nulos: {validations['null_records']}")
            print(f"   - Registros duplicados: {validations['duplicate_records']}")
            
            if validations['warnings']:
                for warning in validations['warnings']:
                    print(f"   ⚠️  {warning}")
            
            return validations
            
        except Exception as e:
            return {
                'valid': False,
                'errors': [f"Error en validación: {str(e)}"],
                'warnings': [],
                'record_count': 0,
                'null_records': 0,
                'duplicate_records': 0
            }
    
    def _extract_source_data(self, database: str, table: str) -> List[Dict[str, Any]]:
        """
        Extrae datos de la tabla origen
        """
        print("📊 Extrayendo datos de origen...")
        
        data = []
        with connections[database if database != 'origen' else 'default'].cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table}")
            columns = [col[0] for col in cursor.description]
            
            for row in cursor.fetchall():
                record = dict(zip(columns, row))
                # Convertir valores especiales para JSON
                for key, value in record.items():
                    if isinstance(value, datetime):
                        record[key] = value.isoformat()
                    elif value is None:
                        record[key] = None
                data.append(record)
        
        print(f"   ✓ {len(data)} registros extraídos")
        return data
    
    def _apply_transformations(self, data: List[Dict], transform_func: callable) -> List[Dict]:
        """
        Aplica transformaciones a los datos
        """
        transformed_data = []
        errors = 0
        
        for record in data:
            try:
                transformed_record = transform_func(record)
                if transformed_record:  # Solo agregar si la transformación fue exitosa
                    transformed_data.append(transformed_record)
            except Exception as e:
                errors += 1
                print(f"   ⚠️  Error transformando registro: {str(e)}")
        
        print(f"   ✓ {len(transformed_data)} registros transformados ({errors} errores)")
        return transformed_data
    
    def _transfer_data_to_destination(self, data: List[Dict], 
                                    target_db: str, proceso_id: str) -> Dict[str, Any]:
        """
        Transfiere datos a la base de datos destino
        """
        print("🚀 Transfiriendo a base de datos destino...")
        
        transferred = 0
        failed = 0
        errors = []
        
        try:
            with transaction.atomic(using=target_db):
                for i, record in enumerate(data):
                    try:
                        # Crear registro en ResultadosProcesados
                        resultado = ResultadosProcesados(
                            ProcesoID=proceso_id,
                            DatosProcesados=json.dumps(record, ensure_ascii=False),
                            UsuarioResponsable='SISTEMA_CARGA',
                            EstadoProceso='TRANSFERIDO',
                            TipoOperacion='CARGA_MASIVA',
                            RegistrosAfectados=1,
                            MetadatosProceso=json.dumps({
                                'indice_registro': i + 1,
                                'timestamp_transferencia': datetime.now().isoformat()
                            })
                        )
                        resultado.save(using=target_db)
                        transferred += 1
                        
                        # Progress feedback cada 100 registros
                        if (i + 1) % 100 == 0:
                            print(f"   📈 Progreso: {i + 1}/{len(data)} registros")
                            
                    except Exception as e:
                        failed += 1
                        error_msg = f"Error en registro {i + 1}: {str(e)}"
                        errors.append(error_msg)
                        print(f"   ❌ {error_msg}")
                        
                        # Si hay demasiados errores, fallar la transacción
                        if failed > len(data) * 0.1:  # Más del 10% de errores
                            raise Exception(f"Demasiados errores en transferencia: {failed}")
            
            success = failed == 0 or failed < len(data) * 0.05  # Menos del 5% de errores
            
            return {
                'success': success,
                'transferred': transferred,
                'failed': failed,
                'total': len(data),
                'errors': errors,
                'status': 'COMPLETADO' if success else ('PARCIAL' if transferred > 0 else 'FALLIDO')
            }
            
        except Exception as e:
            return {
                'success': False,
                'transferred': 0,
                'failed': len(data),
                'total': len(data),
                'errors': [str(e)],
                'status': 'FALLIDO'
            }
    
    def _handle_successful_load(self, proceso_id: str, proceso_nombre: str,
                              source_db: str, source_table: str, target_db: str,
                              transfer_result: Dict, validation_result: Dict,
                              duration: float) -> Dict[str, Any]:
        """
        Maneja una carga exitosa
        """
        print("✅ CARGA COMPLETADA EXITOSAMENTE")
        
        # Crear entrada de log principal
        metadata = {
            'base_datos_origen': source_db,
            'tabla_origen': source_table,
            'base_datos_destino': target_db,
            'registros_origen': validation_result['record_count'],
            'registros_transferidos': transfer_result['transferred'],
            'registros_fallidos': transfer_result['failed'],
            'estado_transferencia': transfer_result['status'],
            'duracion_segundos': round(duration, 2),
            'timestamp_inicio': datetime.now().isoformat(),
            'validaciones': {
                'registros_nulos': validation_result.get('null_records', 0),
                'registros_duplicados': validation_result.get('duplicate_records', 0),
                'advertencias': validation_result.get('warnings', [])
            }
        }
        
        # Finalizar proceso exitosamente
        process_tracker = ProcessTracker(proceso_nombre)
        process_tracker.complete_process(
            proceso_id, 
            metadata=json.dumps(metadata, ensure_ascii=False)
        )
        
        # Crear registro resumen en ResultadosProcesados
        resumen = ResultadosProcesados(
            ProcesoID=f"{proceso_id}_RESUMEN",
            DatosProcesados=json.dumps({
                'tipo': 'RESUMEN_CARGA',
                'proceso_original': proceso_id,
                'resultados': metadata
            }, ensure_ascii=False),
            UsuarioResponsable='SISTEMA_CARGA',
            EstadoProceso='COMPLETADO',
            TipoOperacion='RESUMEN_CARGA',
            RegistrosAfectados=transfer_result['transferred'],
            TiempoEjecucion=round(duration, 2),
            MetadatosProceso=json.dumps(metadata, ensure_ascii=False)
        )
        resumen.save(using='destino')
        
        return {
            'success': True,
            'proceso_id': proceso_id,
            'registros_procesados': transfer_result['transferred'],
            'duracion': duration,
            'metadata': metadata,
            'resumen_id': resumen.ResultadoID
        }
    
    def _handle_failed_load(self, proceso_id: str, proceso_nombre: str,
                          transfer_result: Dict, duration: float) -> Dict[str, Any]:
        """
        Maneja una carga fallida
        """
        print("❌ CARGA FALLIDA")
        
        error_details = {
            'registros_intentados': transfer_result['total'],
            'registros_transferidos': transfer_result['transferred'], 
            'registros_fallidos': transfer_result['failed'],
            'errores': transfer_result['errors'][:10],  # Primeros 10 errores
            'estado': transfer_result['status']
        }
        
        # Finalizar proceso con error
        process_tracker = ProcessTracker(proceso_nombre)
        process_tracker.fail_process(
            proceso_id,
            error_message=f"Transferencia {transfer_result['status']}: {transfer_result['failed']} registros fallidos",
            metadata=json.dumps(error_details, ensure_ascii=False)
        )
        
        return {
            'success': False,
            'proceso_id': proceso_id,
            'error': f"Carga {transfer_result['status']}",
            'detalles': error_details,
            'duracion': duration
        }
    
    def _handle_validation_failure(self, proceso_id: str, proceso_nombre: str,
                                 validation_result: Dict, inicio_proceso: float) -> Dict[str, Any]:
        """
        Maneja fallas de validación
        """
        print("❌ VALIDACIÓN FALLIDA")
        
        duration = time.time() - inicio_proceso
        error_msg = "; ".join(validation_result['errors'])
        
        process_tracker = ProcessTracker(proceso_nombre)
        process_tracker.fail_process(
            proceso_id,
            error_message=f"Validación fallida: {error_msg}",
            metadata=json.dumps(validation_result, ensure_ascii=False)
        )
        
        return {
            'success': False,
            'proceso_id': proceso_id,
            'error': 'Validación fallida',
            'detalles': validation_result,
            'duracion': duration
        }
    
    def _handle_critical_error(self, proceso_id: str, proceso_nombre: str,
                             error: str, duration: float) -> Dict[str, Any]:
        """
        Maneja errores críticos
        """
        print(f"💥 ERROR CRÍTICO: {error}")
        
        process_tracker = ProcessTracker(proceso_nombre)
        process_tracker.fail_process(
            proceso_id,
            error_message=f"Error crítico: {error}"
        )
        
        return {
            'success': False,
            'proceso_id': proceso_id,
            'error': 'Error crítico en carga',
            'detalles': {'error_critico': error},
            'duracion': duration
        }

# Instancia global del servicio
data_load_service = DataLoadService()
