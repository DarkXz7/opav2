"""
Servicio de transferencia segura de datos a SQL Server DestinoAutomatizacion
Proporciona funcionalidades robustas para inserción de datos con manejo de errores,
reintentos automáticos y logging completo.
ACTUALIZACIÓN: Ahora usa tablas dinámicas por proceso en lugar de ResultadosProcesados única.
"""
import logging
import json
import time
import uuid
from decimal import Decimal
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from django.db import transaction, connections
from django.core.exceptions import ValidationError
from django.contrib.auth.models import User
import pyodbc
from contextlib import contextmanager

# Importar el nuevo servicio de tablas dinámicas
from .dynamic_table_service import dynamic_table_manager, DynamicTableError

# Configurar logging específico para transferencia de datos
logger = logging.getLogger('data_transfer')

class DataTransferError(Exception):
    """Excepción personalizada para errores de transferencia de datos"""
    pass

class ConnectionError(DataTransferError):
    """Error específico de conexión a base de datos"""
    pass

class ValidationError(DataTransferError):
    """Error específico de validación de datos"""
    pass

class DataTransferService:
    """
    Servicio principal para transferencia segura de datos a SQL Server
    """
    
    def __init__(self):
        self.max_retries = 3
        self.retry_delay = 1  # segundos
        self.batch_size = 1000
        self.connection_timeout = 30
        
    @contextmanager
    def get_secure_connection(self, database_alias='destino'):
        """
        Context manager para conexiones seguras con manejo automático de recursos
        """
        connection = None
        try:
            connection = connections[database_alias]
            # Verificar que la conexión esté activa
            if connection.connection is None:
                connection.connect()
            
            logger.info(f"Conexión establecida exitosamente a base de datos '{database_alias}'")
            yield connection
            
        except Exception as e:
            logger.error(f"Error estableciendo conexión a '{database_alias}': {str(e)}")
            raise ConnectionError(f"No se pudo conectar a la base de datos: {str(e)}")
        
        finally:
            if connection and connection.connection:
                try:
                    connection.close()
                    logger.debug("Conexión cerrada correctamente")
                except Exception as e:
                    logger.warning(f"Error cerrando conexión: {str(e)}")

    def validate_transfer_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Valida y sanitiza los datos antes de la transferencia
        
        Args:
            data: Diccionario con los datos a transferir
            
        Returns:
            Dict con datos validados y sanitizados
            
        Raises:
            ValidationError: Si los datos no pasan la validación
        """
        if not isinstance(data, dict):
            raise ValidationError("Los datos deben ser un diccionario")
        
        # Campos obligatorios
        required_fields = ['ProcesoID', 'DatosProcesados', 'UsuarioResponsable']
        missing_fields = [field for field in required_fields if field not in data or not data[field]]
        
        if missing_fields:
            raise ValidationError(f"Campos obligatorios faltantes: {', '.join(missing_fields)}")
        
        # Validar ProcesoID (permitir UUID válidos o strings identificadores)
        proceso_id = str(data['ProcesoID']).strip()
        if len(proceso_id) == 0:
            raise ValidationError("ProcesoID no puede estar vacío")
        elif len(proceso_id) > 36:
            # Si es muy largo, truncar pero mantener identificador único
            proceso_id = proceso_id[:32] + "-" + str(hash(proceso_id))[-3:]
        
        # Intentar validar como UUID, pero si no es válido, usar el string tal como está
        try:
            uuid_obj = uuid.UUID(proceso_id)
            data['ProcesoID'] = str(uuid_obj)
        except ValueError:
            # No es UUID válido, pero permitir string identificador
            data['ProcesoID'] = proceso_id
        
        # Validar UsuarioResponsable
        usuario = str(data['UsuarioResponsable']).strip()
        if len(usuario) > 100:
            raise ValidationError("UsuarioResponsable no puede exceder 100 caracteres")
        data['UsuarioResponsable'] = usuario
        
        # Validar y serializar DatosProcesados
        try:
            if isinstance(data['DatosProcesados'], (dict, list)):
                data['DatosProcesados'] = json.dumps(data['DatosProcesados'], ensure_ascii=False)
            else:
                data['DatosProcesados'] = str(data['DatosProcesados'])
        except Exception as e:
            raise ValidationError(f"Error serializando DatosProcesados: {str(e)}")
        
        # Campos opcionales con valores por defecto
        data.setdefault('EstadoProceso', 'COMPLETADO')
        data.setdefault('TipoOperacion', 'TRANSFERENCIA_DATOS')
        data.setdefault('RegistrosAfectados', 0)
        
        # Validar TiempoEjecucion si está presente
        if 'TiempoEjecucion' in data and data['TiempoEjecucion'] is not None:
            try:
                data['TiempoEjecucion'] = Decimal(str(data['TiempoEjecucion']))
            except Exception:
                raise ValidationError("TiempoEjecucion debe ser un número válido")
        
        # Serializar MetadatosProceso si está presente
        if 'MetadatosProceso' in data and data['MetadatosProceso']:
            try:
                if isinstance(data['MetadatosProceso'], (dict, list)):
                    data['MetadatosProceso'] = json.dumps(data['MetadatosProceso'], ensure_ascii=False)
            except Exception as e:
                raise ValidationError(f"Error serializando MetadatosProceso: {str(e)}")
        
        logger.info(f"Datos validados exitosamente para ProcesoID: {data['ProcesoID']}")
        return data

    def execute_with_retry(self, operation, *args, **kwargs):
        """
        Ejecuta una operación con reintentos automáticos en caso de fallos transitorios
        """
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                return operation(*args, **kwargs)
            
            except (pyodbc.OperationalError, pyodbc.DatabaseError) as e:
                last_exception = e
                error_code = getattr(e, 'args', [None])[0]
                
                # Códigos de error que justifican reintento
                retriable_errors = [
                    '08S01',  # Communication link failure
                    '40001',  # Serialization failure
                    '40P01',  # Deadlock detected
                    'HYT00',  # Timeout expired
                ]
                
                if any(code in str(error_code) for code in retriable_errors):
                    wait_time = self.retry_delay * (2 ** attempt)  # Backoff exponencial
                    logger.warning(f"Intento {attempt + 1} falló, reintentando en {wait_time}s: {str(e)}")
                    time.sleep(wait_time)
                    continue
                else:
                    # Error no retriable
                    logger.error(f"Error no retriable: {str(e)}")
                    raise
            
            except Exception as e:
                # Otros errores no retriables
                logger.error(f"Error no retriable en intento {attempt + 1}: {str(e)}")
                raise
        
        # Si llegamos aquí, todos los reintentos fallaron
        logger.error(f"Operación falló después de {self.max_retries} intentos")
        raise ConnectionError(f"Operación falló después de {self.max_retries} intentos: {str(last_exception)}")

    def insert_single_record(self, data: Dict[str, Any]) -> int:
        """
        Inserta un solo registro en ResultadosProcesados usando Django ORM
        
        Args:
            data: Datos validados para insertar
            
        Returns:
            int: ID del registro insertado
        """
        def _insert_operation():
            try:
                from .models_destino import ResultadosProcesados
                
                # Crear instancia del modelo
                resultado = ResultadosProcesados(
                    ProcesoID=data['ProcesoID'],
                    DatosProcesados=data['DatosProcesados'],
                    UsuarioResponsable=data['UsuarioResponsable'],
                    EstadoProceso=data.get('EstadoProceso', 'COMPLETADO'),
                    TipoOperacion=data.get('TipoOperacion', 'TRANSFERENCIA_DATOS'),
                    RegistrosAfectados=data.get('RegistrosAfectados', 0),
                    TiempoEjecucion=data.get('TiempoEjecucion'),
                    MetadatosProceso=data.get('MetadatosProceso')
                )
                
                # Usar la base de datos 'destino'
                resultado.save(using='destino')
                
                logger.info(f"Registro insertado exitosamente con ID: {resultado.ResultadoID}")
                return resultado.ResultadoID
                
            except Exception as e:
                logger.error(f"Error insertando registro: {str(e)}")
                raise DataTransferError(f"Error en inserción: {str(e)}")
        
        return self.execute_with_retry(_insert_operation)

    def insert_batch_records(self, data_list: List[Dict[str, Any]]) -> List[int]:
        """
        Inserta múltiples registros en lotes para optimizar rendimiento
        
        Args:
            data_list: Lista de diccionarios con datos a insertar
            
        Returns:
            List[int]: Lista de IDs de registros insertados
        """
        if not data_list:
            return []
        
        inserted_ids = []
        
        # Procesar en lotes
        for i in range(0, len(data_list), self.batch_size):
            batch = data_list[i:i + self.batch_size]
            logger.info(f"Procesando lote {i//self.batch_size + 1}: {len(batch)} registros")
            
            def _batch_insert_operation():
                with self.get_secure_connection() as connection:
                    with transaction.atomic(using='destino'):
                        batch_ids = []
                        for record_data in batch:
                            validated_data = self.validate_transfer_data(record_data)
                            record_id = self.insert_single_record(validated_data)
                            batch_ids.append(record_id)
                        return batch_ids
            
            batch_ids = self.execute_with_retry(_batch_insert_operation)
            inserted_ids.extend(batch_ids)
        
        logger.info(f"Lote completo procesado: {len(inserted_ids)} registros insertados")
        return inserted_ids

    def transfer_processed_data(self, 
                              proceso_id: str,
                              datos_procesados: Any,
                              usuario_responsable: str,
                              metadata: Optional[Dict[str, Any]] = None,
                              **kwargs) -> Tuple[bool, Dict[str, Any]]:
        """
        Función principal para transferir datos procesados
        
        Args:
            proceso_id: UUID del proceso
            datos_procesados: Datos a transferir
            usuario_responsable: Usuario que ejecuta la transferencia
            metadata: Metadatos adicionales del proceso
            **kwargs: Parámetros adicionales
            
        Returns:
            Tuple[bool, Dict]: (éxito, información_resultado)
        """
        start_time = datetime.now()
        transfer_info = {
            'proceso_id': proceso_id,
            'usuario': usuario_responsable,
            'inicio': start_time,
            'exito': False,
            'resultado_id': None,
            'error': None,
            'tiempo_ejecucion': None
        }
        
        try:
            # Preparar datos para transferencia
            transfer_data = {
                'ProcesoID': proceso_id,
                'DatosProcesados': datos_procesados,
                'UsuarioResponsable': usuario_responsable,
                'EstadoProceso': kwargs.get('estado_proceso', 'COMPLETADO'),
                'TipoOperacion': kwargs.get('tipo_operacion', 'TRANSFERENCIA_DATOS'),
                'RegistrosAfectados': kwargs.get('registros_afectados', 0),
                'TiempoEjecucion': kwargs.get('tiempo_ejecucion'),
                'MetadatosProceso': metadata
            }
            
            # Validar datos
            validated_data = self.validate_transfer_data(transfer_data)
            
            # Insertar registro
            resultado_id = self.insert_single_record(validated_data)
            
            # Calcular tiempo de ejecución
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            # Actualizar información de resultado
            transfer_info.update({
                'exito': True,
                'resultado_id': resultado_id,
                'fin': end_time,
                'tiempo_ejecucion': execution_time
            })
            
            logger.info(f"Transferencia exitosa - ProcesoID: {proceso_id}, ResultadoID: {resultado_id}")
            return True, transfer_info
        
        except Exception as e:
            # Registrar error
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            transfer_info.update({
                'exito': False,
                'error': str(e),
                'fin': end_time,
                'tiempo_ejecucion': execution_time
            })
            
            logger.error(f"Error en transferencia - ProcesoID: {proceso_id}, Error: {str(e)}")
            return False, transfer_info

    def transfer_to_dynamic_table(self, 
                                 process_name: str,
                                 proceso_id: str,
                                 datos_procesados: Any,
                                 usuario_responsable: str,
                                 metadata: Optional[Dict[str, Any]] = None,
                                 recreate_table: bool = True,
                                 **kwargs) -> Tuple[bool, Dict[str, Any]]:
        """
        Transfiere datos a una tabla dinámica específica del proceso
        
        Args:
            process_name: Nombre del proceso (se usará para generar nombre de tabla)
            proceso_id: UUID del proceso
            datos_procesados: Datos a transferir
            usuario_responsable: Usuario que ejecuta la transferencia
            metadata: Metadatos adicionales del proceso
            recreate_table: Si True, trunca/recrea la tabla. Si False, la mantiene
            **kwargs: Parámetros adicionales
            
        Returns:
            Tuple[bool, Dict]: (éxito, información_resultado)
        """
        start_time = datetime.now()
        transfer_info = {
            'proceso_id': proceso_id,
            'process_name': process_name,
            'table_name': None,
            'usuario': usuario_responsable,
            'inicio': start_time,
            'exito': False,
            'resultado_id': None,
            'error': None,
            'tiempo_ejecucion': None
        }
        
        try:
            logger.info(f"🚀 Iniciando transferencia a tabla dinámica para proceso: '{process_name}'")
            
            # 1. Asegurar que la tabla existe (crear o limpiar según recreate_table)
            table_name = dynamic_table_manager.ensure_process_table(
                process_name, 
                recreate=recreate_table
            )
            transfer_info['table_name'] = table_name
            
            logger.info(f"📋 Tabla asegurada: '{table_name}' (recreate={recreate_table})")
            
            # 2. Preparar datos para inserción
            # Asegurar que DatosProcesados siempre sea un JSON válido
            datos_json = self._ensure_json_serializable(datos_procesados)
            metadata_json = self._ensure_json_serializable(metadata) if metadata else None
            
            transfer_data = {
                'ProcesoID': proceso_id,
                'NombreProceso': process_name,  # NUEVO: Nombre del proceso del frontend
                'DatosProcesados': datos_json,
                'UsuarioResponsable': usuario_responsable,
                'EstadoProceso': kwargs.get('estado_proceso', 'COMPLETADO'),
                'TipoOperacion': kwargs.get('tipo_operacion', f'PROCESO_{process_name.upper().replace(" ", "_")}'),
                'RegistrosAfectados': kwargs.get('registros_afectados', 0),
                'TiempoEjecucion': kwargs.get('tiempo_ejecucion'),
                'MetadatosProceso': metadata_json
            }
            
            # 3. Validar datos antes de inserción
            validated_data = self.validate_transfer_data(transfer_data)
            logger.info(f"✅ Datos validados correctamente")
            
            # 4. Insertar en la tabla específica del proceso
            resultado_id = dynamic_table_manager.insert_to_process_table(
                table_name, 
                validated_data
            )
            
            # 5. Calcular tiempo de ejecución
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            # 6. Actualizar información de resultado
            transfer_info.update({
                'exito': True,
                'resultado_id': resultado_id,
                'fin': end_time,
                'tiempo_ejecucion': execution_time
            })
            
            # 7. NUEVO: Guardar resumen en ResultadosProcesados
            try:
                resumen_resultado = self._guardar_resumen_resultados(
                    proceso_id=proceso_id,
                    nombre_proceso=process_name,
                    tabla_destino=table_name,
                    datos_procesados=validated_data,
                    usuario_responsable=usuario_responsable,
                    estado_proceso=kwargs.get('estado_proceso', 'COMPLETADO'),
                    tipo_operacion=kwargs.get('tipo_operacion', f'MIGRACION_{process_name.upper().replace(" ", "_")}'),
                    registros_afectados=kwargs.get('registros_afectados', 0),
                    tiempo_ejecucion=execution_time,
                    metadata=metadata
                )
                transfer_info['resumen_id'] = resumen_resultado
                logger.info(f"📋 Resumen guardado en ResultadosProcesados con ID: {resumen_resultado}")
            except Exception as e:
                logger.warning(f"⚠️ No se pudo guardar resumen en ResultadosProcesados: {str(e)}")
                # No fallar el proceso completo por este error
            
            logger.info(f"🎉 Transferencia exitosa - Proceso: '{process_name}', Tabla: '{table_name}', ResultadoID: {resultado_id}")
            return True, transfer_info
            
        except DynamicTableError as e:
            # Error específico de gestión de tablas dinámicas
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            error_msg = f"Error de tabla dinámica: {str(e)}"
            transfer_info.update({
                'exito': False,
                'error': error_msg,
                'fin': end_time,
                'tiempo_ejecucion': execution_time
            })
            
            logger.error(f"❌ {error_msg} - Proceso: '{process_name}'")
            
            # NUEVO: Guardar resumen de error dinámico en ResultadosProcesados
            try:
                resumen_error = self._guardar_resumen_resultados(
                    proceso_id=proceso_id,
                    nombre_proceso=process_name,
                    tabla_destino=transfer_info.get('table_name', 'Error_tabla_dinamica'),
                    datos_procesados={'error': error_msg, 'tipo': 'dynamic_table_error'},
                    usuario_responsable=usuario_responsable,
                    estado_proceso='ERROR',
                    tipo_operacion=kwargs.get('tipo_operacion', f'MIGRACION_{process_name.upper().replace(" ", "_")}'),
                    registros_afectados=0,
                    tiempo_ejecucion=execution_time,
                    metadata={'error_details': error_msg, 'error_type': 'dynamic_table_error'}
                )
                transfer_info['resumen_id'] = resumen_error
            except Exception as e:
                logger.warning(f"⚠️ No se pudo guardar resumen de error dinámico: {str(e)}")
            
            return False, transfer_info
            
        except Exception as e:
            # Error general
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            error_msg = f"Error inesperado: {str(e)}"
            transfer_info.update({
                'exito': False,
                'error': error_msg,
                'fin': end_time,
                'tiempo_ejecucion': execution_time
            })
            
            logger.error(f"❌ {error_msg} - Proceso: '{process_name}'")
            
            # NUEVO: Guardar resumen de error en ResultadosProcesados
            try:
                resumen_error = self._guardar_resumen_resultados(
                    proceso_id=proceso_id,
                    nombre_proceso=process_name,
                    tabla_destino=transfer_info.get('table_name', 'Error_sin_tabla'),
                    datos_procesados={'error': error_msg, 'tipo': 'error_general'},
                    usuario_responsable=usuario_responsable,
                    estado_proceso='ERROR',
                    tipo_operacion=kwargs.get('tipo_operacion', f'MIGRACION_{process_name.upper().replace(" ", "_")}'),
                    registros_afectados=0,
                    tiempo_ejecucion=execution_time,
                    metadata={'error_details': error_msg, 'error_type': 'general_exception'}
                )
                transfer_info['resumen_id'] = resumen_error
            except Exception as e:
                logger.warning(f"⚠️ No se pudo guardar resumen de error: {str(e)}")
            
            return False, transfer_info
    
    def _guardar_resumen_resultados(self, proceso_id: str, nombre_proceso: str, tabla_destino: str,
                                   datos_procesados: Dict, usuario_responsable: str, estado_proceso: str,
                                   tipo_operacion: str, registros_afectados: int, tiempo_ejecucion: float,
                                   metadata: Optional[Dict] = None) -> int:
        """
        Guarda un resumen del proceso en la tabla ResultadosProcesados
        
        Args:
            proceso_id: UUID del proceso
            nombre_proceso: Nombre asignado por el usuario
            tabla_destino: Nombre de la tabla específica creada (ej: Proceso_Honda)
            datos_procesados: Datos del proceso
            usuario_responsable: Usuario que ejecutó
            estado_proceso: COMPLETADO o ERROR
            tipo_operacion: Tipo de operación (ej: MIGRACION_HONDA)
            registros_afectados: Número de registros procesados
            tiempo_ejecucion: Duración en segundos
            metadata: Metadatos adicionales
            
        Returns:
            int: ID del registro creado en ResultadosProcesados
        """
        from .models_destino import ResultadosProcesados
        
        # Preparar campos/columnas procesadas
        campos_procesados = []
        if isinstance(datos_procesados, dict):
            campos_procesados = list(datos_procesados.keys())
        
        # Crear JSON resumido como requiere el usuario
        datos_json = {
            'tabla_destino': tabla_destino,
            'campos_columnas': campos_procesados,
            'total_registros_cargados': registros_afectados,
            'estado_final': estado_proceso,
            'timestamp_procesamiento': datetime.now().isoformat()
        }
        
        # Agregar información de error si aplica
        if estado_proceso == 'ERROR' and metadata:
            datos_json['detalles_error'] = metadata.get('error_details', 'Error no especificado')
        
        # Metadatos del proceso
        metadatos_proceso = {
            'version_proceso': '1.0',
            'parametros_usados': metadata or {},
            'duracion_segundos': tiempo_ejecucion,
            'tabla_creada': tabla_destino
        }
        
        # Crear registro en ResultadosProcesados
        resultado = ResultadosProcesados(
            ProcesoID=proceso_id,
            NombreProceso=nombre_proceso,
            DatosProcesados=json.dumps(datos_json, ensure_ascii=False),
            UsuarioResponsable=usuario_responsable,
            EstadoProceso=estado_proceso,
            TipoOperacion=tipo_operacion,
            RegistrosAfectados=registros_afectados,
            TiempoEjecucion=tiempo_ejecucion,
            MetadatosProceso=json.dumps(metadatos_proceso, ensure_ascii=False)
        )
        
        # Guardar usando la conexión destino
        resultado.save(using='destino')
        
        logger.info(f"✅ Resumen guardado en ResultadosProcesados - ID: {resultado.ResultadoID}")
        return resultado.ResultadoID
    
    def _ensure_json_serializable(self, data):
        """
        Asegura que los datos sean serializables a JSON
        
        Args:
            data: Cualquier tipo de dato a serializar
            
        Returns:
            str: JSON string válido
        """
        try:
            # Si ya es string, verificar que sea JSON válido
            if isinstance(data, str):
                try:
                    json.loads(data)  # Verificar que sea JSON válido
                    return data
                except json.JSONDecodeError:
                    # Si no es JSON válido, encapsularlo
                    return json.dumps({'raw_string': data})
            
            # Si es dict, list, etc., convertir directamente
            return json.dumps(data, ensure_ascii=False, default=str)
            
        except TypeError as e:
            # Si hay objetos no serializables, crear un resumen
            logger.warning(f"Datos no serializables directamente: {str(e)}")
            
            fallback_data = {
                'error_serializacion': str(e),
                'tipo_dato': str(type(data).__name__),
                'timestamp': datetime.now().isoformat()
            }
            
            # Intentar extraer información básica
            try:
                if hasattr(data, '__dict__'):
                    fallback_data['atributos'] = list(data.__dict__.keys())[:10]
                elif hasattr(data, '__len__'):
                    fallback_data['longitud'] = len(data)
                    if hasattr(data, '__getitem__') and len(data) > 0:
                        fallback_data['primer_elemento_tipo'] = str(type(data[0]).__name__)
            except:
                pass
            
            return json.dumps(fallback_data, ensure_ascii=False)
            
        except Exception as e:
            # Último recurso: JSON con información del error
            error_data = {
                'error_critico_serializacion': str(e),
                'timestamp': datetime.now().isoformat(),
                'fallback': True
            }
            return json.dumps(error_data)

# Instancia global del servicio
data_transfer_service = DataTransferService()
