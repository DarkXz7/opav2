from django.db import models
import json
from django.utils import timezone

class DataSourceType(models.Model):
    """
    Define el tipo de origen de datos (Excel, CSV, SQL Server)
    """
    name = models.CharField(max_length=50)
    description = models.TextField(blank=True, null=True)

    def __str__(self):
        return self.name

class DatabaseConnection(models.Model):
    """
    Almacena información de conexión a servidor de base de datos
    """
    name = models.CharField(max_length=100, unique=True)
    server = models.CharField(max_length=255)
    username = models.CharField(max_length=100)
    password = models.CharField(max_length=255)
    port = models.IntegerField(default=1433)
    created_at = models.DateTimeField(auto_now_add=True)
    last_used = models.DateTimeField(null=True, blank=True)
    selected_database = models.CharField(max_length=100, blank=True, null=True)
    
    # Campo para almacenar todas las bases de datos disponibles
    available_databases = models.JSONField(null=True, blank=True)
    
    def __str__(self):
        if self.selected_database:
            return f"{self.name} - {self.server}/{self.selected_database}"
        return f"{self.name} - {self.server}"
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'server': self.server,
            'selected_database': self.selected_database,
            'username': self.username,
            'password': self.password,
            'port': self.port,
            'available_databases': self.available_databases or []
        }

class DataSource(models.Model):
    """
    Representa una fuente de datos (archivo o conexión a BD)
    Soporta almacenamiento local y en la nube (OneDrive)
    """
    TYPE_CHOICES = [
        ('excel', 'Excel (.xlsx)'),
        ('csv', 'CSV'),
        ('sql', 'SQL Server'),
    ]
    
    STORAGE_CHOICES = [
        ('local', 'Local (Servidor)'),
        ('onedrive', 'OneDrive'),
    ]
    
    name = models.CharField(max_length=255)
    source_type = models.CharField(max_length=20, choices=TYPE_CHOICES)
    file_path = models.CharField(max_length=255, blank=True, null=True)  # Solo para archivos locales
    connection = models.ForeignKey(DatabaseConnection, on_delete=models.SET_NULL, null=True, blank=True)  # Solo para SQL
    
    # 🆕 NUEVOS CAMPOS PARA SOPORTE CLOUD
    storage_type = models.CharField(
        max_length=20,
        choices=STORAGE_CHOICES,
        default='local',
        help_text="Tipo de almacenamiento: local o en la nube"
    )
    onedrive_url = models.CharField(
        max_length=500,
        blank=True,
        null=True,
        help_text="URL del archivo en OneDrive (ej: /drive/items/ID)"
    )
    onedrive_item_id = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text="ID único del item en OneDrive para sincronización"
    )
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        storage_info = f" [{self.get_storage_type_display()}]" if self.storage_type == 'onedrive' else ""
        return f"{self.name} ({self.get_source_type_display()}){storage_info}"
    
    def is_local(self):
        """Retorna True si el archivo está almacenado localmente"""
        return self.storage_type == 'local'
    
    def is_cloud(self):
        """Retorna True si el archivo está en la nube"""
        return self.storage_type == 'onedrive'


class ProcesosGuardados(models.Model):
    """
    Modelo espejo (managed=False) que refleja la tabla dbo.ProcesosGuardados en SQL Server.
    
    Esta tabla centraliza todos los procesos creados en Django, permitiendo:
    - Trazabilidad y auditoría desde SQL Server
    - Consultas directas sin pasar por Django ORM
    - Sincronización automática con el modelo MigrationProcess
    
    ⚠️ IMPORTANTE: 
    - managed=False significa que Django NO creará/modificará esta tabla
    - La tabla debe existir previamente en la base DestinoAutomatizacion
    - Usa el alias 'sqlserver' para todas las operaciones
    
    Sincronización: Se actualiza automáticamente cuando se crea/edita/ejecuta
    un proceso desde el modelo MigrationProcess (ver método save() y run())
    """
    
    # Campo de identidad (auto-incremental en SQL Server)
    id = models.IntegerField(primary_key=True, db_column='Id')
    
    # Información básica del proceso
    nombre_proceso = models.CharField(
        max_length=255, 
        db_column='NombreProceso',
        help_text="Nombre único del proceso (sin caracteres especiales)"
    )
    
    tipo_fuente = models.CharField(
        max_length=50, 
        db_column='TipoFuente',
        help_text="Tipo de fuente: 'Excel' o 'SQL'"
    )
    
    fuente = models.CharField(
        max_length=255, 
        null=True, 
        blank=True, 
        db_column='Fuente',
        help_text="Ruta del archivo Excel o nombre de la conexión SQL"
    )
    
    hoja_tabla = models.CharField(
        max_length=255, 
        null=True, 
        blank=True, 
        db_column='HojaTabla',
        help_text="Nombre de la hoja (Excel) o tabla (SQL) procesada"
    )
    
    destino = models.CharField(
        max_length=255, 
        null=True, 
        blank=True, 
        db_column='Destino',
        help_text="Base o tabla destino en DestinoAutomatizacion"
    )
    
    estado = models.CharField(
        max_length=50, 
        default='Activo', 
        db_column='Estado',
        help_text="Estado lógico del proceso: Activo, Inactivo, Eliminado, etc."
    )
    
    # Campos de auditoría
    fecha_creacion = models.DateTimeField(
        auto_now_add=True, 
        db_column='FechaCreacion',
        help_text="Timestamp de creación del proceso"
    )
    
    fecha_actualizacion = models.DateTimeField(
        null=True, 
        blank=True, 
        db_column='FechaActualizacion',
        help_text="Timestamp de última modificación del proceso"
    )
    
    usuario_creador = models.CharField(
        max_length=255, 
        null=True, 
        blank=True, 
        db_column='UsuarioCreador',
        help_text="Usuario que creó el proceso"
    )
    
    descripcion = models.TextField(
        null=True, 
        blank=True, 
        db_column='Descripcion',
        help_text="Descripción detallada del proceso"
    )
    
    # Campos de control de ejecución
    ultima_ejecucion = models.DateTimeField(
        null=True, 
        blank=True, 
        db_column='UltimaEjecucion',
        help_text="Timestamp de la última vez que se ejecutó el proceso"
    )
    
    version = models.IntegerField(
        default=1, 
        db_column='Version',
        help_text="Versión del proceso (incrementa con cada cambio significativo)"
    )
    
    observaciones = models.TextField(
        null=True, 
        blank=True, 
        db_column='Observaciones',
        help_text="Notas adicionales, cambios realizados, etc."
    )
    
    class Meta:
        managed = False  # Django NO gestionará la tabla (no hará migraciones)
        db_table = 'dbo.ProcesosGuardados'  # Nombre exacto de la tabla en SQL Server
        app_label = 'automatizacion'
        verbose_name = 'Proceso Guardado (SQL Server)'
        verbose_name_plural = 'Procesos Guardados (SQL Server)'
        # Usar conexión 'sqlserver' para todas las operaciones de este modelo
        # (se configura en el router o directamente en las consultas)
    
    def __str__(self):
        return f"{self.nombre_proceso} ({self.tipo_fuente})"


class MigrationProcess(models.Model):
    """
    Representa un proceso completo de migración guardado
    """
    STATUS_CHOICES = [
        ('draft', 'Borrador'),
        ('db_selected', 'Base de datos seleccionada'),
        ('tables_selected', 'Tablas seleccionadas'),
        ('columns_selected', 'Columnas seleccionadas'),
        ('configured', 'Configurado'),
        ('validated', 'Validado'),
        ('ready', 'Listo para ejecutar'),
        ('running', 'En ejecución'),
        ('completed', 'Completado'),
        ('failed', 'Fallido'),
    ]
    
    name = models.CharField(max_length=255, unique=True)
    description = models.TextField(blank=True, null=True)
    source = models.ForeignKey(DataSource, on_delete=models.CASCADE, related_name='processes')
    
    # Para archivos Excel/CSV
    selected_sheets = models.JSONField(null=True, blank=True)  # Lista de hojas seleccionadas
    
    # Para SQL Server
    selected_database = models.CharField(max_length=100, blank=True, null=True)  # Base de datos seleccionada
    selected_tables = models.JSONField(null=True, blank=True)  # Lista de tablas seleccionadas
    
    # Campos compartidos
    selected_columns = models.JSONField(null=True, blank=True)  # Dict de columnas seleccionadas por tabla/hoja
    column_mappings = models.JSONField(null=True, blank=True)  # Dict de mapeos: {'tabla': {'nombre_original': 'nombre_personalizado'}}
    
    # Destino de datos (configuración dinámica)
    destination_connection = models.ForeignKey(
        DatabaseConnection,
        on_delete=models.CASCADE,
        related_name='destination_processes',
        help_text="Conexión SQL Server destino donde se guardarán ResultadosProcesados y ProcesosGuardados",
        null=True,  # Temporal para migración
        blank=True
    )
    destination_database = models.CharField(
        max_length=100,
        default='DestinoAutomatizacion',
        help_text="Nombre de la base de datos destino"
    )
    
    # Campos legacy (mantener por compatibilidad temporal)
    target_db_name = models.CharField(max_length=100, default='DestinoAutomatizacion')
    target_db_connection = models.ForeignKey(DatabaseConnection, on_delete=models.SET_NULL, null=True, blank=True, related_name='target_processes_legacy')
    target_table = models.CharField(max_length=100, blank=True, null=True)  # Tabla de destino
    
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='draft')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)  # Se actualiza automáticamente en cada save()
    last_run = models.DateTimeField(null=True, blank=True)
    
    # Opciones de rollback y checkpoint
    allow_rollback = models.BooleanField(default=True)
    last_checkpoint = models.JSONField(null=True, blank=True)  # Para almacenar puntos de restauración
    
    # 🆕 Campos para sistema de validación y normalización
    type_configuration = models.JSONField(null=True, blank=True)  # Configuración de tipos SQL inferidos por columna
    types_inferred_at = models.DateTimeField(null=True, blank=True)  # Timestamp de cuándo se infirieron los tipos
    
    def __str__(self):
        return self.name
    
    def _get_excel_file(self):
        """
        Obtiene el archivo Excel ya sea desde local o OneDrive.
        
        Returns:
            pd.ExcelFile: Objeto ExcelFile que permite leer múltiples hojas
            
        Raises:
            Exception: Si no se puede obtener el archivo
        """
        import pandas as pd
        
        if self.source.is_cloud():
            # Archivo en OneDrive - descargarlo
            if not self.source.onedrive_url:
                raise Exception('No hay URL de OneDrive configurada')
            
            from .onedrive_service import get_onedrive_service
            import logging
            logger = logging.getLogger(__name__)
            
            logger.info(f"📥 Descargando archivo de OneDrive: {self.source.onedrive_url}")
            print(f"📥 Descargando archivo de OneDrive: {self.source.onedrive_url}")
            
            service = get_onedrive_service()
            file_content = service.download_file_from_url(self.source.onedrive_url)
            
            logger.info("✅ Archivo de OneDrive descargado correctamente")
            print("✅ Archivo de OneDrive descargado correctamente")
            
            # Convertir a ExcelFile para permitir múltiples lecturas
            return pd.ExcelFile(file_content)
        else:
            # Archivo local
            if not self.source.file_path:
                raise Exception('No hay archivo Excel configurado')
            return pd.ExcelFile(self.source.file_path)
    
    def save(self, *args, **kwargs):
        """
        Sobrescribe el método save() para sincronizar automáticamente con SQL Server.
        
        Cada vez que un proceso se crea o modifica en Django, se refleja también
        en la tabla dbo.ProcesosGuardados de SQL Server para trazabilidad.
        
        ⚠️ Importante: La sincronización ocurre DESPUÉS del save en Django,
        por lo que si falla la sincronización con SQL Server, el proceso igual
        quedará guardado en Django (modelo robusto).
        """
        # Guardar primero en Django (SQLite)
        super().save(*args, **kwargs)
        
        # Sincronizar con SQL Server (tabla ProcesosGuardados)
        try:
            from .process_sync import sync_process_to_sqlserver
            
            # Determinar observaciones basadas en si es creación o actualización
            if self._state.adding:
                observaciones = f"Proceso creado en Django (ID Django: {self.id})"
            else:
                observaciones = f"Proceso actualizado en Django (ID Django: {self.id})"
            
            exito, mensaje, proceso_id_sql = sync_process_to_sqlserver(
                self, 
                usuario='sistema', 
                observaciones=observaciones
            )
            
            if exito:
                print(f"✅ Sincronización SQL Server exitosa: {mensaje}")
            else:
                print(f"⚠️ Advertencia: No se pudo sincronizar con SQL Server: {mensaje}")
                # No lanzamos excepción para no bloquear el save de Django
                
        except Exception as e:
            print(f"⚠️ Error inesperado sincronizando con SQL Server: {str(e)}")
            # Registrar el error pero no interrumpir el flujo
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error sincronizando proceso {self.name} con SQL Server", exc_info=True)
    
    def run(self):
        """
        Ejecuta el proceso de migración guardado - PROCESA DATOS REALES DEL ORIGEN
        ✅ CORREGIDO: Usa ProcessTracker para generar IDs consistentes entre ProcesoLog y tabla dinámica
        """
        from .data_transfer_service import data_transfer_service
        from .logs.process_tracker import ProcessTracker
        import json
        
        # ✅ CORRECCIÓN: Refrescar datos desde la base de datos antes de ejecutar
        # Esto asegura que estamos usando la configuración más reciente
        self.refresh_from_db()
        
        print(f"\n{'='*80}")
        print(f"🔍 DEBUG - Iniciando ejecución del proceso: {self.name} (ID: {self.id})")
        print(f"📋 Tablas seleccionadas: {self.selected_tables}")
        print(f"📋 Columnas seleccionadas: {self.selected_columns}")
        print(f"📋 Mapeos de columnas: {self.column_mappings}")
        print(f"{'='*80}\n")
        
        self.status = 'running'
        self.last_run = timezone.now()
        self.save()
        
        # 🔄 SINCRONIZACIÓN SQL SERVER: Actualizar UltimaEjecucion
        try:
            from .process_sync import update_ultima_ejecucion
            exito_sql, msg_sql = update_ultima_ejecucion(self.name, self.last_run)
            if exito_sql:
                print(f"✅ UltimaEjecucion actualizada en SQL Server")
            else:
                print(f"⚠️ Advertencia: {msg_sql}")
        except Exception as e:
            print(f"⚠️ Error actualizando UltimaEjecucion en SQL Server: {str(e)}")
        
        # Crear log de inicio del proceso
        MigrationLog.log(
            process=self,
            stage='connection',
            message=f'Iniciando ejecución del proceso: {self.name}',
            level='info',
            user='sistema'
        )
        
        # CORRECCIÓN 1: Usar ProcessTracker para generar UUID único y crear log en ProcesoLog
        tracker = ProcessTracker(self.name)
        
        try:
            # CORRECCIÓN 2: Iniciar tracking con MigrationProcessID para relación correcta
            parametros_proceso = {
                'migration_process_id': self.id,  # FK al proceso configurado
                'source_type': self.source.source_type if self.source else 'unknown',
                'source_id': self.source.id if self.source else None,
                'selected_tables': self.selected_tables,
                'selected_sheets': self.selected_sheets,
                'selected_columns': self.selected_columns,
                'target_db_name': self.target_db_name
            }
            
            # Iniciar proceso y obtener UUID que se usará en ambas bases de datos
            proceso_id = tracker.iniciar(parametros_proceso)
            
            # NUEVA LÓGICA: Procesar según tipo de fuente
            tiempo_inicio = timezone.now()
            
            if self.source.source_type == 'excel':
                # EXCEL: Procesar cada hoja por separado con tabla independiente
                success, result_info = self._process_excel_sheets_individually(tracker, proceso_id, tiempo_inicio, parametros_proceso)
            elif self.source.source_type == 'sql':
                # SQL: Procesar cada tabla por separado con tabla independiente
                success, result_info = self._process_sql_tables_individually(tracker, proceso_id, tiempo_inicio, parametros_proceso)
            else:
                # CSV: Usar lógica original (una sola tabla)
                datos_origen = self._extract_source_data()
                tiempo_fin = timezone.now()
                duracion_extraccion = (tiempo_fin - tiempo_inicio).total_seconds()
                
                # Calcular estadísticas de los datos extraídos
                registros_procesados = len(datos_origen) if isinstance(datos_origen, list) else 1
                
                # Crear log de extracción de datos
                MigrationLog.log(
                    process=self,
                    stage='data_extraction',
                    message=f'Datos extraídos de {self.source.source_type if self.source else "origen"}',
                    level='info',
                    rows=registros_procesados,
                    duration=int(duracion_extraccion * 1000),
                    user='sistema'
                )
                
                # Actualizar estado del proceso
                tracker.actualizar_estado('PROCESANDO_DATOS', 
                    f'Extrayendo {registros_procesados} registros de {self.source.source_type if self.source else "origen"}')
                
                # Crear resumen de los datos procesados (NO los datos completos)
                resumen_procesamiento = self._crear_resumen_datos(
                    datos_origen, 
                    duracion_extraccion, 
                    registros_procesados
                )
                
                # Preparar metadatos del proceso
                metadata = {
                    'migration_process_id': self.id,
                    'process_name': self.name,
                    'source_type': self.source.source_type if self.source else 'unknown',
                    'source_id': self.source.id if self.source else None,
                    'execution_timestamp': tiempo_inicio.isoformat(),
                    'selected_tables': self.selected_tables,
                    'selected_sheets': self.selected_sheets,
                    'selected_columns': self.selected_columns,
                    'target_db_name': self.target_db_name,
                    'extraction_duration_seconds': duracion_extraccion
                }
                
                # Actualizar estado antes de transferencia
                tracker.actualizar_estado('TRANSFIRIENDO', 'Insertando datos en tabla dinámica')
                
                # Crear log de transferencia de datos
                MigrationLog.log(
                    process=self,
                    stage='data_loading',
                    message='Iniciando transferencia de datos a tabla dinámica',
                    level='info',
                    user='sistema'
                )
                
                # CORRECCIÓN 3: Usar el mismo proceso_id del tracker en la transferencia
                success, result_info = data_transfer_service.transfer_to_dynamic_table(
                    process_name=self.name,
                    proceso_id=proceso_id,  # ✅ Mismo UUID generado por ProcessTracker
                    datos_procesados=resumen_procesamiento,
                    usuario_responsable='sistema_automatizado',
                    metadata=metadata,
                    recreate_table=True,  # Cada ejecución limpia y recrea los datos
                    estado_proceso='COMPLETADO',
                    tipo_operacion=f'MIGRACION_{self.name.upper().replace(" ", "_")}',
                    registros_afectados=registros_procesados
                )
            
            if success:
                self.status = 'completed'
                
                # Manejo diferente según tipo de proceso
                if result_info.get('process_type') == 'excel_multi_sheet':
                    # EXCEL: Múltiples hojas procesadas
                    hojas_exitosas = result_info.get('hojas_procesadas', 0)
                    total_registros = result_info.get('total_registros', 0)
                    duracion_total = result_info.get('duracion_total', 0)
                    
                    detalles_exito = f"Excel procesado: {hojas_exitosas} hojas exitosas, {total_registros} registros totales, {duracion_total:.2f}s"
                    
                    # Crear log de éxito para Excel
                    MigrationLog.log(
                        process=self,
                        stage='completion',
                        message=detalles_exito,
                        level='success',
                        rows=total_registros,
                        duration=int(duracion_total * 1000),
                        user='sistema'
                    )
                    
                    tracker.finalizar_exito(detalles_exito)
                    
                    print(f"✅ Proceso Excel '{self.name}' ejecutado exitosamente.")
                    print(f"   📋 Hojas procesadas: {hojas_exitosas}")
                    print(f"   📊 Total registros: {total_registros}")
                    print(f"   ⏱️ Duración total: {duracion_total:.2f}s")
                    print(f"   🔗 ProcesoID principal: {proceso_id}")
                    
                    # Mostrar detalle de cada hoja
                    for hoja in result_info.get('detalles_hojas_exitosas', []):
                        print(f"      🍃 Hoja '{hoja['sheet_name']}': {hoja['registros']} registros → Tabla '{hoja['table_name']}'")
                elif result_info.get('process_type') == 'sql_multi_table':
                    # SQL: Múltiples tablas procesadas
                    tablas_exitosas = result_info.get('tablas_procesadas', 0)
                    total_registros = result_info.get('total_registros', 0)
                    duracion_total = result_info.get('duracion_total', 0)
                    
                    detalles_exito = f"SQL procesado: {tablas_exitosas} tablas exitosas, {total_registros} registros totales, {duracion_total:.2f}s"
                    
                    # Crear log de éxito para SQL
                    MigrationLog.log(
                        process=self,
                        stage='completion',
                        message=detalles_exito,
                        level='success',
                        rows=total_registros,
                        duration=int(duracion_total * 1000),
                        user='sistema'
                    )
                    
                    tracker.finalizar_exito(detalles_exito)
                    
                    print(f"✅ Proceso SQL '{self.name}' ejecutado exitosamente.")
                    print(f"   📋 Tablas procesadas: {tablas_exitosas}")
                    print(f"   📊 Total registros: {total_registros}")
                    print(f"   ⏱️ Duración total: {duracion_total:.2f}s")
                    print(f"   🔗 ProcesoID principal: {proceso_id}")
                    
                    # Mostrar detalle de cada tabla
                    detalles_tablas = result_info.get('detalles_tablas', {})
                    for tabla_nombre, registros_tabla in detalles_tablas.items():
                        nombre_tabla_destino = f"{self.name.replace(' ', '_')}_{tabla_nombre}"
                        print(f"      📊 Tabla '{tabla_nombre}': {registros_tabla} registros → Tabla '{nombre_tabla_destino}'")
                else:
                    # CSV: Una sola tabla
                    table_name = result_info.get('table_name', 'Desconocida')
                    resultado_id = result_info.get('resultado_id', 'N/A')
                    registros_procesados = len(datos_origen) if 'datos_origen' in locals() and isinstance(datos_origen, list) else 1
                    
                    detalles_exito = f"Tabla: {table_name}, ResultadoID: {resultado_id}, Registros: {registros_procesados}"
                    
                    # Crear log de éxito para CSV
                    MigrationLog.log(
                        process=self,
                        stage='completion',
                        message=detalles_exito,
                        level='success',
                        rows=registros_procesados,
                        user='sistema'
                    )
                    
                    tracker.finalizar_exito(detalles_exito)
                    
                    print(f"✅ Proceso '{self.name}' ejecutado exitosamente.")
                    print(f"   📋 Tabla específica: '{table_name}'")
                    print(f"   📊 Registros procesados: {registros_procesados}")
                    print(f"   🆔 ResultadoID: {resultado_id}")
                    print(f"   🔗 ProcesoID (consistente): {proceso_id}")
                
                # ✅ CORRECCIÓN: Devolver el resultado exitoso
                return success, result_info
            else:
                self.status = 'failed'
                
                # Manejo de errores diferente según tipo
                if result_info.get('process_type') == 'excel_multi_sheet_error':
                    # Error en Excel
                    error_msg = result_info.get('error', 'Error desconocido procesando Excel')
                    hojas_fallidas = result_info.get('hojas_con_error', 0)
                    error_completo = f"Error procesando Excel '{self.name}': {error_msg}, {hojas_fallidas} hojas fallaron"
                elif result_info.get('process_type') == 'sql_processing' or result_info.get('process_type') == 'sql_multi_table':
                    # Error en SQL
                    error_msg = result_info.get('error', 'Error desconocido procesando SQL')
                    tablas_fallidas = result_info.get('tablas_con_error', 0)
                    error_completo = f"Error procesando SQL '{self.name}': {error_msg}, {tablas_fallidas} tablas fallaron"
                else:
                    # Error en CSV
                    error_msg = result_info.get('error', 'Error desconocido')
                    table_name = result_info.get('table_name', 'No determinada')
                    error_completo = f"Error en transferencia a tabla '{table_name}': {error_msg}"
                
                # Crear log de error
                MigrationLog.log(
                    process=self,
                    stage='data_loading',
                    message='Error durante la transferencia de datos',
                    level='error',
                    error=error_completo,
                    user='sistema'
                )
                
                # CORRECCIÓN 5: Registrar error en ProcessTracker antes de lanzar excepción
                tracker.finalizar_error(Exception(error_completo))
                raise Exception(error_completo)
                
        except Exception as e:
            self.status = 'failed'
            
            # 🐛 DEBUG: Log detallado con traceback completo
            import traceback
            error_traceback = traceback.format_exc()
            
            error_details = {
                'error_type': type(e).__name__,
                'error_message': str(e),
                'traceback': error_traceback,
                'process_id': self.id,
                'process_name': self.name,
                'source_type': self.source.source_type if self.source else 'unknown',
                'selected_tables': self.selected_tables,
                'selected_sheets': self.selected_sheets,
                'selected_columns': self.selected_columns,
                'column_mappings': self.column_mappings
            }
            
            print(f"\n{'='*80}")
            print(f"❌ ERROR CRÍTICO EJECUTANDO PROCESO: {self.name} (ID: {self.id})")
            print(f"{'='*80}")
            print(f"🔴 Tipo de error: {type(e).__name__}")
            print(f"🔴 Mensaje: {str(e)}")
            print(f"\n📋 CONTEXTO DEL PROCESO:")
            print(f"   - Source Type: {self.source.source_type if self.source else 'N/A'}")
            print(f"   - Selected Tables: {self.selected_tables}")
            print(f"   - Selected Sheets: {self.selected_sheets}")
            print(f"   - Selected Columns: {self.selected_columns}")
            print(f"   - Column Mappings: {self.column_mappings}")
            print(f"\n🔍 TRACEBACK COMPLETO:")
            print(error_traceback)
            print(f"{'='*80}\n")
            
            # Crear log de error general con detalles completos
            MigrationLog.log(
                process=self,
                stage='data_loading',
                message=f'Error general: {type(e).__name__}',
                level='critical',
                error=str(e),
                details=error_details,
                user='sistema'
            )
            
            # CORRECCIÓN 6: Asegurar que el error se registre en ProcesoLog
            if 'tracker' in locals():
                tracker.finalizar_error(e)
            
            raise e
        finally:
            self.save()
    
    def _crear_resumen_datos(self, datos_origen, duracion_extraccion, registros_procesados):
        """
        Crea un resumen JSON de los datos procesados en lugar de guardar todos los datos
        
        Returns:
            dict: Resumen estructurado del procesamiento
        """
        try:
            resumen = {
                'proceso_ejecutado': self.name,
                'timestamp_ejecucion': timezone.now().isoformat(),
                'estadisticas': {
                    'total_registros': registros_procesados,
                    'duracion_extraccion_segundos': round(duracion_extraccion, 2),
                    'tipo_fuente': self.source.source_type if self.source else 'unknown'
                },
                'configuracion': {
                    'tablas_seleccionadas': self.selected_tables,
                    'hojas_seleccionadas': self.selected_sheets,
                    'columnas_seleccionadas': self.selected_columns,
                    'base_datos_destino': self.target_db_name or 'DestinoAutomatizacion'
                }
            }
            
            # Agregar información específica según el tipo de datos
            if isinstance(datos_origen, list):
                if len(datos_origen) > 0:
                    # Muestra de las primeras columnas/campos encontrados (sin datos sensibles)
                    primer_registro = datos_origen[0] if datos_origen else {}
                    if isinstance(primer_registro, dict):
                        resumen['estructura_datos'] = {
                            'columnas_detectadas': list(primer_registro.keys()),
                            'numero_columnas': len(primer_registro.keys()),
                            'muestra_primer_registro': {k: str(v)[:50] + '...' if len(str(v)) > 50 else str(v) 
                                                     for k, v in list(primer_registro.items())[:3]}
                        }
                
                resumen['estadisticas']['registros_validos'] = len([r for r in datos_origen if not (isinstance(r, dict) and r.get('error'))])
                resumen['estadisticas']['registros_con_error'] = len([r for r in datos_origen if isinstance(r, dict) and r.get('error')])
            
            elif isinstance(datos_origen, dict):
                if datos_origen.get('error'):
                    resumen['error_extraccion'] = datos_origen['error']
                    resumen['estadisticas']['extracion_exitosa'] = False
                else:
                    resumen['estadisticas']['extracion_exitosa'] = True
                    # Incluir keys del diccionario pero no valores completos
                    resumen['estructura_datos'] = {
                        'campos_principales': list(datos_origen.keys())[:10]
                    }
            
            # Agregar información de rendimiento
            if registros_procesados > 0 and duracion_extraccion > 0:
                resumen['rendimiento'] = {
                    'registros_por_segundo': round(registros_procesados / duracion_extraccion, 2)
                }
            
            return resumen
            
        except Exception as e:
            # En caso de error creando el resumen, devolver un resumen básico
            return {
                'proceso_ejecutado': self.name,
                'error_creando_resumen': str(e),
                'timestamp_ejecucion': timezone.now().isoformat(),
                'registros_procesados': registros_procesados,
                'duracion_extraccion_segundos': duracion_extraccion
            }
    
    def _extract_source_data(self):
        """
        Extrae datos reales de la fuente configurada (Excel, CSV o SQL)
        
        Returns:
            list|dict: Datos extraídos de la fuente
        """
        if not self.source:
            return {
                'error': 'No hay fuente configurada',
                'proceso': self.name,
                'timestamp': timezone.now().isoformat()
            }
        
        try:
            if self.source.source_type == 'excel':
                return self._extract_excel_data()
            elif self.source.source_type == 'csv':
                return self._extract_csv_data()
            elif self.source.source_type == 'sql':
                return self._extract_sql_data()
            else:
                return {
                    'error': f'Tipo de fuente no soportado: {self.source.source_type}',
                    'proceso': self.name,
                    'timestamp': timezone.now().isoformat()
                }
        except Exception as e:
            return {
                'error': f'Error extrayendo datos: {str(e)}',
                'proceso': self.name,
                'source_type': self.source.source_type,
                'timestamp': timezone.now().isoformat()
            }
    
    def _extract_excel_data(self):
        """Extrae datos de archivo Excel (soporta local y OneDrive)"""
        import pandas as pd
        import json
        
        try:
            # ✅ CORREGIDO: Usar helper que soporta OneDrive (retorna pd.ExcelFile)
            excel_file = self._get_excel_file()
            
            # Obtener hojas seleccionadas
            selected_sheets = self.selected_sheets if isinstance(self.selected_sheets, list) else (json.loads(self.selected_sheets) if self.selected_sheets else [])
            if not selected_sheets:
                return {'error': 'No hay hojas seleccionadas'}
            
            data = []
            for sheet_name in selected_sheets:
                # Usar parse() en lugar de read_excel() ya que excel_file es pd.ExcelFile
                df = excel_file.parse(sheet_name=sheet_name)
                
                # Filtrar columnas si están especificadas
                if self.selected_columns:
                    selected_cols = (self.selected_columns.get(sheet_name, []) if isinstance(self.selected_columns, dict) else json.loads(self.selected_columns).get(sheet_name, [])) if self.selected_columns else []
                    if selected_cols:
                        df = df[selected_cols]
                
                # Convertir a diccionarios
                sheet_data = df.to_dict('records')
                data.extend([{
                    'sheet': sheet_name,
                    'row_index': idx,
                    **row
                } for idx, row in enumerate(sheet_data)])
            
            return data
            
        except Exception as e:
            return {'error': f'Error procesando Excel: {str(e)}'}
    
    def _process_excel_sheets_individually(self, main_tracker, main_proceso_id, tiempo_inicio, parametros_proceso):
        """
        NUEVA FUNCIÓN: Procesa cada hoja de Excel por separado creando tabla individual
        Cada hoja genera su propio registro en ProcesoLog y ResultadosProcesados
        
        Returns:
            Tuple[bool, Dict]: (éxito_general, información_consolidada)
        """
        from .data_transfer_service import data_transfer_service
        from .logs.process_tracker import ProcessTracker
        import pandas as pd
        import json
        import logging
        
        # Configurar logging a archivo
        log_file = 'c:\\Users\\migue\\OneDrive\\Escritorio\\DJANGO DE NUEVO\\opav\\proyecto_automatizacion\\debug_process.log'
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, mode='a'),
                logging.StreamHandler()
            ]
        )
        logger = logging.getLogger(__name__)
        
        logger.info(f"="*80)
        logger.info(f"INICIANDO PROCESAMIENTO EXCEL MULTIHOJA")
        logger.info(f"Proceso: {self.name}")
        logger.info(f"Archivo: {self.source.file_path if self.source and self.source.file_path else ('OneDrive: ' + str(self.source.onedrive_url) if self.source and self.source.is_cloud() else 'N/A')}")
        logger.info(f"="*80)
        
        try:
            # ✅ CORREGIDO: Usar helper que soporta OneDrive
            excel_file = self._get_excel_file()
            
            # Obtener hojas seleccionadas
            selected_sheets = self.selected_sheets if isinstance(self.selected_sheets, list) else (json.loads(self.selected_sheets) if self.selected_sheets else [])
            if not selected_sheets:
                logger.error('No hay hojas seleccionadas en el Excel')
                raise Exception('No hay hojas seleccionadas en el Excel')
            
            logger.info(f'Hojas seleccionadas: {selected_sheets}')
            logger.info(f'Columnas seleccionadas: {self.selected_columns}')
            
            # Crear log de inicio de procesamiento Excel
            MigrationLog.log(
                process=self,
                stage='data_extraction',
                message=f'Iniciando procesamiento de Excel: {len(selected_sheets)} hojas seleccionadas',
                level='info',
                user='sistema'
            )
            
            # Variables para consolidar resultados
            hojas_procesadas = []
            hojas_con_error = []
            total_registros_procesados = 0
            
            main_tracker.actualizar_estado('PROCESANDO_HOJAS', f'Procesando {len(selected_sheets)} hojas de Excel por separado')
            
            # 🔍 DEBUG CRÍTICO: Verificar cantidad de hojas antes del bucle
            logger.info(f"🔍 DEBUG CRÍTICO: Iniciando bucle con {len(selected_sheets)} hojas: {selected_sheets}")
            print(f"🔍 DEBUG CRÍTICO: Iniciando bucle con {len(selected_sheets)} hojas: {selected_sheets}")
            
            # PROCESAR CADA HOJA POR SEPARADO
            hoja_index = 0
            for sheet_name in selected_sheets:
                hoja_index += 1
                logger.info(f"🔍 DEBUG: Procesando hoja {hoja_index}/{len(selected_sheets)}: '{sheet_name}'")
                print(f"🔍 DEBUG: Procesando hoja {hoja_index}/{len(selected_sheets)}: '{sheet_name}'")
                
                hoja_inicio = timezone.now()
                logger.info(f"🚀 Procesando hoja Excel: '{sheet_name}'")
                print(f"🚀 Procesando hoja Excel: '{sheet_name}'")
                
                try:
                    # 1. Crear tracker individual para esta hoja
                    nombre_proceso_hoja = f"{self.name} - Hoja: {sheet_name}"
                    tracker_hoja = ProcessTracker(nombre_proceso_hoja)
                    
                    # Parámetros específicos de la hoja
                    parametros_hoja = parametros_proceso.copy()
                    parametros_hoja.update({
                        'sheet_name': sheet_name,
                        'parent_process_id': main_proceso_id,
                        'is_excel_sheet': True,
                        'sheet_index': hoja_index  # 🆕 Agregar índice de hoja
                    })
                    
                    # Iniciar proceso individual para esta hoja
                    proceso_id_hoja = tracker_hoja.iniciar(parametros_hoja)
                    
                    # 2. Extraer datos específicos de esta hoja
                    source_info = self.source.file_path if self.source.file_path else f"OneDrive: {self.source.onedrive_url}"
                    print(f"📊 DEBUG: Leyendo hoja '{sheet_name}' desde {source_info}")
                    # Usar parse() en lugar de read_excel() ya que excel_file es pd.ExcelFile
                    df = excel_file.parse(sheet_name=sheet_name)
                    print(f"📊 DEBUG: Hoja leída. Shape original: {df.shape}, Columnas: {list(df.columns)}")
                    
                    # Aplicar limpieza de datos (nombres de columnas y valores NaN)
                    df = self._clean_excel_dataframe(df)
                    print(f"📊 DEBUG: Después de limpieza. Shape: {df.shape}, Columnas: {list(df.columns)}")
                    
                    # Filtrar columnas si están especificadas para esta hoja
                    if self.selected_columns:
                        selected_cols = (self.selected_columns.get(sheet_name, []) if isinstance(self.selected_columns, dict) 
                                       else json.loads(self.selected_columns).get(sheet_name, [])) if self.selected_columns else []
                        print(f"📊 DEBUG: Columnas seleccionadas para '{sheet_name}': {selected_cols}")
                        if selected_cols:
                            df = df[selected_cols]
                            print(f"📊 DEBUG: Después de filtrar columnas. Shape: {df.shape}, Columnas: {list(df.columns)}")
                    
                    # Convertir a diccionarios para transferencia
                    datos_hoja = df.to_dict('records')
                    registros_hoja = len(datos_hoja)
                    print(f"📊 DEBUG: Datos convertidos. Registros: {registros_hoja}")
                    
                    tracker_hoja.actualizar_estado('EXTRAYENDO_DATOS', f'Extraídos {registros_hoja} registros de la hoja {sheet_name}')
                    
                    # 3. Calcular duración de procesamiento
                    hoja_fin = timezone.now()
                    duracion_hoja = (hoja_fin - hoja_inicio).total_seconds()
                    
                    # 4. Transferir DATOS REALES de esta hoja a su tabla individual
                    tracker_hoja.actualizar_estado('TRANSFIRIENDO', f'Creando tabla individual para hoja {sheet_name}')
                    
                    # 🆕 NUEVO: Obtener nombre personalizado de la hoja si existe
                    custom_sheet_name = sheet_name
                    if self.column_mappings and '__sheet_names__' in self.column_mappings:
                        sheet_mappings = self.column_mappings['__sheet_names__']
                        if sheet_name in sheet_mappings:
                            custom_sheet_name = sheet_mappings[sheet_name]
                            print(f"🔄 DEBUG: Usando nombre personalizado para hoja '{sheet_name}' → '{custom_sheet_name}'")
                    
                    # 🔧 FIX: Generar nombre de tabla sin número en el nombre de la hoja, solo usar índice del loop
                    # Formato: Proceso_hoja1, Proceso_hoja2, etc. (donde el número es el índice del loop)
                    nombre_tabla_destino = f"{self.name}_hoja{hoja_index}".replace(' ', '_').replace('-', '_')
                    # Limpiar caracteres especiales del nombre
                    import re
                    nombre_tabla_destino = re.sub(r'[^\w]', '_', nombre_tabla_destino)
                    nombre_tabla_destino = re.sub(r'_+', '_', nombre_tabla_destino).strip('_')
                    
                    print(f"📋 DEBUG: Nombre final de tabla destino: '{nombre_tabla_destino}'")
                    
                    # ✅ GUARDAR DATOS REALES DEL DATAFRAME (NO METADATOS)
                    success_hoja, result_info_hoja = self._save_dataframe_to_destination(
                        df_datos=df,  # DataFrame con los datos reales
                        nombre_tabla_destino=nombre_tabla_destino,  # Nombre dinámico de la tabla
                        proceso_id=proceso_id_hoja,
                        usuario_responsable='sistema_automatizado',
                        source_table_name=sheet_name  # Pasar nombre de hoja ORIGINAL para aplicar mapeos de columnas
                    )
                    
                    # DEBUG: Logging adicional para detectar el problema
                    # Debug logging removido para producción
                    # if not success_hoja:
                    #     print(f"DEBUG Error info: {result_info_hoja}")
                    
                    if success_hoja:
                        # ✅ Hoja procesada exitosamente
                        table_name = result_info_hoja.get('table_name', f'Proceso_{sheet_name}')
                        resultado_id = result_info_hoja.get('resultado_id', 'N/A')
                        
                        # Finalizar proceso exitosamente para esta hoja
                        detalles_exito_hoja = f"Hoja Excel '{sheet_name}' procesada exitosamente. Tabla: {table_name}, ResultadoID: {resultado_id}, Registros: {registros_hoja}"
                        tracker_hoja.finalizar_exito(detalles_exito_hoja)
                        
                        # Agregar a resultados exitosos
                        hojas_procesadas.append({
                            'sheet_name': sheet_name,
                            'table_name': table_name,
                            'registros': registros_hoja,
                            'resultado_id': resultado_id,
                            'proceso_id': proceso_id_hoja,
                            'duracion': duracion_hoja
                        })
                        
                        total_registros_procesados += registros_hoja
                        
                        logger.info(f"✅ Hoja {hoja_index}/{len(selected_sheets)} '{sheet_name}' procesada exitosamente - Tabla: {table_name}")
                        print(f"✅ Hoja '{sheet_name}' procesada exitosamente:")
                        print(f"   📋 Tabla creada: '{table_name}'")
                        print(f"   📊 Registros: {registros_hoja}")
                        print(f"   🆔 ResultadoID: {resultado_id}")
                        print(f"   🔗 ProcesoID: {proceso_id_hoja}")
                        
                    else:
                        # ❌ Error procesando esta hoja
                        error_msg_hoja = result_info_hoja.get('error', 'Error desconocido procesando hoja')
                        error_completo_hoja = f"Error procesando hoja '{sheet_name}': {error_msg_hoja}"
                        
                        # LOG DETALLADO DEL ERROR
                        logger.error(f"❌ ERROR en hoja {hoja_index}/{len(selected_sheets)} '{sheet_name}': {error_msg_hoja}")
                        print(f"❌ ============================================")
                        print(f"❌ ERROR PROCESANDO HOJA '{sheet_name}'")
                        print(f"❌ Error message: {error_msg_hoja}")
                        print(f"❌ Result info completo: {result_info_hoja}")
                        print(f"❌ ============================================")
                        
                        # Registrar error para esta hoja
                        tracker_hoja.finalizar_error(Exception(error_completo_hoja))
                        
                        hojas_con_error.append({
                            'sheet_name': sheet_name,
                            'error': error_msg_hoja,
                            'proceso_id': proceso_id_hoja
                        })
                        
                        print(f"❌ Error procesando hoja '{sheet_name}': {error_msg_hoja}")
                
                except Exception as e_hoja:
                    # Error específico procesando esta hoja
                    error_hoja = f"Error procesando hoja '{sheet_name}': {str(e_hoja)}"
                    logger.error(f"❌ ============================================")
                    logger.error(f"❌ EXCEPCIÓN AL PROCESAR HOJA '{sheet_name}'")
                    logger.error(f"❌ Tipo de error: {type(e_hoja).__name__}")
                    logger.error(f"❌ Mensaje: {str(e_hoja)}")
                    import traceback
                    logger.error(f"❌ Traceback completo:")
                    logger.error(traceback.format_exc())
                    logger.error(f"❌ ============================================")
                    
                    print(f"❌ ============================================")
                    print(f"❌ EXCEPCIÓN AL PROCESAR HOJA '{sheet_name}'")
                    print(f"❌ Tipo de error: {type(e_hoja).__name__}")
                    print(f"❌ Mensaje: {str(e_hoja)}")
                    print(f"❌ Traceback completo:")
                    traceback.print_exc()
                    print(f"❌ ============================================")
                    
                    # Si tenemos tracker para esta hoja, registrar error
                    if 'tracker_hoja' in locals():
                        tracker_hoja.finalizar_error(e_hoja)
                    
                    hojas_con_error.append({
                        'sheet_name': sheet_name,
                        'error': str(e_hoja),
                        'proceso_id': locals().get('proceso_id_hoja', 'N/A')
                    })
                    
                    print(f"❌ Error procesando hoja '{sheet_name}': {str(e_hoja)}")
            
            # 🔍 DEBUG CRÍTICO: Verificar resultado del bucle
            logger.info(f"🔍 DEBUG CRÍTICO: Bucle completado. Hojas procesadas: {len(hojas_procesadas)}, Con error: {len(hojas_con_error)}")
            print(f"🔍 DEBUG CRÍTICO: Bucle completado. Hojas procesadas: {len(hojas_procesadas)}, Con error: {len(hojas_con_error)}")
            
            # CONSOLIDAR RESULTADOS FINALES
            tiempo_fin_total = timezone.now()
            duracion_total = (tiempo_fin_total - tiempo_inicio).total_seconds()
            
            hojas_exitosas = len(hojas_procesadas)
            hojas_fallidas = len(hojas_con_error)
            
            # Determinar éxito general
            success_general = hojas_exitosas > 0  # Al menos una hoja debe procesarse exitosamente
            
            # Preparar información consolidada
            result_info_consolidado = {
                'success': success_general,
                'hojas_procesadas': hojas_exitosas,
                'hojas_con_error': hojas_fallidas,
                'total_hojas': len(selected_sheets),
                'total_registros': total_registros_procesados,
                'duracion_total': duracion_total,
                'detalles_hojas_exitosas': hojas_procesadas,
                'detalles_hojas_error': hojas_con_error,
                'table_name': f'Múltiples tablas creadas ({hojas_exitosas} exitosas)',
                'resultado_id': f'{hojas_exitosas} tablas creadas',
                'process_type': 'excel_multi_sheet'
            }
            
            # Actualizar estado del tracker principal
            if success_general:
                estado_final = f"Excel procesado: {hojas_exitosas}/{len(selected_sheets)} hojas exitosas, {total_registros_procesados} registros totales"
                main_tracker.actualizar_estado('COMPLETADO', estado_final)
                
                # Crear log de éxito para procesamiento Excel completo
                MigrationLog.log(
                    process=self,
                    stage='completion',
                    message=estado_final,
                    level='success',
                    rows=total_registros_procesados,
                    duration=int(duracion_total * 1000),
                    user='sistema'
                )
            else:
                estado_final = f"Error procesando Excel: {hojas_fallidas}/{len(selected_sheets)} hojas fallaron"
                main_tracker.actualizar_estado('ERROR', estado_final)
                
                # Crear log de error para procesamiento Excel
                MigrationLog.log(
                    process=self,
                    stage='data_loading',
                    message=f'Error procesando Excel: {hojas_fallidas} hojas fallaron',
                    level='error',
                    error=estado_final,
                    user='sistema'
                )
            
            print(f"\n📊 RESUMEN PROCESAMIENTO EXCEL:")
            print(f"   📋 Total hojas: {len(selected_sheets)}")
            print(f"   ✅ Hojas exitosas: {hojas_exitosas}")
            print(f"   ❌ Hojas con error: {hojas_fallidas}")
            print(f"   📊 Total registros: {total_registros_procesados}")
            print(f"   ⏱️ Duración total: {duracion_total:.2f}s")
            
            return success_general, result_info_consolidado
            
        except Exception as e:
            # Error general procesando el archivo Excel
            error_msg = f'Error general procesando Excel: {str(e)}'
            main_tracker.actualizar_estado('ERROR', error_msg)
            
            # Crear log de error crítico para Excel
            MigrationLog.log(
                process=self,
                stage='data_extraction',
                message='Error crítico procesando archivo Excel',
                level='critical',
                error=error_msg,
                user='sistema'
            )
            
            return False, {
                'success': False,
                'error': error_msg,
                'hojas_procesadas': 0,
                'hojas_con_error': len(selected_sheets) if 'selected_sheets' in locals() else 0,
                'process_type': 'excel_multi_sheet_error'
            }
    
    def _extract_csv_data(self):
        """Extrae datos de archivo CSV"""
        import pandas as pd
        import json
        
        try:
            if not self.source.file_path:
                return {'error': 'No hay archivo CSV configurado'}
            
            df = pd.read_csv(self.source.file_path)
            
            # Filtrar columnas si están especificadas
            if self.selected_columns:
                # Manejar tanto lista como JSON string para selected_columns
                if isinstance(self.selected_columns, (list, dict)):
                    selected_cols = self.selected_columns
                elif isinstance(self.selected_columns, str):
                    selected_cols = json.loads(self.selected_columns)
                else:
                    selected_cols = []
                
                if isinstance(selected_cols, list) and selected_cols:
                    df = df[selected_cols]
            
            # Convertir a diccionarios
            data = df.to_dict('records')
            return [{'row_index': idx, **row} for idx, row in enumerate(data)]
            
        except Exception as e:
            return {'error': f'Error procesando CSV: {str(e)}'}
    
    def _extract_sql_data(self):
        """Extrae datos de base de datos SQL"""
        from .legacy_utils import SQLServerConnector
        import json
        import pyodbc
        
        try:
            if not self.source.connection:
                return {'error': 'No hay conexión SQL configurada'}
            
            # Obtener tablas seleccionadas - manejar tanto lista como JSON string
            if isinstance(self.selected_tables, list):
                selected_tables = self.selected_tables
            elif isinstance(self.selected_tables, str):
                try:
                    # Intentar parsearlo como JSON array
                    selected_tables = json.loads(self.selected_tables)
                except json.JSONDecodeError:
                    # Si falla JSON parsing, tratarlo como string simple
                    selected_tables = [self.selected_tables]
            else:
                selected_tables = self.selected_tables if self.selected_tables else []
            
            # Si no hay tablas seleccionadas o las tablas seleccionadas no existen,
            # intentar usar la tabla de prueba
            from .sql_validation import get_valid_tables, ensure_test_table
            
            # Variable para seguimiento si se usa tabla de prueba
            using_fallback = False
            
            # Verificar si hay tablas seleccionadas
            if not selected_tables:
                print(f"⚠️ No hay tablas seleccionadas en proceso '{self.name}', intentando usar tabla de prueba...")
                TEST_TABLE_NAME = ensure_test_table(self.source.connection)
                if TEST_TABLE_NAME:
                    selected_tables = [TEST_TABLE_NAME]
                    self.selected_tables = selected_tables
                    self.save()
                    using_fallback = True
                    print(f"✅ Configurado proceso para usar tabla de prueba: {TEST_TABLE_NAME}")
                else:
                    return {'error': 'No hay tablas seleccionadas y no se pudo crear tabla de prueba'}
            else:
                # Verificar si las tablas seleccionadas realmente existen
                valid_tables = get_valid_tables(self.source.connection, selected_tables)
                
                if not valid_tables:
                    print(f"⚠️ Ninguna de las tablas seleccionadas existe en la BD. Intentando usar tabla de prueba...")
                    TEST_TABLE_NAME = ensure_test_table(self.source.connection)
                    if TEST_TABLE_NAME:
                        selected_tables = [TEST_TABLE_NAME]
                        self.selected_tables = selected_tables
                        self.save()
                        using_fallback = True
                        print(f"✅ Configurado proceso para usar tabla de prueba: {TEST_TABLE_NAME}")
                    else:
                        return {'error': 'Las tablas seleccionadas no existen y no se pudo crear tabla de prueba'}
                else:
                    # Actualizar a solo tablas válidas si es diferente
                    if len(valid_tables) != len(selected_tables):
                        print(f"⚠️ Solo {len(valid_tables)} de {len(selected_tables)} tablas existen. Actualizando selección...")
                        selected_tables = valid_tables
                        self.selected_tables = selected_tables
                        self.save()
            
            connection = self.source.connection
            connector = SQLServerConnector(
                connection.server,
                connection.username,
                connection.password,
                connection.port
            )
            
            # Conectar a la base de datos
            if not connector.select_database(connection.selected_database):
                return {'error': f'No se pudo conectar a la base de datos {connection.selected_database}'}
            
            all_data = []
            
            for table_info in selected_tables:
                # Determinar identificador y esquema de la tabla
                if isinstance(table_info, dict):
                    full_name = table_info.get('full_name') or table_info.get('name')
                elif isinstance(table_info, str):
                    full_name = table_info
                else:
                    full_name = None

                if not full_name:
                    continue

                full_name = str(full_name).strip()
                schema_name = None
                base_table_name = full_name

                if '.' in full_name:
                    parts = [p for p in full_name.split('.') if p]
                    if len(parts) >= 2:
                        schema_name = parts[-2]
                        base_table_name = parts[-1]
                    else:
                        base_table_name = parts[-1]

                if schema_name:
                    table_key = f"{schema_name}.{base_table_name}"
                    safe_table_ref = f"[{schema_name}].[{base_table_name}]"
                else:
                    table_key = base_table_name
                    safe_table_ref = f"[{base_table_name}]"

                # Obtener datos de la tabla
                try:
                    cursor = connector.conn.cursor()
                    
                    # Construir consulta SELECT
                    if self.selected_columns:
                        # Manejar tanto diccionario como JSON string para selected_columns
                        if isinstance(self.selected_columns, dict):
                            selected_cols = (
                                self.selected_columns.get(table_key)
                                or self.selected_columns.get(full_name)
                                or self.selected_columns.get(base_table_name, [])
                            )
                        elif isinstance(self.selected_columns, str):
                            cols_dict = json.loads(self.selected_columns)
                            selected_cols = (
                                cols_dict.get(table_key)
                                or cols_dict.get(full_name)
                                or cols_dict.get(base_table_name, [])
                            )
                        else:
                            selected_cols = []
                        
                        if selected_cols:
                            columns = ', '.join([f'[{col}]' for col in selected_cols])
                            query = f"SELECT {columns} FROM {safe_table_ref}"
                        else:
                            query = f"SELECT * FROM {safe_table_ref}"
                    else:
                        query = f"SELECT * FROM {safe_table_ref}"
                    
                    cursor.execute(query)
                    
                    # Obtener nombres de columnas
                    column_names = [column[0] for column in cursor.description]
                    
                    # Obtener datos
                    rows = cursor.fetchall()
                    
                    # Convertir a diccionarios
                    table_data = []
                    for row_idx, row in enumerate(rows):
                        row_dict = {
                            'table_name': table_key,
                            'row_index': row_idx
                        }
                        for col_idx, value in enumerate(row):
                            row_dict[column_names[col_idx]] = value
                        table_data.append(row_dict)
                    
                    all_data.extend(table_data)
                    
                    # Agregar entrada de metadatos para la tabla
                    all_data.append({
                        'table_name': table_key,
                        'schema': schema_name,
                        'columns': column_names,
                        'row_count': len(rows),
                        'metadata': True
                    })
                    
                except Exception as table_error:
                    # Agregar error pero continuar con otras tablas
                    all_data.append({
                        'table_name': table_key,
                        'error': f'Error procesando tabla: {str(table_error)}'
                    })
            
            # Si no se agregó ninguna entrada (por ejemplo, tabla vacía), crear metadatos mínimos
            if all_data == []:
                for table_info in selected_tables:
                    if isinstance(table_info, dict):
                        full_name = table_info.get('full_name') or table_info.get('name')
                    else:
                        full_name = table_info
                    if not full_name:
                        continue
                    meta_schema = None
                    base_name = full_name
                    if '.' in full_name:
                        parts = [p for p in full_name.split('.') if p]
                        if len(parts) >= 2:
                            meta_schema = parts[-2]
                            base_name = parts[-1]
                        else:
                            base_name = parts[-1]
                    if meta_schema:
                        table_key_meta = f"{meta_schema}.{base_name}"
                    else:
                        table_key_meta = base_name
                    all_data.append({
                        'table_name': table_key_meta,
                        'schema': meta_schema,
                        'columns': [],
                        'row_count': 0,
                        'metadata': True
                    })
            
            return all_data
            
        except Exception as e:
            return {'error': f'Error procesando SQL: {str(e)}'}
        finally:
            try:
                connector.disconnect()
            except:
                pass

    def _process_sql_tables_individually(self, tracker, proceso_id, tiempo_inicio, parametros_proceso):
        """
        Procesa cada tabla SQL por separado, creando una tabla destino individual
        para cada tabla origen con los datos reales de la tabla (NO metadatos del proceso)
        
        Args:
            tracker: Instancia de ProcessTracker para logging
            proceso_id: UUID del proceso
            tiempo_inicio: Timestamp de inicio
            parametros_proceso: Dict con parámetros del proceso
            
        Returns:
            Tuple[bool, Dict]: (éxito, información_resultado)
        """
        import pandas as pd
        import json
        from django.utils import timezone
        
        try:
            # Asegurarse de que exista la tabla de prueba antes de intentar extracción
            from .sql_validation import ensure_test_table
            
            # Verificar conexión antes de continuar
            if not self.source or not self.source.connection:
                error_msg = "No hay conexión SQL configurada"
                print(f"❌ ERROR: {error_msg}")
                return False, {
                    'success': False,
                    'error': error_msg,
                    'process_type': 'sql_processing'
                }
            
            # Asegurar que la tabla de prueba esté disponible como fallback
            TEST_TABLE_NAME = ensure_test_table(self.source.connection)
            if not TEST_TABLE_NAME:
                print("⚠️ No se pudo crear la tabla de prueba, pero intentaremos continuar...")
            
            # Extraer datos de todas las tablas SQL
            datos_sql = self._extract_sql_data()
            tiempo_extraccion = timezone.now()
            duracion_extraccion = (tiempo_extraccion - tiempo_inicio).total_seconds()
            
            if isinstance(datos_sql, dict) and 'error' in datos_sql:
                # Si hay error con las tablas seleccionadas, intentar usar tabla de prueba
                if datos_sql['error'] == 'No hay tablas seleccionadas' and TEST_TABLE_NAME:
                    print(f"⚠️ Usando tabla de prueba {TEST_TABLE_NAME} como fallback...")
                    self.selected_tables = [TEST_TABLE_NAME]
                    self.save()
                    
                    # Reintentar extracción con la tabla de prueba
                    datos_sql = self._extract_sql_data()
                    if isinstance(datos_sql, dict) and 'error' in datos_sql:
                        return False, {
                            'success': False,
                            'error': f"Error incluso con tabla de prueba: {datos_sql['error']}",
                            'process_type': 'sql_processing'
                        }
                else:
                    return False, {
                        'success': False,
                        'error': datos_sql['error'],
                        'process_type': 'sql_processing'
                    }
            
            # Agrupar datos por tabla
            tablas_data = {}
            total_registros = 0
            tablas_con_error = []
            tablas_columnas = {}
            
            for registro in datos_sql:
                if registro.get('metadata'):
                    nombre_tabla_meta = registro.get('table_name')
                    if nombre_tabla_meta:
                        tablas_columnas[nombre_tabla_meta] = registro.get('columns', [])
                        if registro.get('row_count', 0) == 0 and nombre_tabla_meta not in tablas_data:
                            tablas_data[nombre_tabla_meta] = []
                    continue
                
                if 'error' in registro:
                    print(f"⚠️  Error en tabla {registro.get('table_name', 'desconocida')}: {registro['error']}")
                    # Guardar errores de tablas para reportar después
                    tablas_con_error.append({
                        'tabla': registro.get('table_name', 'desconocida'),
                        'error': registro['error']
                    })
                    continue
                    
                table_name = registro.get('table_name')
                if not table_name:
                    continue
                
                if table_name not in tablas_data:
                    tablas_data[table_name] = []
                
                # Eliminar campos de control para obtener datos limpios
                datos_limpios = {k: v for k, v in registro.items() 
                               if k not in ['table_name', 'row_index']}
                tablas_data[table_name].append(datos_limpios)
                total_registros += 1
                
            # Si todas las tablas tienen errores y no hay datos válidos, reportar error
            if tablas_con_error and len(tablas_con_error) == len(datos_sql):
                error_msg = f"Error en todas las tablas seleccionadas"
                if len(tablas_con_error) == 1:
                    error_msg = f"Error en tabla {tablas_con_error[0]['tabla']}: {tablas_con_error[0]['error']}"
                return False, {
                    'success': False,
                    'error': error_msg,
                    'process_type': 'sql_processing',
                    'tablas_con_error': tablas_con_error
                }
            
            # Si no hay tablas con datos pero sí con metadatos (tablas vacías), no es un error
            if not tablas_data and not tablas_columnas:
                error_msg = "No se encontraron tablas válidas o con datos para procesar."
                tracker.finalizar('ERROR', error_msg)
                return False, {
                    'success': False,
                    'error': error_msg,
                    'process_type': 'sql_processing',
                }
            
            # Asegurar que todas las tablas de 'tablas_columnas' existan en 'tablas_data'
            for nombre_tabla in tablas_columnas:
                if nombre_tabla not in tablas_data:
                    tablas_data[nombre_tabla] = []
            
            # Actualizar estado
            tracker.actualizar_estado('PROCESANDO_DATOS', 
                f'Procesando {len(tablas_data)} tablas SQL con {total_registros} registros totales')
            
            # Procesar cada tabla por separado
            tablas_exitosas = 0
            tablas_con_error = 0
            
            for nombre_tabla, datos_tabla in tablas_data.items():
                print(f"\n📊 Procesando tabla SQL: {nombre_tabla}")
                print(f"   📈 Registros encontrados: {len(datos_tabla)}")
                
                # Convertir datos a DataFrame
                df_datos = pd.DataFrame(datos_tabla)
                if df_datos.empty and nombre_tabla in tablas_columnas:
                    df_datos = pd.DataFrame(columns=tablas_columnas[nombre_tabla])
                
                if df_datos.empty:
                    print(f"⚠️  Tabla {nombre_tabla} está vacía en origen, se creará estructura vacía en destino...")
                
                print(f"   📋 Columnas detectadas: {list(df_datos.columns)}")
                
                # Generar nombre de tabla destino: proceso_nombreTabla (sin caracteres problemáticos)
                nombre_tabla_normalizada = nombre_tabla.replace('.', '_')
                nombre_tabla_destino = f"{self.name.replace(' ', '_')}_{nombre_tabla_normalizada}"
                
                # Actualizar estado para esta tabla específica
                duracion_tabla = (timezone.now() - tiempo_extraccion).total_seconds()
                tracker.actualizar_estado('GUARDANDO_DATOS', 
                    f'Guardando tabla {nombre_tabla} ({len(datos_tabla)} registros)')
                
                # Guardar DataFrame en la base de datos destino
                exito_guardado, resultado_guardado = self._save_dataframe_to_destination(
                    df_datos=df_datos,
                    nombre_tabla_destino=nombre_tabla_destino,
                    proceso_id=proceso_id,
                    usuario_responsable='sistema_automatizado',
                    source_table_name=nombre_tabla  # Pasar nombre de tabla origen para aplicar mapeos
                )
                
                if exito_guardado:
                    tablas_exitosas += 1
                    print(f"✅ Tabla {nombre_tabla} guardada exitosamente como {nombre_tabla_destino}")
                else:
                    tablas_con_error += 1
                    print(f"❌ Error guardando tabla {nombre_tabla}: {resultado_guardado.get('error', 'Error desconocido')}")
            
            # Calcular duración total
            tiempo_fin = timezone.now()
            duracion_total = (tiempo_fin - tiempo_inicio).total_seconds()
            
            # Determinar éxito general
            success = tablas_exitosas > 0
            
            # Crear resultado final
            result_info = {
                'success': success,
                'process_type': 'sql_multi_table',
                'tablas_procesadas': tablas_exitosas,
                'tablas_con_error': tablas_con_error,
                'total_registros': total_registros,
                'duracion_total': duracion_total,
                'duracion_extraccion': duracion_extraccion,
                'proceso_id': proceso_id,
                'detalles_tablas': {
                    tabla: len(datos) for tabla, datos in tablas_data.items()
                }
            }
            
            # Actualizar estado final
            if success:
                tracker.finalizar('COMPLETADO', 
                    f'SQL procesado: {tablas_exitosas} tablas exitosas, {total_registros} registros totales')
            else:
                tracker.finalizar('ERROR', 
                    f'Error procesando SQL: {tablas_con_error} tablas con error')
            
            return success, result_info
            
        except Exception as e:
            # Crear un mensaje de error más descriptivo
            error_msg = f"Error procesando tablas SQL individualmente: {str(e)}"
            print(f"❌ {error_msg}")
            
            # Incluir detalles adicionales sobre las tablas con problemas
            detalles_error = f"Error procesando SQL '{self.name}': "
            if 'tablas_con_error' in locals() and tablas_con_error:
                detalles_error += ", ".join([f"Tabla {t['tabla']}: {t['error']}" for t in tablas_con_error[:3]])
                if len(tablas_con_error) > 3:
                    detalles_error += f" y {len(tablas_con_error) - 3} errores más"
            else:
                detalles_error += str(e)
            
            tracker.finalizar('ERROR', detalles_error)
            
            return False, {
                'success': False,
                'error': error_msg,
                'process_type': 'sql_processing'
            }
    
    def to_dict(self):
        """
        Convierte el proceso a un diccionario para facilitar la serialización
        """
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'source_type': self.source.source_type,
            'source_id': self.source.id,
            'selected_sheets': self.selected_sheets,
            'selected_tables': self.selected_tables,
            'selected_columns': self.selected_columns,
            'target_db_name': self.target_db_name,
            'status': self.status,
            'created_at': self.created_at.isoformat(),
            'last_run': self.last_run.isoformat() if self.last_run else None
        }
    
    def _normalize_table_name(self, sheet_name):
        """
        Normaliza el nombre de una hoja de Excel para usar como nombre de tabla
        
        Args:
            sheet_name (str): Nombre original de la hoja
            
        Returns:
            str: Nombre normalizado apto para usar como tabla
        """
        import re
        
        # Remover/reemplazar caracteres especiales
        normalized = sheet_name
        
        # Reemplazos específicos para caracteres problemáticos
        normalized = normalized.replace('Ñ', 'N').replace('ñ', 'n')
        normalized = normalized.replace('Á', 'A').replace('á', 'a')
        normalized = normalized.replace('É', 'E').replace('é', 'e')
        normalized = normalized.replace('Í', 'I').replace('í', 'i')
        normalized = normalized.replace('Ó', 'O').replace('ó', 'o')
        normalized = normalized.replace('Ú', 'U').replace('ú', 'u')
        normalized = normalized.replace('Ü', 'U').replace('ü', 'u')
        
        # Reemplazar espacios y caracteres especiales con guiones bajos
        normalized = re.sub(r'[^\w\s-]', '_', normalized)
        normalized = re.sub(r'[\s-]+', '_', normalized)
        
        # Remover guiones bajos al inicio y final
        normalized = normalized.strip('_')
        
        # Asegurar que no esté vacío y que sea válido
        if not normalized or normalized.isdigit():
            normalized = f'Hoja_{normalized or "1"}'
        
        # Limitar longitud
        if len(normalized) > 50:
            normalized = normalized[:50].rstrip('_')
            
        return normalized
    
    def _clean_excel_dataframe(self, df):
        """
        Limpia el DataFrame de Excel: renombra columnas Unnamed y reemplaza valores NaN
        """
        import pandas as pd
        import numpy as np
        
        # Limpiar nombres de columnas
        new_columns = []
        unnamed_counter = 1
        
        for col in df.columns:
            col_str = str(col)
            if col_str.startswith('Unnamed'):
                # Renombrar columnas Unnamed con un nombre más descriptivo
                new_name = f'Columna_{unnamed_counter}'
                unnamed_counter += 1
            elif pd.isna(col) or col_str.lower() in ['nan', 'null', '']:
                # Manejar columnas con nombres nulos o vacíos
                new_name = f'Columna_{unnamed_counter}'
                unnamed_counter += 1
            else:
                new_name = col_str
            
            new_columns.append(new_name)
        
        # Aplicar nuevos nombres de columnas
        df.columns = new_columns
        
        # Reemplazar valores NaN, None, y variantes de 'nan'
        df = df.fillna('')  # Reemplazar NaN con cadena vacía
        
        # Reemplazar valores de texto que son 'nan', 'null', etc.
        for col in df.columns:
            if df[col].dtype == 'object':  # Columnas de texto
                df[col] = df[col].astype(str)
                df[col] = df[col].replace({
                    'nan': '',
                    'NaN': '',
                    'null': '',
                    'NULL': '',
                    'None': '',
                    '<NA>': ''
                })
        
        return df

    def _save_dataframe_to_destination(self, df_datos, nombre_tabla_destino, proceso_id, usuario_responsable, source_table_name=None):
        """
        Guarda un DataFrame directamente a la base de datos destino como una tabla
        con la estructura exacta del DataFrame (NO metadatos del proceso)
        
        Args:
            df_datos: DataFrame de Pandas con los datos reales del archivo/tabla
            nombre_tabla_destino: Nombre que tendrá la tabla en la BD destino
            proceso_id: UUID del proceso para logging
            usuario_responsable: Usuario responsable del proceso
            source_table_name: Nombre de la tabla/hoja origen (para aplicar column_mappings)
            
        Returns:
            Tuple[bool, Dict]: (éxito, información_resultado)
        """
        import pandas as pd
        import pyodbc
        from django.conf import settings
        
        try:
            print(f"🔍 DEBUG: Iniciando guardado de DataFrame '{nombre_tabla_destino}'")
            print(f"🔍 DEBUG: DataFrame shape: {df_datos.shape}")
            print(f"🔍 DEBUG: DataFrame columnas: {list(df_datos.columns)}")
            
            # Usar conexión directa pyodbc para evitar problemas con Django ORM
            destino_config = settings.DATABASES['destino']
            
            # Construir connection string con parámetros correctos
            server = destino_config.get('HOST', 'localhost')
            port = destino_config.get('PORT')
            database = destino_config.get('NAME', 'DestinoAutomatizacion')
            username = destino_config.get('USER', '')
            password = destino_config.get('PASSWORD', '')
            
            # Construir servidor con puerto solo si está especificado
            if port:
                server_with_port = f"{server},{port}"
            else:
                server_with_port = server
            
            connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={server_with_port};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
                f"TrustServerCertificate=yes;"
            )
            
            print(f"🔍 DEBUG: Conectando a BD - Server: {server_with_port}, DB: {database}, User: {username}")
            print(f"🔍 DEBUG: Connection string (sin pwd): DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_with_port};DATABASE={database};UID={username};TrustServerCertificate=yes;")
            
            conn = pyodbc.connect(connection_string)
            cursor = conn.cursor()
            print(f"✅ DEBUG: Conexión a BD exitosa")
            
            # 1. Crear tabla con estructura del DataFrame
            print(f"📋 Creando tabla '{nombre_tabla_destino}' con estructura del DataFrame...")
            
            # Generar SQL CREATE TABLE basado en las columnas del DataFrame
            create_table_sql = self._generate_create_table_sql(df_datos, nombre_tabla_destino, source_table_name)
            
            # Eliminar tabla si existe y crearla nueva
            cursor.execute(f"IF OBJECT_ID('{nombre_tabla_destino}', 'U') IS NOT NULL DROP TABLE [{nombre_tabla_destino}]")
            cursor.execute(create_table_sql)
            
            # Commit inmediato después de crear la tabla para que persista incluso si hay errores de inserción
            conn.commit()
            
            print(f"✅ Tabla '{nombre_tabla_destino}' creada exitosamente")
            print(f"   📊 Columnas: {list(df_datos.columns)}")
            print(f"   📈 Filas a insertar: {len(df_datos)}")
            
            # 2. Insertar datos del DataFrame
            registros_insertados = 0
            if not df_datos.empty:
                # Obtener mapeos de columnas si existen
                column_mappings = {}
                if self.column_mappings and source_table_name and source_table_name in self.column_mappings:
                    column_mappings = self.column_mappings[source_table_name]
                
                # Preparar SQL INSERT con columnas limpias (usando nombres mapeados)
                clean_columns_list = []
                for col in df_datos.columns:
                    # 🔧 FIX: column_mappings es un dict de dicts, no un dict simple
                    # Estructura: {'col1': {'renamed_to': 'nuevo_nombre', 'sql_type': ..., ...}}
                    if isinstance(column_mappings.get(col), dict):
                        custom_name = column_mappings[col].get('renamed_to', col)
                    else:
                        custom_name = col
                    
                    clean_col = custom_name.replace(' ', '_').replace('-', '_')
                    clean_col = ''.join(c for c in clean_col if c.isalnum() or c == '_')
                    clean_columns_list.append(clean_col)
                
                columns_sql = ', '.join([f'[{col}]' for col in clean_columns_list])
                placeholders = ', '.join(['?' for _ in clean_columns_list])
                insert_sql = f"INSERT INTO [{nombre_tabla_destino}] ({columns_sql}) VALUES ({placeholders})"
                
                print(f"🔍 SQL INSERT: {insert_sql}")

                # 🆕 PASO 1: Validar y normalizar con el nuevo sistema de validadores
                from .utils.validators import validate_column_mappings, normalize_dataframe_by_mappings
                import logging
                logger = logging.getLogger(__name__)
                
                # Validar configuración de columnas antes de procesar
                if column_mappings:
                    is_valid, validation_errors = validate_column_mappings(df_datos, column_mappings)
                    
                    if not is_valid:
                        logger.error(f"❌ Errores de validación en '{nombre_tabla_destino}':")
                        for error in validation_errors:
                            logger.error(f"  • {error}")
                        # Continuar pero registrar errores
                    else:
                        logger.info(f"✅ Validación de configuración exitosa para '{nombre_tabla_destino}'")
                
                # Normalizar DataFrame usando column_mappings
                if column_mappings:
                    df_normalized, normalization_warnings = normalize_dataframe_by_mappings(df_datos, column_mappings)
                    
                    if normalization_warnings:
                        logger.warning(f"⚠️ Advertencias de normalización en '{nombre_tabla_destino}':")
                        for warning in normalization_warnings:
                            logger.warning(f"  • {warning}")
                    
                    logger.info(f"✅ Normalización completada: {len(df_datos)} filas procesadas")
                else:
                    # Si no hay column_mappings, usar normalización básica
                    try:
                        from .sql_utils import normalize_df_for_sql
                    except Exception as e:
                        print(f"ERROR: No se pudo importar sql_utils.normalize_df_for_sql: {e}")
                        raise

                    df_normalized, normalization_issues = normalize_df_for_sql(df_datos, strict=False)
                    if normalization_issues:
                        # Registrar advertencias de normalización en tracker/log
                        print(f"⚠️ Advertencias de normalización antes de insertar: {normalization_issues}")
                        
                        # Crear mensaje detallado para el usuario
                        warning_msg = f"⚠️ Normalización de datos para '{nombre_tabla_destino}':\n"
                        for issue in normalization_issues:
                            warning_msg += f"  • Columna '{issue['column']}': {issue['count']} valores inválidos convertidos a NULL\n"
                            warning_msg += f"    Ejemplo: '{issue.get('example', 'N/A')}'\n"
                        
                        # Imprimir warning completo en el log
                        print(warning_msg)

                # Aplicar defaults usando la función dedicada (PASO 2 - ya existente)
                # Obtener configuración de columnas para esta hoja
                column_configs = {}
                if self.column_mappings and source_table_name and source_table_name in self.column_mappings:
                    column_configs = self.column_mappings[source_table_name]
                
                # Aplicar defaults usando la función dedicada
                if column_configs:
                    try:
                        from .sql_utils import apply_default_values_from_mappings
                        df_with_defaults = apply_default_values_from_mappings(df_normalized, column_configs)
                        print(f"✅ Valores por defecto aplicados según column_mappings")
                    except Exception as e:
                        print(f"⚠️ Error aplicando valores por defecto: {e}")
                        df_with_defaults = df_normalized
                else:
                    df_with_defaults = df_normalized
                
                # PASO 3: Convertir DataFrame a lista de tuplas para inserción SQL
                valores_a_insertar = []
                for _, row in df_with_defaults.iterrows():
                    valores_fila = []
                    for col in df_with_defaults.columns:
                        valor = row[col]
                        
                        # Convertir tipos especiales de pandas/numpy a tipos Python nativos
                        if pd.isna(valor):
                            valores_fila.append(None)
                        elif isinstance(valor, pd.Timestamp):
                            valores_fila.append(valor.to_pydatetime())
                        else:
                            # Si es numpy types, convertir a Python native
                            try:
                                if hasattr(valor, 'item'):
                                    valores_fila.append(valor.item())
                                else:
                                    valores_fila.append(valor)
                            except Exception:
                                valores_fila.append(valor)
                    valores_a_insertar.append(tuple(valores_fila))

                try:
                    if valores_a_insertar:
                        # Usar executemany para una inserción eficiente
                        cursor.executemany(insert_sql, valores_a_insertar)
                        registros_insertados = cursor.rowcount if cursor.rowcount != -1 else len(valores_a_insertar)
                        print(f"   ✅ Inserción masiva exitosa. Registros afectados: {registros_insertados}")
                    else:
                        print("   ⚠️ No hay datos para insertar.")

                except Exception as insert_error:
                    # Si executemany falla, intentar inserción fila por fila para depurar
                    print(f"⚠️  Error en inserción masiva: {insert_error}. Intentando fila por fila...")
                    registros_insertados = 0
                    for i, valores_fila in enumerate(valores_a_insertar):
                        try:
                            cursor.execute(insert_sql, valores_fila)
                            registros_insertados += 1
                        except Exception as single_insert_error:
                            print(f"❌ Error insertando fila {i}: {single_insert_error}")
                            print(f"   📊 Valores: {valores_fila}")
                            # Opcional: decidir si continuar o detenerse
                            # continue
            else:
                print("   ⚠️ DataFrame vacío, no se insertarán datos.")
            
            # Confirmar transacción
            conn.commit()
            
            print(f"✅ Datos insertados exitosamente:")
            print(f"   📊 Registros insertados: {registros_insertados}")
            print(f"   📋 Tabla final: '{nombre_tabla_destino}'")
            
            # Cerrar conexión
            cursor.close()
            conn.close()
            
            # 🆕 GUARDAR RESUMEN EN ResultadosProcesados
            try:
                from .models_destino import ResultadosProcesados
                from datetime import datetime
                import json
                
                # Calcular tiempo de ejecución (aproximado)
                tiempo_ejecucion = 0.0  # Se puede mejorar pasando tiempo_inicio como parámetro
                
                # Preparar datos procesados para el resumen
                datos_json = {
                    'tabla_destino': nombre_tabla_destino,
                    'campos_columnas': list(df_datos.columns),
                    'total_registros_cargados': registros_insertados,
                    'estado_final': 'COMPLETADO',
                    'timestamp_procesamiento': datetime.now().isoformat(),
                    'fuente': source_table_name if source_table_name else 'N/A'
                }
                
                # Metadatos del proceso
                metadatos_proceso = {
                    'version_proceso': '1.0',
                    'tabla_creada': nombre_tabla_destino,
                    'columnas_procesadas': len(df_datos.columns),
                    'hoja_origen': source_table_name if source_table_name else None
                }
                
                # Crear registro en ResultadosProcesados
                resultado = ResultadosProcesados(
                    ProcesoID=proceso_id,
                    NombreProceso=self.name,
                    DatosProcesados=json.dumps(datos_json, ensure_ascii=False),
                    UsuarioResponsable=usuario_responsable,
                    EstadoProceso='COMPLETADO',
                    TipoOperacion=f'MIGRACION_{nombre_tabla_destino.upper()}',
                    RegistrosAfectados=registros_insertados,
                    TiempoEjecucion=tiempo_ejecucion,
                    MetadatosProceso=json.dumps(metadatos_proceso, ensure_ascii=False)
                )
                
                # Guardar usando la conexión destino
                resultado.save(using='destino')
                
                print(f"✅ Resumen guardado en ResultadosProcesados - ID: {resultado.ResultadoID}")
                
            except Exception as e:
                print(f"⚠️ Advertencia: No se pudo guardar resumen en ResultadosProcesados: {str(e)}")
                # No detener el proceso por este error
            
            return True, {
                'success': True,
                'table_name': nombre_tabla_destino,
                'records_inserted': registros_insertados,
                'columns': list(df_datos.columns),
                'proceso_id': proceso_id
            }
            
        except Exception as e:
            error_msg = f"Error guardando DataFrame en tabla '{nombre_tabla_destino}': {str(e)}"
            print(f"❌ {error_msg}")
            
            # Cerrar conexiones en caso de error
            try:
                if 'cursor' in locals():
                    cursor.close()
                if 'conn' in locals():
                    conn.close()
            except:
                pass
            
            # 🆕 GUARDAR RESUMEN DE ERROR EN ResultadosProcesados
            try:
                from .models_destino import ResultadosProcesados
                from datetime import datetime
                import json
                
                # Preparar datos de error
                datos_json = {
                    'tabla_destino': nombre_tabla_destino,
                    'estado_final': 'ERROR',
                    'timestamp_procesamiento': datetime.now().isoformat(),
                    'detalles_error': error_msg,
                    'fuente': source_table_name if source_table_name else 'N/A'
                }
                
                # Metadatos del error
                metadatos_proceso = {
                    'version_proceso': '1.0',
                    'tabla_objetivo': nombre_tabla_destino,
                    'error_completo': str(e),
                    'hoja_origen': source_table_name if source_table_name else None
                }
                
                # Crear registro de error en ResultadosProcesados
                resultado = ResultadosProcesados(
                    ProcesoID=proceso_id,
                    NombreProceso=self.name,
                    DatosProcesados=json.dumps(datos_json, ensure_ascii=False),
                    UsuarioResponsable=usuario_responsable,
                    EstadoProceso='ERROR',
                    TipoOperacion=f'MIGRACION_{nombre_tabla_destino.upper()}',
                    RegistrosAfectados=0,
                    TiempoEjecucion=0.0,
                    MetadatosProceso=json.dumps(metadatos_proceso, ensure_ascii=False)
                )
                
                # Guardar usando la conexión destino
                resultado.save(using='destino')
                
                print(f"✅ Resumen de error guardado en ResultadosProcesados - ID: {resultado.ResultadoID}")
                
            except Exception as error_log:
                print(f"⚠️ Advertencia: No se pudo guardar resumen de error en ResultadosProcesados: {str(error_log)}")
                # No detener el proceso por este error
            
            return False, {
                'success': False,
                'error': error_msg,
                'table_name': nombre_tabla_destino,
                'proceso_id': proceso_id
            }

    def _generate_create_table_sql(self, df, table_name, source_table_name=None):
        """
        Genera SQL CREATE TABLE basado en las columnas y tipos del DataFrame
        
        Args:
            df: DataFrame de Pandas
            table_name: Nombre de la tabla destino a crear
            source_table_name: Nombre de la tabla/hoja origen (para aplicar column_mappings)
            
        Returns:
            str: SQL CREATE TABLE statement
        """
        import pandas as pd
        from datetime import datetime
        
        columns_definitions = []
        
        # Obtener mapeos de columnas si existen
        column_mappings = {}
        if self.column_mappings and source_table_name and source_table_name in self.column_mappings:
            column_mappings = self.column_mappings[source_table_name]
            print(f"🔍 DEBUG: Aplicando mapeos de columnas para '{source_table_name}': {column_mappings}")
        
        for column in df.columns:
            # Obtener configuración de la columna (puede ser string simple o dict completo)
            column_config = column_mappings.get(column)
            
            # Compatibilidad con formato antiguo (string) y nuevo (dict)
            if isinstance(column_config, str):
                # Formato antiguo: solo nombre renombrado
                custom_column_name = column_config
                nullable = True
                default_value = None
                sql_type_override = None
            elif isinstance(column_config, dict):
                # Formato nuevo: configuración completa
                custom_column_name = column_config.get('renamed_to', column)
                nullable = column_config.get('nullable', True)
                default_value = column_config.get('default_value')
                sql_type_override = column_config.get('sql_type')
            else:
                # Sin configuración: usar valores por defecto
                custom_column_name = column
                nullable = True
                default_value = None
                sql_type_override = None
            
            # Limpiar nombre de columna para SQL
            clean_column = str(custom_column_name).replace(' ', '_').replace('-', '_')
            clean_column = ''.join(c for c in clean_column if c.isalnum() or c == '_')
            
            # Determinar tipo SQL
            if sql_type_override:
                sql_type = sql_type_override
            else:
                # Determinar tipo SQL basado en el tipo del DataFrame
                dtype = df[column].dtype
                
                if pd.api.types.is_integer_dtype(dtype):
                    sql_type = 'INT'
                elif pd.api.types.is_float_dtype(dtype):
                    sql_type = 'FLOAT'
                elif pd.api.types.is_bool_dtype(dtype):
                    sql_type = 'BIT'
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    sql_type = 'DATETIME2'
                else:
                    # Para strings y otros tipos, usar NVARCHAR
                    max_length = df[column].astype(str).str.len().max()
                    if pd.isna(max_length) or max_length == 0:
                        max_length = 255
                    elif max_length < 50:
                        max_length = 50
                    elif max_length > 4000:
                        max_length = 4000
                    
                    sql_type = f'NVARCHAR({int(max_length)})'
            
            # Construir definición de columna con nullable y default
            null_clause = '' if nullable else 'NOT NULL'
            default_clause = ''
            
            if not nullable and default_value:
                # Manejar diferentes tipos de valores por defecto
                if default_value == 'GETDATE()':
                    default_clause = 'DEFAULT GETDATE()'
                elif isinstance(default_value, str) and default_value.startswith("'") and default_value.endswith("'"):
                    # Ya tiene comillas (ej: "' '")
                    default_clause = f'DEFAULT {default_value}'
                elif isinstance(default_value, str) and default_value.replace('.', '', 1).replace('-', '', 1).isdigit():
                    # Es un número (ej: "0", "0.00")
                    default_clause = f'DEFAULT {default_value}'
                elif isinstance(default_value, (int, float)):
                    # Número nativo de Python
                    default_clause = f'DEFAULT {default_value}'
                elif isinstance(default_value, str):
                    # String sin comillas: agregárselas
                    default_clause = f"DEFAULT '{default_value}'"
                else:
                    # Otros tipos: convertir a string
                    default_clause = f"DEFAULT '{str(default_value)}'"
            
            column_def = f'[{clean_column}] {sql_type} {null_clause} {default_clause}'.strip()
            columns_definitions.append(column_def)
        
        # Construir SQL CREATE TABLE
        create_sql = f"""
        CREATE TABLE [{table_name}] (
            {', '.join(columns_definitions)}
        )
        """
        
        return create_sql

class MigrationLog(models.Model):
    """
    Registra eventos y resultados de cada ejecución del proceso de migración
    """
    LOG_LEVELS = [
        ('info', 'Información'),
        ('warning', 'Advertencia'),
        ('error', 'Error'),
        ('critical', 'Crítico'),
        ('success', 'Éxito'),
        ('debug', 'Depuración')
    ]
    
    LOG_STAGES = [
        ('connection', 'Conexión'),
        ('database_list', 'Listar bases de datos'),
        ('database_select', 'Selección de base de datos'),
        ('table_list', 'Listar tablas'),
        ('table_select', 'Selección de tablas'),
        ('column_list', 'Listar columnas'),
        ('column_select', 'Selección de columnas'),
        ('validation', 'Validación'),
        ('data_extraction', 'Extracción de datos'),
        ('data_transformation', 'Transformación de datos'),
        ('data_loading', 'Carga de datos'),
        ('rollback', 'Rollback'),
        ('completion', 'Finalización')
    ]
    
    process = models.ForeignKey(MigrationProcess, on_delete=models.CASCADE, related_name='logs')
    timestamp = models.DateTimeField(auto_now_add=True)
    stage = models.CharField(max_length=30, choices=LOG_STAGES)
    level = models.CharField(max_length=20, choices=LOG_LEVELS, default='info')
    message = models.TextField()
    rows_processed = models.IntegerField(default=0)
    duration_ms = models.IntegerField(default=0)
    error_message = models.TextField(blank=True, null=True)
    details = models.JSONField(null=True, blank=True)
    user = models.CharField(max_length=100, blank=True, null=True)  # Para registrar qué usuario realizó la acción
    
    def __str__(self):
        return f"Log {self.id} - {self.process.name} - {self.stage} - {self.level}"
    
    @classmethod
    def log(cls, process, stage, message, level='info', rows=0, duration=0, error=None, details=None, user=None):
        """
        Método de clase para crear un nuevo registro de log
        """
        return cls.objects.create(
            process=process,
            stage=stage,
            message=message,
            level=level,
            rows_processed=rows,
            duration_ms=duration,
            error_message=error,
            details=details or {},
            user=user
        )
        
    def complete_log(self, stage, message=None, rows_processed=0, duration_ms=0, error_message=None):
        """
        Actualiza un registro de log existente con información de finalización
        """
        self.stage = stage
        if message:
            self.message = message
        self.rows_processed = rows_processed
        self.duration_ms = duration_ms
        self.error_message = error_message
        self.save()
        return self