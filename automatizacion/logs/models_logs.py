from django.db import models

class ProcesoLog(models.Model):
    """
    Modelo para almacenar registros de logs de procesos en SQL Server Express
    Mapea a la tabla existente 'ProcesoLog' en la base de datos 'LogsAutomatizacion'
    
    IMPORTANTE sobre los IDs:
    - LogID: PK autoincremental único para cada registro de log
    - ProcesoID: UUID de ejecución específica (NO es FK a MigrationProcess.id)
    - MigrationProcessID: FK opcional al proceso configurado (si aplica)
    """
    LogID = models.AutoField(primary_key=True)
    ProcesoID = models.CharField(max_length=36, null=True, blank=True, 
                                help_text="UUID único de la ejecución específica")
    # Agregamos campo para relacionar con MigrationProcess cuando sea relevante
    MigrationProcessID = models.IntegerField(null=True, blank=True,
                                           help_text="FK al proceso configurado (automatizacion_migrationprocess.id)")
    NombreProceso = models.CharField(max_length=255, null=True, blank=True)
    FechaEjecucion = models.DateTimeField()
    Estado = models.CharField(max_length=20)  # Coincide con SQL Server (varchar(20))
    ParametrosEntrada = models.TextField(null=True, blank=True,
                                       help_text="JSON optimizado con parámetros esenciales")
    DuracionSegundos = models.IntegerField(null=True, blank=True)
    MensajeError = models.TextField(null=True, blank=True)
    
    class Meta:
        managed = False  # Django no gestiona esta tabla (ya existe)
        db_table = 'ProcesoLog'  # Nombre exacto de la tabla en SQL Server
        app_label = 'automatizacion'
        verbose_name = 'Log de Proceso'
        verbose_name_plural = 'Logs de Procesos'
    
    def __str__(self):
        return f"Proceso {self.ProcesoID}: {self.Estado} ({self.FechaEjecucion})"
