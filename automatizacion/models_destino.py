"""
Modelos para la base de datos DestinoAutomatizacion SQL Server
"""
from django.db import models
from django.contrib.auth.models import User
import uuid

class ResultadosProcesados(models.Model):
    """
    Modelo para almacenar resultados procesados en SQL Server DestinoAutomatizacion
    Mapea a la tabla 'ResultadosProcesados' existente
    """
    ResultadoID = models.AutoField(primary_key=True)
    ProcesoID = models.CharField(max_length=36, help_text="UUID del proceso que generó este resultado")
    NombreProceso = models.CharField(max_length=200, help_text="Nombre del proceso asignado por el usuario")
    FechaRegistro = models.DateTimeField(auto_now_add=True, help_text="Timestamp automático de creación")
    DatosProcesados = models.TextField(help_text="Datos procesados en formato JSON")
    UsuarioResponsable = models.CharField(max_length=100, help_text="Usuario que ejecutó el proceso")
    
    # Campos adicionales para trazabilidad
    EstadoProceso = models.CharField(max_length=50, default='COMPLETADO', help_text="Estado del proceso")
    TipoOperacion = models.CharField(max_length=100, blank=True, null=True, help_text="Tipo de operación realizada")
    RegistrosAfectados = models.IntegerField(default=0, help_text="Número de registros procesados")
    TiempoEjecucion = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True, help_text="Tiempo de ejecución en segundos")
    MetadatosProceso = models.TextField(blank=True, null=True, help_text="Metadatos adicionales del proceso")
    
    class Meta:
        managed = False  # Django no gestiona esta tabla (debe existir en SQL Server)
        db_table = 'ResultadosProcesados'
        app_label = 'automatizacion'
        verbose_name = 'Resultado Procesado'
        verbose_name_plural = 'Resultados Procesados'
        ordering = ['-FechaRegistro']
    
    def __str__(self):
        return f"Resultado {self.ResultadoID} - Proceso {self.ProcesoID[:8]}... - {self.FechaRegistro}"

class UsuariosDestino(models.Model):
    """
    Modelo para la tabla dbo.Usuarios en DestinoAutomatizacion
    """
    UsuarioID = models.AutoField(primary_key=True)
    NombreUsuario = models.CharField(max_length=100, unique=True)
    Email = models.EmailField(max_length=255)
    NombreCompleto = models.CharField(max_length=200)
    FechaCreacion = models.DateTimeField(auto_now_add=True)
    Activo = models.BooleanField(default=True)
    UltimoAcceso = models.DateTimeField(null=True, blank=True)
    
    class Meta:
        managed = False
        db_table = 'dbo.Usuarios'
        app_label = 'automatizacion'
        verbose_name = 'Usuario Destino'
        verbose_name_plural = 'Usuarios Destino'
    
    def __str__(self):
        return f"{self.NombreCompleto} ({self.NombreUsuario})"
