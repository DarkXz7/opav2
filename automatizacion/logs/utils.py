"""
Utilidades para el registro de procesos en SQL Server
Facilita el registro de eventos en la tabla ProcesoLog
"""

import datetime
import json
import time
from automatizacion.logs.models_logs import ProcesoLog

class ProcesoLogger:
    """
    Clase para gestionar el registro de procesos en la tabla ProcesoLog
    Permite registrar inicio, fin, éxito y error de procesos
    """
    
    def __init__(self, nombre_proceso):
        """
        Inicializa un nuevo registro de proceso
        
        Args:
            nombre_proceso (str): Nombre o identificador del proceso
        """
        self.nombre_proceso = nombre_proceso
        self.tiempo_inicio = None
        self.parametros = {}
        self.proceso_id = None
    
    def iniciar(self, parametros=None):
        """
        Registra el inicio de un proceso
        
        Args:
            parametros (dict, optional): Parámetros de entrada del proceso
        
        Returns:
            str: ID del proceso registrado
        """
        import uuid
        self.tiempo_inicio = time.time()
        self.parametros = parametros or {}
        self.proceso_id = str(uuid.uuid4())  # Generar UUID único
        
        # Crear registro inicial
        log = ProcesoLog(
            ProcesoID=self.proceso_id,  # Usar el UUID generado
            NombreProceso=self.nombre_proceso[:255],  # Usar campo NombreProceso
            FechaEjecucion=datetime.datetime.now(),
            Estado="Iniciando"[:20],  # Solo el estado, sin el nombre del proceso
            ParametrosEntrada=json.dumps(self.parametros) if self.parametros else None,
            DuracionSegundos=0,
            MensajeError=None
        )
        
        # Guardar en la base de datos SQL Server
        log.save(using='logs')
        # El proceso_id ya está establecido arriba
        
        return self.proceso_id
    
    def finalizar_exito(self, detalles=None):
        """
        Registra la finalización exitosa de un proceso
        
        Args:
            detalles (str, optional): Detalles adicionales del éxito
        
        Returns:
            str: ID del proceso actualizado
        """
        if not self.tiempo_inicio:
            raise ValueError("Debe llamar a iniciar() antes de finalizar_exito()")
        
        duracion = time.time() - self.tiempo_inicio
        
        # Actualizar el registro existente
        try:
            log = ProcesoLog.objects.using('logs').get(ProcesoID=self.proceso_id)
            log.Estado = "Completado"[:20]
            log.DuracionSegundos = int(round(duracion))
            if detalles:
                log.MensajeError = detalles
            log.save(using='logs')
        except ProcesoLog.DoesNotExist:
            # Si no existe, crear uno nuevo
            log = ProcesoLog(
                ProcesoID=self.proceso_id,
                NombreProceso=self.nombre_proceso[:255],
                FechaEjecucion=datetime.datetime.now(),
                Estado="Completado"[:20],
                ParametrosEntrada=json.dumps(self.parametros) if self.parametros else None,
                DuracionSegundos=int(round(duracion)),
                MensajeError=detalles
            )
            log.save(using='logs')
        
        return self.proceso_id
    
    def finalizar_error(self, error):
        """
        Registra la finalización con error de un proceso
        
        Args:
            error (str): Detalles del error
        
        Returns:
            str: ID del proceso actualizado
        """
        if not self.tiempo_inicio:
            raise ValueError("Debe llamar a iniciar() antes de finalizar_error()")
        
        duracion = time.time() - self.tiempo_inicio
        
        # Actualizar el registro existente
        try:
            log = ProcesoLog.objects.using('logs').get(ProcesoID=self.proceso_id)
            log.Estado = "Error"[:20]
            log.DuracionSegundos = int(round(duracion))
            log.MensajeError = str(error)
            log.save(using='logs')
        except ProcesoLog.DoesNotExist:
            # Si no existe, crear uno nuevo
            log = ProcesoLog(
                ProcesoID=self.proceso_id,
                NombreProceso=self.nombre_proceso[:255],
                FechaEjecucion=datetime.datetime.now(),
                Estado="Error"[:20],
                ParametrosEntrada=json.dumps(self.parametros) if self.parametros else None,
                DuracionSegundos=int(round(duracion)),
                MensajeError=str(error)
            )
            log.save(using='logs')
        
        return self.proceso_id


def registrar_evento(nombre_evento, estado, parametros=None, error=None):
    """
    Función auxiliar para registrar un evento simple
    
    Args:
        nombre_evento (str): Nombre del evento
        estado (str): Estado del evento (Completado, Error, etc)
        parametros (dict, optional): Parámetros relevantes
        error (str, optional): Detalles de error si existe
    
    Returns:
        str: ID del proceso registrado
    """
    import uuid
    proceso_id = str(uuid.uuid4())  # Generar UUID único
    
    log = ProcesoLog(
        ProcesoID=proceso_id,
        NombreProceso=nombre_evento[:255],
        FechaEjecucion=datetime.datetime.now(),
        Estado=estado[:20],  # Solo el estado
        ParametrosEntrada=json.dumps(parametros) if parametros else None,
        DuracionSegundos=0,
        MensajeError=error
    )
    
    log.save(using='logs')
    return proceso_id
