"""
Clase ProcessTracker para optimizar el registro de procesos
"""

import uuid
import json
import datetime
import time
from django.db import transaction

class ProcessTracker:
    """
    Clase para gestionar el seguimiento y registro de un proceso completo,
    minimizando las entradas en la base de datos mediante la actualización
    de un único registro para todo el ciclo de vida del proceso.
    """
    
    def __init__(self, nombre_proceso):
        """
        Inicializa un nuevo seguimiento de proceso con registro único
        
        Args:
            nombre_proceso (str): Nombre o identificador del proceso
        """
        # Importar desde el modelo principal, no desde logs/
        from automatizacion.logs.models_logs import ProcesoLog
        self.nombre_proceso = nombre_proceso
        self.tiempo_inicio = time.time()
        self.proceso_id = str(uuid.uuid4())  # Generamos un ID único para todo el ciclo de vida
        self.historial = []
        self.ProcesoLog = ProcesoLog
        self._registro = None  # Almacenará la referencia al registro en la BD
        self._estados = {
            'INICIADO': 'Proceso iniciado',
            'EN_PROGRESO': 'En progreso',
            'COMPLETADO': 'Completado exitosamente',
            'ERROR': 'Error en ejecución',
            'CANCELADO': 'Cancelado por usuario'
        }
    
    def _actualizar_historial(self, accion, detalles=None, error=None):
        """
        Actualiza el historial interno del proceso
        
        Args:
            accion (str): Acción o estado actual
            detalles (str, optional): Detalles adicionales
            error (Exception, optional): Error si existe
        """
        entrada = {
            'timestamp': datetime.datetime.now().isoformat(),
            'accion': accion,
            'detalles': detalles
        }
        
        if error:
            entrada['error'] = str(error)
        
        self.historial.append(entrada)
    
    def _actualizar_estado(self, estado, detalles=None, error=None):
        """
        Actualiza el estado del proceso en la base de datos de manera eficiente
        
        Args:
            estado (str): Nuevo estado del proceso
            detalles (str, optional): Detalles del cambio de estado
            error (Exception, optional): Error si existe
        """
        self._actualizar_historial(estado, detalles, error)
        
        if self._registro:
            with transaction.atomic():
                # Actualizar el registro existente en lugar de crear uno nuevo
                duracion = time.time() - self.tiempo_inicio
                self._registro.Estado = self._estados.get(estado, estado)[:20]
                self._registro.DuracionSegundos = int(duracion)
                self._registro.ParametrosEntrada = json.dumps({
                    'proceso_unique_id': self.proceso_id,
                    'historial': self.historial[-3:],  # Solo los últimos 3 eventos
                    'estado_actual': estado
                })
                if error:
                    self._registro.MensajeError = str(error)[:1000]  # Limitar tamaño
                self._registro.save(using='logs')
    
    def _obtener_parametros(self, parametros_adicionales=None):
        """
        Genera parámetros optimizados usando el nuevo sistema
        
        Args:
            parametros_adicionales (dict, optional): Parámetros adicionales
            
        Returns:
            str: JSON optimizado para ParametrosEntrada
        """
        from .parametros_optimizer import optimizar_parametros_entrada
        
        # Preparar datos base
        datos_completos = {
            'proceso_unique_id': str(self.proceso_id),
            'process_name': self.nombre_proceso,
            'timestamp_inicio': datetime.datetime.now().isoformat(),
            'contexto': 'process_tracker'
        }
        
        # Agregar parámetros adicionales si existen
        if parametros_adicionales:
            datos_completos.update(parametros_adicionales)
        
        # Usar el optimizador para generar JSON conciso
        return optimizar_parametros_entrada(datos_completos)
    
    def iniciar(self, parametros=None):
        """
        Registra el inicio de un proceso
        
        Args:
            parametros (dict, optional): Parámetros de entrada del proceso
        
        Returns:
            str: ID único del proceso
        """
        self._actualizar_historial('Iniciando', detalles=f"Iniciando {self.nombre_proceso}")
        
        # Mantener proceso_id como string para usar en parametros JSON
        proceso_id_str = self.proceso_id
        
        with transaction.atomic():
            # Crear UN SOLO registro en la base de datos que se actualizará durante todo el proceso
            print(f"DEBUG: Creando registro en BD para proceso '{self.nombre_proceso}' con ID {proceso_id_str}")
            
            # Obtener parámetros optimizados (ya viene como JSON string)
            parametros_optimizados = self._obtener_parametros(parametros)
            
            # Extraer MigrationProcessID de los parámetros si existe
            migration_process_id = None
            if parametros and isinstance(parametros, dict):
                migration_process_id = parametros.get('migration_process_id')
            
            self._registro = self.ProcesoLog(
                ProcesoID=proceso_id_str,  # UUID único de esta ejecución específica
                MigrationProcessID=migration_process_id,  # FK al proceso configurado (si aplica)
                FechaEjecucion=datetime.datetime.now(),
                Estado="Iniciando"[:20],  # Solo el estado, sin nombre del proceso
                ParametrosEntrada=parametros_optimizados,  # JSON optimizado y conciso
                DuracionSegundos=0,
                MensajeError=None,
                NombreProceso=self.nombre_proceso[:255]  # Nombre del proceso del frontend
            )
            print(f"DEBUG: Guardando registro usando base de datos 'logs'...")
            self._registro.save(using='logs')
            print(f"DEBUG: Registro guardado exitosamente con parámetros optimizados")
        
        return proceso_id_str
    
    def actualizar_estado(self, estado, detalles=None):
        """
        Actualiza el estado del proceso sin finalizar
        
        Args:
            estado (str): Nuevo estado del proceso
            detalles (str, optional): Detalles adicionales
        
        Returns:
            str: ID del proceso
        """
        duracion = int(round(time.time() - self.tiempo_inicio))
        self._actualizar_historial(estado, detalles=detalles)
        
        # Solo actualizar el registro existente, NO crear uno nuevo
        if self._registro:
            with transaction.atomic():
                # Actualizar registro existente en lugar de crear uno nuevo
                self._registro.Estado = f"{estado}"[:20]  # Solo el estado actual
                self._registro.ParametrosEntrada = json.dumps(self._obtener_parametros())
                self._registro.DuracionSegundos = duracion
                self._registro.ProcesoID = self.proceso_id  # Asegurar que el ProcesoID esté presente
                # Siempre poner mensaje más presentable, incluso para estados intermedios
                if detalles:
                    self._registro.MensajeError = detalles
                else:
                    self._registro.MensajeError = f"Estado actualizado a: {estado}"
                self._registro.save(using='logs')
        
        return self.proceso_id
    
    def finalizar_exito(self, detalles=None):
        """
        Registra la finalización exitosa de un proceso
        
        Args:
            detalles (str, optional): Detalles adicionales del éxito
        
        Returns:
            str: ID del proceso
        """
        duracion = int(round(time.time() - self.tiempo_inicio))
        self._actualizar_historial('Completado', detalles=detalles)
        
        # Solo actualizar el registro existente
        if self._registro:
            with transaction.atomic():
                # Finalizar el registro existente
                self._registro.Estado = "Completado"[:20]
                self._registro.ParametrosEntrada = json.dumps(self._obtener_parametros())
                self._registro.DuracionSegundos = duracion
                self._registro.ProcesoID = self.proceso_id  # Asegurar que el ProcesoID esté presente
                # En caso de éxito, poner mensaje más presentable en lugar de NULL
                self._registro.MensajeError = detalles if detalles else "Proceso completado exitosamente"
                self._registro.save(using='logs')
        
        return self.proceso_id
    
    def finalizar_error(self, error):
        """
        Registra la finalización con error usando método eficiente
        
        Args:
            error (Exception): Error ocurrido
        
        Returns:
            str: ID del proceso
        """
        # Usar el método eficiente de actualización
        self._actualizar_estado('ERROR', error=error)
        return self.proceso_id
        
    def finalizar(self, estado, detalles=None):
        """
        Registra la finalización de un proceso con un estado específico
        
        Args:
            estado (str): Estado final del proceso (COMPLETADO, ERROR, etc)
            detalles (str, optional): Detalles adicionales
            
        Returns:
            str: ID del proceso
        """
        duracion = int(round(time.time() - self.tiempo_inicio))
        self._actualizar_historial(estado, detalles=detalles)
        
        # Solo actualizar el registro existente
        if self._registro:
            with transaction.atomic():
                # Finalizar el registro existente
                self._registro.Estado = estado[:20]
                self._registro.ParametrosEntrada = json.dumps(self._obtener_parametros())
                self._registro.DuracionSegundos = duracion
                self._registro.ProcesoID = self.proceso_id  # Asegurar que el ProcesoID esté presente
                self._registro.MensajeError = detalles if detalles else f"Proceso finalizado con estado: {estado}"
                self._registro.save(using='logs')
        
        return self.proceso_id


def registrar_evento_unificado(nombre_evento, estado, parametros=None, error=None):
    """
    Función auxiliar para registrar un evento simple de forma unificada
    
    Args:
        nombre_evento (str): Nombre del evento
        estado (str): Estado del evento (Completado, Error, etc)
        parametros (dict, optional): Parámetros relevantes
        error (str, optional): Detalles de error si existe
    
    Returns:
        str: ID del proceso registrado
    """
    from automatizacion.logs.models_logs import ProcesoLog
    
    # Crear string UUID directamente
    proceso_id_str = str(uuid.uuid4())
    
    historial = [{
        'timestamp': datetime.datetime.now().isoformat(),
        'accion': estado,
        'detalles': nombre_evento
    }]
    
    if error:
        historial[0]['error'] = str(error)
    
    params = {
        'proceso_unique_id': proceso_id_str,
        'historial': historial
    }
    
    if parametros:
        params.update(parametros)
    
    # Crear registro para evento simple
    log = ProcesoLog(
        ProcesoID=proceso_id_str,  # Usar el UUID generado
        FechaEjecucion=datetime.datetime.now(),
        Estado=f"{estado}"[:20],  # Solo el estado
        ParametrosEntrada=json.dumps(params),
        DuracionSegundos=0,
        MensajeError=error,
        NombreProceso=nombre_evento[:255]  # Usar el nombre del evento como nombre del proceso
    )
    log.save(using='logs')
    
    return proceso_id_str
