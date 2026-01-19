class DataTransferRouter:
    """
    Router de base de datos mejorado para dirigir modelos a sus bases de datos correspondientes:
    - ProcesoLog -> 'logs' (LogsAutomatizacion)
    - ResultadosProcesados, UsuariosDestino -> 'destino' (DestinoAutomatizacion)
    - Resto de modelos -> 'default' (SQLite)
    """
    
    # Mapeo de modelos a bases de datos
    LOGS_MODELS = {'ProcesoLog'}
    DESTINO_MODELS = {'ResultadosProcesados', 'UsuariosDestino'}
    
    def db_for_read(self, model, **hints):
        """
        Determina qué base de datos usar para leer un modelo específico.
        """
        if model._meta.app_label == 'automatizacion':
            if model.__name__ in self.LOGS_MODELS:
                return 'logs'
            elif model.__name__ in self.DESTINO_MODELS:
                return 'destino'
        return None

    def db_for_write(self, model, **hints):
        """
        Determina qué base de datos usar para escribir un modelo específico.
        """
        if model._meta.app_label == 'automatizacion':
            if model.__name__ in self.LOGS_MODELS:
                return 'logs'
            elif model.__name__ in self.DESTINO_MODELS:
                return 'destino'
        return None

    def allow_relation(self, obj1, obj2, **hints):
        """
        Permite relaciones entre modelos de la misma aplicación.
        """
        if obj1._meta.app_label == 'automatizacion' and obj2._meta.app_label == 'automatizacion':
            return True
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Controla qué migraciones se aplican a qué bases de datos.
        """
        if app_label == 'automatizacion':
            if model_name and model_name.lower() in [m.lower() for m in self.LOGS_MODELS]:
                return db == 'logs'
            elif model_name and model_name.lower() in [m.lower() for m in self.DESTINO_MODELS]:
                return db == 'destino'
            elif db in ['logs', 'destino']:
                return False  # No migrar otros modelos a estas bases de datos
        return db == 'default'
