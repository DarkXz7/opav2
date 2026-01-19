-- ====================================================================
-- Script de creación de tabla ProcesosGuardados
-- Base de datos: DestinoAutomatizacion
-- Propósito: Almacenar y sincronizar procesos creados en Django
-- Fecha: 2025-10-20
-- ====================================================================

USE DestinoAutomatizacion;
GO

-- Verificar si la tabla ya existe
IF OBJECT_ID('dbo.ProcesosGuardados', 'U') IS NOT NULL
BEGIN
    PRINT '⚠️ ADVERTENCIA: La tabla dbo.ProcesosGuardados ya existe.';
    PRINT '   Si deseas recrearla, ejecuta primero: DROP TABLE dbo.ProcesosGuardados;';
    PRINT '   ⚠️ ESTO ELIMINARÁ TODOS LOS DATOS EXISTENTES.';
    -- No continuar para evitar pérdida accidental de datos
    RETURN;
END
GO

-- Crear la tabla
CREATE TABLE dbo.ProcesosGuardados (
    -- Campo de identidad (auto-incremental)
    Id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Información básica del proceso
    NombreProceso NVARCHAR(255) NOT NULL,
    TipoFuente NVARCHAR(50) NOT NULL,              -- 'EXCEL', 'SQL', 'CSV'
    Fuente NVARCHAR(255) NULL,                     -- Ruta del archivo o nombre de conexión
    HojaTabla NVARCHAR(255) NULL,                  -- Nombre de la hoja Excel o tabla SQL
    Destino NVARCHAR(255) NULL,                    -- Base de datos o tabla destino
    Estado NVARCHAR(50) DEFAULT 'Activo',          -- 'Activo', 'Inactivo', 'Eliminado', etc.
    
    -- Auditoría
    FechaCreacion DATETIME DEFAULT GETDATE(),
    FechaActualizacion DATETIME NULL,
    UsuarioCreador NVARCHAR(255) NULL,
    
    -- Información descriptiva
    Descripcion NVARCHAR(MAX) NULL,
    
    -- Control de ejecución
    UltimaEjecucion DATETIME NULL,
    Version INT DEFAULT 1,
    Observaciones NVARCHAR(MAX) NULL,
    
    -- Constraints
    CONSTRAINT CK_ProcesosGuardados_TipoFuente 
        CHECK (TipoFuente IN ('EXCEL', 'SQL', 'CSV')),
    CONSTRAINT CK_ProcesosGuardados_Estado 
        CHECK (Estado IN ('Activo', 'Inactivo', 'Eliminado', 'Borrador', 'Configurado', 
                          'Listo', 'En_Ejecucion', 'Completado', 'Fallido'))
);
GO

-- Crear índice único en NombreProceso para evitar duplicados
CREATE UNIQUE INDEX UX_ProcesosGuardados_NombreProceso 
ON dbo.ProcesosGuardados(NombreProceso);
GO

-- Crear índice para búsquedas por TipoFuente
CREATE INDEX IX_ProcesosGuardados_TipoFuente 
ON dbo.ProcesosGuardados(TipoFuente);
GO

-- Crear índice para búsquedas por Estado
CREATE INDEX IX_ProcesosGuardados_Estado 
ON dbo.ProcesosGuardados(Estado);
GO

-- Crear índice para búsquedas por FechaCreacion
CREATE INDEX IX_ProcesosGuardados_FechaCreacion 
ON dbo.ProcesosGuardados(FechaCreacion DESC);
GO

-- Crear índice para búsquedas por UltimaEjecucion
CREATE INDEX IX_ProcesosGuardados_UltimaEjecucion 
ON dbo.ProcesosGuardados(UltimaEjecucion DESC)
WHERE UltimaEjecucion IS NOT NULL;
GO

PRINT '✅ Tabla dbo.ProcesosGuardados creada exitosamente';
PRINT '';
PRINT '📊 Estructura de la tabla:';
PRINT '   - Id (INT, IDENTITY, PRIMARY KEY)';
PRINT '   - NombreProceso (NVARCHAR(255), UNIQUE)';
PRINT '   - TipoFuente (NVARCHAR(50)) → EXCEL, SQL, CSV';
PRINT '   - Fuente (NVARCHAR(255))';
PRINT '   - HojaTabla (NVARCHAR(255))';
PRINT '   - Destino (NVARCHAR(255))';
PRINT '   - Estado (NVARCHAR(50)) → Activo, Inactivo, etc.';
PRINT '   - FechaCreacion (DATETIME, DEFAULT GETDATE())';
PRINT '   - FechaActualizacion (DATETIME)';
PRINT '   - UsuarioCreador (NVARCHAR(255))';
PRINT '   - Descripcion (NVARCHAR(MAX))';
PRINT '   - UltimaEjecucion (DATETIME)';
PRINT '   - Version (INT, DEFAULT 1)';
PRINT '   - Observaciones (NVARCHAR(MAX))';
PRINT '';
PRINT '🔑 Índices creados:';
PRINT '   - UX_ProcesosGuardados_NombreProceso (UNIQUE)';
PRINT '   - IX_ProcesosGuardados_TipoFuente';
PRINT '   - IX_ProcesosGuardados_Estado';
PRINT '   - IX_ProcesosGuardados_FechaCreacion';
PRINT '   - IX_ProcesosGuardados_UltimaEjecucion (FILTERED)';
PRINT '';
PRINT '🎯 Próximos pasos:';
PRINT '   1. Configurar Django: Verificar alias "sqlserver" en settings.py';
PRINT '   2. Migrar procesos existentes: python manage.py sync_processes_to_sqlserver';
PRINT '   3. Crear/editar procesos: La sincronización es automática';
GO

-- Consulta de verificación
SELECT 
    TABLE_NAME AS 'Tabla',
    COLUMN_NAME AS 'Columna',
    DATA_TYPE AS 'Tipo',
    CHARACTER_MAXIMUM_LENGTH AS 'Longitud',
    IS_NULLABLE AS 'Nullable',
    COLUMN_DEFAULT AS 'Default'
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'ProcesosGuardados'
ORDER BY ORDINAL_POSITION;
GO
