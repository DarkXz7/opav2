"""
Script para crear la tabla ResultadosProcesados en SQL Server DestinoAutomatizacion
"""

import pyodbc
import sys

def crear_tabla_resultados_procesados():
    """
    Crea la tabla ResultadosProcesados en SQL Server si no existe
    """
    
    # Configuración de conexión
    server = 'localhost\\SQLEXPRESS'
    database = 'DestinoAutomatizacion'
    username = 'miguel'
    password = input("Ingresa la contraseña para SQL Server (usuario 'miguel'): ")
    
    connection_string = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password};'
        f'TrustServerCertificate=yes;'
    )
    
    # SQL para crear la tabla
    create_table_sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ResultadosProcesados' AND xtype='U')
    BEGIN
        CREATE TABLE dbo.ResultadosProcesados (
            ResultadoID INT IDENTITY(1,1) PRIMARY KEY,
            ProcesoID NVARCHAR(36) NOT NULL,
            NombreProceso NVARCHAR(200) NOT NULL,
            FechaRegistro DATETIME2 DEFAULT GETDATE(),
            DatosProcesados NVARCHAR(MAX),
            UsuarioResponsable NVARCHAR(100),
            EstadoProceso NVARCHAR(50) DEFAULT 'COMPLETADO',
            TipoOperacion NVARCHAR(100),
            RegistrosAfectados INT DEFAULT 0,
            TiempoEjecucion DECIMAL(10, 2),
            MetadatosProceso NVARCHAR(MAX)
        );
        
        PRINT 'Tabla ResultadosProcesados creada exitosamente';
    END
    ELSE
    BEGIN
        PRINT 'La tabla ResultadosProcesados ya existe';
    END
    """
    
    try:
        print("="*80)
        print("CREANDO TABLA ResultadosProcesados EN SQL SERVER")
        print("="*80)
        print(f"\nConectando a:")
        print(f"  Servidor: {server}")
        print(f"  Base de datos: {database}")
        print(f"  Usuario: {username}")
        
        # Conectar a SQL Server
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        print("\n✅ Conexión exitosa")
        
        # Ejecutar script de creación
        print("\n📋 Ejecutando script de creación...")
        cursor.execute(create_table_sql)
        conn.commit()
        
        print("\n✅ Script ejecutado exitosamente")
        
        # Verificar que la tabla existe
        cursor.execute("""
            SELECT 
                COLUMN_NAME, 
                DATA_TYPE, 
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'ResultadosProcesados'
            ORDER BY ORDINAL_POSITION
        """)
        
        columns = cursor.fetchall()
        
        if columns:
            print("\n📋 Estructura de la tabla ResultadosProcesados:")
            print("  " + "-"*76)
            print(f"  {'Columna':<25} {'Tipo':<20} {'Longitud':<12} {'Nullable':<10}")
            print("  " + "-"*76)
            for col in columns:
                col_name = col[0]
                data_type = col[1]
                max_length = col[2] if col[2] else 'N/A'
                nullable = 'Sí' if col[3] == 'YES' else 'No'
                print(f"  {col_name:<25} {data_type:<20} {str(max_length):<12} {nullable:<10}")
            print("  " + "-"*76)
        
        # Verificar registros existentes
        cursor.execute("SELECT COUNT(*) FROM ResultadosProcesados")
        count = cursor.fetchone()[0]
        print(f"\n📊 Registros existentes en ResultadosProcesados: {count}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*80)
        print("✅ PROCESO COMPLETADO EXITOSAMENTE")
        print("="*80)
        print("\n💡 La tabla ResultadosProcesados está lista para usar.")
        print("   Ahora los procesos ejecutados se guardarán automáticamente.")
        
        return True
        
    except pyodbc.Error as e:
        print(f"\n❌ Error de SQL Server: {e}")
        return False
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")
        return False

if __name__ == "__main__":
    exito = crear_tabla_resultados_procesados()
    sys.exit(0 if exito else 1)
