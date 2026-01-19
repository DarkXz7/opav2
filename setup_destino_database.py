#!/usr/bin/env python
"""
Script para crear las tablas necesarias en la base de datos DestinoAutomatizacion
"""
import os
import django
import pyodbc
import uuid
import json
from datetime import datetime

# Configure Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proyecto_automatizacion.settings')
django.setup()

def crear_base_datos_destino():
    """
    Crea la base de datos DestinoAutomatizacion y las tablas necesarias
    """
    print("=== CONFIGURACIÓN DE BASE DE DATOS DESTINO ===")
    
    try:
        # Conectar a SQL Server (sin especificar base de datos)
        conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost\\SQLEXPRESS;Trusted_Connection=yes;'
        conn = pyodbc.connect(conn_str)
        conn.autocommit = True
        cursor = conn.cursor()
        
        print("✓ Conexión establecida a SQL Server")
        
        # 1. Crear base de datos si no existe
        print("\n1. Creando base de datos DestinoAutomatizacion...")
        try:
            cursor.execute("""
                IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'DestinoAutomatizacion')
                BEGIN
                    CREATE DATABASE DestinoAutomatizacion
                    PRINT 'Base de datos DestinoAutomatizacion creada'
                END
                ELSE
                BEGIN
                    PRINT 'Base de datos DestinoAutomatizacion ya existe'
                END
            """)
            print("✓ Base de datos DestinoAutomatizacion configurada")
        except Exception as e:
            print(f"Error creando base de datos: {e}")
            return False
        
        cursor.close()
        conn.close()
        
        # 2. Conectar a la base de datos específica
        conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost\\SQLEXPRESS;DATABASE=DestinoAutomatizacion;Trusted_Connection=yes;'
        conn = pyodbc.connect(conn_str)
        conn.autocommit = True
        cursor = conn.cursor()
        
        print("\n2. Creando tabla ResultadosProcesados...")
        try:
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ResultadosProcesados' AND xtype='U')
                BEGIN
                    CREATE TABLE ResultadosProcesados (
                        ResultadoID int IDENTITY(1,1) PRIMARY KEY,
                        ProcesoID nvarchar(36) NOT NULL,
                        FechaRegistro datetime2 DEFAULT GETDATE(),
                        DatosProcesados ntext NOT NULL,
                        UsuarioResponsable nvarchar(100) NOT NULL,
                        EstadoProceso nvarchar(50) DEFAULT 'COMPLETADO',
                        TipoOperacion nvarchar(100) NULL,
                        RegistrosAfectados int DEFAULT 0,
                        TiempoEjecucion decimal(10,2) NULL,
                        MetadatosProceso ntext NULL
                    )
                    
                    -- Crear índices para optimizar consultas
                    CREATE INDEX IX_ResultadosProcesados_ProcesoID ON ResultadosProcesados(ProcesoID)
                    CREATE INDEX IX_ResultadosProcesados_FechaRegistro ON ResultadosProcesados(FechaRegistro)
                    CREATE INDEX IX_ResultadosProcesados_UsuarioResponsable ON ResultadosProcesados(UsuarioResponsable)
                    
                    PRINT 'Tabla ResultadosProcesados creada con índices'
                END
                ELSE
                BEGIN
                    PRINT 'Tabla ResultadosProcesados ya existe'
                END
            """)
            print("✓ Tabla ResultadosProcesados configurada")
        except Exception as e:
            print(f"Error creando tabla ResultadosProcesados: {e}")
        
        print("\n3. Creando tabla dbo.Usuarios...")
        try:
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Usuarios' AND xtype='U')
                BEGIN
                    CREATE TABLE dbo.Usuarios (
                        UsuarioID int IDENTITY(1,1) PRIMARY KEY,
                        NombreUsuario nvarchar(100) UNIQUE NOT NULL,
                        Email nvarchar(255) NOT NULL,
                        NombreCompleto nvarchar(200) NOT NULL,
                        FechaCreacion datetime2 DEFAULT GETDATE(),
                        Activo bit DEFAULT 1,
                        UltimoAcceso datetime2 NULL
                    )
                    
                    -- Crear índices
                    CREATE INDEX IX_Usuarios_NombreUsuario ON dbo.Usuarios(NombreUsuario)
                    CREATE INDEX IX_Usuarios_Email ON dbo.Usuarios(Email)
                    
                    PRINT 'Tabla dbo.Usuarios creada con índices'
                END
                ELSE
                BEGIN
                    PRINT 'Tabla dbo.Usuarios ya existe'
                END
            """)
            print("✓ Tabla dbo.Usuarios configurada")
        except Exception as e:
            print(f"Error creando tabla Usuarios: {e}")
        
        print("\n4. Insertando datos de prueba...")
        try:
            # Insertar usuario de prueba en dbo.Usuarios
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM dbo.Usuarios WHERE NombreUsuario = 'admin')
                BEGIN
                    INSERT INTO dbo.Usuarios (NombreUsuario, Email, NombreCompleto, Activo)
                    VALUES ('admin', 'admin@test.com', 'Administrador del Sistema', 1)
                    PRINT 'Usuario de prueba insertado'
                END
            """)
            
            # Insertar resultado de prueba
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM ResultadosProcesados WHERE ProcesoID = 'TEST-SETUP-PROCESS')
                BEGIN
                    INSERT INTO ResultadosProcesados (
                        ProcesoID, DatosProcesados, UsuarioResponsable, 
                        EstadoProceso, TipoOperacion, RegistrosAfectados
                    )
                    VALUES (
                        'TEST-SETUP-PROCESS',
                        '{"mensaje": "Configuración inicial completada", "timestamp": "' + CONVERT(nvarchar, GETDATE(), 126) + '"}',
                        'SYSTEM',
                        'CONFIGURACION_INICIAL',
                        'SETUP_DATABASE',
                        2
                    )
                    PRINT 'Registro de prueba insertado'
                END
            """)
            print("✓ Datos de prueba insertados")
        except Exception as e:
            print(f"Error insertando datos de prueba: {e}")
        
        print("\n5. Verificando configuración...")
        try:
            # Verificar tablas creadas
            cursor.execute("""
                SELECT 
                    t.name as tabla,
                    (SELECT COUNT(*) FROM sys.columns c WHERE c.object_id = t.object_id) as columnas
                FROM sys.tables t 
                WHERE t.name IN ('ResultadosProcesados', 'Usuarios')
                ORDER BY t.name
            """)
            
            tablas = cursor.fetchall()
            print("Tablas configuradas:")
            for tabla in tablas:
                print(f"  - {tabla[0]}: {tabla[1]} columnas")
            
            # Verificar registros
            cursor.execute("SELECT COUNT(*) FROM ResultadosProcesados")
            count_resultados = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM dbo.Usuarios") 
            count_usuarios = cursor.fetchone()[0]
            
            print(f"\nRegistros:")
            print(f"  - ResultadosProcesados: {count_resultados} registros")
            print(f"  - dbo.Usuarios: {count_usuarios} registros")
            
        except Exception as e:
            print(f"Error verificando configuración: {e}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*50)
        print("🎉 CONFIGURACIÓN COMPLETADA EXITOSAMENTE")
        print("📊 Base de datos DestinoAutomatizacion lista para usar")
        print("🔗 Endpoint disponible: /automatizacion/sql/connection/18/table/dbo.Usuarios/columns/")
        print("💾 Tablas: ResultadosProcesados, dbo.Usuarios")
        
        return True
        
    except Exception as e:
        print(f"❌ Error general: {e}")
        return False

def test_data_transfer():
    """
    Prueba la funcionalidad de transferencia de datos
    """
    print("\n=== PRUEBA DE TRANSFERENCIA DE DATOS ===")
    
    try:
        from automatizacion.data_transfer_service import data_transfer_service
        import uuid
        
        # Generar datos de prueba
        proceso_id = str(uuid.uuid4())
        datos_prueba = {
            "test": "Prueba de transferencia",
            "timestamp": datetime.now().isoformat(),
            "registros_procesados": 100
        }
        
        print(f"Probando transferencia con ProcesoID: {proceso_id}")
        
        # Ejecutar transferencia
        success, result = data_transfer_service.transfer_processed_data(
            proceso_id=proceso_id,
            datos_procesados=datos_prueba,
            usuario_responsable="SYSTEM_TEST",
            metadata={"test": True, "setup": True}
        )
        
        if success:
            print(f"✓ Transferencia exitosa")
            print(f"  ResultadoID: {result['resultado_id']}")
            print(f"  Tiempo ejecución: {result['tiempo_ejecucion']:.2f}s")
        else:
            print(f"✗ Transferencia falló: {result.get('error')}")
            
        return success
        
    except Exception as e:
        print(f"❌ Error en prueba: {e}")
        return False

if __name__ == "__main__":
    print("INICIANDO CONFIGURACIÓN DE SISTEMA DE TRANSFERENCIA DE DATOS")
    print("=" * 60)
    
    # Configurar base de datos
    db_success = crear_base_datos_destino()
    
    if db_success:
        # Probar transferencia
        transfer_success = test_data_transfer()
        
        if transfer_success:
            print("\n🎉 SISTEMA COMPLETAMENTE CONFIGURADO Y PROBADO")
        else:
            print("\n⚠️ Base de datos configurada, pero prueba de transferencia falló")
    else:
        print("\n❌ CONFIGURACIÓN DE BASE DE DATOS FALLÓ")
