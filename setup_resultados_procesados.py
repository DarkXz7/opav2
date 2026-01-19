#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para verificar y actualizar la estructura de ResultadosProcesados
"""
import os
import django
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Configure Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proyecto_automatizacion.settings')
django.setup()

def verificar_estructura_tabla():
    """Verifica la estructura actual de ResultadosProcesados"""
    try:
        from django.db import connections
        
        print("🔍 VERIFICANDO ESTRUCTURA DE ResultadosProcesados")
        print("=" * 60)
        
        with connections['destino'].cursor() as cursor:
            # Obtener estructura actual
            cursor.execute("""
                SELECT 
                    COLUMN_NAME, 
                    DATA_TYPE, 
                    CHARACTER_MAXIMUM_LENGTH,
                    IS_NULLABLE, 
                    COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'ResultadosProcesados'
                ORDER BY ORDINAL_POSITION
            """)
            
            columns = cursor.fetchall()
            print("📋 ESTRUCTURA ACTUAL:")
            print("Columna | Tipo | Longitud | Null | Default")
            print("-" * 60)
            for col in columns:
                length = f"({col[2]})" if col[2] else ""
                print(f"{col[0]:<20} | {col[1]}{length:<15} | {col[3]:<4} | {col[4] or 'None'}")
        
        return columns
        
    except Exception as e:
        print(f"❌ Error verificando estructura: {e}")
        return None

def agregar_campo_nombre_proceso():
    """Agrega el campo NombreProceso si no existe"""
    try:
        from django.db import connections
        
        print("\n🔧 AGREGANDO CAMPO NombreProceso")
        print("=" * 40)
        
        with connections['destino'].cursor() as cursor:
            # Verificar si ya existe
            cursor.execute("""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'ResultadosProcesados' 
                AND COLUMN_NAME = 'NombreProceso'
            """)
            
            exists = cursor.fetchone()[0] > 0
            
            if exists:
                print("✅ El campo NombreProceso ya existe")
                return True
            
            # Agregar el campo
            print("➕ Agregando campo NombreProceso...")
            cursor.execute("""
                ALTER TABLE ResultadosProcesados 
                ADD NombreProceso NVARCHAR(200) NULL
            """)
            
            print("✅ Campo NombreProceso agregado exitosamente")
            
            # Verificar que se agregó
            cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'ResultadosProcesados' 
                AND COLUMN_NAME = 'NombreProceso'
            """)
            
            result = cursor.fetchone()
            if result:
                print(f"✅ Verificación: {result[0]} ({result[1]}({result[2]}))")
                return True
            else:
                print("❌ No se pudo verificar la creación del campo")
                return False
                
    except Exception as e:
        print(f"❌ Error agregando campo: {e}")
        return False

def crear_tabla_si_no_existe():
    """Crea la tabla completa si no existe"""
    try:
        from django.db import connections
        
        print("\n🏗️ VERIFICANDO/CREANDO TABLA ResultadosProcesados")
        print("=" * 50)
        
        with connections['destino'].cursor() as cursor:
            # Verificar si existe la tabla
            cursor.execute("""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = 'ResultadosProcesados'
            """)
            
            table_exists = cursor.fetchone()[0] > 0
            
            if table_exists:
                print("✅ La tabla ResultadosProcesados ya existe")
                return True
            
            print("📋 Creando tabla ResultadosProcesados...")
            cursor.execute("""
                CREATE TABLE ResultadosProcesados (
                    ResultadoID INT IDENTITY(1,1) PRIMARY KEY,
                    ProcesoID NVARCHAR(36) NOT NULL,
                    NombreProceso NVARCHAR(200) NULL,
                    FechaRegistro DATETIME2 DEFAULT GETDATE(),
                    DatosProcesados NVARCHAR(MAX) NULL,
                    UsuarioResponsable NVARCHAR(100) NULL,
                    EstadoProceso NVARCHAR(50) DEFAULT 'COMPLETADO',
                    TipoOperacion NVARCHAR(100) NULL,
                    RegistrosAfectados INT DEFAULT 0,
                    TiempoEjecucion DECIMAL(10,2) NULL,
                    MetadatosProceso NVARCHAR(MAX) NULL
                )
            """)
            
            print("✅ Tabla ResultadosProcesados creada exitosamente")
            return True
            
    except Exception as e:
        print(f"❌ Error creando tabla: {e}")
        return False

def verificar_despues_cambios():
    """Verificación final después de los cambios"""
    try:
        from django.db import connections
        
        print("\n🔍 VERIFICACIÓN FINAL")
        print("=" * 30)
        
        with connections['destino'].cursor() as cursor:
            # Contar registros
            cursor.execute("SELECT COUNT(*) FROM ResultadosProcesados")
            count = cursor.fetchone()[0]
            print(f"📊 Total de registros: {count}")
            
            # Mostrar estructura final
            cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'ResultadosProcesados'
                ORDER BY ORDINAL_POSITION
            """)
            
            columns = cursor.fetchall()
            print("\n📋 ESTRUCTURA FINAL:")
            for col in columns:
                length = f"({col[2]})" if col[2] else ""
                nullable = "NULL" if col[3] == "YES" else "NOT NULL"
                print(f"   {col[0]:<20} {col[1]}{length:<15} {nullable}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en verificación final: {e}")
        return False

if __name__ == '__main__':
    print("🔧 CONFIGURACIÓN DE TABLA ResultadosProcesados")
    print("=" * 60)
    
    # 1. Verificar estructura actual
    estructura = verificar_estructura_tabla()
    
    if estructura is None:
        # La tabla no existe, crearla completa
        exito_creacion = crear_tabla_si_no_existe()
        if not exito_creacion:
            print("❌ No se pudo crear la tabla")
            exit(1)
    else:
        # La tabla existe, verificar si falta NombreProceso
        nombres_columnas = [col[0] for col in estructura]
        if 'NombreProceso' not in nombres_columnas:
            exito_campo = agregar_campo_nombre_proceso()
            if not exito_campo:
                print("❌ No se pudo agregar el campo NombreProceso")
                exit(1)
    
    # 2. Verificación final
    verificar_despues_cambios()
    
    print("\n🎉 CONFIGURACIÓN COMPLETADA")
    print("✅ La tabla ResultadosProcesados está lista para usar")