#!/usr/bin/env python
"""
Script para agregar la columna MigrationProcessID a la tabla ProcesoLog existente
"""
import os
import django
import sys

# Configurar Django
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proyecto_automatizacion.settings')
django.setup()

from django.db import connections

def actualizar_tabla_proceso_log():
    """
    Agrega la columna MigrationProcessID a la tabla ProcesoLog si no existe
    """
    print("=" * 60)
    print("ACTUALIZANDO ESTRUCTURA DE TABLA ProcesoLog")
    print("=" * 60)
    
    try:
        # Conectar a la base de datos de logs
        with connections['logs'].cursor() as cursor:
            print("\n1. Verificando estructura actual de ProcesoLog...")
            
            # Verificar si la columna ya existe
            cursor.execute("""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'ProcesoLog' 
                AND TABLE_CATALOG = 'LogsAutomatizacion'
                AND COLUMN_NAME = 'MigrationProcessID'
            """)
            
            columna_existe = cursor.fetchone() is not None
            
            if columna_existe:
                print("   ✅ Columna MigrationProcessID ya existe")
            else:
                print("   ⚠️  Columna MigrationProcessID no existe - agregándola...")
                
                # Agregar la columna
                alter_sql = """
                ALTER TABLE ProcesoLog 
                ADD MigrationProcessID INT NULL
                """
                
                cursor.execute(alter_sql)
                print("   ✅ Columna MigrationProcessID agregada exitosamente")
            
            # Verificar estructura final
            print("\n2. Verificando estructura final...")
            cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'ProcesoLog' 
                AND TABLE_CATALOG = 'LogsAutomatizacion'
                ORDER BY ORDINAL_POSITION
            """)
            
            columnas = cursor.fetchall()
            print("   📋 Estructura actual de ProcesoLog:")
            
            for col_name, data_type, is_nullable in columnas:
                nullable_str = "NULL" if is_nullable == "YES" else "NOT NULL"
                print(f"      - {col_name} ({data_type}) {nullable_str}")
            
            print("\n" + "=" * 60)
            print("✅ ACTUALIZACIÓN COMPLETADA")
            print("La tabla ProcesoLog ahora tiene la estructura correcta")
            print("=" * 60)
            
            return True
            
    except Exception as e:
        print(f"\n❌ ERROR actualizando tabla: {str(e)}")
        return False

if __name__ == "__main__":
    exito = actualizar_tabla_proceso_log()
    
    if exito:
        print("\n🎉 Tabla actualizada correctamente")
        print("Ahora puedes ejecutar las pruebas de mejoras")
    else:
        print("\n❌ Falló la actualización de la tabla")
        
    sys.exit(0 if exito else 1)