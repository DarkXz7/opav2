#!/usr/bin/env python
"""
Script para verificar las tablas creadas en SQL Server.
"""
import os
import sys
import django

# Configurar Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proyecto_automatizacion.settings')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
django.setup()

import pyodbc
from django.conf import settings

print("=" * 80)
print("TABLAS CREADAS EN SQL SERVER DESTINO")
print("=" * 80)

try:
    destino_config = settings.DATABASES['destino']
    
    server = destino_config.get('HOST', 'localhost')
    port = destino_config.get('PORT')
    database = destino_config.get('NAME', 'DestinoAutomatizacion')
    username = destino_config.get('USER', '')
    password = destino_config.get('PASSWORD', '')
    
    if port:
        server_with_port = f"{server},{port}"
    else:
        server_with_port = server
    
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server_with_port};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
    )
    
    print(f"\n🔌 Conectando a: {server_with_port}/{database}")
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    print("✅ Conexión exitosa")
    
    # Obtener todas las tablas
    print(f"\n📋 TABLAS EN LA BASE DE DATOS:")
    print("-" * 60)
    
    cursor.execute("""
        SELECT TABLE_NAME, TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_CATALOG = ?
        ORDER BY TABLE_NAME
    """, [database])
    
    tables = cursor.fetchall()
    
    # Agrupar tablas por proceso (buscar patrones de nombre)
    process_tables = {}
    system_tables = []
    
    for table_name, table_type in tables:
        # Intentar identificar tablas de procesos (formato: Proceso_Hoja)
        parts = table_name.split('_')
        if len(parts) >= 2 and not table_name.startswith(('ResultadosProcesados', 'ProcesoLog', 'ProcesosGuardados', 'UsuariosDestino', '__')):
            # Posible tabla de proceso
            base_name = '_'.join(parts[:-1])  # Todo excepto la última parte (hoja)
            if base_name not in process_tables:
                process_tables[base_name] = []
            process_tables[base_name].append(table_name)
        else:
            system_tables.append(table_name)
    
    print("\n📊 TABLAS DE SISTEMA:")
    for t in system_tables:
        print(f"   - {t}")
    
    print(f"\n📊 TABLAS DE PROCESOS (agrupadas por proceso):")
    for process_name, tables_list in sorted(process_tables.items()):
        print(f"\n   📌 Proceso: {process_name}")
        for t in tables_list:
            # Contar registros
            try:
                cursor.execute(f"SELECT COUNT(*) FROM [{t}]")
                count = cursor.fetchone()[0]
                print(f"      - {t} ({count} registros)")
            except:
                print(f"      - {t} (error al contar)")
    
    # Buscar tablas específicas de algunos procesos con 2 hojas
    print("\n" + "=" * 60)
    print("🔍 BUSCANDO TABLAS DE PROCESOS CON 2 HOJAS:")
    print("=" * 60)
    
    # Procesos con 2 hojas conocidos
    test_processes = [
        'amiguitos3',
        'fecha883', 
        'adferfg',
        'test de prueba 12',
        'TestDuplicado15555'
    ]
    
    for process_name in test_processes:
        # Limpiar nombre para buscar en tablas
        clean_name = process_name.replace(' ', '_').replace('-', '_')
        print(f"\n   Proceso '{process_name}' (buscar: {clean_name}*):")
        
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME LIKE ?
            ORDER BY TABLE_NAME
        """, [f"{clean_name}%"])
        
        matching_tables = cursor.fetchall()
        if matching_tables:
            for (t,) in matching_tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM [{t}]")
                    count = cursor.fetchone()[0]
                    print(f"      ✅ {t} ({count} registros)")
                except:
                    print(f"      ⚠️ {t} (error)")
        else:
            print(f"      ❌ No se encontraron tablas")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("FIN DEL DIAGNÓSTICO DE TABLAS")
print("=" * 80)
