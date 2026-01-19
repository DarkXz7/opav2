#!/usr/bin/env python3
"""
Script para depurar problemas específicos con procesos SQL
Diagnostica: "No hay conexión SQL configurada" y errores de JSON parsing
"""

import os
import sys
import django

# Configurar Django
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proyecto_automatizacion.settings')

try:
    django.setup()
except Exception as e:
    print(f"Error configurando Django: {e}")
    sys.exit(1)

def debug_sql_processes():
    """
    Revisa todos los procesos SQL y diagnostica problemas comunes
    """
    from automatizacion.models import MigrationProcess, DataSource, DatabaseConnection
    
    print("=== DEPURACIÓN: Procesos SQL ===\n")
    
    # 1. Buscar todos los procesos SQL
    sql_processes = MigrationProcess.objects.filter(source__source_type='sql')
    
    print(f"📊 Procesos SQL encontrados: {sql_processes.count()}")
    
    if sql_processes.count() == 0:
        print("   ⚠️  No hay procesos SQL para revisar")
        return
    
    for i, process in enumerate(sql_processes, 1):
        print(f"\n🔍 PROCESO {i}: '{process.name}' (ID: {process.id})")
        print(f"   📅 Creado: {process.created_at}")
        print(f"   🔄 Estado: {process.status}")
        print(f"   📝 Descripción: {process.description or 'Sin descripción'}")
        
        # Revisar fuente de datos
        print(f"\n   📊 FUENTE DE DATOS:")
        if process.source:
            print(f"      🏷️  Nombre: {process.source.name}")
            print(f"      📦 Tipo: {process.source.source_type}")
            print(f"      🆔 ID: {process.source.id}")
            
            # Revisar conexión SQL
            print(f"\n   🔗 CONEXIÓN SQL:")
            if process.source.connection:
                conn = process.source.connection
                print(f"      ✅ Conexión configurada: {conn.name}")
                print(f"      🖥️  Servidor: {conn.server}")
                print(f"      👤 Usuario: {conn.username}")
                print(f"      🔌 Puerto: {conn.port}")
                print(f"      🗄️  BD Seleccionada: {conn.selected_database}")
                print(f"      📅 Último uso: {conn.last_used}")
            else:
                print(f"      ❌ NO HAY CONEXIÓN CONFIGURADA")
                print(f"      🚫 Este es el problema: process.source.connection = None")
        else:
            print(f"      ❌ NO HAY FUENTE DE DATOS")
            
        # Revisar configuración de tablas y columnas
        print(f"\n   ⚙️  CONFIGURACIÓN:")
        print(f"      📊 Tablas seleccionadas: {type(process.selected_tables)} - {process.selected_tables}")
        print(f"      📋 Columnas seleccionadas: {type(process.selected_columns)} - {process.selected_columns}")
        print(f"      🗄️  Base de datos destino: {process.target_db_name}")
        
        # Probar extracción de datos SQL
        print(f"\n   🧪 PRUEBA DE EXTRACCIÓN:")
        try:
            # Simular verificación de conexión como en el código real
            if not process.source.connection:
                print(f"      ❌ Error: 'No hay conexión SQL configurada'")
            else:
                print(f"      ✅ Conexión disponible, probando extracción...")
                # NO ejecutar _extract_sql_data completa, solo verificar setup
                print(f"      📊 Setup OK para extracción de datos")
        except Exception as e:
            print(f"      ❌ Error en prueba: {str(e)}")
            
        print(f"   " + "="*60)

def debug_sql_connections():
    """
    Revisa todas las conexiones SQL disponibles
    """
    from automatizacion.models import DatabaseConnection
    
    print(f"\n=== DEPURACIÓN: Conexiones SQL ===\n")
    
    connections = DatabaseConnection.objects.all()
    print(f"🔗 Conexiones SQL totales: {connections.count()}")
    
    for i, conn in enumerate(connections, 1):
        print(f"\n🔗 CONEXIÓN {i}: '{conn.name}' (ID: {conn.id})")
        print(f"   🖥️  Servidor: {conn.server}")
        print(f"   👤 Usuario: {conn.username}")
        print(f"   🔌 Puerto: {conn.port}")
        print(f"   🗄️  BD Seleccionada: {conn.selected_database}")
        print(f"   📅 Creado: {conn.created_at}")
        print(f"   📅 Último uso: {conn.last_used}")
        
        # Verificar fuentes de datos asociadas
        associated_sources = conn.datasource_set.all()
        print(f"   📊 Fuentes asociadas: {associated_sources.count()}")
        
        for source in associated_sources:
            print(f"      📦 Fuente: {source.name} (ID: {source.id})")

def debug_specific_process(process_name):
    """
    Depura un proceso específico por nombre (ej: CESAR_10)
    """
    from automatizacion.models import MigrationProcess
    
    print(f"\n=== DEPURACIÓN ESPECÍFICA: '{process_name}' ===\n")
    
    try:
        process = MigrationProcess.objects.get(name=process_name)
        
        print(f"🎯 PROCESO ENCONTRADO: '{process.name}'")
        print(f"   🆔 ID: {process.id}")
        print(f"   📦 Tipo fuente: {process.source.source_type}")
        print(f"   🔄 Estado: {process.status}")
        
        # Problema 1: Verificar conexión
        print(f"\n🔍 DIAGNÓSTICO 1: Conexión SQL")
        if process.source and process.source.connection:
            print(f"   ✅ Conexión existe: {process.source.connection.name}")
        else:
            print(f"   ❌ NO HAY CONEXIÓN - Este es el problema principal")
            if process.source:
                print(f"   📊 Source existe pero connection=None")
            else:
                print(f"   📊 Source no existe")
        
        # Problema 2: Verificar JSON fields
        print(f"\n🔍 DIAGNÓSTICO 2: Campos JSON")
        print(f"   📊 selected_tables tipo: {type(process.selected_tables)}")
        print(f"   📊 selected_tables valor: {process.selected_tables}")
        print(f"   📋 selected_columns tipo: {type(process.selected_columns)}")
        
        # Problema 3: Simular ejecución
        print(f"\n🔍 DIAGNÓSTICO 3: Simulación de ejecución")
        print(f"   🚀 Iniciando simulación de process.run()...")
        
        # Simular verificaciones del método run()
        if not process.source:
            print(f"   ❌ Falla: No hay fuente configurada")
        elif process.source.source_type != 'sql':
            print(f"   ⚠️  Proceso no es SQL: {process.source.source_type}")
        else:
            print(f"   ✅ Tipo SQL confirmado")
            
            # Simular _process_sql_tables_individually
            print(f"   🔄 Llamaría a _process_sql_tables_individually...")
            
            # Simular _extract_sql_data
            print(f"   📊 Simulando _extract_sql_data...")
            if not process.source.connection:
                print(f"   ❌ FALLA AQUÍ: 'No hay conexión SQL configurada'")
            else:
                print(f"   ✅ Conexión OK, continuaría con extracción")
                
    except MigrationProcess.DoesNotExist:
        print(f"❌ Proceso '{process_name}' no encontrado")
        
        # Buscar procesos similares
        similar = MigrationProcess.objects.filter(name__icontains=process_name[:5])
        if similar.exists():
            print(f"🔍 Procesos similares encontrados:")
            for p in similar:
                print(f"   📌 {p.name} (ID: {p.id})")

def main():
    """
    Ejecuta la depuración completa de procesos SQL
    """
    print("🚀 INICIANDO DEPURACIÓN DE PROCESOS SQL")
    print("=" * 70)
    
    # 1. Revisar todos los procesos SQL
    debug_sql_processes()
    
    # 2. Revisar conexiones disponibles
    debug_sql_connections()
    
    # 3. Depurar proceso específico si se menciona
    debug_specific_process("CESAR_10")
    
    print("\n" + "=" * 70)
    print("🏁 DEPURACIÓN COMPLETADA")
    print("\n📋 RESUMEN DE PROBLEMAS COMUNES:")
    print("   1. ❌ No hay conexión SQL configurada (process.source.connection = None)")
    print("   2. ⚠️  JSON parsing (campos ya son objetos Python, no strings)")
    print("   3. 🔧 Configuración incorrecta de fuente de datos SQL")

if __name__ == '__main__':
    main()