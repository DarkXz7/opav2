import os
import sys
import json
import django

# Configurar entorno Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proyecto_automatizacion.settings')
django.setup()

from automatizacion.models import MigrationProcess

def inspect_selected_tables(process_id):
    """Inspecciona el campo selected_tables de un proceso para diagnosticar problemas"""
    try:
        process = MigrationProcess.objects.get(id=process_id)
        print(f"\n===== DIAGNÓSTICO DEL PROCESO '{process.name}' (ID: {process_id}) =====")
        print(f"Tipo de fuente: {process.source.source_type if process.source else 'Sin fuente'}")
        
        # Inspeccionar conexión SQL si aplica
        if process.source and process.source.source_type == 'sql':
            if process.source.connection:
                print(f"Conexión SQL configurada: {process.source.connection}")
                print(f"  - Servidor: {process.source.connection.server}")
                print(f"  - Base de datos: {process.source.connection.selected_database}")
            else:
                print("ERROR: No hay conexión SQL configurada para este proceso")
        
        # Inspeccionar selected_tables
        print("\n--- SELECTED_TABLES ---")
        print(f"Tipo de selected_tables: {type(process.selected_tables).__name__}")
        print(f"Valor de selected_tables: {process.selected_tables}")
        
        if process.selected_tables is None:
            print("ERROR: selected_tables es None (no hay tablas seleccionadas)")
        elif process.selected_tables == []:
            print("ERROR: selected_tables es una lista vacía (no hay tablas seleccionadas)")
        elif process.selected_tables == '':
            print("ERROR: selected_tables es una cadena vacía (no hay tablas seleccionadas)")
            
        # Si es string, intentar parsear como JSON
        if isinstance(process.selected_tables, str) and process.selected_tables:
            try:
                parsed_tables = json.loads(process.selected_tables)
                print(f"  ✅ Parseado como JSON correctamente: {parsed_tables}")
                print(f"  📊 Tipo después de parseo: {type(parsed_tables).__name__}")
                if not parsed_tables:
                    print("  ❌ ERROR: El JSON parseado está vacío")
            except json.JSONDecodeError as e:
                print(f"  ❌ ERROR: No se pudo parsear como JSON: {str(e)}")
                # Si no es JSON válido, podría ser una tabla simple
                print(f"  ℹ️ Posiblemente sea una tabla simple en formato string")
        
        # Sugerencias
        print("\n--- POSIBLES SOLUCIONES ---")
        if process.selected_tables is None or process.selected_tables == [] or process.selected_tables == '':
            print("1. Editar el proceso y seleccionar al menos una tabla")
            print("2. Establecer selected_tables directamente en la base de datos:")
            print("   - Para una tabla simple: ['nombre_tabla']")
            print("   - Para múltiples tablas: [{'name': 'tabla1'}, {'name': 'tabla2'}]")
        elif isinstance(process.selected_tables, str) and process.selected_tables and not process.selected_tables.startswith('['):
            print(f"El valor actual parece ser una cadena simple '{process.selected_tables}', debería ser un array JSON.")
            print("Sugerencia: Modificar a formato array JSON: ['{}']".format(process.selected_tables))
            
        print("\n--- CÓMO PROBAR LA CORRECCIÓN ---")
        print("Ejecuta el siguiente código para corregir el problema:")
        print("from automatizacion.models import MigrationProcess")
        print(f"p = MigrationProcess.objects.get(id={process_id})")
        
        if not process.selected_tables:
            print("# Si necesitas asignar una tabla simple:")
            print("p.selected_tables = ['nombre_de_la_tabla']")
        elif isinstance(process.selected_tables, str) and not process.selected_tables.startswith('['):
            print(f"# Corregir formato de tabla simple:")
            print(f"p.selected_tables = ['{process.selected_tables}']")
        
        print("p.save()")
        print("print('Proceso actualizado con éxito')")
        
    except MigrationProcess.DoesNotExist:
        print(f"No se encontró ningún proceso con ID {process_id}")
    except Exception as e:
        print(f"Error al inspeccionar el proceso: {str(e)}")

if __name__ == '__main__':
    if len(sys.argv) > 1:
        process_id = int(sys.argv[1])
        inspect_selected_tables(process_id)
    else:
        print("Uso: python debug_selected_tables.py <id_del_proceso>")
        # Si no se proporciona ID, usar el 34 por defecto
        print("\nUsando ID 34 por defecto:")
        inspect_selected_tables(34)