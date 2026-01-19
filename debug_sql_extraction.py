import os
import sys
import django

# Configurar entorno Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proyecto_automatizacion.settings')
django.setup()

from automatizacion.models import MigrationProcess

def debug_sql_extraction(process_id):
    """Debug de la extracción SQL para un proceso específico"""
    try:
        process = MigrationProcess.objects.get(id=process_id)
        print(f"\n===== DEBUG EXTRACCIÓN SQL PARA '{process.name}' (ID: {process_id}) =====")
        
        # Verificar source y tipo
        if not process.source:
            print("❌ ERROR: Este proceso no tiene una fuente (source) configurada.")
            return
            
        if process.source.source_type != 'sql':
            print(f"❌ ERROR: Este proceso es de tipo '{process.source.source_type}', no SQL.")
            return
        
        # Verificar conexión
        if not process.source.connection:
            print("❌ ERROR: No hay conexión SQL configurada para este proceso.")
            return
            
        print(f"✅ Fuente configurada: {process.source.name}")
        print(f"✅ Conexión: {process.source.connection}")
        print(f"   - Servidor: {process.source.connection.server}")
        print(f"   - Base de datos: {process.source.connection.selected_database}")
        
        # Verificar tablas seleccionadas
        print("\n--- TABLAS SELECCIONADAS ---")
        print(f"Valor: {process.selected_tables}")
        
        if not process.selected_tables:
            print("❌ ERROR: No hay tablas seleccionadas.")
            return
            
        # Ejecutar _extract_sql_data directamente para ver el error completo
        print("\n--- EJECUTANDO _extract_sql_data ---")
        try:
            datos_sql = process._extract_sql_data()
            
            print("\nResultado:")
            if isinstance(datos_sql, dict) and 'error' in datos_sql:
                print(f"❌ ERROR DEVUELTO: {datos_sql['error']}")
            else:
                print(f"✅ Datos extraídos correctamente: {len(datos_sql)} registros")
                
                # Verificar si hay errores de tabla
                errores_tabla = [r for r in datos_sql if 'error' in r]
                if errores_tabla:
                    print("\n--- ERRORES EN TABLAS ---")
                    for error in errores_tabla:
                        print(f"❌ Tabla: {error.get('table_name', 'desconocida')} - Error: {error.get('error')}")
                
                # Verificar datos extraídos
                datos_validos = [r for r in datos_sql if 'error' not in r]
                if datos_validos:
                    print(f"\n✅ {len(datos_validos)} registros válidos extraídos")
                else:
                    print("\n❌ No se extrajeron datos válidos de ninguna tabla")
                    
        except Exception as e:
            print(f"\n❌ ERROR AL EJECUTAR _extract_sql_data: {str(e)}")
            print("\nStacktrace completo:")
            import traceback
            traceback.print_exc()
            
        # Sugerencias
        print("\n--- SUGERENCIAS ---")
        if not process.selected_tables:
            print("1. Configure tablas seleccionadas para este proceso")
        elif all('error' in r for r in datos_sql if isinstance(r, dict)):
            print("1. Verifique que las tablas configuradas existan en la base de datos")
            print(f"   - Tablas configuradas: {process.selected_tables}")
        
    except MigrationProcess.DoesNotExist:
        print(f"No se encontró ningún proceso con ID {process_id}")
    except Exception as e:
        print(f"Error general: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    process_id = 34  # CESAR_10
    debug_sql_extraction(process_id)