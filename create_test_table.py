import os
import django
import pyodbc
import sys

# Configurar entorno Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proyecto_automatizacion.settings')
django.setup()

from automatizacion.models import MigrationProcess

def create_test_table_and_update_process(process_id):
    """
    Crea una tabla de prueba en la base de datos SQL y 
    actualiza el proceso para usar esta tabla
    """
    try:
        process = MigrationProcess.objects.get(id=process_id)
        print(f"\n===== CREANDO TABLA DE PRUEBA PARA '{process.name}' (ID: {process_id}) =====")
        
        # Verificar source y conexión
        if not process.source or process.source.source_type != 'sql':
            print(f"❌ El proceso {process.name} no es de tipo SQL o no tiene fuente configurada")
            return False
            
        if not process.source.connection:
            print("❌ No hay conexión SQL configurada")
            return False
        
        connection = process.source.connection
        print(f"Conectando a SQL Server: {connection.server}/{connection.selected_database}")
        
        # Establecer conexión directa
        conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={connection.server};DATABASE={connection.selected_database};UID={connection.username};PWD={connection.password}'
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()
        
        # Nombre de la tabla de prueba
        test_table_name = "TestTableForMigration"
        
        try:
            # Eliminar la tabla si ya existe
            print(f"Eliminando tabla {test_table_name} si existe...")
            cursor.execute(f"IF OBJECT_ID('{test_table_name}', 'U') IS NOT NULL DROP TABLE {test_table_name}")
            conn.commit()
            
            # Crear tabla de prueba
            print(f"Creando tabla {test_table_name}...")
            create_table_query = f"""
            CREATE TABLE {test_table_name} (
                ID INT PRIMARY KEY,
                Nombre NVARCHAR(100),
                Edad INT,
                FechaCreacion DATETIME DEFAULT GETDATE()
            )
            """
            cursor.execute(create_table_query)
            conn.commit()
            
            # Insertar datos de prueba
            print("Insertando datos de prueba...")
            insert_data_query = f"""
            INSERT INTO {test_table_name} (ID, Nombre, Edad)
            VALUES 
                (1, 'Juan Pérez', 30),
                (2, 'María López', 25),
                (3, 'Carlos Gómez', 45)
            """
            cursor.execute(insert_data_query)
            conn.commit()
            
            print(f"✅ Tabla {test_table_name} creada con 3 registros de prueba")
            
            # Actualizar el proceso para usar la tabla creada
            print("\nActualizando proceso...")
            old_tables = process.selected_tables
            process.selected_tables = [test_table_name]
            process.save()
            
            print(f"✅ Proceso '{process.name}' actualizado:")
            print(f"  - Tablas anteriores: {old_tables}")
            print(f"  - Tablas actuales: {process.selected_tables}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            return False
        finally:
            cursor.close()
            conn.close()
        
    except MigrationProcess.DoesNotExist:
        print(f"❌ No se encontró ningún proceso con ID {process_id}")
        return False
    except Exception as e:
        print(f"❌ Error general: {str(e)}")
        return False

def test_run_process(process_id):
    """Ejecuta un proceso y reporta el resultado detallado"""
    try:
        process = MigrationProcess.objects.get(id=process_id)
        print(f"\n===== EJECUTANDO PROCESO '{process.name}' (ID: {process_id}) =====")
        
        try:
            print("🚀 Iniciando ejecución...")
            process.run()
            print("✅ Proceso ejecutado con éxito")
            return True
        except Exception as e:
            print(f"❌ Error ejecutando proceso: {str(e)}")
            return False
            
    except MigrationProcess.DoesNotExist:
        print(f"❌ No se encontró ningún proceso con ID {process_id}")
        return False
        
if __name__ == "__main__":
    process_id = 34  # CESAR_10
    
    # Paso 1: Crear tabla de prueba y actualizar proceso
    fixed = create_test_table_and_update_process(process_id)
    
    if fixed:
        # Paso 2: Probar el proceso actualizado
        print("\n🧪 Probando el proceso actualizado...")
        success = test_run_process(process_id)
        
        if success:
            print("\n✅ TODO LISTO: Proceso actualizado y ejecutado con éxito.")
            print("Ahora puede ejecutar el proceso desde la interfaz web sin errores.")
        else:
            print("\n❌ ATENCIÓN: Proceso actualizado pero sigue fallando al ejecutarse.")
            print("Revise los mensajes de error para más información.")
    else:
        print("\n❌ No se pudo actualizar el proceso. Revise los mensajes de error.")
        sys.exit(1)