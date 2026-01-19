import os
import django
import pyodbc
import sys

# Configurar entorno Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proyecto_automatizacion.settings')
django.setup()

from automatizacion.models import DatabaseConnection, MigrationProcess
from django.db.models import Q

def ensure_test_table_exists():
    """
    Asegura que todas las conexiones SQL tengan una tabla de prueba
    para usar como fallback cuando no existan las tablas seleccionadas
    """
    # Nombre estándar para la tabla de prueba
    TEST_TABLE_NAME = "AutomatizacionTestTable"
    
    # Obtener todas las conexiones SQL
    connections = DatabaseConnection.objects.all()
    
    if not connections:
        print("⚠️ No hay conexiones SQL configuradas en el sistema.")
        return False
    
    print(f"📊 Creando tabla de prueba '{TEST_TABLE_NAME}' en todas las bases de datos configuradas...")
    
    successful_connections = 0
    
    for conn in connections:
        if not conn.selected_database:
            print(f"⚠️ Conexión '{conn.name}' no tiene base de datos seleccionada.")
            continue
            
        try:
            # Crear string de conexión
            conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={conn.server};DATABASE={conn.selected_database};UID={conn.username};PWD={conn.password}'
            
            # Conectar a la base de datos
            print(f"\n🔌 Conectando a {conn.server}/{conn.selected_database}...")
            sql_conn = pyodbc.connect(conn_string)
            cursor = sql_conn.cursor()
            
            # Verificar si la tabla ya existe
            cursor.execute(f"""
                IF OBJECT_ID('{TEST_TABLE_NAME}', 'U') IS NULL
                    SELECT 'NOT_EXISTS' AS TableStatus
                ELSE
                    SELECT 'EXISTS' AS TableStatus
            """)
            
            result = cursor.fetchone()
            
            if result and result[0] == 'EXISTS':
                print(f"✅ Tabla '{TEST_TABLE_NAME}' ya existe en {conn.selected_database}")
            else:
                # Crear la tabla de prueba
                print(f"🔧 Creando tabla '{TEST_TABLE_NAME}' en {conn.selected_database}...")
                
                # Eliminar la tabla si ya existe (por si acaso)
                cursor.execute(f"IF OBJECT_ID('{TEST_TABLE_NAME}', 'U') IS NOT NULL DROP TABLE {TEST_TABLE_NAME}")
                sql_conn.commit()
                
                # Crear tabla con estructura estándar
                create_table_query = f"""
                CREATE TABLE {TEST_TABLE_NAME} (
                    ID INT PRIMARY KEY,
                    Nombre NVARCHAR(100),
                    Descripcion NVARCHAR(255),
                    Cantidad INT,
                    FechaCreacion DATETIME DEFAULT GETDATE()
                )
                """
                cursor.execute(create_table_query)
                sql_conn.commit()
                
                # Insertar datos de prueba
                print(f"📝 Insertando datos de prueba...")
                insert_data_query = f"""
                INSERT INTO {TEST_TABLE_NAME} (ID, Nombre, Descripcion, Cantidad)
                VALUES 
                    (1, 'Producto A', 'Producto de prueba A', 100),
                    (2, 'Producto B', 'Producto de prueba B', 200),
                    (3, 'Producto C', 'Producto de prueba C', 300),
                    (4, 'Producto D', 'Producto de prueba D', 400),
                    (5, 'Producto E', 'Producto de prueba E', 500)
                """
                cursor.execute(insert_data_query)
                sql_conn.commit()
                
                print(f"✅ Tabla '{TEST_TABLE_NAME}' creada con 5 registros de prueba")
            
            # Cerrar conexión
            cursor.close()
            sql_conn.close()
            
            successful_connections += 1
            
        except Exception as e:
            print(f"❌ Error en conexión '{conn.name}': {str(e)}")
    
    if successful_connections > 0:
        print(f"\n✅ Tabla de prueba creada en {successful_connections} conexiones.")
        return True
    else:
        print("\n❌ No se pudo crear la tabla de prueba en ninguna conexión.")
        return False

def update_existing_processes():
    """Actualiza los procesos SQL existentes para usar la tabla de prueba si es necesario"""
    TEST_TABLE_NAME = "AutomatizacionTestTable"
    
    # Obtener todos los procesos SQL
    sql_processes = MigrationProcess.objects.filter(Q(source__source_type='sql'))
    
    if not sql_processes:
        print("\n⚠️ No hay procesos SQL configurados en el sistema.")
        return
        
    print(f"\n📋 Actualizando {sql_processes.count()} procesos SQL existentes...")
    
    for process in sql_processes:
        try:
            if not process.selected_tables or process.selected_tables == []:
                print(f"✏️ Proceso '{process.name}' (ID: {process.id}): No tiene tablas seleccionadas, configurando tabla de prueba...")
                process.selected_tables = [TEST_TABLE_NAME]
                process.save()
                print(f"   ✅ Actualizado a: {process.selected_tables}")
        except Exception as e:
            print(f"❌ Error actualizando proceso '{process.name}': {str(e)}")
    
    print("\n✅ Actualización de procesos existentes completada.")

if __name__ == "__main__":
    print("🔄 Iniciando mantenimiento de tablas de prueba...")
    
    # Paso 1: Crear tabla de prueba en todas las bases de datos
    if ensure_test_table_exists():
        # Paso 2: Actualizar procesos existentes
        update_existing_processes()
        print("\n✅ LISTO: El sistema ha sido configurado con tablas de prueba.")
    else:
        print("\n❌ ERROR: No se pudieron crear las tablas de prueba. Revise las conexiones SQL.")
        sys.exit(1)