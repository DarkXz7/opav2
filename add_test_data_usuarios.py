import os
import django
import sys

# Configurar Django
sys.path.append(r'c:\Users\migue\OneDrive\Escritorio\DJANGO DE NUEVO\opav\proyecto_automatizacion')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proyecto_automatizacion.settings')
django.setup()

from automatizacion.models import DatabaseConnection
from automatizacion.utils import SQLServerConnector

def add_test_data():
    """
    Agrega datos de prueba a la tabla dbo.Usuarios para verificar la inserción completa
    """
    
    print("=== AGREGANDO DATOS DE PRUEBA A dbo.Usuarios ===")
    
    try:
        # Obtener la conexión que usa el proceso isaac 5
        connection = DatabaseConnection.objects.get(id=57)  # ring2
        
        print(f"✅ Conexión encontrada: {connection.name}")
        print(f"   📂 Servidor: {connection.server}")
        print(f"   📊 Base de datos: {connection.selected_database}")
        
        # Conectar usando SQLServerConnector
        connector = SQLServerConnector(
            connection.server,
            connection.username,
            connection.password,
            connection.port
        )
        
        if not connector.select_database(connection.selected_database):
            print(f"❌ No se pudo conectar a la base de datos {connection.selected_database}")
            return
        
        print(f"✅ Conectado a {connection.selected_database}")
        
        cursor = connector.conn.cursor()
        
        # Verificar si la tabla existe y está vacía
        cursor.execute("SELECT COUNT(*) FROM dbo.Usuarios")
        count = cursor.fetchone()[0]
        
        print(f"📊 Registros actuales en dbo.Usuarios: {count}")
        
        if count == 0:
            print("📝 Insertando datos de prueba...")
            
            # Insertar datos de prueba (sin especificar ID ya que es identity)
            test_data = [
                ('Isaac Hernández', 'isaac@email.com'),
                ('María González', 'maria@email.com'),
                ('Carlos López', 'carlos@email.com'),
                ('Ana Martínez', 'ana@email.com')
            ]
            
            cursor.executemany(
                "INSERT INTO dbo.Usuarios (Nombre, Email) VALUES (?, ?)",
                test_data
            )
            
            connector.conn.commit()
            
            # Verificar la inserción
            cursor.execute("SELECT COUNT(*) FROM dbo.Usuarios")
            new_count = cursor.fetchone()[0]
            
            print(f"✅ Datos insertados exitosamente!")
            print(f"   📊 Registros totales: {new_count}")
            
            # Mostrar los datos insertados
            cursor.execute("SELECT Id, Nombre, Email FROM dbo.Usuarios")
            rows = cursor.fetchall()
            
            print(f"📋 Datos en dbo.Usuarios:")
            for row in rows:
                print(f"   ID: {row[0]}, Nombre: {row[1]}, Email: {row[2]}")
                
        else:
            print(f"✅ La tabla ya tiene datos ({count} registros)")
            
            # Mostrar algunos datos existentes
            cursor.execute("SELECT TOP 5 Id, Nombre, Email FROM dbo.Usuarios")
            rows = cursor.fetchall()
            
            print(f"📋 Muestra de datos en dbo.Usuarios:")
            for row in rows:
                print(f"   ID: {row[0]}, Nombre: {row[1]}, Email: {row[2]}")
        
        connector.disconnect()
        print(f"\n🎉 ¡Listo! Ahora la tabla dbo.Usuarios tiene datos para probar la inserción completa.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    add_test_data()