"""
Test directo del problema de SCOPE_IDENTITY()
"""
import os, sys, django
sys.path.append('.')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proyecto_automatizacion.settings')
django.setup()
from django.db import connections

# Test directo de SCOPE_IDENTITY()
print("🔧 Testing SCOPE_IDENTITY() problem...")

with connections['destino'].cursor() as cursor:
    table_name = "Proceso_TestDirecto"
    
    # Crear tabla de test
    cursor.execute(f"""
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}')
    BEGIN
        CREATE TABLE [{table_name}] (
            ResultadoID INT IDENTITY(1,1) PRIMARY KEY,
            ProcesoID NVARCHAR(36) NOT NULL,
            NombreProceso NVARCHAR(255) NOT NULL,
            FechaRegistro DATETIME2 DEFAULT GETDATE(),
            TestData NVARCHAR(100)
        )
    END
    """)
    
    # Insertar registro y obtener ID
    import uuid
    test_uuid = str(uuid.uuid4())
    
    cursor.execute(f"""
    INSERT INTO [{table_name}] (ProcesoID, NombreProceso, TestData)
    VALUES (%s, %s, %s)
    """, [test_uuid, "Test Directo", "Test data"])
    
    # Método 1: SCOPE_IDENTITY()
    try:
        cursor.execute("SELECT SCOPE_IDENTITY()")
        row = cursor.fetchone()
        scope_id = int(row[0]) if row and row[0] else None
        print(f"SCOPE_IDENTITY(): {scope_id}")
    except Exception as e:
        print(f"Error SCOPE_IDENTITY(): {e}")
    
    # Método 2: @@IDENTITY
    try:
        cursor.execute("SELECT @@IDENTITY")
        row = cursor.fetchone()
        identity_id = int(row[0]) if row and row[0] else None
        print(f"@@IDENTITY: {identity_id}")
    except Exception as e:
        print(f"Error @@IDENTITY: {e}")
    
    # Verificar qué se insertó realmente
    cursor.execute(f"SELECT TOP 1 ResultadoID FROM [{table_name}] WHERE ProcesoID = %s", [test_uuid])
    row = cursor.fetchone()
    actual_id = row[0] if row else None
    print(f"ID real insertado: {actual_id}")
    
    # Limpiar
    cursor.execute(f"DROP TABLE [{table_name}]")