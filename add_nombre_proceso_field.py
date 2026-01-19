import pyodbc

# Conectar a SQL Server
conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost\\SQLEXPRESS;DATABASE=LogsAutomatizacion;Trusted_Connection=yes;'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

try:
    # Agregar campo NombreProceso a la tabla ProcesoLog
    cursor.execute("""
    ALTER TABLE ProcesoLog 
    ADD NombreProceso NVARCHAR(255) NULL
    """)
    conn.commit()
    print("✓ Campo NombreProceso agregado exitosamente a la tabla ProcesoLog")
    
    # Verificar la estructura actualizada
    cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH 
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'ProcesoLog'
    ORDER BY ORDINAL_POSITION
    """)
    
    columns = cursor.fetchall()
    print("\nEstructura actualizada de la tabla ProcesoLog:")
    for col in columns:
        max_len = col[2] if col[2] else ""
        print(f'{col[0]:<20} {col[1]:<15} {max_len}')
        
except Exception as e:
    if "already exists" in str(e) or "Duplicate column name" in str(e):
        print("✓ Campo NombreProceso ya existe en la tabla")
    else:
        print(f"Error al agregar campo: {str(e)}")
finally:
    conn.close()
