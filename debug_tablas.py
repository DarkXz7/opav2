import os, sys, django
sys.path.append('.')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proyecto_automatizacion.settings')
django.setup()
from django.db import connections

# Buscar tablas que empiecen con "Proceso_Test"
with connections['destino'].cursor() as cursor:
    cursor.execute("""
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME LIKE 'Proceso_Test%' 
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
    """)
    
    tablas = cursor.fetchall()
    print("Tablas encontradas:")
    for tabla in tablas:
        print(f"  - {tabla[0]}")
        
        # Mostrar el ProcesoID más reciente
        cursor.execute(f"SELECT TOP 1 ResultadoID, ProcesoID FROM [{tabla[0]}] ORDER BY ResultadoID DESC")
        reg = cursor.fetchone()
        if reg:
            print(f"    Último registro: ResultadoID={reg[0]}, ProcesoID={reg[1]}")