import os, sys, django
sys.path.append('.')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proyecto_automatizacion.settings')
django.setup()
from django.db import connections

with connections['destino'].cursor() as cursor:
    # Verificar si existe la tabla
    cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Proceso_TestSimpleConsistencia'")
    existe = cursor.fetchone()[0]
    print(f'Tabla existe: {existe > 0}')
    
    if existe:
        # Mostrar registros
        cursor.execute('SELECT TOP 3 * FROM [Proceso_TestSimpleConsistencia] ORDER BY ResultadoID DESC')
        registros = cursor.fetchall()
        print(f'Registros encontrados: {len(registros)}')
        for reg in registros:
            print(f'  ResultadoID: {reg[0]}, ProcesoID: {reg[1]}')