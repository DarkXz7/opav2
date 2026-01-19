"""
Test para diagnosticar el problema de consistencia de IDs entre ProcesoLog y tablas dinámicas
"""

# Configurar Django
import os
import sys
import django
import uuid
from datetime import datetime

# Configurar path y Django
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proyecto_automatizacion.settings')
django.setup()

from django.db import connections
from automatizacion.data_transfer_service import data_transfer_service
from automatizacion.logs.models_logs import ProcesoLog

def main():
    print("🔍 DIAGNÓSTICO: Problemas de consistencia de IDs")
    print("=" * 60)
    
    # 1. Crear un proceso de prueba para ver cómo se generan los IDs
    process_name = "DiagnosticoIDs"
    proceso_uuid = str(uuid.uuid4())  # Generar UUID manualmente para comparar
    
    print(f"📝 UUID generado para el proceso: {proceso_uuid}")
    
    # 2. Transferir datos a tabla dinámica
    success, result_info = data_transfer_service.transfer_to_dynamic_table(
        process_name=process_name,
        proceso_id=proceso_uuid,  # Usar nuestro UUID
        datos_procesados={"test": "diagnostico", "timestamp": datetime.now().isoformat()},
        usuario_responsable="test_diagnostico",
        metadata={"migration_process_id": 999},  # ID de prueba
        estado_proceso="DIAGNOSTICO",
        tipo_operacion="TEST_CONSISTENCIA"
    )
    
    if not success:
        print(f"❌ Error en transferencia: {result_info.get('error')}")
        return
        
    print(f"✅ Datos transferidos exitosamente")
    print(f"   📋 Tabla: {result_info['table_name']}")
    print(f"   🆔 ResultadoID: {result_info['resultado_id']}")
    
    # 3. Consultar la tabla dinámica para ver qué ProcesoID se guardó
    table_name = result_info['table_name']
    resultado_id = result_info['resultado_id']
    
    with connections['destino'].cursor() as cursor:
        cursor.execute(f"""
            SELECT ResultadoID, ProcesoID, NombreProceso, EstadoProceso 
            FROM [{table_name}] 
            WHERE ResultadoID = ?
        """, [resultado_id])
        
        row = cursor.fetchone()
        if row:
            tabla_resultado_id, tabla_proceso_id, tabla_nombre_proceso, tabla_estado = row
            print(f"\n📊 DATOS EN TABLA DINÁMICA '{table_name}':")
            print(f"   ResultadoID (PK): {tabla_resultado_id}")
            print(f"   ProcesoID: {tabla_proceso_id}")
            print(f"   NombreProceso: {tabla_nombre_proceso}")
            print(f"   EstadoProceso: {tabla_estado}")
            
            # Verificar si coincide con nuestro UUID
            if tabla_proceso_id == proceso_uuid:
                print("   ✅ ProcesoID coincide con el UUID generado")
            else:
                print(f"   ❌ ProcesoID NO coincide:")
                print(f"      Esperado: {proceso_uuid}")
                print(f"      En tabla: {tabla_proceso_id}")
    
    # 4. Consultar ProcesoLog para ver si hay registros con este ProcesoID
    print(f"\n📊 BÚSQUEDA EN ProcesoLog:")
    
    try:
        # Buscar por el UUID que usamos
        logs_con_uuid = ProcesoLog.objects.using('logs').filter(ProcesoID=proceso_uuid)
        print(f"   Logs con ProcesoID={proceso_uuid}: {logs_con_uuid.count()}")
        
        if logs_con_uuid.exists():
            for log in logs_con_uuid[:3]:  # Mostrar máximo 3
                print(f"     - LogID: {log.LogID}, ProcesoID: {log.ProcesoID}")
                print(f"       MigrationProcessID: {log.MigrationProcessID}")
                print(f"       Estado: {log.Estado}")
        
        # Buscar logs recientes para ver qué ProcesoIDs se están generando
        logs_recientes = ProcesoLog.objects.using('logs').order_by('-LogID')[:5]
        print(f"\n   📋 Últimos 5 logs en ProcesoLog:")
        for log in logs_recientes:
            print(f"     - LogID: {log.LogID}")
            print(f"       ProcesoID: {log.ProcesoID}")
            print(f"       MigrationProcessID: {log.MigrationProcessID}")
            print(f"       NombreProceso: {log.NombreProceso}")
            print(f"       Estado: {log.Estado}")
            print(f"       Fecha: {log.FechaEjecucion}")
            print()
            
    except Exception as e:
        print(f"   ❌ Error consultando ProcesoLog: {e}")
    
    print("\n" + "=" * 60)
    print("🎯 CONCLUSIONES DEL DIAGNÓSTICO:")
    print(f"   - UUID generado: {proceso_uuid}")
    print(f"   - Tabla dinámica: {table_name}")
    print(f"   - ResultadoID: {resultado_id}")
    print("   - Revisar si los ProcesoIDs coinciden entre tabla dinámica y logs")

if __name__ == "__main__":
    main()