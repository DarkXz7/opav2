"""
Diagnóstico para identificar por qué MigrationProcessID siempre se guarda con valor 4
"""

# Configurar Django
import os
import sys
import django
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proyecto_automatizacion.settings')
django.setup()

from automatizacion.models import MigrationProcess
from automatizacion.logs.models_logs import ProcesoLog
from django.db import connections

def main():
    print("🔍 DIAGNÓSTICO: MigrationProcessID siempre valor 4")
    print("=" * 60)
    
    # 1. Listar todos los procesos MigrationProcess disponibles
    print("📋 PROCESOS DISPONIBLES en MigrationProcess:")
    procesos = MigrationProcess.objects.all()
    
    if not procesos.exists():
        print("   ❌ No hay procesos configurados en MigrationProcess")
        return
    
    for proceso in procesos:
        print(f"   🆔 ID: {proceso.id} - Nombre: '{proceso.name}' - Estado: {proceso.status}")
    
    # 2. Revisar logs recientes en ProcesoLog
    print(f"\n📊 LOGS RECIENTES en ProcesoLog (últimos 10):")
    try:
        logs_recientes = ProcesoLog.objects.using('logs').order_by('-LogID')[:10]
        
        migration_ids_encontrados = set()
        
        for log in logs_recientes:
            print(f"   LogID: {log.LogID}")
            print(f"   ├─ ProcesoID: {log.ProcesoID}")
            print(f"   ├─ MigrationProcessID: {log.MigrationProcessID}")  # 🎯 AQUÍ ESTÁ EL PROBLEMA
            print(f"   ├─ NombreProceso: {log.NombreProceso}")
            print(f"   └─ Estado: {log.Estado}")
            print()
            
            if log.MigrationProcessID:
                migration_ids_encontrados.add(log.MigrationProcessID)
        
        print(f"🎯 VALORES DE MigrationProcessID encontrados: {list(migration_ids_encontrados)}")
        
        if len(migration_ids_encontrados) == 1 and 4 in migration_ids_encontrados:
            print("❌ CONFIRMADO: Todos los logs tienen MigrationProcessID = 4")
            
            # Buscar qué proceso tiene ID = 4
            try:
                proceso_4 = MigrationProcess.objects.get(id=4)
                print(f"   📋 Proceso ID=4: '{proceso_4.name}' (status: {proceso_4.status})")
            except MigrationProcess.DoesNotExist:
                print("   ⚠️  El proceso ID=4 no existe en MigrationProcess")
                
        elif len(migration_ids_encontrados) > 1:
            print("✅ Los MigrationProcessID varían correctamente")
        else:
            print("⚠️  Situación inusual con MigrationProcessID")
    
    except Exception as e:
        print(f"   ❌ Error consultando ProcesoLog: {e}")
    
    # 3. Test directo: ejecutar diferentes procesos para ver si cambia
    print(f"\n🧪 TEST DIRECTO: Ejecutar procesos y verificar MigrationProcessID")
    
    # Tomar los primeros 2 procesos diferentes para probar
    procesos_test = list(procesos[:2])
    
    if len(procesos_test) < 2:
        print("   ⚠️  Se necesitan al menos 2 procesos para hacer la prueba")
        return
    
    for i, proceso in enumerate(procesos_test, 1):
        print(f"\n   {i}. Probando proceso ID={proceso.id}: '{proceso.name}'")
        
        # Simular lo que hace MigrationProcess.run() en el tracking
        from automatizacion.logs.process_tracker import ProcessTracker
        
        tracker = ProcessTracker(proceso.name)
        
        # Los parámetros que deberían pasar el MigrationProcessID correcto
        parametros_proceso = {
            'migration_process_id': proceso.id,  # 🎯 ESTE DEBERÍA SER EL ID CORRECTO
            'test_diagnostico': True,
            'proceso_nombre': proceso.name
        }
        
        print(f"      🔧 Parámetros enviados: migration_process_id = {proceso.id}")
        
        # Ejecutar iniciar (sin correr el proceso completo)
        proceso_uuid = tracker.iniciar(parametros_proceso)
        
        # Verificar qué se guardó realmente
        log_nuevo = ProcesoLog.objects.using('logs').filter(ProcesoID=proceso_uuid).first()
        
        if log_nuevo:
            print(f"      ✅ Log creado:")
            print(f"         LogID: {log_nuevo.LogID}")
            print(f"         ProcesoID: {log_nuevo.ProcesoID}")
            print(f"         MigrationProcessID: {log_nuevo.MigrationProcessID}")  # 🎯 VERIFICAR
            print(f"         NombreProceso: {log_nuevo.NombreProceso}")
            
            if log_nuevo.MigrationProcessID == proceso.id:
                print(f"      ✅ SUCCESS: MigrationProcessID correcto ({proceso.id})")
            else:
                print(f"      ❌ PROBLEMA: MigrationProcessID incorrecto")
                print(f"         Esperado: {proceso.id}")
                print(f"         Obtenido: {log_nuevo.MigrationProcessID}")
        else:
            print(f"      ❌ No se encontró el log creado")
        
        # Finalizar para limpiar
        tracker.finalizar_exito("Test completado")
    
    print(f"\n" + "=" * 60)
    print("🎯 CONCLUSIÓN:")
    print("   Si todos los MigrationProcessID salen iguales (ej: 4),")
    print("   el problema está en cómo se pasan los parámetros al ProcessTracker")
    print("   o en cómo el ProcessTracker extrae el migration_process_id")

if __name__ == "__main__":
    main()