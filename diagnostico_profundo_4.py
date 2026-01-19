"""
Diagnóstico profundo: ¿Por qué todos los MigrationProcessID son 4?
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
from automatizacion.logs.process_tracker import ProcessTracker

def main():
    print("🔍 DIAGNÓSTICO PROFUNDO: ¿Por qué MigrationProcessID siempre es 4?")
    print("=" * 70)
    
    # 1. Listar TODOS los procesos MigrationProcess
    print("📋 TODOS LOS PROCESOS MigrationProcess:")
    procesos = MigrationProcess.objects.all().order_by('id')
    
    procesos_por_nombre = {}
    for proceso in procesos:
        print(f"   ID: {proceso.id:2d} - Nombre: '{proceso.name}' - Estado: {proceso.status}")
        procesos_por_nombre[proceso.name] = proceso.id
    
    # 2. Verificar específicamente los procesos que están fallando
    procesos_problema = ['Kawasaki', 'Suzuki', 'Yamaha']
    
    print(f"\n🎯 VERIFICACIÓN DE PROCESOS PROBLEMÁTICOS:")
    for nombre in procesos_problema:
        if nombre in procesos_por_nombre:
            id_real = procesos_por_nombre[nombre]
            print(f"   {nombre:10} → ID real: {id_real}")
        else:
            print(f"   {nombre:10} → ❌ NO EXISTE")
    
    # 3. Test directo de ProcessTracker para cada proceso
    print(f"\n🧪 TEST DIRECTO ProcessTracker:")
    
    for nombre in procesos_problema:
        if nombre in procesos_por_nombre:
            id_correcto = procesos_por_nombre[nombre]
            
            print(f"\n   📝 Probando {nombre} (ID real: {id_correcto})")
            
            # Crear ProcessTracker manualmente
            tracker = ProcessTracker(nombre)
            
            # Parámetros con el ID correcto
            parametros = {
                'migration_process_id': id_correcto,  # 🎯 ID CORRECTO
                'test_diagnostico': True,
                'proceso_nombre': nombre
            }
            
            print(f"      Enviando migration_process_id = {id_correcto}")
            
            # Ejecutar
            proceso_uuid = tracker.iniciar(parametros)
            
            # Verificar qué se guardó
            log_creado = ProcesoLog.objects.using('logs').filter(ProcesoID=proceso_uuid).first()
            
            if log_creado:
                migration_id_guardado = log_creado.MigrationProcessID
                print(f"      Guardado: MigrationProcessID = {migration_id_guardado}")
                
                if migration_id_guardado == id_correcto:
                    print(f"      ✅ CORRECTO: {id_correcto} == {migration_id_guardado}")
                else:
                    print(f"      ❌ INCORRECTO: esperado {id_correcto}, obtenido {migration_id_guardado}")
                    
                    # Investigar por qué cambió
                    print(f"      🔍 INVESTIGANDO:")
                    print(f"         ParametrosEntrada: {log_creado.ParametrosEntrada}")
            
            # Finalizar
            tracker.finalizar_exito("Test completado")
    
    # 4. Verificar si hay algún problema en el ProcessTracker
    print(f"\n🔍 REVISANDO ProcessTracker.iniciar():")
    
    # Simulación paso a paso
    tracker_debug = ProcessTracker("DebugTest")
    
    # Parámetros de prueba
    parametros_debug = {
        'migration_process_id': 999,  # Valor único para detectar
        'debug_test': True
    }
    
    print(f"   📝 Enviando migration_process_id = 999 (valor único)")
    print(f"   📝 Parámetros completos: {parametros_debug}")
    
    # Simular la lógica de ProcessTracker.iniciar()
    print(f"\n   🔧 Simulando ProcessTracker.iniciar():")
    
    # Paso 1: Extraer MigrationProcessID
    migration_process_id = None
    if parametros_debug and isinstance(parametros_debug, dict):
        migration_process_id = parametros_debug.get('migration_process_id')
    
    print(f"      1. Extracción: migration_process_id = {migration_process_id}")
    
    # Paso 2: Crear registro (sin guardar realmente)
    print(f"      2. Se crearía ProcesoLog con:")
    print(f"         MigrationProcessID = {migration_process_id}")
    
    # 5. Buscar valores hardcodeados
    print(f"\n🔍 BUSCANDO VALORES HARDCODEADOS:")
    
    # Buscar en el código si hay algún 4 hardcodeado
    import re
    
    archivos_revisar = [
        'automatizacion/logs/process_tracker.py',
        'automatizacion/models.py', 
        'automatizacion/views.py'
    ]
    
    for archivo in archivos_revisar:
        try:
            with open(archivo, 'r', encoding='utf-8') as f:
                contenido = f.read()
                
            # Buscar patrones sospechosos
            patron_4 = re.findall(r'.*4.*', contenido)
            patron_migration = re.findall(r'.*[Mm]igration.*[Pp]rocess.*[Ii][Dd].*', contenido)
            
            if '= 4' in contenido or 'migration_process_id.*4' in contenido:
                print(f"   ⚠️  {archivo}: Contiene '= 4' o referencias a 4")
                
        except Exception as e:
            print(f"   ❌ Error leyendo {archivo}: {e}")
    
    print(f"\n" + "=" * 70)
    print("🎯 PRÓXIMOS PASOS:")
    print("   1. Si ProcessTracker funciona bien con ID=999, el problema está en otro lado")
    print("   2. Si ProcessTracker también falla, revisar su código interno")
    print("   3. Verificar si hay cache del servidor Django")
    print("   4. Buscar valores hardcodeados en el código")

if __name__ == "__main__":
    main()