"""
Management command para sincronizar todos los procesos Django existentes
con la tabla ProcesosGuardados en SQL Server.

Este comando se ejecuta UNA SOLA VEZ para migrar los procesos históricos.
Después de esto, la sincronización es automática (mediante save() y run()).

Uso:
    python manage.py sync_processes_to_sqlserver
    
Opciones:
    --dry-run: Simula la sincronización sin hacer cambios
    --force: Fuerza la actualización incluso si ya existe el proceso en SQL Server
"""

from django.core.management.base import BaseCommand, CommandError
from automatizacion.models import MigrationProcess
from automatizacion.process_sync import sync_process_to_sqlserver


class Command(BaseCommand):
    help = 'Sincroniza todos los procesos Django (MigrationProcess) con SQL Server (ProcesosGuardados)'
    
    def add_arguments(self, parser):
        """Agregar argumentos opcionales al comando"""
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Simula la sincronización sin hacer cambios reales en SQL Server',
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Fuerza la actualización de todos los procesos (sobrescribe datos existentes)',
        )
    
    def handle(self, *args, **options):
        """Lógica principal del comando"""
        
        dry_run = options['dry_run']
        force = options['force']
        
        # Banner informativo
        self.stdout.write(self.style.SUCCESS('=' * 80))
        self.stdout.write(self.style.SUCCESS('🔄 SINCRONIZACIÓN DE PROCESOS: Django → SQL Server'))
        self.stdout.write(self.style.SUCCESS('=' * 80))
        
        if dry_run:
            self.stdout.write(self.style.WARNING('\n⚠️  MODO DRY-RUN: No se harán cambios reales\n'))
        
        # Obtener todos los procesos de Django
        try:
            procesos = MigrationProcess.objects.all().order_by('created_at')
            total_procesos = procesos.count()
            
            if total_procesos == 0:
                self.stdout.write(self.style.WARNING('⚠️  No hay procesos para sincronizar'))
                return
            
            self.stdout.write(f"📊 Total de procesos encontrados: {total_procesos}\n")
            
        except Exception as e:
            raise CommandError(f'Error al obtener procesos de Django: {str(e)}')
        
        # Contadores para estadísticas
        exitosos = 0
        actualizados = 0
        errores = 0
        omitidos = 0
        
        # Procesar cada proceso
        for i, proceso in enumerate(procesos, 1):
            self.stdout.write(f"\n[{i}/{total_procesos}] Procesando: {proceso.name}")
            self.stdout.write(f"    📁 Fuente: {proceso.source.source_type if proceso.source else 'N/A'}")
            self.stdout.write(f"    📅 Creado: {proceso.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
            self.stdout.write(f"    📊 Estado: {proceso.get_status_display()}")
            
            if dry_run:
                self.stdout.write(self.style.WARNING('    [DRY-RUN] Simulando sincronización...'))
                exitosos += 1
                continue
            
            try:
                # Sincronizar con SQL Server
                exito, mensaje, proceso_id_sql = sync_process_to_sqlserver(
                    proceso,
                    usuario='admin',
                    observaciones=f'Migrado mediante comando sync_processes_to_sqlserver (ID Django: {proceso.id})'
                )
                
                if exito:
                    if 'actualizado' in mensaje.lower():
                        actualizados += 1
                        self.stdout.write(self.style.SUCCESS(f'    ✅ {mensaje}'))
                    else:
                        exitosos += 1
                        self.stdout.write(self.style.SUCCESS(f'    ✅ {mensaje}'))
                else:
                    errores += 1
                    self.stdout.write(self.style.ERROR(f'    ❌ Error: {mensaje}'))
                    
            except Exception as e:
                errores += 1
                self.stdout.write(self.style.ERROR(f'    ❌ Excepción: {str(e)}'))
        
        # Resumen final
        self.stdout.write('\n' + '=' * 80)
        self.stdout.write(self.style.SUCCESS('📊 RESUMEN DE SINCRONIZACIÓN'))
        self.stdout.write('=' * 80)
        self.stdout.write(f"Total de procesos: {total_procesos}")
        self.stdout.write(self.style.SUCCESS(f"✅ Exitosos (nuevos): {exitosos}"))
        self.stdout.write(self.style.SUCCESS(f"🔄 Actualizados: {actualizados}"))
        
        if errores > 0:
            self.stdout.write(self.style.ERROR(f"❌ Errores: {errores}"))
        
        if omitidos > 0:
            self.stdout.write(self.style.WARNING(f"⏭️  Omitidos: {omitidos}"))
        
        self.stdout.write('=' * 80)
        
        if dry_run:
            self.stdout.write(self.style.WARNING('\n⚠️  Ejecución en DRY-RUN completada. No se hicieron cambios reales.'))
            self.stdout.write(self.style.WARNING('    Ejecuta sin --dry-run para aplicar los cambios.\n'))
        else:
            self.stdout.write(self.style.SUCCESS('\n✅ Sincronización completada exitosamente!\n'))
            
            if errores > 0:
                self.stdout.write(self.style.WARNING(f'⚠️  Revisa los {errores} errores mostrados arriba\n'))
