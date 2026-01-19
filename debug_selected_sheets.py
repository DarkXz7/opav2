#!/usr/bin/env python
"""
Script de diagnóstico para revisar selected_sheets en procesos guardados.
"""
import os
import sys
import django

# Configurar Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proyecto_automatizacion.settings')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
django.setup()

from automatizacion.models import MigrationProcess, DataSource
import json

print("=" * 80)
print("DIAGNÓSTICO DE SELECTED_SHEETS EN PROCESOS EXCEL")
print("=" * 80)

# Buscar procesos Excel
excel_sources = DataSource.objects.filter(source_type='excel')
print(f"\n📁 Fuentes de datos Excel encontradas: {excel_sources.count()}")

for source in excel_sources:
    print(f"\n   - {source.name}: {source.file_path or source.onedrive_url}")

# Buscar procesos que usan fuentes Excel
excel_processes = MigrationProcess.objects.filter(source__source_type='excel')
print(f"\n📋 Procesos con fuente Excel: {excel_processes.count()}")

for process in excel_processes:
    print(f"\n{'='*60}")
    print(f"📌 Proceso: {process.name}")
    print(f"   ID: {process.id}")
    print(f"   Fuente: {process.source.name if process.source else 'N/A'}")
    print(f"   Status: {process.status}")
    
    # Analizar selected_sheets
    print(f"\n   📊 selected_sheets:")
    print(f"      Tipo: {type(process.selected_sheets)}")
    print(f"      Valor raw: {process.selected_sheets}")
    
    if process.selected_sheets:
        if isinstance(process.selected_sheets, list):
            print(f"      Es lista: ✅")
            print(f"      Cantidad de hojas: {len(process.selected_sheets)}")
            print(f"      Hojas: {process.selected_sheets}")
        elif isinstance(process.selected_sheets, str):
            print(f"      Es string: ⚠️ (debería ser lista)")
            try:
                parsed = json.loads(process.selected_sheets)
                print(f"      Parseado: {parsed}")
                print(f"      Tipo parseado: {type(parsed)}")
                print(f"      Cantidad de hojas: {len(parsed)}")
            except:
                print(f"      No se pudo parsear como JSON")
    else:
        print(f"      ⚠️ selected_sheets está vacío o None")
    
    # Analizar selected_columns
    print(f"\n   📊 selected_columns:")
    print(f"      Tipo: {type(process.selected_columns)}")
    if process.selected_columns:
        if isinstance(process.selected_columns, dict):
            print(f"      Hojas con columnas: {list(process.selected_columns.keys())}")
            for sheet, cols in process.selected_columns.items():
                if sheet != '__sheet_names__':
                    print(f"         '{sheet}': {len(cols)} columnas")
        elif isinstance(process.selected_columns, str):
            try:
                parsed = json.loads(process.selected_columns)
                print(f"      (Parseado como JSON)")
                print(f"      Hojas con columnas: {list(parsed.keys())}")
            except:
                print(f"      No se pudo parsear")
    else:
        print(f"      ⚠️ selected_columns está vacío o None")

print(f"\n{'='*80}")
print("FIN DEL DIAGNÓSTICO")
print("=" * 80)
