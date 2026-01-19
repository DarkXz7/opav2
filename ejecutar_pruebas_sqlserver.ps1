# Script para ejecutar pruebas de integración con SQL Server en PowerShell
# Este script facilita la ejecución de scripts Python en Django shell desde PowerShell

Write-Host "===================================================" -ForegroundColor Cyan
Write-Host "   EJECUTOR DE PRUEBAS PARA SQL SERVER EXPRESS" -ForegroundColor Cyan
Write-Host "===================================================" -ForegroundColor Cyan

$ErrorActionPreference = "Stop"

function Run-DjangoScript {
    param(
        [string]$ScriptPath
    )
    
    Write-Host "`nEjecutando script: $ScriptPath" -ForegroundColor Yellow
    
    if (-not (Test-Path $ScriptPath)) {
        Write-Host "ERROR: El archivo $ScriptPath no existe!" -ForegroundColor Red
        return
    }
    
    try {
        Write-Host "Iniciando ejecución..." -ForegroundColor Gray
        $content = Get-Content $ScriptPath -Raw
        $content | python manage.py shell
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "`n✅ Ejecución completada exitosamente`n" -ForegroundColor Green
        } else {
            Write-Host "`n❌ Ejecución finalizada con errores (código: $LASTEXITCODE)`n" -ForegroundColor Red
        }
    } catch {
        Write-Host "`n❌ Error durante la ejecución: $_`n" -ForegroundColor Red
    }
}

Write-Host "`n📋 Seleccione una opción:" -ForegroundColor Cyan
Write-Host "1. Ejecutar prueba básica (test_proceso_log.py)"
Write-Host "2. Ejecutar prueba completa (test_proceso_log_completo.py)"
Write-Host "3. Salir"

$opcion = Read-Host "`nIngrese el número de la opción"

switch ($opcion) {
    "1" {
        Run-DjangoScript -ScriptPath "test_proceso_log.py"
    }
    "2" {
        Run-DjangoScript -ScriptPath "test_proceso_log_completo.py"
    }
    "3" {
        Write-Host "Saliendo..." -ForegroundColor Yellow
        exit
    }
    default {
        Write-Host "Opción no válida. Saliendo..." -ForegroundColor Red
        exit
    }
}

Write-Host "===================================================" -ForegroundColor Cyan
