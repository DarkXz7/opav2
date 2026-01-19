from django.contrib import admin
from .models import DataSourceType, DataSource, DatabaseConnection, MigrationProcess, MigrationLog

# Configuración de modelos en el admin

@admin.register(DataSourceType)
class DataSourceTypeAdmin(admin.ModelAdmin):
    list_display = ('name', 'description')
    search_fields = ('name',)

@admin.register(DatabaseConnection)
class DatabaseConnectionAdmin(admin.ModelAdmin):
    list_display = ('name', 'server', 'selected_database', 'username', 'created_at', 'last_used')
    search_fields = ('name', 'server', 'selected_database')
    list_filter = ('created_at', 'last_used')

@admin.register(DataSource)
class DataSourceAdmin(admin.ModelAdmin):
    list_display = ('name', 'source_type', 'file_path', 'connection', 'created_at')
    search_fields = ('name', 'file_path')
    list_filter = ('source_type', 'created_at')

@admin.register(MigrationProcess)
class MigrationProcessAdmin(admin.ModelAdmin):
    list_display = ('name', 'source', 'status', 'created_at', 'last_run')
    search_fields = ('name', 'description')
    list_filter = ('status', 'created_at', 'last_run')
    readonly_fields = ('created_at', 'last_run')

@admin.register(MigrationLog)
class MigrationLogAdmin(admin.ModelAdmin):
    list_display = ('process', 'timestamp', 'stage', 'level', 'rows_processed')
    search_fields = ('process__name', 'level', 'message', 'error_message')
    list_filter = ('level', 'stage', 'timestamp')
    readonly_fields = ('timestamp', 'duration_ms')
