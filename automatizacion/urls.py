from django.urls import path
from . import views
from . import log_views
from . import data_transfer_views
from . import data_load_views_simple as data_load_views

app_name = 'automatizacion'

urlpatterns = [
    # Rutas principales
    path('', views.index, name='index'),
    path('process/new/', views.new_process, name='new_process'),
    path('process/list/', views.list_processes, name='list_processes'),
    path('process/<int:process_id>/', views.view_process, name='view_process'),
    path('process/<int:process_id>/edit/', views.edit_process, name='edit_process'),
    path('process/<int:process_id>/run/', views.run_process, name='run_process'),
    path('process/<int:process_id>/delete/', views.delete_process, name='delete_process'),
    
    # Rutas para Excel/CSV
    path('excel/upload/', views.upload_excel, name='upload_excel'),
    # ELIMINADO: Vista intermedia /sheets/ - ahora redirige directo a multi-config
    # path('excel/<int:source_id>/sheets/', views.list_excel_sheets, name='list_excel_sheets'),
    path('excel/<int:source_id>/multi-config/', views.list_excel_multi_sheet_columns, name='list_excel_multi_sheet_columns'),
    path('excel/<int:source_id>/sheet/<str:sheet_name>/columns/', views.list_excel_columns, name='list_excel_columns'),
    
    # Nuevas rutas AJAX para validación en tiempo real
    path('api/validate-sheet-rename/', views.validate_sheet_rename, name='validate_sheet_rename'),
    path('api/excel/<int:source_id>/infer-types/', views.infer_column_types, name='infer_column_types'),
    
    # Rutas para SQL Server
    path('sql/connect/', views.connect_sql, name='connect_sql'),
    path('sql/connections/', views.list_connections, name='list_connections'),
    path('sql/connection/<int:connection_id>/', views.view_connection, name='view_connection'),
    path('sql/connection/<int:connection_id>/databases/', views.list_sql_databases, name='list_sql_databases'),
    path('sql/connection/<int:connection_id>/select_database/', views.select_database, name='select_database'),
    path('sql/connection/<int:connection_id>/tables/', views.list_sql_tables, name='list_sql_tables'),
    path('sql/connection/<int:connection_id>/table/<str:table_name>/columns/', views.list_sql_columns, name='list_sql_columns'),
    
    # Rutas para API AJAX
    path('api/save_process/', views.save_process, name='save_process'),
    path('api/save_excel_multi_process/', views.save_excel_multi_process, name='save_excel_multi_process'),
    path('api/delete_connection/<int:connection_id>/', views.delete_connection, name='delete_connection'),
    path('api/process/<int:process_id>/load_columns/', views.load_process_columns, name='load_process_columns'),
    
    # Rutas para Transferencia Segura de Datos  
    path('sql/connection/<int:connection_id>/table/<str:table_name>/transfer/', 
         data_transfer_views.SecureDataTransferView.as_view(), 
         name='secure_data_transfer'),
    path('sql/connection/<int:connection_id>/test/', 
         data_transfer_views.test_destination_connection, 
         name='test_destination_connection'),
    path('sql/connection/<int:connection_id>/table/<str:table_name>/structure/', 
         data_transfer_views.get_table_structure, 
         name='get_table_structure'),
    path('api/transfer/results/', 
         data_transfer_views.list_transfer_results, 
         name='list_transfer_results'),
    
    # Rutas para Carga Robusta de Datos
    path('data/load/', 
         data_load_views.DataLoadView.as_view(), 
         name='data_load'),
    path('data/load/status/<str:proceso_id>/', 
         data_load_views.LoadStatusView.as_view(), 
         name='load_status_detail'),
    path('data/validate/', 
         data_load_views.DataValidationView.as_view(), 
         name='data_validation'),
    path('data/loads/recent/', 
         data_load_views.list_recent_loads, 
         name='list_recent_loads'),
    path('data/loads/statistics/', 
         data_load_views.load_statistics, 
         name='load_statistics'),
    
    # Rutas para Sistema de Logs (SQL Server)
    path('logs/', log_views.view_logs, name='view_logs'),
    path('logs/<str:log_id>/', log_views.view_log_detail, name='view_log_detail'),
    
    # Nueva plantilla moderna
    path('modern/', views.modern_view, name='modern_view'),
]
