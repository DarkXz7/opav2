"""
Vistas específicas para el sistema de logs
"""

from django.shortcuts import render
from django.contrib.admin.views.decorators import staff_member_required
from django.core.paginator import Paginator
from django.utils.dateformat import format
from .logs.models_logs import ProcesoLog
import json

@staff_member_required
def view_logs(request):
    """
    Muestra los logs de procesos almacenados en SQL Server
    Solo accesible para personal administrativo
    """
    # Parámetros de filtrado y paginación
    page = request.GET.get('page', 1)
    status_filter = request.GET.get('status', '')
    date_from = request.GET.get('date_from', '')
    date_to = request.GET.get('date_to', '')
    process_id = request.GET.get('process_id', '')
    
    # Consultar logs usando la conexión 'logs'
    logs_query = ProcesoLog.objects.using('logs').all().order_by('-FechaEjecucion')
    
    # Aplicar filtros si se especificaron
    if status_filter:
        logs_query = logs_query.filter(Estado__icontains=status_filter)
    
    if process_id:
        logs_query = logs_query.filter(ProcesoID=process_id)
    
    # Paginación
    paginator = Paginator(logs_query, 20)  # 20 logs por página
    logs = paginator.get_page(page)
    
    # Procesamiento de datos para mejor visualización
    for log in logs:
        # Formatear fecha
        log.formatted_date = format(log.FechaEjecucion, 'j F Y - H:i:s')
        
        # Intentar parsear parámetros JSON
        if log.ParametrosEntrada:
            try:
                log.parsed_params = json.loads(log.ParametrosEntrada)
            except:
                log.parsed_params = None
    
    context = {
        'logs': logs,
        'status_filter': status_filter,
        'process_id': process_id,
        'page_obj': logs,  # Para compatibilidad con paginador de Django
    }
    
    return render(request, 'automatizacion/logs/view_logs.html', context)

@staff_member_required
def view_log_detail(request, log_id):
    """
    Muestra los detalles de un log específico
    Solo accesible para personal administrativo
    """
    # Obtener el log usando la conexión 'logs'
    log = ProcesoLog.objects.using('logs').get(ProcesoID=log_id)
    
    # Procesar parámetros JSON si existen
    params_json = None
    if log.ParametrosEntrada:
        try:
            params_json = json.loads(log.ParametrosEntrada)
        except:
            params_json = log.ParametrosEntrada
    
    context = {
        'log': log,
        'params_json': params_json,
    }
    
    return render(request, 'automatizacion/logs/log_detail.html', context)
