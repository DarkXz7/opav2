# Template filter para acceder a elementos de un diccionario por clave o lista por índice

from django import template

register = template.Library()

@register.filter
def get_item(dictionary_or_list, key):
    """
    Obtiene un elemento de un diccionario por su clave o de una lista por índice
    Funciona tanto con dict.get(key) como con list[index]
    """
    if isinstance(dictionary_or_list, dict):
        return dictionary_or_list.get(key, None)
    elif isinstance(dictionary_or_list, (list, tuple)):
        try:
            # Convertir key a int si es posible (para índices de lista)
            index = int(key) if isinstance(key, str) and key.isdigit() else key
            return dictionary_or_list[index]
        except (IndexError, ValueError, TypeError):
            return None
    else:
        return None

@register.filter
def apply_column_mapping(columns, args):
    """
    Aplica mapeos de nombres a una lista de columnas.
    Args debe ser una cadena "table_name,column_mappings_json"
    Retorna lista de nombres mapeados o los originales si no hay mapeo.
    
    Uso: {{ columns|apply_column_mapping:"table_name,process.column_mappings" }}
    """
    if not columns or not isinstance(columns, list):
        return columns
    
    if not args:
        return columns
    
    # Separar argumentos
    parts = args.split(',', 1)
    if len(parts) != 2:
        return columns
    
    table_name = parts[0]
    column_mappings = parts[1]
    
    # Si column_mappings es un string, intentar evaluar como dict
    if isinstance(column_mappings, str):
        try:
            import json
            column_mappings = json.loads(column_mappings)
        except:
            return columns
    
    # Si no hay mapeos o no es un dict, retornar columnas originales
    if not column_mappings or not isinstance(column_mappings, dict):
        return columns
    
    # Si no hay mapeo para esta tabla, retornar columnas originales
    if table_name not in column_mappings:
        return columns
    
    # Aplicar mapeos
    table_mappings = column_mappings[table_name]
    mapped_columns = [table_mappings.get(col, col) for col in columns]
    
    return mapped_columns

@register.simple_tag
def get_mapped_columns(columns, table_name, column_mappings):
    """
    Template tag para obtener columnas con nombres mapeados.
    Retorna la lista de columnas con sus nombres personalizados si existen.
    
    Maneja dos formatos de column_mappings:
    1. Formato simple (string): {'renamed_to': 'nuevo_nombre'}
    2. Formato completo (dict): {'renamed_to': 'nuevo_nombre', 'sql_type': '...', ...}
    
    Uso: {% get_mapped_columns columns table_name process.column_mappings as mapped_cols %}
    """
    if not columns or not isinstance(columns, list):
        return columns
    
    # Si no hay mapeos, retornar originales
    if not column_mappings or not isinstance(column_mappings, dict):
        return columns
    
    # Si no hay mapeo para esta tabla, retornar originales
    if table_name not in column_mappings:
        return columns
    
    # Aplicar mapeos
    table_mappings = column_mappings[table_name]
    mapped_columns = []
    
    for col in columns:
        col_mapping = table_mappings.get(col, col)
        
        # Si el mapeo es un diccionario (formato nuevo con metadata)
        if isinstance(col_mapping, dict):
            # Extraer el nombre renombrado, o usar la columna original
            mapped_name = col_mapping.get('renamed_to', col)
        else:
            # Si es un string simple, usarlo directamente
            mapped_name = col_mapping
        
        mapped_columns.append(mapped_name)
    
    return mapped_columns

@register.simple_tag
def get_sample_values(sample_data_dict, sheet_name, column_index):
    """
    Extrae los primeros valores de una columna específica del sample_data.
    
    Uso: {% get_sample_values sample_data sheet_name forloop.counter0 as values %}
    """
    if not sample_data_dict or sheet_name not in sample_data_dict:
        return []
    
    try:
        sheet_data = sample_data_dict[sheet_name]
        if 'rows' not in sheet_data or not sheet_data['rows']:
            return []
        
        # Extraer valores de la columna especificada
        values = []
        for row in sheet_data['rows'][:3]:  # Primeras 3 filas
            if isinstance(row, (list, tuple)) and column_index < len(row):
                val = row[column_index]
                # Truncar si es muy largo
                if isinstance(val, str) and len(val) > 30:
                    val = val[:27] + "..."
                values.append(str(val) if val is not None else "-")
        
        return values
    except (IndexError, KeyError, TypeError):
        return []
