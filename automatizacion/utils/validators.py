"""
Validadores y normalizadores para el sistema de automatización.

Este módulo contiene funciones para:
1. Normalizar nombres de hojas y columnas
2. Inferir y validar tipos de datos
3. Normalizar valores según tipo SQL
4. Validar configuraciones antes de guardar
"""

import re
import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional
import logging

logger = logging.getLogger(__name__)


# ============================================
# NORMALIZACIÓN DE NOMBRES
# ============================================

def normalize_name(name: str, existing_names: List[str] = None) -> str:
    """
    Normaliza un nombre de hoja o columna según reglas SQL Server.
    
    Reglas aplicadas:
    - Convertir a lowercase
    - Reemplazar espacios por guiones bajos
    - Eliminar caracteres especiales (excepto _ y -)
    - No puede empezar con número
    - Máximo 128 caracteres
    - Evitar duplicados añadiendo sufijo incremental
    
    Args:
        name: Nombre original
        existing_names: Lista de nombres ya existentes (para evitar duplicados)
    
    Returns:
        Nombre normalizado y único
    
    Examples:
        >>> normalize_name("Hoja 1")
        'hoja_1'
        >>> normalize_name("Datos-Ventas!")
        'datos_ventas'
        >>> normalize_name("123tabla")
        'tabla_123'
        >>> normalize_name("Hoja", ["hoja", "hoja_1"])
        'hoja_2'
    """
    if not name:
        name = "sin_nombre"
    
    # 1. Convertir a lowercase
    normalized = name.lower().strip()
    
    # 2. Reemplazar espacios por guiones bajos
    normalized = normalized.replace(' ', '_')
    
    # 3. Eliminar caracteres especiales (solo permitir letras, números, _ y -)
    normalized = re.sub(r'[^a-z0-9_\-]', '', normalized)
    
    # 4. Reemplazar múltiples guiones/underscores consecutivos por uno solo
    normalized = re.sub(r'[_\-]+', '_', normalized)
    
    # 5. No puede empezar con número
    if normalized and normalized[0].isdigit():
        normalized = f"tabla_{normalized}"
    
    # 6. No puede estar vacío después de limpieza
    if not normalized:
        normalized = "sin_nombre"
    
    # 7. Limitar a 128 caracteres
    normalized = normalized[:128]
    
    # 8. Evitar duplicados
    if existing_names:
        existing_lower = [n.lower() for n in existing_names]
        
        if normalized in existing_lower:
            # Buscar sufijo disponible
            counter = 1
            base_name = normalized
            
            # Si ya tiene sufijo numérico, extraerlo
            match = re.search(r'_(\d+)$', normalized)
            if match:
                counter = int(match.group(1)) + 1
                base_name = normalized[:match.start()]
            
            while f"{base_name}_{counter}" in existing_lower:
                counter += 1
            
            normalized = f"{base_name}_{counter}"
    
    return normalized


def validate_sheet_name(name: str, existing_names: List[str] = None) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Valida un nombre de hoja y proporciona versión normalizada.
    
    Args:
        name: Nombre a validar
        existing_names: Lista de nombres existentes
    
    Returns:
        Tuple (es_valido, nombre_normalizado, mensaje_error)
    
    Examples:
        >>> validate_sheet_name("Ventas 2024")
        (True, "ventas_2024", None)
        >>> validate_sheet_name("")
        (False, None, "El nombre no puede estar vacío")
        >>> validate_sheet_name("Hoja", ["hoja"])
        (False, None, "El nombre 'hoja' ya existe")
    """
    if not name or not name.strip():
        return False, None, "El nombre no puede estar vacío"
    
    # Normalizar
    normalized = normalize_name(name, existing_names)
    
    # Validar longitud
    if len(normalized) > 128:
        return False, None, "El nombre excede 128 caracteres"
    
    # Validar que no empiece con número (aunque normalize_name ya lo maneja)
    if normalized[0].isdigit():
        return False, None, "El nombre no puede empezar con número"
    
    # Validar duplicados
    if existing_names and normalized.lower() in [n.lower() for n in existing_names]:
        return False, None, f"El nombre '{normalized}' ya existe"
    
    return True, normalized, None


def validate_column_name(name: str, existing_names: List[str] = None) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Valida un nombre de columna (mismas reglas que hojas).
    
    Args:
        name: Nombre a validar
        existing_names: Lista de nombres existentes
    
    Returns:
        Tuple (es_valido, nombre_normalizado, mensaje_error)
    """
    return validate_sheet_name(name, existing_names)


# ============================================
# INFERENCIA Y VALIDACIÓN DE TIPOS
# ============================================

def infer_sql_type(series: pd.Series, sample_size: int = 1000) -> Dict[str, Any]:
    """
    Infiere el tipo SQL más apropiado para una serie de pandas.
    
    Analiza los datos reales y sugiere el tipo SQL óptimo con parámetros.
    
    Args:
        series: Serie de pandas a analizar
        sample_size: Número de muestras a analizar (para rendimiento)
    
    Returns:
        Dict con:
            - sql_type: Tipo SQL sugerido (ej: "INT", "FLOAT", "VARCHAR(255)")
            - confidence: Nivel de confianza (0.0 - 1.0)
            - nullable: Si debería permitir NULL
            - default_value: Valor por defecto sugerido
            - warnings: Lista de advertencias
            - mixed_types: Si contiene tipos mixtos
    
    Examples:
        >>> s = pd.Series([1, 2, 3, None])
        >>> infer_sql_type(s)
        {
            'sql_type': 'INT',
            'confidence': 1.0,
            'nullable': True,
            'default_value': '0',
            'warnings': [],
            'mixed_types': False
        }
    """
    # Tomar muestra si es muy grande
    if len(series) > sample_size:
        sample = series.sample(n=sample_size, random_state=42)
    else:
        sample = series
    
    result = {
        'sql_type': 'NVARCHAR(255)',  # Default seguro
        'confidence': 0.0,
        'nullable': False,
        'default_value': None,
        'warnings': [],
        'mixed_types': False
    }
    
    # Contar valores nulos
    null_count = sample.isna().sum()
    total_count = len(sample)
    non_null = sample.dropna()
    
    # Determinar si debe ser nullable
    null_percentage = (null_count / total_count) if total_count > 0 else 0
    result['nullable'] = null_percentage > 0.05  # >5% nulos → nullable
    
    if len(non_null) == 0:
        # Toda la columna es NULL
        result['warnings'].append("Columna completamente vacía")
        result['nullable'] = True
        return result
    
    # Analizar tipos de datos
    dtype = non_null.dtype
    
    # TIPO 1: BOOLEANO
    unique_values = set(non_null.dropna().astype(str).str.lower().unique())
    bool_values = {'true', 'false', '1', '0', 's', 'n', 'si', 'sí', 'no', 'yes'}
    
    if unique_values.issubset(bool_values) and len(unique_values) <= 4:
        result['sql_type'] = 'BIT'
        result['confidence'] = 1.0
        result['default_value'] = '0'
        return result
    
    # TIPO 2: NUMÉRICO ENTERO
    if pd.api.types.is_integer_dtype(dtype):
        min_val = non_null.min()
        max_val = non_null.max()
        
        # Determinar tamaño apropiado
        if min_val >= 0 and max_val <= 255:
            result['sql_type'] = 'TINYINT'
        elif min_val >= -32768 and max_val <= 32767:
            result['sql_type'] = 'SMALLINT'
        elif min_val >= -2147483648 and max_val <= 2147483647:
            result['sql_type'] = 'INT'
        else:
            result['sql_type'] = 'BIGINT'
        
        result['confidence'] = 1.0
        result['default_value'] = '0'
        return result
    
    # TIPO 3: NUMÉRICO DECIMAL
    if pd.api.types.is_float_dtype(dtype):
        result['sql_type'] = 'FLOAT'
        result['confidence'] = 1.0
        result['default_value'] = '0.0'
        return result
    
    # TIPO 4: FECHA/HORA
    if pd.api.types.is_datetime64_any_dtype(dtype):
        result['sql_type'] = 'DATETIME2'
        result['confidence'] = 1.0
        result['default_value'] = 'GETDATE()'
        return result
    
    # TIPO 5: TEXTO - Intentar detectar números/fechas en strings
    if pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
        # Intentar convertir a numérico
        numeric_converted = pd.to_numeric(non_null, errors='coerce')
        numeric_success_rate = numeric_converted.notna().sum() / len(non_null)
        
        if numeric_success_rate > 0.9:  # >90% son números
            # Determinar si INT o FLOAT
            if (numeric_converted == numeric_converted.astype(int, errors='ignore')).all():
                result['sql_type'] = 'INT'
                result['default_value'] = '0'
            else:
                result['sql_type'] = 'FLOAT'
                result['default_value'] = '0.0'
            
            result['confidence'] = numeric_success_rate
            
            if numeric_success_rate < 1.0:
                result['mixed_types'] = True
                result['warnings'].append(
                    f"Columna contiene {int((1-numeric_success_rate)*100)}% de valores no numéricos"
                )
            
            return result
        
        # Intentar convertir a fecha
        date_converted = pd.to_datetime(non_null, errors='coerce')
        date_success_rate = date_converted.notna().sum() / len(non_null)
        
        if date_success_rate > 0.9:  # >90% son fechas
            result['sql_type'] = 'DATE'
            result['confidence'] = date_success_rate
            result['default_value'] = 'GETDATE()'
            
            if date_success_rate < 1.0:
                result['mixed_types'] = True
                result['warnings'].append(
                    f"Columna contiene {int((1-date_success_rate)*100)}% de valores no convertibles a fecha"
                )
            
            return result
        
        # Es texto puro - determinar longitud óptima
        max_length = non_null.astype(str).str.len().max()
        
        if max_length <= 50:
            varchar_size = 50
        elif max_length <= 100:
            varchar_size = 100
        elif max_length <= 255:
            varchar_size = 255
        elif max_length <= 500:
            varchar_size = 500
        else:
            varchar_size = max(int(max_length * 1.25), 1000)  # +25% margen
        
        result['sql_type'] = f'NVARCHAR({varchar_size})'
        result['confidence'] = 1.0
        result['default_value'] = "''"
        
        return result
    
    # Tipo desconocido - usar VARCHAR seguro
    result['warnings'].append(f"Tipo de dato desconocido: {dtype}")
    return result


# ============================================
# NORMALIZACIÓN DE VALORES
# ============================================

def normalize_value_by_type(value: Any, sql_type: str, nullable: bool = True, default_value: Any = None) -> Any:
    """
    Normaliza un valor individual según el tipo SQL configurado.
    
    Implementa las reglas de la pizarra:
    1. TEXTO: NULL o '' según nullable
    2. NÚMERO: NULL, 0 o default según nullable
    3. FECHA: NULL, GETDATE() o default según nullable
    4. BOOLEANO: NULL, False o default según nullable
    
    Args:
        value: Valor a normalizar
        sql_type: Tipo SQL destino
        nullable: Si permite NULL
        default_value: Valor por defecto configurado
    
    Returns:
        Valor normalizado
    
    Examples:
        >>> normalize_value_by_type(None, 'INT', nullable=False, default_value='0')
        0
        >>> normalize_value_by_type('', 'VARCHAR(50)', nullable=True)
        None
        >>> normalize_value_by_type('123abc', 'INT', nullable=False)
        0  # No convertible → usa default
    """
    upper_type = sql_type.upper()
    
    # Caso 1: Valor es NaN, None o string vacío
    is_empty = pd.isna(value) or value == '' or (isinstance(value, str) and not value.strip())
    
    if is_empty:
        if nullable:
            return None
        else:
            # Usar default_value o fallback según tipo
            if default_value is not None:
                # Si el default_value es una función SQL de fecha, convertir a timestamp real
                if isinstance(default_value, str) and default_value.upper() in ['GETDATE()', 'NOW()', 'CURRENT_TIMESTAMP']:
                    return pd.Timestamp.now()
                return default_value
            
            # Fallbacks según tipo
            if any(t in upper_type for t in ['INT', 'BIGINT', 'SMALLINT', 'TINYINT']):
                return 0
            elif any(t in upper_type for t in ['FLOAT', 'DECIMAL', 'NUMERIC', 'MONEY', 'REAL']):
                return 0.0
            elif any(t in upper_type for t in ['DATE', 'DATETIME', 'TIME', 'TIMESTAMP']):
                return pd.Timestamp.now()  # Usar timestamp real, no string
            elif 'BIT' in upper_type:
                return 0  # False
            else:  # VARCHAR, CHAR, TEXT
                return ''
    
    # Caso 2: Valor tiene contenido - normalizar según tipo
    
    # TIPO 1: NÚMEROS ENTEROS
    if any(t in upper_type for t in ['INT', 'BIGINT', 'SMALLINT', 'TINYINT']):
        try:
            # Intentar convertir a int
            if isinstance(value, str):
                value = value.strip()
            
            converted = int(float(value))
            return converted
        except (ValueError, TypeError):
            logger.warning(f"No se pudo convertir '{value}' a INT, usando default")
            return default_value if default_value is not None else (None if nullable else 0)
    
    # TIPO 2: NÚMEROS DECIMALES
    elif any(t in upper_type for t in ['FLOAT', 'DECIMAL', 'NUMERIC', 'MONEY', 'REAL']):
        try:
            if isinstance(value, str):
                value = value.strip()
            
            converted = float(value)
            return converted
        except (ValueError, TypeError):
            logger.warning(f"No se pudo convertir '{value}' a FLOAT, usando default")
            return default_value if default_value is not None else (None if nullable else 0.0)
    
    # TIPO 3: FECHAS
    elif any(t in upper_type for t in ['DATE', 'DATETIME', 'TIME', 'TIMESTAMP']):
        # Si ya es una función SQL string, convertir a timestamp real
        if isinstance(value, str) and value.upper() in ['GETDATE()', 'NOW()', 'CURRENT_TIMESTAMP']:
            return pd.Timestamp.now()
        
        try:
            # Intentar parsear fecha
            if isinstance(value, str):
                parsed_date = pd.to_datetime(value)
                return parsed_date  # Devolver como Timestamp, no como string
            elif isinstance(value, (datetime, pd.Timestamp)):
                return value  # Ya es fecha, devolverla tal cual
            else:
                return value
        except (ValueError, TypeError):
            logger.warning(f"No se pudo convertir '{value}' a DATE, usando default")
            if default_value is not None:
                if isinstance(default_value, str) and default_value.upper() in ['GETDATE()', 'NOW()', 'CURRENT_TIMESTAMP']:
                    return pd.Timestamp.now()
                return default_value
            return None if nullable else pd.Timestamp.now()
    
    # TIPO 4: BOOLEANO
    elif 'BIT' in upper_type:
        # Mapear representaciones comunes
        if isinstance(value, bool):
            return 1 if value else 0
        
        if isinstance(value, (int, float)):
            return 1 if value != 0 else 0
        
        if isinstance(value, str):
            value_lower = value.lower().strip()
            
            true_values = ['true', '1', 's', 'si', 'sí', 'yes', 'y']
            false_values = ['false', '0', 'n', 'no']
            
            if value_lower in true_values:
                return 1
            elif value_lower in false_values:
                return 0
        
        logger.warning(f"No se pudo convertir '{value}' a BIT, usando default")
        return default_value if default_value is not None else (None if nullable else 0)
    
    # TIPO 5: TEXTO
    else:
        # Convertir a string
        if value is None or pd.isna(value):
            return None if nullable else ''
        
        str_value = str(value)
        
        # Validar longitud para VARCHAR
        if 'VARCHAR' in upper_type or 'CHAR' in upper_type:
            match = re.search(r'\((\d+)\)', sql_type)
            if match:
                max_length = int(match.group(1))
                if len(str_value) > max_length:
                    logger.warning(f"Valor '{str_value[:50]}...' excede VARCHAR({max_length}), truncando")
                    str_value = str_value[:max_length]
        
        return str_value


def normalize_dataframe_by_mappings(
    df: pd.DataFrame,
    column_mappings: Dict[str, Dict[str, Any]]
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Normaliza un DataFrame completo según los mappings de columnas.
    
    Args:
        df: DataFrame a normalizar
        column_mappings: Dict con configuración por columna:
            {
                'columna_original': {
                    'renamed_to': 'nombre_sql',
                    'sql_type': 'INT',
                    'nullable': False,
                    'default_value': '0'
                }
            }
    
    Returns:
        Tuple (df_normalizado, warnings)
    
    Examples:
        >>> df = pd.DataFrame({'edad': ['25', None, 'abc']})
        >>> mappings = {'edad': {'renamed_to': 'edad', 'sql_type': 'INT', 'nullable': False, 'default_value': '0'}}
        >>> normalized_df, warnings = normalize_dataframe_by_mappings(df, mappings)
        >>> list(normalized_df['edad'])
        [25, 0, 0]
    """
    df_result = df.copy()
    warnings = []
    
    for original_col, config in column_mappings.items():
        if original_col not in df_result.columns:
            warnings.append({
                'column': original_col,
                'message': f"Columna '{original_col}' no existe en el DataFrame"
            })
            continue
        
        sql_type = config.get('sql_type', 'NVARCHAR(255)')
        nullable = config.get('nullable', True)
        default_value = config.get('default_value')
        
        # Normalizar cada valor de la columna
        try:
            df_result[original_col] = df_result[original_col].apply(
                lambda x: normalize_value_by_type(x, sql_type, nullable, default_value)
            )
        except Exception as e:
            warnings.append({
                'column': original_col,
                'message': f"Error al normalizar: {str(e)}"
            })
            logger.error(f"Error normalizando columna '{original_col}': {e}", exc_info=True)
    
    return df_result, warnings


# ============================================
# VALIDACIÓN DE CONFIGURACIONES
# ============================================

def validate_column_mappings(
    df: pd.DataFrame,
    column_mappings: Dict[str, Dict[str, Any]]
) -> Tuple[bool, List[Dict[str, Any]]]:
    """
    Valida que los mappings de columnas sean correctos antes de procesar.
    
    Verifica:
    1. Todas las columnas mapeadas existen en el DataFrame
    2. No hay duplicados en nombres SQL
    3. Tipos SQL son válidos
    4. Valores por defecto son compatibles con tipos
    5. Nombres SQL son válidos (sin caracteres especiales, longitud, etc.)
    
    Args:
        df: DataFrame a validar
        column_mappings: Configuración de columnas
    
    Returns:
        Tuple (es_valido, lista_de_errores)
    """
    errors = []
    
    # 1. Verificar que columnas existen
    for original_col in column_mappings.keys():
        if original_col not in df.columns:
            errors.append({
                'type': 'COLUMN_NOT_FOUND',
                'column': original_col,
                'message': f"Columna '{original_col}' no existe en el archivo"
            })
    
    # 2. Verificar duplicados en nombres SQL
    sql_names = []
    for original_col, config in column_mappings.items():
        sql_name = config.get('renamed_to', original_col).lower()
        if sql_name in sql_names:
            errors.append({
                'type': 'DUPLICATE_SQL_NAME',
                'column': original_col,
                'sql_name': sql_name,
                'message': f"Nombre SQL '{sql_name}' está duplicado"
            })
        sql_names.append(sql_name)
    
    # 3. Validar tipos SQL
    valid_types = [
        'INT', 'BIGINT', 'SMALLINT', 'TINYINT',
        'FLOAT', 'REAL', 'DECIMAL', 'NUMERIC', 'MONEY',
        'DATE', 'DATETIME', 'DATETIME2', 'TIME', 'TIMESTAMP',
        'BIT',
        'VARCHAR', 'NVARCHAR', 'CHAR', 'NCHAR', 'TEXT', 'NTEXT'
    ]
    
    for original_col, config in column_mappings.items():
        sql_type = config.get('sql_type', '').upper()
        
        # Extraer tipo base (sin parámetros)
        base_type = re.sub(r'\(.*\)', '', sql_type)
        
        if not any(vt in sql_type for vt in valid_types):
            errors.append({
                'type': 'INVALID_SQL_TYPE',
                'column': original_col,
                'sql_type': sql_type,
                'message': f"Tipo SQL '{sql_type}' no es válido"
            })
    
    # 4. Validar nombres SQL
    for original_col, config in column_mappings.items():
        sql_name = config.get('renamed_to', original_col)
        
        # Validar longitud
        if len(sql_name) > 128:
            errors.append({
                'type': 'NAME_TOO_LONG',
                'column': original_col,
                'sql_name': sql_name,
                'message': f"Nombre SQL '{sql_name}' excede 128 caracteres"
            })
        
        # Validar caracteres
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', sql_name):
            errors.append({
                'type': 'INVALID_SQL_NAME',
                'column': original_col,
                'sql_name': sql_name,
                'message': f"Nombre SQL '{sql_name}' contiene caracteres inválidos"
            })
    
    return len(errors) == 0, errors
