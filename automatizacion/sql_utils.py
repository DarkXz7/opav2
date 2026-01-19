"""Utilities for normalizing and validating pandas DataFrame values before SQL insertion.

This module provides a function `normalize_df_for_sql` which coerces column values
to appropriate Python types (or None) according to inferred target SQL types.
It is designed to be safe (non-strict) by default: invalid values are converted to
None and reported in the returned errors list. If strict=True, it raises on errors.

The function is intentionally generic so it can be reused by other SQL processes.
"""
from typing import Tuple, List, Dict
import pandas as pd
import numpy as np


def _is_empty_string_like(x):
    try:
        return isinstance(x, str) and x.strip() == ''
    except Exception:
        return False


def normalize_df_for_sql(df: pd.DataFrame, strict: bool = False) -> Tuple[pd.DataFrame, List[Dict]]:
    """Return a normalized copy of `df` suitable for insertion into SQL and a list of
    normalization issues.

    Rules implemented:
    - Tries to infer numeric columns from object dtype and coerce with pd.to_numeric
    - Tries to infer datetime columns and coerce with pd.to_datetime
    - Boolean columns: map common truthy/falsey strings and values
    - String/object columns: strip whitespace; empty strings -> None
    - All invalid values -> None (SQL NULL)

    Parameters:
        df: pandas DataFrame to normalize (not modified in-place)
        strict: if True, raises ValueError when any invalid value is found

    Returns:
        (df_normalized, errors)
        df_normalized: pandas DataFrame with normalized values (None used for SQL NULL)
        errors: list of dicts {column, count, example}
    """
    df_norm = df.copy()
    errors = []

    for col in df_norm.columns:
        series = df_norm[col]
        original_non_na = series.notna()
        
        # 1. Try datetime conversion first (before numeric, since dates might look numeric)
        if pd.api.types.is_datetime64_any_dtype(series):
            # Already datetime, just ensure valid
            coerced = pd.to_datetime(series, errors='coerce')
            invalid_mask = coerced.isna() & original_non_na
            if invalid_mask.any():
                errors.append({'column': col, 'count': int(invalid_mask.sum()), 'example': str(series[invalid_mask].iloc[0])})
            df_norm[col] = coerced
            continue
        
        # 2. Try numeric conversion FIRST for object columns (before datetime)
        # Numeric is more specific than datetime (dates can look like numbers)
        if pd.api.types.is_numeric_dtype(series):
            # Already numeric, just standardize None
            df_norm[col] = series.where(series.notna(), None)
            continue
        
        # Try coercing object columns to numeric
        if series.dtype == 'object':
            try:
                test_numeric = pd.to_numeric(series, errors='coerce')
                # Count non-empty original values (exclude empty strings and 'None' strings)
                non_empty_mask = original_non_na & ~series.apply(_is_empty_string_like) & (series.astype(str).str.lower() != 'none')
                
                # If more than 50% of non-empty values convert successfully, treat as numeric
                valid_nums = test_numeric.notna() & non_empty_mask
                if non_empty_mask.sum() > 0 and valid_nums.sum() / non_empty_mask.sum() > 0.5:
                    invalid_mask = test_numeric.isna() & non_empty_mask
                    if invalid_mask.any():
                        errors.append({'column': col, 'count': int(invalid_mask.sum()), 'example': str(series[invalid_mask].iloc[0])})
                    df_norm[col] = test_numeric
                    continue
            except Exception:
                pass
        
        # Try coercing object columns to datetime (after numeric)
        if series.dtype == 'object':
            try:
                test_datetime = pd.to_datetime(series, errors='coerce', format='mixed')
                # If more than 50% of non-null values convert successfully, treat as datetime
                # AND check if values don't look like pure numbers
                valid_dates = test_datetime.notna() & original_non_na
                if valid_dates.sum() > 0.5 * original_non_na.sum():
                    # Double check: if column has patterns like YYYY-MM-DD, treat as datetime
                    sample_valid = series[original_non_na].head(3).astype(str)
                    looks_like_date = any('-' in str(v) or '/' in str(v) for v in sample_valid if v)
                    
                    if looks_like_date:
                        invalid_mask = test_datetime.isna() & original_non_na & ~series.apply(_is_empty_string_like)
                        if invalid_mask.any():
                            errors.append({'column': col, 'count': int(invalid_mask.sum()), 'example': str(series[invalid_mask].iloc[0])})
                        df_norm[col] = test_datetime
                        continue
            except Exception:
                pass
        
        # 3. Try boolean conversion
        if pd.api.types.is_bool_dtype(series):
            df_norm[col] = series.where(series.notna(), None)
            continue
        
        # Try coercing object columns to boolean
        if series.dtype == 'object':
            def _to_bool(v):
                if pd.isna(v) or _is_empty_string_like(v):
                    return None
                s = str(v).strip().lower()
                if s in ('1', 'true', 't', 'yes', 'y', 'si', 'sí'):
                    return True
                if s in ('0', 'false', 'f', 'no', 'n'):
                    return False
                return None
            
            # Check if column looks boolean
            try:
                test_bool = series.map(_to_bool)
                valid_bools = test_bool.notna()
                if valid_bools.sum() > 0.5 * original_non_na.sum():
                    invalid_mask = test_bool.isna() & original_non_na
                    if invalid_mask.any():
                        errors.append({'column': col, 'count': int(invalid_mask.sum()), 'example': str(series[invalid_mask].iloc[0])})
                    df_norm[col] = test_bool
                    continue
            except Exception:
                pass
        
        # 4. Fallback: treat as string -> strip whitespace, empty -> None
        try:
            def _clean_string(v):
                if pd.isna(v):
                    return None
                s = str(v).strip()
                return None if s == '' or s.lower() == 'none' or s.lower() == 'nan' else s
            
            df_norm[col] = series.map(_clean_string)
        except Exception as e:
            errors.append({'column': col, 'count': int(original_non_na.sum()), 'example': str(e)})

    if errors and strict:
        raise ValueError(f"Normalization errors detected: {errors}")

    return df_norm, errors


def apply_default_values_from_mappings(df: pd.DataFrame, column_mappings: dict) -> pd.DataFrame:
    """
    Aplica valores por defecto según column_mappings para celdas vacías/None.
    
    Esta función se ejecuta DESPUÉS de normalize_df_for_sql(), por lo que el DataFrame
    ya tiene valores normalizados (None para vacíos/inválidos).
    
    Reglas aplicadas:
    - Si nullable=True → mantener None (se insertará como NULL en SQL)
    - Si nullable=False y hay None/vacío:
        - INT/BIGINT/SMALLINT/TINYINT/FLOAT/REAL/DECIMAL/NUMERIC:
            → Aplicar default_value si existe, sino usar 0
        - DATE/DATETIME/DATETIME2/SMALLDATETIME/TIME:
            → Aplicar default_value si existe, sino usar timestamp actual (GETDATE())
        - VARCHAR/NVARCHAR/CHAR/NCHAR/TEXT/NTEXT:
            → Aplicar default_value si existe, sino usar string vacío ''
    
    Args:
        df: DataFrame ya normalizado con normalize_df_for_sql()
        column_mappings: Dict con configuración de columnas:
            {
                'columna1': {
                    'renamed_to': 'nombre_en_sql',
                    'sql_type': 'INT',
                    'nullable': False,
                    'default_value': '0'
                },
                ...
            }
    
    Returns:
        DataFrame con valores por defecto aplicados según configuración
    
    Ejemplo:
        >>> df = pd.DataFrame({'cantidad': [5, None, 10]})
        >>> mappings = {'cantidad': {'sql_type': 'INT', 'nullable': False, 'default_value': '0'}}
        >>> result = apply_default_values_from_mappings(df, mappings)
        >>> # result['cantidad'] = [5, 0, 10]
    """
    df_result = df.copy()
    
    if not column_mappings:
        return df_result
    
    for col in df_result.columns:
        # Verificar si existe configuración para esta columna
        config = column_mappings.get(col)
        if not config or not isinstance(config, dict):
            continue
        
        # Obtener configuración
        nullable = config.get('nullable', True)
        default_value = config.get('default_value')
        sql_type = config.get('sql_type', 'VARCHAR').upper()
        
        # Si nullable=True, mantener los None (se insertarán como NULL)
        if nullable:
            continue
        
        # Si nullable=False, aplicar valores por defecto a las celdas None/NaN
        mask = df_result[col].isna()
        
        if not mask.any():
            # No hay valores vacíos en esta columna
            continue
        
        # Determinar el valor por defecto según el tipo SQL
        final_default = None
        
        # Tipos numéricos
        if any(numeric_type in sql_type for numeric_type in [
            'INT', 'BIGINT', 'SMALLINT', 'TINYINT', 
            'FLOAT', 'REAL', 'DECIMAL', 'NUMERIC', 'MONEY'
        ]):
            if default_value is not None and str(default_value).strip():
                try:
                    # Intentar convertir el default_value a número
                    final_default = float(default_value) if 'FLOAT' in sql_type or 'REAL' in sql_type or 'DECIMAL' in sql_type else int(default_value)
                except (ValueError, TypeError):
                    final_default = 0
            else:
                final_default = 0.0 if 'FLOAT' in sql_type or 'REAL' in sql_type else 0
        
        # Tipos de fecha/hora
        elif any(date_type in sql_type for date_type in [
            'DATE', 'DATETIME', 'DATETIME2', 'SMALLDATETIME', 'TIME', 'TIMESTAMP'
        ]):
            if default_value and str(default_value).strip().upper() == 'GETDATE()':
                # Usar timestamp actual
                final_default = pd.Timestamp.now()
            elif default_value is not None and str(default_value).strip():
                try:
                    # Intentar parsear como fecha
                    final_default = pd.to_datetime(default_value)
                except Exception:
                    # Si falla, usar fecha actual
                    final_default = pd.Timestamp.now()
            else:
                # Sin default_value especificado, usar fecha actual
                final_default = pd.Timestamp.now()
        
        # Tipos de texto
        elif any(text_type in sql_type for text_type in [
            'VARCHAR', 'NVARCHAR', 'CHAR', 'NCHAR', 'TEXT', 'NTEXT'
        ]):
            if default_value is not None:
                # Limpiar comillas si las tiene: "' '" -> " "
                str_default = str(default_value).strip()
                if str_default.startswith("'") and str_default.endswith("'"):
                    str_default = str_default[1:-1]
                final_default = str_default
            else:
                final_default = ''
        
        # Tipo BIT (booleano)
        elif 'BIT' in sql_type:
            if default_value is not None:
                try:
                    final_default = bool(int(default_value))
                except (ValueError, TypeError):
                    final_default = False
            else:
                final_default = False
        
        # Para otros tipos SQL no reconocidos, usar string vacío
        else:
            final_default = '' if default_value is None else str(default_value)
        
        # Aplicar el valor por defecto a las celdas vacías
        if final_default is not None:
            df_result.loc[mask, col] = final_default
    
    return df_result


def validate_structure_before_execution(
    df: pd.DataFrame, 
    column_mappings: dict, 
    table_name: str = None
) -> tuple:
    """
    Valida la estructura del DataFrame antes de insertar en SQL Server.
    
    Validaciones realizadas:
    1. NOMBRES: Valida nombres de tabla y columnas (longitud, caracteres válidos)
    2. COLUMNAS: Valida existencia, duplicados, y consistencia
    3. TIPOS: Valida compatibilidad de tipos SQL con los datos
    4. SIZE: Valida que valores no excedan el tamaño definido (VARCHAR, etc.)
    
    Args:
        df: DataFrame con los datos a insertar
        column_mappings: Dict con configuración de columnas:
            {
                'columna1': {
                    'renamed_to': 'nombre_sql',
                    'sql_type': 'INT',
                    'nullable': False,
                    'default_value': '0'
                },
                ...
            }
        table_name: Nombre de la tabla destino (opcional)
    
    Returns:
        Tuple (errores: List[Dict], advertencias: List[Dict], sugerencias: List[Dict])
        
        errores: Lista de errores críticos que impiden la ejecución
        advertencias: Lista de advertencias que permiten continuar
        sugerencias: Lista de correcciones automáticas sugeridas
        
    Ejemplo de error:
        {
            'tipo': 'TIPO_INCOMPATIBLE',
            'columna': 'edad',
            'mensaje': 'Tipo SQL es INT pero contiene valores no numéricos',
            'detalle': 'Valor encontrado: "abc"',
            'severidad': 'ERROR'
        }
    
    Ejemplo de advertencia:
        {
            'tipo': 'SIZE_EXCEDIDO',
            'columna': 'descripcion',
            'mensaje': 'Valor más largo excede el tamaño del campo',
            'detalle': 'VARCHAR(50) pero valor máximo: 75 caracteres',
            'severidad': 'WARNING'
        }
    
    Ejemplo de sugerencia:
        {
            'tipo': 'AUTO_FIX',
            'columna': 'descripcion',
            'accion': 'CAMBIAR_TIPO',
            'valor_actual': 'VARCHAR(50)',
            'valor_sugerido': 'VARCHAR(100)',
            'razon': 'Ajustar al tamaño máximo de datos + 25% margen'
        }
    """
    import re
    
    errores = []
    advertencias = []
    sugerencias = []
    
    # ============================================
    # 1. VALIDACIÓN DE NOMBRES
    # ============================================
    
    # Validar nombre de tabla
    if table_name:
        # Longitud máxima en SQL Server: 128 caracteres
        if len(table_name) > 128:
            errores.append({
                'tipo': 'NOMBRE_TABLA_LARGO',
                'tabla': table_name,
                'mensaje': f'Nombre de tabla excede 128 caracteres',
                'detalle': f'Longitud actual: {len(table_name)}',
                'severidad': 'ERROR'
            })
        
        # Caracteres inválidos en SQL Server
        invalid_chars = re.findall(r'[^\w\-\_]', table_name)
        if invalid_chars:
            errores.append({
                'tipo': 'NOMBRE_TABLA_INVALIDO',
                'tabla': table_name,
                'mensaje': 'Nombre de tabla contiene caracteres inválidos',
                'detalle': f'Caracteres inválidos: {", ".join(set(invalid_chars))}',
                'severidad': 'ERROR'
            })
        
        # No puede empezar con número
        if table_name and table_name[0].isdigit():
            errores.append({
                'tipo': 'NOMBRE_TABLA_EMPIEZA_NUMERO',
                'tabla': table_name,
                'mensaje': 'Nombre de tabla no puede empezar con número',
                'detalle': f'Empieza con: {table_name[0]}',
                'severidad': 'ERROR'
            })
    
    # Validar nombres de columnas
    nombres_sql = set()
    for col in df.columns:
        if col not in column_mappings:
            continue
        
        config = column_mappings[col]
        nombre_sql = config.get('renamed_to', col)
        
        # Longitud máxima
        if len(nombre_sql) > 128:
            errores.append({
                'tipo': 'NOMBRE_COLUMNA_LARGO',
                'columna': col,
                'nombre_sql': nombre_sql,
                'mensaje': f'Nombre de columna excede 128 caracteres',
                'detalle': f'Longitud actual: {len(nombre_sql)}',
                'severidad': 'ERROR'
            })
        
        # Caracteres inválidos
        invalid_chars = re.findall(r'[^\w\-\_]', nombre_sql)
        if invalid_chars:
            errores.append({
                'tipo': 'NOMBRE_COLUMNA_INVALIDO',
                'columna': col,
                'nombre_sql': nombre_sql,
                'mensaje': 'Nombre de columna contiene caracteres inválidos',
                'detalle': f'Caracteres inválidos: {", ".join(set(invalid_chars))}',
                'severidad': 'ERROR'
            })
        
        # No puede empezar con número
        if nombre_sql and nombre_sql[0].isdigit():
            errores.append({
                'tipo': 'NOMBRE_COLUMNA_EMPIEZA_NUMERO',
                'columna': col,
                'nombre_sql': nombre_sql,
                'mensaje': 'Nombre de columna no puede empezar con número',
                'detalle': f'Empieza con: {nombre_sql[0]}',
                'severidad': 'ERROR'
            })
        
        # Detectar duplicados
        if nombre_sql.lower() in nombres_sql:
            errores.append({
                'tipo': 'NOMBRE_COLUMNA_DUPLICADO',
                'columna': col,
                'nombre_sql': nombre_sql,
                'mensaje': 'Nombre de columna duplicado (ya existe otra con el mismo nombre)',
                'detalle': f'Nombre en conflicto: {nombre_sql}',
                'severidad': 'ERROR'
            })
        
        nombres_sql.add(nombre_sql.lower())
    
    # ============================================
    # 2. VALIDACIÓN DE COLUMNAS
    # ============================================
    
    # Verificar que todas las columnas en column_mappings existen en el DataFrame
    columnas_faltantes = []
    for col in column_mappings.keys():
        if col not in df.columns:
            columnas_faltantes.append(col)
    
    if columnas_faltantes:
        errores.append({
            'tipo': 'COLUMNAS_FALTANTES',
            'columnas': columnas_faltantes,
            'mensaje': f'Columnas configuradas no existen en el archivo',
            'detalle': f'Columnas faltantes: {", ".join(columnas_faltantes)}',
            'severidad': 'ERROR'
        })
    
    # Verificar columnas duplicadas en DataFrame
    duplicadas = df.columns[df.columns.duplicated()].tolist()
    if duplicadas:
        errores.append({
            'tipo': 'COLUMNAS_DUPLICADAS',
            'columnas': duplicadas,
            'mensaje': 'Columnas duplicadas en el archivo',
            'detalle': f'Columnas duplicadas: {", ".join(set(duplicadas))}',
            'severidad': 'ERROR'
        })
    
    # Verificar que hay al menos una columna seleccionada
    if not column_mappings or len(column_mappings) == 0:
        errores.append({
            'tipo': 'SIN_COLUMNAS',
            'mensaje': 'No hay columnas configuradas para procesar',
            'detalle': 'Debe seleccionar al menos una columna',
            'severidad': 'ERROR'
        })
    
    # ============================================
    # 3. VALIDACIÓN DE TIPOS
    # ============================================
    
    for col in df.columns:
        if col not in column_mappings:
            continue
        
        config = column_mappings[col]
        sql_type = config.get('sql_type', 'NVARCHAR(255)').upper()
        
        # Obtener serie sin valores nulos para validación
        serie_valida = df[col].dropna()
        
        if len(serie_valida) == 0:
            # Columna completamente vacía
            continue
        
        # VALIDACIÓN: Tipos numéricos enteros (INT, BIGINT, SMALLINT, TINYINT)
        if any(int_type in sql_type for int_type in ['INT', 'BIGINT', 'SMALLINT', 'TINYINT']):
            try:
                # Intentar convertir a numérico
                valores_numericos = pd.to_numeric(serie_valida, errors='coerce')
                
                # Verificar si hay valores que no se pudieron convertir
                valores_invalidos = serie_valida[valores_numericos.isna()]
                
                if len(valores_invalidos) > 0:
                    ejemplo = valores_invalidos.iloc[0]
                    errores.append({
                        'tipo': 'TIPO_INCOMPATIBLE_INT',
                        'columna': col,
                        'sql_type': sql_type,
                        'mensaje': f'Tipo SQL es {sql_type} pero contiene valores no numéricos',
                        'detalle': f'Ejemplo de valor inválido: "{ejemplo}"',
                        'cantidad_invalidos': len(valores_invalidos),
                        'severidad': 'ERROR'
                    })
                    
                    # Sugerencia: cambiar a VARCHAR
                    sugerencias.append({
                        'tipo': 'AUTO_FIX',
                        'columna': col,
                        'accion': 'CAMBIAR_TIPO',
                        'valor_actual': sql_type,
                        'valor_sugerido': 'NVARCHAR(255)',
                        'razon': 'Columna contiene valores no numéricos'
                    })
                else:
                    # Validar rangos según el tipo
                    max_val = valores_numericos.max()
                    min_val = valores_numericos.min()
                    
                    if 'TINYINT' in sql_type:
                        # TINYINT: 0 a 255
                        if max_val > 255 or min_val < 0:
                            errores.append({
                                'tipo': 'RANGO_EXCEDIDO',
                                'columna': col,
                                'sql_type': sql_type,
                                'mensaje': f'Valor excede rango de TINYINT (0-255)',
                                'detalle': f'Rango actual: {min_val} a {max_val}',
                                'severidad': 'ERROR'
                            })
                            sugerencias.append({
                                'tipo': 'AUTO_FIX',
                                'columna': col,
                                'accion': 'CAMBIAR_TIPO',
                                'valor_actual': sql_type,
                                'valor_sugerido': 'INT' if max_val <= 2147483647 else 'BIGINT',
                                'razon': f'Ajustar al rango de datos ({min_val} a {max_val})'
                            })
                    
                    elif 'SMALLINT' in sql_type:
                        # SMALLINT: -32,768 a 32,767
                        if max_val > 32767 or min_val < -32768:
                            errores.append({
                                'tipo': 'RANGO_EXCEDIDO',
                                'columna': col,
                                'sql_type': sql_type,
                                'mensaje': f'Valor excede rango de SMALLINT (-32,768 a 32,767)',
                                'detalle': f'Rango actual: {min_val} a {max_val}',
                                'severidad': 'ERROR'
                            })
                            sugerencias.append({
                                'tipo': 'AUTO_FIX',
                                'columna': col,
                                'accion': 'CAMBIAR_TIPO',
                                'valor_actual': sql_type,
                                'valor_sugerido': 'INT' if max_val <= 2147483647 else 'BIGINT',
                                'razon': f'Ajustar al rango de datos ({min_val} a {max_val})'
                            })
                    
                    elif sql_type == 'INT':
                        # INT: -2,147,483,648 a 2,147,483,647
                        if max_val > 2147483647 or min_val < -2147483648:
                            errores.append({
                                'tipo': 'RANGO_EXCEDIDO',
                                'columna': col,
                                'sql_type': sql_type,
                                'mensaje': f'Valor excede rango de INT (-2,147,483,648 a 2,147,483,647)',
                                'detalle': f'Rango actual: {min_val} a {max_val}',
                                'severidad': 'ERROR'
                            })
                            sugerencias.append({
                                'tipo': 'AUTO_FIX',
                                'columna': col,
                                'accion': 'CAMBIAR_TIPO',
                                'valor_actual': sql_type,
                                'valor_sugerido': 'BIGINT',
                                'razon': f'Ajustar al rango de datos ({min_val} a {max_val})'
                            })
            
            except Exception as e:
                errores.append({
                    'tipo': 'ERROR_VALIDACION_TIPO',
                    'columna': col,
                    'sql_type': sql_type,
                    'mensaje': f'Error al validar tipo numérico',
                    'detalle': str(e),
                    'severidad': 'ERROR'
                })
        
        # VALIDACIÓN: Tipos decimales (FLOAT, REAL, DECIMAL)
        elif any(dec_type in sql_type for dec_type in ['FLOAT', 'REAL', 'DECIMAL', 'NUMERIC', 'MONEY']):
            try:
                valores_numericos = pd.to_numeric(serie_valida, errors='coerce')
                valores_invalidos = serie_valida[valores_numericos.isna()]
                
                if len(valores_invalidos) > 0:
                    ejemplo = valores_invalidos.iloc[0]
                    errores.append({
                        'tipo': 'TIPO_INCOMPATIBLE_DECIMAL',
                        'columna': col,
                        'sql_type': sql_type,
                        'mensaje': f'Tipo SQL es {sql_type} pero contiene valores no numéricos',
                        'detalle': f'Ejemplo de valor inválido: "{ejemplo}"',
                        'cantidad_invalidos': len(valores_invalidos),
                        'severidad': 'ERROR'
                    })
                    
                    sugerencias.append({
                        'tipo': 'AUTO_FIX',
                        'columna': col,
                        'accion': 'CAMBIAR_TIPO',
                        'valor_actual': sql_type,
                        'valor_sugerido': 'NVARCHAR(255)',
                        'razon': 'Columna contiene valores no numéricos'
                    })
            
            except Exception as e:
                errores.append({
                    'tipo': 'ERROR_VALIDACION_TIPO',
                    'columna': col,
                    'sql_type': sql_type,
                    'mensaje': f'Error al validar tipo decimal',
                    'detalle': str(e),
                    'severidad': 'ERROR'
                })
        
        # VALIDACIÓN: Tipos de fecha (DATE, DATETIME, etc.)
        elif any(date_type in sql_type for date_type in ['DATE', 'DATETIME', 'TIME', 'TIMESTAMP']):
            try:
                # Intentar parsear fechas
                fechas_parseadas = pd.to_datetime(serie_valida, errors='coerce')
                fechas_invalidas = serie_valida[fechas_parseadas.isna()]
                
                # Filtrar valores que claramente NO son fechas (no solo vacíos)
                fechas_invalidas = fechas_invalidas[
                    ~fechas_invalidas.astype(str).str.upper().isin(['GETDATE()', 'NOW()', ''])
                ]
                
                if len(fechas_invalidas) > 0:
                    ejemplo = fechas_invalidas.iloc[0]
                    advertencias.append({
                        'tipo': 'TIPO_FECHA_INCOMPATIBLE',
                        'columna': col,
                        'sql_type': sql_type,
                        'mensaje': f'Algunos valores no se pueden parsear como fecha',
                        'detalle': f'Ejemplo: "{ejemplo}" (se convertirá según default_value)',
                        'cantidad_invalidos': len(fechas_invalidas),
                        'severidad': 'WARNING'
                    })
            
            except Exception as e:
                advertencias.append({
                    'tipo': 'ERROR_VALIDACION_FECHA',
                    'columna': col,
                    'sql_type': sql_type,
                    'mensaje': f'Error al validar fechas',
                    'detalle': str(e),
                    'severidad': 'WARNING'
                })
        
        # VALIDACIÓN: Tipos booleanos (BIT)
        elif 'BIT' in sql_type:
            # BIT acepta muchos formatos (True, False, 1, 0, yes, no)
            # Solo advertir si hay valores muy extraños
            valores_str = serie_valida.astype(str).str.lower()
            valores_validos = valores_str.isin([
                'true', 'false', '1', '0', 'yes', 'no', 
                'si', 'sí', 'y', 'n', 't', 'f'
            ])
            
            if not valores_validos.all():
                ejemplos_invalidos = serie_valida[~valores_validos].head(3).tolist()
                advertencias.append({
                    'tipo': 'TIPO_BIT_INUSUAL',
                    'columna': col,
                    'sql_type': sql_type,
                    'mensaje': 'Valores inusuales para tipo BIT',
                    'detalle': f'Ejemplos: {ejemplos_invalidos} (se convertirán a True/False)',
                    'severidad': 'WARNING'
                })
    
    # ============================================
    # 4. VALIDACIÓN DE SIZE (Tamaño/Longitud)
    # ============================================
    
    for col in df.columns:
        if col not in column_mappings:
            continue
        
        config = column_mappings[col]
        sql_type = config.get('sql_type', 'NVARCHAR(255)').upper()
        
        # Solo aplica a tipos VARCHAR/NVARCHAR/CHAR
        if any(text_type in sql_type for text_type in ['VARCHAR', 'NVARCHAR', 'CHAR']):
            # Extraer tamaño: VARCHAR(50) → 50
            match = re.search(r'\((\d+)\)', sql_type)
            if match:
                max_length_definido = int(match.group(1))
                
                # Convertir todos los valores a string y obtener longitud máxima
                longitudes = df[col].dropna().astype(str).str.len()
                
                if len(longitudes) > 0:
                    max_length_real = int(longitudes.max())
                    
                    if max_length_real > max_length_definido:
                        # ERROR: Valor excede el tamaño
                        # Encontrar ejemplo del valor más largo
                        idx_max = longitudes.idxmax()
                        valor_ejemplo = str(df[col].iloc[idx_max])
                        
                        errores.append({
                            'tipo': 'SIZE_EXCEDIDO',
                            'columna': col,
                            'sql_type': sql_type,
                            'mensaje': f'Valor más largo excede {sql_type}',
                            'detalle': f'Tamaño definido: {max_length_definido} | Tamaño real: {max_length_real}',
                            'ejemplo': valor_ejemplo[:100] + ('...' if len(valor_ejemplo) > 100 else ''),
                            'cantidad_registros_exceden': int((longitudes > max_length_definido).sum()),
                            'severidad': 'ERROR'
                        })
                        
                        # Sugerencia: aumentar tamaño con 25% margen
                        nuevo_tamano = int(max_length_real * 1.25)
                        tipo_base = sql_type.split('(')[0]
                        
                        sugerencias.append({
                            'tipo': 'AUTO_FIX',
                            'columna': col,
                            'accion': 'CAMBIAR_TIPO',
                            'valor_actual': sql_type,
                            'valor_sugerido': f'{tipo_base}({nuevo_tamano})',
                            'razon': f'Ajustar al tamaño máximo de datos ({max_length_real}) + 25% margen'
                        })
                    
                    elif max_length_real < max_length_definido * 0.5:
                        # ADVERTENCIA: Tamaño definido es muy grande (más del doble del necesario)
                        advertencias.append({
                            'tipo': 'SIZE_SOBREDIMENSIONADO',
                            'columna': col,
                            'sql_type': sql_type,
                            'mensaje': f'Tamaño definido es mayor al necesario',
                            'detalle': f'Tamaño definido: {max_length_definido} | Tamaño real máximo: {max_length_real}',
                            'ahorro_potencial': f'{max_length_definido - max_length_real} caracteres por registro',
                            'severidad': 'WARNING'
                        })
                        
                        # Sugerencia: optimizar tamaño
                        nuevo_tamano = int(max_length_real * 1.25)
                        tipo_base = sql_type.split('(')[0]
                        
                        sugerencias.append({
                            'tipo': 'OPTIMIZACION',
                            'columna': col,
                            'accion': 'CAMBIAR_TIPO',
                            'valor_actual': sql_type,
                            'valor_sugerido': f'{tipo_base}({nuevo_tamano})',
                            'razon': f'Optimizar almacenamiento (ahorro: {max_length_definido - nuevo_tamano} chars/registro)'
                        })
    
    return errores, advertencias, sugerencias
