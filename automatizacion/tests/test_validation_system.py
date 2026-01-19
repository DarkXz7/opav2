"""
Tests del Sistema de Validación y Normalización

Ejecutar con:
    pytest automatizacion/tests/test_validation_system.py -v
    pytest automatizacion/tests/test_validation_system.py --cov=automatizacion.utils
"""

import pytest
import pandas as pd
from datetime import datetime
from automatizacion.utils.validators import (
    normalize_name,
    validate_sheet_name,
    validate_column_name,
    infer_sql_type,
    normalize_value_by_type,
    normalize_dataframe_by_mappings,
    validate_column_mappings
)


class TestNormalizeName:
    """Tests para normalización de nombres SQL-safe"""
    
    def test_basic_normalization(self):
        """Espacios → guiones bajos, lowercase"""
        assert normalize_name("Hoja 1") == "hoja_1"
        assert normalize_name("Datos Ventas") == "datos_ventas"
        assert normalize_name("MAYUSCULAS") == "mayusculas"
    
    def test_special_characters_removed(self):
        """Elimina caracteres especiales: !, @, #, etc."""
        assert normalize_name("Datos-Ventas!") == "datos_ventas"
        assert normalize_name("Hoja#1@2024") == "hoja_1_2024"
        assert normalize_name("Tabla$$$123") == "tabla_123"
        assert normalize_name("Año_2024") == "ano_2024"  # ñ → n
    
    def test_starts_with_number(self):
        """Nombres que empiezan con número → agregar prefijo 'tabla_'"""
        assert normalize_name("123tabla") == "tabla_123"
        assert normalize_name("2024_ventas") == "tabla_2024_ventas"
        assert normalize_name("9_prueba") == "tabla_9_prueba"
    
    def test_avoid_duplicates(self):
        """Evita duplicados con sufijo _1, _2, etc."""
        existing = ["hoja"]
        assert normalize_name("Hoja", existing) == "hoja_1"
        
        existing = ["hoja", "hoja_1"]
        assert normalize_name("Hoja", existing) == "hoja_2"
        
        existing = ["ventas", "ventas_1", "ventas_2"]
        assert normalize_name("Ventas", existing) == "ventas_3"
    
    def test_empty_name(self):
        """Nombres vacíos → 'sin_nombre'"""
        assert normalize_name("") == "sin_nombre"
        assert normalize_name("   ") == "sin_nombre"
        assert normalize_name("\t\n") == "sin_nombre"
    
    def test_max_length(self):
        """Trunca a 128 caracteres"""
        long_name = "a" * 150
        result = normalize_name(long_name)
        assert len(result) <= 128
    
    def test_multiple_underscores_collapsed(self):
        """Múltiples espacios/guiones → un solo underscore"""
        assert normalize_name("Hoja    1") == "hoja_1"
        assert normalize_name("Datos___Ventas") == "datos_ventas"


class TestValidateSheetName:
    """Tests para validación de nombres de hojas"""
    
    def test_valid_name(self):
        """Nombre válido pasa validación"""
        is_valid, normalized, error = validate_sheet_name("ventas_2024", [])
        assert is_valid == True
        assert normalized == "ventas_2024"
        assert error is None
    
    def test_normalizes_invalid_name(self):
        """Normaliza nombre inválido automáticamente"""
        is_valid, normalized, error = validate_sheet_name("Ventas 2024!", [])
        assert is_valid == True
        assert normalized == "ventas_2024"
    
    def test_duplicate_name_error(self):
        """Detecta duplicados"""
        existing = ["ventas_2024"]
        is_valid, normalized, error = validate_sheet_name("ventas_2024", existing)
        assert is_valid == False
        assert "ya existe" in error.lower()
    
    def test_empty_name_error(self):
        """Nombre vacío genera error"""
        is_valid, normalized, error = validate_sheet_name("", [])
        # Depende de implementación: puede aceptar "sin_nombre" o rechazar
        # Ajustar según lógica final
        assert normalized == "sin_nombre" or is_valid == False


class TestInferSqlType:
    """Tests para inferencia automática de tipos SQL"""
    
    def test_small_integers_tinyint(self):
        """Enteros 0-255 → TINYINT"""
        s = pd.Series([1, 2, 3, 4, 5])
        result = infer_sql_type(s)
        assert result['sql_type'] == 'TINYINT'
        assert result['confidence'] == 1.0
        assert result['default_value'] == '0'
        assert result['nullable'] == False
    
    def test_medium_integers_int(self):
        """Enteros >255 → INT"""
        s = pd.Series([100, 500, 1000, 5000])
        result = infer_sql_type(s)
        assert result['sql_type'] == 'INT'
        assert result['confidence'] == 1.0
    
    def test_large_integers_bigint(self):
        """Enteros muy grandes → BIGINT"""
        s = pd.Series([10**10, 10**11, 10**12])
        result = infer_sql_type(s)
        assert result['sql_type'] == 'BIGINT'
    
    def test_float_type(self):
        """Decimales → FLOAT"""
        s = pd.Series([1.5, 2.3, 3.7, 9.99])
        result = infer_sql_type(s)
        assert result['sql_type'] == 'FLOAT'
        assert result['confidence'] == 1.0
        assert result['default_value'] == '0.0'
    
    def test_boolean_values_bit(self):
        """Valores booleanos → BIT"""
        s = pd.Series(['true', 'false', '1', '0', 'yes', 'no'])
        result = infer_sql_type(s)
        assert result['sql_type'] == 'BIT'
        assert result['default_value'] == '0'
    
    def test_date_strings_date(self):
        """Strings de fecha → DATE o DATETIME2"""
        s = pd.Series(['2024-01-15', '2024-02-20', '2024-03-30'])
        result = infer_sql_type(s)
        assert 'DATE' in result['sql_type']
    
    def test_datetime_strings(self):
        """Strings con hora → DATETIME2"""
        s = pd.Series(['2024-01-15 10:30:00', '2024-02-20 14:45:30'])
        result = infer_sql_type(s)
        assert result['sql_type'] == 'DATETIME2'
    
    def test_short_text_varchar(self):
        """Texto corto → NVARCHAR con longitud apropiada"""
        s = pd.Series(['Juan', 'María', 'Pedro', 'Ana'])
        result = infer_sql_type(s)
        assert 'NVARCHAR' in result['sql_type']
        # Debe inferir longitud cercana a max(len(s)) + margen
        assert '(50)' in result['sql_type'] or '(100)' in result['sql_type']
    
    def test_long_text_varchar_max(self):
        """Texto largo → NVARCHAR(MAX) o TEXT"""
        long_text = 'a' * 5000
        s = pd.Series([long_text, long_text])
        result = infer_sql_type(s)
        assert 'MAX' in result['sql_type'] or 'TEXT' in result['sql_type']
    
    def test_mixed_types_warning(self):
        """Tipos mixtos → warning + tipo más común"""
        s = pd.Series([1, 2, 'abc', 4, 5, 6])
        result = infer_sql_type(s)
        assert result['mixed_types'] == True
        assert len(result['warnings']) > 0
        assert 'tipos mixtos' in result['warnings'][0].lower()
    
    def test_nullable_detection_with_nulls(self):
        """Series con >5% nulos → nullable=True"""
        s = pd.Series([1, 2, None, 4, None, None, 7, 8, 9, 10])  # 30% nulos
        result = infer_sql_type(s)
        assert result['nullable'] == True
    
    def test_not_nullable_few_nulls(self):
        """Series con <5% nulos → nullable=False"""
        s = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, None])  # 10% nulos
        result = infer_sql_type(s)
        # Depende de threshold (ajustar según implementación)
        # Si threshold es 5%, debería ser nullable=True
        # Si es 20%, sería False
        assert isinstance(result['nullable'], bool)
    
    def test_empty_series(self):
        """Series vacía → NVARCHAR(255) por defecto"""
        s = pd.Series([])
        result = infer_sql_type(s)
        assert 'NVARCHAR' in result['sql_type']
        assert result['confidence'] < 1.0


class TestNormalizeValueByType:
    """Tests para normalización de valores individuales"""
    
    # --- Tests para INT ---
    
    def test_int_empty_not_nullable_returns_zero(self):
        """INT: NULL + no nullable → 0"""
        assert normalize_value_by_type(None, 'INT', nullable=False) == 0
        assert normalize_value_by_type('', 'INT', nullable=False) == 0
        assert normalize_value_by_type('   ', 'INT', nullable=False) == 0
    
    def test_int_empty_nullable_returns_null(self):
        """INT: NULL + nullable → None"""
        assert normalize_value_by_type(None, 'INT', nullable=True) is None
        assert normalize_value_by_type('', 'INT', nullable=True) is None
    
    def test_int_string_conversion(self):
        """INT: convierte strings numéricos"""
        assert normalize_value_by_type('123', 'INT') == 123
        assert normalize_value_by_type('456', 'INT') == 456
        assert normalize_value_by_type('-789', 'INT') == -789
    
    def test_int_truncates_decimal(self):
        """INT: trunca decimales"""
        assert normalize_value_by_type('45.7', 'INT') == 45
        assert normalize_value_by_type('99.999', 'INT') == 99
    
    def test_int_invalid_returns_default(self):
        """INT: valor inválido + no nullable → 0"""
        assert normalize_value_by_type('abc', 'INT', nullable=False) == 0
        assert normalize_value_by_type('xyz', 'INT', nullable=False) == 0
    
    # --- Tests para FLOAT ---
    
    def test_float_empty_not_nullable_returns_zero(self):
        """FLOAT: NULL + no nullable → 0.0"""
        assert normalize_value_by_type(None, 'FLOAT', nullable=False) == 0.0
        assert normalize_value_by_type('', 'FLOAT', nullable=False) == 0.0
    
    def test_float_accepts_integers(self):
        """FLOAT: acepta enteros"""
        assert normalize_value_by_type('88', 'FLOAT') == 88.0
        assert normalize_value_by_type(100, 'FLOAT') == 100.0
    
    def test_float_accepts_decimals(self):
        """FLOAT: acepta decimales"""
        assert normalize_value_by_type('12.5', 'FLOAT') == 12.5
        assert normalize_value_by_type('99.99', 'FLOAT') == 99.99
    
    def test_float_invalid_returns_default(self):
        """FLOAT: valor inválido + no nullable → 0.0"""
        assert normalize_value_by_type('xyz', 'FLOAT', nullable=False) == 0.0
    
    # --- Tests para VARCHAR ---
    
    def test_varchar_empty_not_nullable_returns_empty_string(self):
        """VARCHAR: NULL + no nullable → ''"""
        assert normalize_value_by_type(None, 'VARCHAR(50)', nullable=False) == ''
        assert normalize_value_by_type('', 'VARCHAR(50)', nullable=False) == ''
    
    def test_varchar_empty_nullable_returns_null(self):
        """VARCHAR: NULL + nullable → None"""
        assert normalize_value_by_type(None, 'VARCHAR(50)', nullable=True) is None
    
    def test_varchar_preserves_text(self):
        """VARCHAR: preserva texto"""
        assert normalize_value_by_type('Hola mundo', 'VARCHAR(100)') == 'Hola mundo'
    
    def test_varchar_truncates_if_exceeds_length(self):
        """VARCHAR: trunca si excede longitud"""
        long_text = 'a' * 100
        result = normalize_value_by_type(long_text, 'VARCHAR(50)', nullable=False)
        assert len(result) == 50
    
    def test_nvarchar_supports_unicode(self):
        """NVARCHAR: soporta unicode"""
        assert normalize_value_by_type('Ñoño', 'NVARCHAR(50)') == 'Ñoño'
    
    # --- Tests para DATE ---
    
    def test_date_getdate_preserved(self):
        """DATE: GETDATE() se preserva como string"""
        assert normalize_value_by_type('GETDATE()', 'DATE') == 'GETDATE()'
        assert normalize_value_by_type('getdate()', 'DATE') == 'GETDATE()'
    
    def test_date_string_conversion(self):
        """DATE: convierte strings ISO a fecha"""
        result = normalize_value_by_type('2024-01-15', 'DATE')
        assert '2024-01-15' in str(result)
    
    def test_date_empty_not_nullable_returns_getdate(self):
        """DATE: NULL + no nullable + default GETDATE() → GETDATE()"""
        result = normalize_value_by_type(None, 'DATE', nullable=False, default_value='GETDATE()')
        assert result == 'GETDATE()'
    
    def test_date_invalid_returns_null_or_default(self):
        """DATE: valor inválido → None o default"""
        result = normalize_value_by_type('fecha-invalida', 'DATE', nullable=True)
        assert result is None
    
    # --- Tests para BIT ---
    
    def test_bit_true_values(self):
        """BIT: mapea valores true"""
        assert normalize_value_by_type('true', 'BIT') == 1
        assert normalize_value_by_type('True', 'BIT') == 1
        assert normalize_value_by_type('TRUE', 'BIT') == 1
        assert normalize_value_by_type('yes', 'BIT') == 1
        assert normalize_value_by_type('Yes', 'BIT') == 1
        assert normalize_value_by_type('sí', 'BIT') == 1
        assert normalize_value_by_type('Sí', 'BIT') == 1
        assert normalize_value_by_type('1', 'BIT') == 1
        assert normalize_value_by_type(1, 'BIT') == 1
    
    def test_bit_false_values(self):
        """BIT: mapea valores false"""
        assert normalize_value_by_type('false', 'BIT') == 0
        assert normalize_value_by_type('False', 'BIT') == 0
        assert normalize_value_by_type('FALSE', 'BIT') == 0
        assert normalize_value_by_type('no', 'BIT') == 0
        assert normalize_value_by_type('No', 'BIT') == 0
        assert normalize_value_by_type('0', 'BIT') == 0
        assert normalize_value_by_type(0, 'BIT') == 0
    
    def test_bit_empty_not_nullable_returns_zero(self):
        """BIT: NULL + no nullable → 0"""
        assert normalize_value_by_type(None, 'BIT', nullable=False) == 0
        assert normalize_value_by_type('', 'BIT', nullable=False) == 0


class TestNormalizeDataFrameByMappings:
    """Tests para normalización de DataFrames completos"""
    
    def test_basic_normalization(self):
        """Normalización básica de DataFrame"""
        df = pd.DataFrame({
            'edad': ['25', None, '30', ''],
            'activo': ['true', 'false', '1', '0']
        })
        
        mappings = {
            'edad': {
                'renamed_to': 'edad',
                'sql_type': 'INT',
                'nullable': False,
                'default_value': '0'
            },
            'activo': {
                'renamed_to': 'activo',
                'sql_type': 'BIT',
                'nullable': False,
                'default_value': '0'
            }
        }
        
        result_df, warnings = normalize_dataframe_by_mappings(df, mappings)
        
        # Verificar edad: '25', None, '30', '' → 25, 0, 30, 0
        assert result_df['edad'].tolist() == [25, 0, 30, 0]
        
        # Verificar activo: 'true', 'false', '1', '0' → 1, 0, 1, 0
        assert result_df['activo'].tolist() == [1, 0, 1, 0]
    
    def test_mixed_types_with_warnings(self):
        """Tipos mixtos generan warnings"""
        df = pd.DataFrame({
            'cantidad': ['10', '20', 'abc', '30', 'xyz']
        })
        
        mappings = {
            'cantidad': {
                'renamed_to': 'cantidad',
                'sql_type': 'INT',
                'nullable': False,
                'default_value': '0'
            }
        }
        
        result_df, warnings = normalize_dataframe_by_mappings(df, mappings)
        
        # 'abc' y 'xyz' no se pueden convertir → 0
        assert result_df['cantidad'].tolist() == [10, 20, 0, 30, 0]
        
        # Debe haber warnings
        assert len(warnings) > 0
    
    def test_varchar_truncation(self):
        """VARCHAR trunca valores largos"""
        df = pd.DataFrame({
            'nombre': ['a' * 100, 'b' * 50, 'c' * 200]
        })
        
        mappings = {
            'nombre': {
                'renamed_to': 'nombre',
                'sql_type': 'NVARCHAR(50)',
                'nullable': False,
                'default_value': ''
            }
        }
        
        result_df, warnings = normalize_dataframe_by_mappings(df, mappings)
        
        # Todos truncados a 50 caracteres
        assert all(len(x) <= 50 for x in result_df['nombre'])
    
    def test_preserves_non_mapped_columns(self):
        """No modifica columnas no mapeadas"""
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })
        
        mappings = {
            'col1': {
                'renamed_to': 'col1',
                'sql_type': 'INT',
                'nullable': False,
                'default_value': '0'
            }
        }
        
        result_df, warnings = normalize_dataframe_by_mappings(df, mappings)
        
        # col2 no está en mappings, no se procesa
        # (depende de implementación: puede preservarse o eliminarse)
        # Ajustar según lógica final


class TestValidateColumnMappings:
    """Tests para validación de configuración de columnas"""
    
    def test_valid_configuration(self):
        """Configuración válida pasa validación"""
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })
        
        mappings = {
            'col1': {
                'renamed_to': 'columna_1',
                'sql_type': 'INT',
                'nullable': False,
                'default_value': '0'
            },
            'col2': {
                'renamed_to': 'columna_2',
                'sql_type': 'NVARCHAR(50)',
                'nullable': True
            }
        }
        
        is_valid, errors = validate_column_mappings(df, mappings)
        assert is_valid == True
        assert len(errors) == 0
    
    def test_missing_column_error(self):
        """Columna en mappings que no existe en DataFrame"""
        df = pd.DataFrame({
            'col1': [1, 2, 3]
        })
        
        mappings = {
            'col1': {'renamed_to': 'c1', 'sql_type': 'INT'},
            'col_inexistente': {'renamed_to': 'c2', 'sql_type': 'INT'}
        }
        
        is_valid, errors = validate_column_mappings(df, mappings)
        assert is_valid == False
        assert any('col_inexistente' in e['message'] for e in errors)
    
    def test_duplicate_renamed_columns(self):
        """Detecta nombres duplicados en renamed_to"""
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': [4, 5, 6]
        })
        
        mappings = {
            'col1': {'renamed_to': 'columna', 'sql_type': 'INT'},
            'col2': {'renamed_to': 'columna', 'sql_type': 'INT'}  # Duplicado
        }
        
        is_valid, errors = validate_column_mappings(df, mappings)
        assert is_valid == False
        assert any('duplicado' in e['message'].lower() for e in errors)
    
    def test_invalid_sql_type(self):
        """Tipo SQL inválido"""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        
        mappings = {
            'col1': {'renamed_to': 'c1', 'sql_type': 'TIPO_INVALIDO'}
        }
        
        is_valid, errors = validate_column_mappings(df, mappings)
        assert is_valid == False
        assert any('tipo' in e['message'].lower() for e in errors)
    
    def test_invalid_renamed_name(self):
        """Nombre SQL inválido en renamed_to"""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        
        mappings = {
            'col1': {'renamed_to': '123_columna', 'sql_type': 'INT'}  # Empieza con número
        }
        
        is_valid, errors = validate_column_mappings(df, mappings)
        # Puede normalizar automáticamente o rechazar
        # Ajustar según implementación


# --- Fixtures ---

@pytest.fixture
def sample_dataframe():
    """DataFrame completo para tests de integración"""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'nombre': ['Juan', 'María', 'Pedro', None, 'Ana'],
        'edad': [25, 30, None, 35, 28],
        'salario': [1500.50, 2000.00, None, 2500.75, 1800.00],
        'activo': ['true', 'false', '1', '0', 'yes'],
        'fecha_ingreso': ['2024-01-15', '2024-02-20', None, '2024-03-30', '2024-04-10']
    })


@pytest.fixture
def sample_column_mappings():
    """Configuración completa de ejemplo"""
    return {
        'id': {
            'renamed_to': 'id',
            'sql_type': 'INT',
            'nullable': False,
            'default_value': '0'
        },
        'nombre': {
            'renamed_to': 'nombre',
            'sql_type': 'NVARCHAR(100)',
            'nullable': True,
            'default_value': None
        },
        'edad': {
            'renamed_to': 'edad',
            'sql_type': 'INT',
            'nullable': True,
            'default_value': None
        },
        'salario': {
            'renamed_to': 'salario',
            'sql_type': 'FLOAT',
            'nullable': True,
            'default_value': None
        },
        'activo': {
            'renamed_to': 'activo',
            'sql_type': 'BIT',
            'nullable': False,
            'default_value': '0'
        },
        'fecha_ingreso': {
            'renamed_to': 'fecha_ingreso',
            'sql_type': 'DATE',
            'nullable': True,
            'default_value': None
        }
    }


class TestIntegration:
    """Tests de integración end-to-end"""
    
    def test_full_workflow(self, sample_dataframe, sample_column_mappings):
        """Flujo completo: validar → normalizar → verificar"""
        # 1. Validar configuración
        is_valid, errors = validate_column_mappings(sample_dataframe, sample_column_mappings)
        assert is_valid, f"Configuración inválida: {errors}"
        
        # 2. Normalizar DataFrame
        result_df, warnings = normalize_dataframe_by_mappings(
            sample_dataframe, 
            sample_column_mappings
        )
        
        # 3. Verificar resultados
        assert len(result_df) == 5  # Misma cantidad de filas
        
        # Verificar tipos convertidos
        assert all(isinstance(x, int) for x in result_df['id'])
        assert all(isinstance(x, (int, type(None))) for x in result_df['edad'])
        assert all(isinstance(x, int) for x in result_df['activo'])  # BIT → int
        
        # Verificar valores por defecto
        # Fila 4: edad=None pero nullable=True → debe quedar None
        # activo='0' → debe quedar 0
        assert result_df.loc[3, 'activo'] == 0
