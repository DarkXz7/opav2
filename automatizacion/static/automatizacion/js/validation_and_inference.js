/**
 * ========================================
 * SISTEMA DE VALIDACIÓN E INFERENCIA
 * ========================================
 * Funciones para validar nombres de hojas y columnas en tiempo real,
 * e inferir tipos SQL automáticamente.
 */

// =====================================================
// 1. VALIDACIÓN DE RENOMBRADO DE HOJAS (AJAX)
// =====================================================

/**
 * Valida el renombrado de una hoja usando el endpoint AJAX
 * @param {string} originalName - Nombre original de la hoja
 * @param {string} newName - Nuevo nombre propuesto
 * @param {Array<string>} existingNames - Lista de nombres ya usados
 * @returns {Promise<object>} - {valid, normalized, error}
 */
async function validateSheetRename(originalName, newName, existingNames = []) {
    try {
        const response = await fetch('/automatizacion/api/validate-sheet-rename/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCookie('csrftoken')
            },
            body: JSON.stringify({
                original_name: originalName,
                new_name: newName,
                existing_names: existingNames
            })
        });

        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error validando nombre de hoja:', error);
        return {
            valid: false,
            normalized: '',
            error: 'Error de conexión al servidor'
        };
    }
}

/**
 * Actualiza el UI con el resultado de la validación de nombre de hoja
 * @param {HTMLInputElement} input - Input donde el usuario escribe el nombre
 * @param {object} validation - Resultado de validateSheetRename()
 */
function updateSheetRenameUI(input, validation) {
    const errorDiv = document.getElementById('rename-error');
    const successDiv = document.getElementById('rename-success');

    // Limpiar estados previos
    input.classList.remove('is-valid', 'is-invalid');
    errorDiv.classList.add('d-none');
    successDiv.classList.add('d-none');

    if (validation.valid) {
        input.classList.add('is-valid');
        successDiv.classList.remove('d-none');
        successDiv.textContent = `✅ Nombre válido: "${validation.normalized}"`;
    } else {
        input.classList.add('is-invalid');
        errorDiv.classList.remove('d-none');
        errorDiv.textContent = `❌ ${validation.error}`;
    }
}

// =====================================================
// 2. INFERENCIA DE TIPOS SQL (AJAX)
// =====================================================

/**
 * Infiere el tipo SQL para columnas de una hoja usando el endpoint AJAX
 * @param {number} sourceId - ID de la fuente de datos
 * @param {string} sheetName - Nombre de la hoja
 * @param {Array<string>} columns - Lista de columnas a inferir
 * @returns {Promise<object>} - {types: {col: {sql_type, confidence, nullable, ...}}}
 */
async function inferColumnTypes(sourceId, sheetName, columns) {
    try {
        const response = await fetch(`/automatizacion/api/excel/${sourceId}/infer-types/`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCookie('csrftoken')
            },
            body: JSON.stringify({
                sheet_name: sheetName,
                columns: columns
            })
        });

        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error infiriendo tipos de columnas:', error);
        return {
            types: {}
        };
    }
}

/**
 * Actualiza el select de tipo SQL con el tipo inferido
 * @param {string} columnName - Nombre de la columna
 * @param {object} typeInfo - Información del tipo inferido
 */
function updateTypeSelect(columnName, typeInfo) {
    const selectId = `type-${columnName.replace(/[^a-zA-Z0-9]/g, '-')}`;
    const select = document.getElementById(selectId);
    
    if (!select) return;

    // Establecer el tipo inferido
    select.value = typeInfo.sql_type || 'NVARCHAR(255)';

    // Mostrar hint de confianza
    const hintDiv = select.parentElement.querySelector('.type-hint');
    if (hintDiv) {
        const confidence = Math.round((typeInfo.confidence || 0) * 100);
        hintDiv.innerHTML = `💡 Sugerido: <strong>${typeInfo.sql_type}</strong> (${confidence}% confianza)`;
        hintDiv.classList.remove('d-none');
    }
}

// =====================================================
// 3. ACTUALIZACIÓN DE SELECCIÓN DE COLUMNAS
// =====================================================

/**
 * Actualiza el estado de los controles cuando se selecciona/deselecciona una columna
 * @param {string} sheetName - Nombre de la hoja
 * @param {string} columnName - Nombre de la columna
 * @param {number} index - Índice de la columna
 */
function updateColumnSelection(sheetName, columnName, index) {
    const checkbox = document.getElementById(`col-${sheetName.replace(/[^a-zA-Z0-9]/g, '-')}-${index}`);
    const configDiv = document.getElementById(`config-${sheetName.replace(/[^a-zA-Z0-9]/g, '-')}-${index}`);

    if (!checkbox || !configDiv) return;

    if (checkbox.checked) {
        // ✅ COLUMNA SELECCIONADA: Mostrar configuración y habilitar campos
        configDiv.style.display = 'block';

        // Habilitar todos los campos de configuración
        const renameInput = configDiv.querySelector('.rename-input');
        const typeSelect = configDiv.querySelector('.type-select');
        const nullableCheckbox = configDiv.querySelector('.nullable-checkbox');
        const defaultInput = configDiv.querySelector('.default-input');

        if (renameInput) renameInput.disabled = false;
        if (typeSelect) typeSelect.disabled = false;
        if (nullableCheckbox) nullableCheckbox.disabled = false;
        if (defaultInput) {
            // Default input depende de nullable
            defaultInput.disabled = !nullableCheckbox.checked;
        }

        // Inferir tipo automáticamente si está disponible
        const sourceId = document.getElementById('source-id')?.value;
        if (sourceId) {
            inferColumnTypes(parseInt(sourceId), sheetName, [columnName])
                .then(result => {
                    if (result.types && result.types[columnName]) {
                        updateTypeSelect(columnName, result.types[columnName]);
                        updatePlaceholderForType(defaultInput, result.types[columnName].sql_type);
                    }
                });
        }

    } else {
        // ❌ COLUMNA DESELECCIONADA: Ocultar configuración y deshabilitar campos
        configDiv.style.display = 'none';

        const renameInput = configDiv.querySelector('.rename-input');
        const typeSelect = configDiv.querySelector('.type-select');
        const nullableCheckbox = configDiv.querySelector('.nullable-checkbox');
        const defaultInput = configDiv.querySelector('.default-input');

        if (renameInput) renameInput.disabled = true;
        if (typeSelect) typeSelect.disabled = true;
        if (nullableCheckbox) nullableCheckbox.disabled = true;
        if (defaultInput) defaultInput.disabled = true;
    }

    // Actualizar contador de columnas seleccionadas
    updateSelectedColumnsCount(sheetName);
}

// =====================================================
// 4. TOGGLE VALOR POR DEFECTO (FIX BUG NULLABLE)
// =====================================================

/**
 * Habilita/deshabilita el input de valor por defecto según el estado de nullable
 * @param {HTMLInputElement} nullableCheckbox - Checkbox de "Puede ser NULL"
 * @param {HTMLInputElement} defaultInput - Input de valor por defecto
 */
function toggleDefaultInput(nullableCheckbox, defaultInput) {
    if (!defaultInput) return;

    if (nullableCheckbox.checked) {
        // ✅ Si puede ser NULL, HABILITAR input de default
        defaultInput.disabled = false;
        defaultInput.placeholder = 'NULL (dejar vacío) o valor específico';
    } else {
        // ❌ Si NO puede ser NULL, el default es OBLIGATORIO
        defaultInput.disabled = false;
        defaultInput.placeholder = 'Valor requerido';
        
        // Si está vacío, poner un valor por defecto según el tipo
        if (!defaultInput.value) {
            const sqlType = defaultInput.dataset.sqlType || 'NVARCHAR';
            updatePlaceholderForType(defaultInput, sqlType);
        }
    }
}

// =====================================================
// 5. PLACEHOLDER DINÁMICO SEGÚN TIPO SQL
// =====================================================

/**
 * Actualiza el placeholder del input de valor por defecto según el tipo SQL
 * @param {HTMLInputElement} input - Input de valor por defecto
 * @param {string} sqlType - Tipo SQL seleccionado
 */
function updatePlaceholderForType(input, sqlType) {
    if (!input) return;

    const placeholders = {
        'TINYINT': '0',
        'SMALLINT': '0',
        'INT': '0',
        'BIGINT': '0',
        'FLOAT': '0.0',
        'DECIMAL': '0.00',
        'MONEY': '0.00',
        'BIT': '0',
        'DATE': 'GETDATE()',
        'DATETIME': 'GETDATE()',
        'DATETIME2': 'GETDATE()',
        'NVARCHAR': "''",
        'VARCHAR': "''",
        'NTEXT': "''",
        'TEXT': "''"
    };

    // Buscar el tipo base (ej: NVARCHAR(50) → NVARCHAR)
    const baseType = sqlType.split('(')[0].toUpperCase();
    input.placeholder = placeholders[baseType] || "''";
    input.dataset.sqlType = baseType;
}

// =====================================================
// 6. VALIDACIÓN DE VALORES POR DEFECTO
// =====================================================

/**
 * Valida que el valor por defecto sea compatible con el tipo SQL
 * @param {string} value - Valor a validar
 * @param {string} sqlType - Tipo SQL
 * @returns {object} - {valid: boolean, error: string|null}
 */
function validateDefaultValue(value, sqlType) {
    if (!value || value.trim() === '') {
        return { valid: true, error: null }; // Vacío es válido (se interpreta como NULL si nullable=true)
    }

    const baseType = sqlType.split('(')[0].toUpperCase();

    switch (baseType) {
        case 'TINYINT':
        case 'SMALLINT':
        case 'INT':
        case 'BIGINT':
            if (!/^-?\d+$/.test(value)) {
                return { valid: false, error: 'Debe ser un número entero' };
            }
            break;

        case 'FLOAT':
        case 'DECIMAL':
        case 'MONEY':
            if (!/^-?\d+(\.\d+)?$/.test(value)) {
                return { valid: false, error: 'Debe ser un número decimal' };
            }
            break;

        case 'BIT':
            if (!['0', '1', 'true', 'false'].includes(value.toLowerCase())) {
                return { valid: false, error: 'Debe ser 0, 1, true o false' };
            }
            break;

        case 'DATE':
        case 'DATETIME':
        case 'DATETIME2':
            if (value.toUpperCase() !== 'GETDATE()' && !/^\d{4}-\d{2}-\d{2}/.test(value)) {
                return { valid: false, error: 'Debe ser GETDATE() o fecha en formato YYYY-MM-DD' };
            }
            break;
    }

    return { valid: true, error: null };
}

/**
 * Actualiza el UI del input con el resultado de la validación
 * @param {HTMLInputElement} input - Input de valor por defecto
 * @param {object} validation - Resultado de validateDefaultValue()
 */
function updateDefaultValueUI(input, validation) {
    const feedbackDiv = input.parentElement.parentElement.querySelector('.invalid-feedback');

    // Limpiar clases previas
    input.classList.remove('is-invalid', 'is-valid');
    
    if (validation.valid) {
        // ✅ Valor válido
        if (input.value && input.value.trim() !== '') {
            input.classList.add('is-valid');
        }
        if (feedbackDiv) {
            feedbackDiv.classList.add('d-none');
        }
    } else {
        // ❌ Valor inválido
        input.classList.add('is-invalid');
        if (feedbackDiv) {
            feedbackDiv.textContent = `❌ ${validation.error}`;
            feedbackDiv.classList.remove('d-none');
        }
    }
}

// =====================================================
// 7. ACTUALIZAR CONTADOR DE COLUMNAS SELECCIONADAS
// =====================================================

/**
 * Actualiza el contador de columnas seleccionadas para una hoja
 * @param {string} sheetName - Nombre de la hoja
 */
function updateSelectedColumnsCount(sheetName) {
    const checkboxes = document.querySelectorAll(`.column-selector[data-sheet="${sheetName}"]:checked`);
    const countElement = document.getElementById(`count-${sheetName.replace(/[^a-zA-Z0-9]/g, '-')}`);
    
    if (countElement) {
        countElement.textContent = checkboxes.length;
    }
}

// =====================================================
// 8. INICIALIZACIÓN AL CARGAR LA PÁGINA
// =====================================================

document.addEventListener('DOMContentLoaded', function() {
    console.log('🚀 Sistema de validación e inferencia cargado');

    // Configurar event listeners para validación de renombrado de hoja activa
    const renameInput = document.getElementById('active-sheet-rename-input');
    const validateBtn = document.getElementById('validate-rename-btn');

    if (renameInput && validateBtn) {
        validateBtn.addEventListener('click', async function() {
            const originalName = renameInput.dataset.originalName;
            const newName = renameInput.value;
            const existingNames = []; // TODO: obtener de las demás hojas

            const validation = await validateSheetRename(originalName, newName, existingNames);
            updateSheetRenameUI(renameInput, validation);
        });

        // Validación en tiempo real mientras escribe
        renameInput.addEventListener('input', debounce(async function() {
            const originalName = this.dataset.originalName;
            const newName = this.value;
            const existingNames = [];

            const validation = await validateSheetRename(originalName, newName, existingNames);
            updateSheetRenameUI(this, validation);
        }, 500));
    }

    // Configurar event listeners para inputs de valor por defecto
    document.querySelectorAll('.default-input').forEach(input => {
        input.addEventListener('blur', function() {
            const sqlType = this.dataset.sqlType || 'NVARCHAR';
            const validation = validateDefaultValue(this.value, sqlType);
            updateDefaultValueUI(this, validation);
        });
    });

    // Configurar event listeners para cambios de tipo SQL
    document.querySelectorAll('.type-select').forEach(select => {
        select.addEventListener('change', function() {
            const defaultInput = this.closest('.config-section')?.querySelector('.default-input');
            if (defaultInput) {
                updatePlaceholderForType(defaultInput, this.value);
            }
        });
    });
});

// =====================================================
// 9. UTILIDADES
// =====================================================

/**
 * Debounce para no hacer requests en cada tecla
 * @param {Function} func - Función a ejecutar
 * @param {number} wait - Milisegundos de espera
 * @returns {Function}
 */
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func.apply(this, args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

// =====================================================
// 10. FUNCIONES REQUERIDAS POR EL TEMPLATE HTML
// =====================================================

/**
 * Función llamada cuando cambia el tipo SQL de una columna
 * @param {string} sheetName - Nombre de la hoja
 * @param {string} columnName - Nombre de la columna 
 * @param {number} index - Índice de la columna
 * @param {string} sheetSlug - Nombre de la hoja slugificado
 */
function onSqlTypeChange(sheetName, columnName, index, sheetSlug) {
    const typeSelect = document.getElementById(`type-${sheetSlug}-${index}`);
    const defaultInput = document.getElementById(`default-${sheetSlug}-${index}`);
    const nullableCheckbox = document.getElementById(`nullable-${sheetSlug}-${index}`);
    
    if (!typeSelect || !defaultInput) return;
    
    const newSqlType = typeSelect.value;
    console.log(`🔄 Tipo SQL cambiado: ${columnName} → ${newSqlType}`);
    
    // 1. Actualizar placeholder según el nuevo tipo
    updatePlaceholderForType(defaultInput, newSqlType);
    
    // 2. Si no está marcado como nullable y el input está vacío, sugerir valor
    if (!nullableCheckbox?.checked && (!defaultInput.value || defaultInput.value.trim() === '')) {
        const suggestedValue = getDefaultValueForType(newSqlType);
        defaultInput.value = suggestedValue;
        console.log(`💡 Valor sugerido para ${newSqlType}: ${suggestedValue}`);
    }
    
    // 3. Validar el valor actual con el nuevo tipo
    if (defaultInput.value && defaultInput.value !== 'NULL') {
        const validation = validateDefaultValue(defaultInput.value, newSqlType);
        updateDefaultValueUI(defaultInput, validation);
        
        if (!validation.valid) {
            console.log(`⚠️ Valor "${defaultInput.value}" no es válido para ${newSqlType}: ${validation.error}`);
        }
    }
}

/**
 * Obtiene un valor por defecto apropiado según el tipo SQL
 * @param {string} sqlType - Tipo SQL
 * @returns {string} Valor por defecto sugerido
 */
function getDefaultValueForType(sqlType) {
    const upperType = sqlType.toUpperCase();
    
    if (upperType.includes('INT') || upperType.includes('DECIMAL') || upperType.includes('FLOAT') || upperType.includes('NUMERIC')) {
        return '0';
    }
    if (upperType.includes('BIT') || upperType.includes('BOOLEAN')) {
        return '0';
    }
    if (upperType.includes('DATE')) {
        return '1900-01-01';
    }
    if (upperType.includes('TIME')) {
        return '00:00:00';
    }
    // Para tipos de texto (VARCHAR, NVARCHAR, TEXT, etc.)
    return "''";
}

/**
 * Función llamada cuando cambia el estado del checkbox nullable
 * @param {string} sheetName - Nombre de la hoja
 * @param {string} columnName - Nombre de la columna
 * @param {number} index - Índice de la columna
 * @param {string} sheetSlug - Nombre de la hoja slugificado
 */
function toggleDefaultValueInput(sheetName, columnName, index, sheetSlug) {
    const nullableCheckbox = document.getElementById(`nullable-${sheetSlug}-${index}`);
    const defaultInput = document.getElementById(`default-${sheetSlug}-${index}`);
    const suggestBtn = document.getElementById(`suggest-${sheetSlug}-${index}`);
    
    if (!nullableCheckbox || !defaultInput) return;
    
    if (nullableCheckbox.checked) {
        // ✅ Si puede ser NULL, DESHABILITAR input (muestra NULL)
        defaultInput.disabled = true;
        defaultInput.value = 'NULL';
        defaultInput.placeholder = 'NULL (valor vacío)';
        if (suggestBtn) suggestBtn.disabled = true;
        
        // Limpiar validación
        defaultInput.classList.remove('is-valid', 'is-invalid');
        
    } else {
        // ❌ Si NO puede ser NULL, HABILITAR input (requiere valor)
        defaultInput.disabled = false;
        if (suggestBtn) suggestBtn.disabled = false;
        
        // Si tenía NULL, limpiarlo y poner valor por defecto
        if (defaultInput.value === 'NULL') {
            defaultInput.value = '';
        }
        
        // Actualizar placeholder según tipo SQL
        const typeSelect = document.getElementById(`type-${sheetSlug}-${index}`);
        const sqlType = typeSelect ? typeSelect.value : 'NVARCHAR(255)';
        updatePlaceholderForType(defaultInput, sqlType);
        
        // Si está vacío, poner valor sugerido
        if (!defaultInput.value || defaultInput.value.trim() === '') {
            defaultInput.value = defaultInput.placeholder;
        }
        
        // Validar el valor
        const validation = validateDefaultValue(defaultInput.value, sqlType);
        updateDefaultValueUI(defaultInput, validation);
    }
    
    console.log(`🔘 Nullable cambiado: ${columnName} → ${nullableCheckbox.checked ? 'NULL permitido' : 'Valor requerido'}`);
}

/**
 * Validación en tiempo real del valor por defecto
 * @param {string} sheetSlug - Nombre de la hoja slugificado
 * @param {number} index - Índice de la columna
 */
function validateDefaultValueReal(sheetSlug, index) {
    const defaultInput = document.getElementById(`default-${sheetSlug}-${index}`);
    const typeSelect = document.getElementById(`type-${sheetSlug}-${index}`);
    
    if (!defaultInput || !typeSelect) return;
    
    const sqlType = typeSelect.value;
    const validation = validateDefaultValue(defaultInput.value, sqlType);
    updateDefaultValueUI(defaultInput, validation);
}

/**
 * Sugiere un valor por defecto apropiado según el tipo SQL
 * @param {string} sheetSlug - Nombre de la hoja slugificado
 * @param {number} index - Índice de la columna
 * @param {string} sqlType - Tipo SQL actual
 */
function suggestDefaultValue(sheetSlug, index, sqlType) {
    const defaultInput = document.getElementById(`default-${sheetSlug}-${index}`);
    const typeSelect = document.getElementById(`type-${sheetSlug}-${index}`);
    
    if (!defaultInput) return;
    
    // Usar el tipo del select (puede haber cambiado desde el inicial)
    const currentSqlType = typeSelect ? typeSelect.value : sqlType;
    const baseType = currentSqlType.split('(')[0].toUpperCase();
    
    const suggestions = {
        'INT': '0',
        'BIGINT': '0',
        'SMALLINT': '0',
        'TINYINT': '0',
        'FLOAT': '0.0',
        'REAL': '0.0',
        'DECIMAL': '0.00',
        'MONEY': '0.00',
        'BIT': '0',
        'DATE': 'GETDATE()',
        'DATETIME': 'GETDATE()',
        'DATETIME2': 'GETDATE()',
        'NVARCHAR': "''",
        'VARCHAR': "''",
        'NTEXT': "''",
        'TEXT': "''"
    };
    
    const suggestedValue = suggestions[baseType] || "''";
    defaultInput.value = suggestedValue;
    
    // Validar el valor sugerido
    const validation = validateDefaultValue(suggestedValue, currentSqlType);
    updateDefaultValueUI(defaultInput, validation);
    
    console.log(`💡 Valor sugerido: ${baseType} → ${suggestedValue}`);
}

/**
 * Obtiene el valor de una cookie por nombre
 * @param {string} name - Nombre de la cookie
 * @returns {string|null}
 */
function getCookie(name) {
    let cookieValue = null;
    if (document.cookie && document.cookie !== '') {
        const cookies = document.cookie.split(';');
        for (let i = 0; i < cookies.length; i++) {
            const cookie = cookies[i].trim();
            if (cookie.substring(0, name.length + 1) === (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}
