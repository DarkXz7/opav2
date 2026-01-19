# 📊 Integración Local vs OneDrive - Guía de Uso

## ✨ ¿Qué es esto?

Ahora el sistema soporta dos formas de usar archivos Excel:

1. **Local (Tradicional)**: Subes el archivo desde tu PC al servidor Django
2. **OneDrive (Nuevo)**: Proporcionas la URL compartida de tu archivo en OneDrive

## 🎯 Ventajas de cada método

### 📁 Método Local
```
✅ Funciona offline
✅ Más rápido para archivos pequeños
❌ Necesitas volver a subir el archivo si lo cambias
```

### ☁️ Método OneDrive
```
✅ Siempre obtiene la versión más reciente
✅ No necesitas subir de nuevo si editas el archivo
✅ Perfecto para archivos compartidos en equipo
✅ El botón "Cargar Columnas" SIEMPRE ve los cambios
❌ Requiere conexión a internet
```

---

## 🚀 Cómo usar cada método

### 📁 Cargar desde tu PC (Local)

**Paso 1: Ir a nuevo proceso**
```
1. Haz clic en "Nuevo Proceso"
2. Selecciona "Excel" o "CSV"
3. Haz clic en "Cargar Archivo"
```

**Paso 2: Seleccionar tu archivo**
```
1. La opción "Desde mi PC" está seleccionada por defecto
2. Haz clic en "Seleccionar Archivo" o arrastra el archivo
3. El archivo se subirá al servidor
```

**Paso 3: Continuar normalmente**
```
1. Selecciona hojas/columnas
2. Configura mapeos
3. Guarda el proceso
```

---

### ☁️ Cargar desde OneDrive

**Paso 1: Compartir tu archivo en OneDrive**
```
1. Abre OneDrive en tu navegador
2. Busca el archivo Excel que quieres usar
3. Haz clic derecho → "Compartir"
4. Copia el enlace de compartición
```

**Paso 2: En el sistema Django**
```
1. Haz clic en "Nuevo Proceso"
2. Selecciona "Excel"
3. Haz clic en "Cargar Archivo"
```

**Paso 3: Seleccionar "Desde OneDrive"**
```
1. Haz clic en el botón "☁️ Desde OneDrive"
2. Se mostrarán dos campos:
   - URL de Compartición: Pega el enlace que copiaste
   - Nombre del Archivo: Nombre descriptivo (ej: datos_ventas.xlsx)
```

**Paso 4: Verificación**
```
1. El sistema valida que la URL sea accesible
2. Si todo está bien, continúa al siguiente paso
3. Si hay error, revisa que la URL sea correcta
```

**Paso 5: Continuar normalmente**
```
1. Selecciona hojas/columnas
2. Configura mapeos
3. Guarda el proceso
```

---

## 🔄 El botón "Cargar Columnas" ahora es más potente

### 📁 Con archivos Local
```
El botón SIEMPRE relee el archivo del servidor
- Si alguien lo editó en el servidor → Lo ve
- Si solo lo editaste en tu PC → NO lo ve
```

### ☁️ Con archivos OneDrive
```
El botón SIEMPRE descarga la versión actual de OneDrive
✅ Si alguien editó el archivo en OneDrive → Lo ve automáticamente
✅ Si agregaron columnas nuevas → Las ve
✅ Si eliminaron columnas → Las detecta
```

**Ejemplo práctico:**
```
1. Creas un proceso con OneDrive
2. Alguien edita el archivo en OneDrive (agrega 2 columnas)
3. Tú entras a editar el proceso
4. Haces clic en "Cargar Columnas"
5. El sistema descarga la versión actual y muestra LAS 2 COLUMNAS NUEVAS ✅
```

---

## ⚙️ Configuración técnica

### Para administradores

**1. Paquetes instalados:**
```
✅ msgraph-core - Para conectar con Microsoft Graph
✅ azure-identity - Para autenticación
✅ requests - Para descargas HTTP
```

**2. Clases principales:**

**`OneDriveService` (onedrive_service.py)**
```python
- download_file_from_url(url)
- validate_share_url(url)
- get_file_metadata(item_id)
```

**`ExcelProcessor` (legacy_utils.py) - ACTUALIZADA**
```python
# Ahora soporta:
- Archivos locales (como antes)
- Archivos desde OneDrive (NUEVO)

processor = ExcelProcessor(
    file_path=file_path,
    source=source_object  # Detecta si es local o cloud
)
```

### Cambios en el modelo

**DataSource (models.py) - Nuevos campos:**
```python
storage_type = 'local' | 'onedrive'  # Tipo de almacenamiento
onedrive_url = "URL compartida"       # URL del archivo
onedrive_item_id = "ID del item"      # Identificador único
```

---

## 🛡️ Seguridad y Privacidad

✅ **No se almacenan credenciales**
- Se usa URL de compartición, no autenticación OAuth2

✅ **El archivo se descarga temporalmente**
- Se procesa en memoria
- No se guarda permanentemente en el servidor

✅ **URL debe ser compartida**
- OneDrive valida el acceso antes de descargar

⚠️ **Consideraciones:**
- La URL debe estar compartida públicamente O ser de tu OneDrive
- Si revokes la compartición, el sistema no podrá acceder al archivo

---

## 📋 Casos de uso recomendados

### Usa Local para:
```
✅ Archivos que NO cambiarán
✅ Datos históricos/archivados
✅ Procesamiento único
✅ Cuando NO tienes OneDrive
```

### Usa OneDrive para:
```
✅ Archivos que se actualizen frecuentemente
✅ Datos en tiempo real (ej: ventas del día)
✅ Archivos compartidos en equipo
✅ Cuando necesitas "refrescar" automáticamente
✅ Integración con Microsoft 365
```

---

## 🐛 Solución de problemas

### "Error: No se puede acceder a la URL de OneDrive"

**Causas:**
- URL no es correcta
- El archivo no está compartido
- La compartición venció

**Solución:**
```
1. Ve a OneDrive
2. Busca el archivo
3. Haz clic derecho → Compartir
4. Copia el enlace NUEVAMENTE
5. Intenta de nuevo
```

### "Las columnas no se ven al hacer clic en 'Cargar Columnas'"

**Para Local:**
```
- Verifica que el archivo local esté en el servidor
- Si lo cambiaste en tu PC, sube la versión nueva
```

**Para OneDrive:**
```
- Verifica que la URL siga siendo válida
- Si la compartición expiró, comparte nuevamente
- Intenta otro navegador
```

### "El proceso se ve lento con OneDrive"

**Normal.** El sistema está:
1. Descargando el archivo de OneDrive
2. Parseándolo
3. Extrayendo columnas

Para archivos grandes (>10MB), puede tardar unos segundos.

---

## 📞 Próximos pasos (Futuro)

- [ ] Autenticación OAuth2 para OneDrive empresarial
- [ ] Soporte para Google Drive
- [ ] Soporte para Dropbox
- [ ] Caché automático de metadatos
- [ ] Sincronización programada

---

**¿Preguntas?** Revisa la sección de [Desarrollo](#) en el README principal.
