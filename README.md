# Sistema de Migración de Datos - Automatización

## Nombre y descripción del proyecto

Sistema de Migración de Datos - Automatización

Este proyecto provee una plataforma para la extracción, normalización y sincronización de datos desde múltiples orígenes (Excel, CSV y SQL Server) hacia bases de datos destino en SQL Server. Incluye una interfaz web (Django) para configurar procesos de importación, previsualizar datos, mapear columnas, aplicar reglas de transformación y guardar procesos para su ejecución y auditoría.


## Responsable del proyecto

- Nombre: Miguel Angel Galeano Castañeda (usuario principal del repositorio)
- Contacto: revisar configuración del repositorio o equipo (no incluido en el proyecto)


## Alcance y objetivos del proyecto

Objetivo general:
- Facilitar la creación y ejecución de procesos de migración de datos desde archivos y bases de datos hacia un entorno centralizado en SQL Server, con capacidades de previsualización, validación y registro de resultados.

Alcance:
- Extracción de datos desde archivos Excel y CSV, y desde servidores SQL Server.
- Normalización y limpieza de datos (valores nulos, tipos, formatos de fecha, etc.).
- Configuración de procesos reutilizables (guardados como "Process") con mapeos y reglas.
- Escritura/sincronización de los resultados en una base de datos destino (SQL Server).
- Registro de logs de ejecución en una base de datos de logs.


## Personas involucradas

- Equipo/Usuario principal: Miguel (responsable del desarrollo y pruebas)
- Usuarios finales: Analistas de datos y administradores que configuren procesos de migración
- Stakeholders: Equipo de operaciones que mantiene las instancias SQL Server y la infraestructura


## Fuentes de datos

- Archivos Excel (.xlsx) subidos a través de la interfaz
- Archivos CSV cargados a través de la interfaz
- Servidores SQL Server accesibles por ODBC (instancias como `SQLEXPRESS`)


## Orígenes y destinos de la información

Orígenes:
- Archivos locales subidos por el usuario (Excel/CSV)
- Bases de datos SQL Server (conexiones configurables)

Destinos:
- Base de datos `DestinoAutomatizacion` en SQL Server: almacena los datos migrados
- Base de datos `LogsAutomatizacion` en SQL Server: almacena los logs y auditoría
- Base de datos local de Django (SQLite) para almacenar la configuración de procesos y metadatos


## Tecnologías utilizadas

- Backend: Django 4.2 (Python)
- ORM y modelos: Django ORM
- Frontend: HTML, Bootstrap 5, JavaScript (jQuery en algunas partes)
- Conectividad SQL Server: pyodbc y ODBC Driver 17 for SQL Server
- Bases de datos: SQLite (configuración y metadatos), SQL Server (orígenes y destinos)
- Formatos de importación: Excel (.xlsx) y CSV


## Fases del proyecto

1. Diseño y planificación
2. Implementación de carga de archivos (Excel/CSV)
3. Implementación de conexión a SQL Server y listado de bases/tablas
4. Implementación de selección y mapeo de columnas
5. Implementación de previsualización y validación de datos
6. Implementación de guardado de procesos y sincronización con SQL Server
7. Implementación de logging y auditoría
8. Optimización y corrección de UI/UX
9. Pruebas e integración continua


## Agrupaciones

- Módulo `automatizacion`: Contiene vistas, modelos y utilidades para la gestión de procesos y conexiones
- Plantillas `templates/automatizacion`: Vistas HTML para la interfaz de usuario
- Utilidades `utils.py`: Conectores y funciones de auditoría/validación
- Scripts de mantenimiento: Archivos Python de corrección y migraciones de datos


## Detalle de los objetivos en cada fase

1) Diseño y planificación
- Definir requerimientos de negocio: tipos de orígenes, destinos y reglas de transformación
- Especificar la estructura de modelos (DatabaseConnection, DataSource, MigrationProcess)
- Definir rutas de despliegue y esquema de logging

2) Implementación de carga de archivos (Excel/CSV)
- Crear vistas y endpoints para subir archivos
- Extraer hojas y columnas desde Excel
- Generar previews (primeras filas) y metadatos
- Guardar archivos temporales en `TEMP_DIR`

3) Implementación de conexión a SQL Server
- Implementar `SQLServerConnector` con pyodbc
- Probar la lista de bases, tablas y columnas
- Manejar errores de driver/ODBC y autenticación

4) Implementación de selección y mapeo de columnas
- Interfaz para mapear columnas origen → destino
- Guardar mapeos en `MigrationProcess.column_mappings`
- Validaciones de tipos y reglas por columna

5) Implementación de previsualización y validación de datos
- Mostrar las primeras filas por tabla/hoja
- Detectar valores nulos, formatos de fecha, tipos inconsistentes
- Permitir aplicar transformaciones básicas desde la UI

6) Implementación de guardado de procesos y sincronización
- Crear endpoints para guardar procesos completos
- Configurar `DataTransferRouter` para direccionar operaciones a SQL Server
- Implementar escritura batch a la base destino

7) Implementación de logging y auditoría
- Guardar entradas de log en `LogsAutomatizacion`
- Registrar resultados de ejecución, errores y métricas

8) Optimización y corrección UI/UX
- Resolver problemas de desbordamiento, modales y z-index
- Mejorar layout de steps y acordeones
- Añadir feedback y mensajes al usuario

9) Pruebas e integración
- Crear pruebas unitarias y de integración básicas
- Validar flujos con archivos de prueba y bases de datos locales


---

## 📋 Requisitos previos

Antes de trabajar en este proyecto, asegúrate de tener:

### Software necesario:
- **Python 3.8+** (recomendado 3.11)
- **SQL Server Express** (o cualquier instancia de SQL Server)
- **ODBC Driver 17 for SQL Server**
- **Git** para control de versiones


## 🚀 Instalación y configuración

### 1. Clonar el repositorio
```bash
git clone [URL_DEL_REPOSITORIO]
cd proyecto_automatizacion
```

### 2. Crear y activar entorno virtual
```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/Mac
python3 -m venv venv
source venv/bin/activate
```

### 3. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 4. Configurar SQL Server

#### Instalar SQL Server Express:
1. Descargar desde Microsoft SQL Server Express
2. Instalar con autenticación mixta
3. Crear usuario `miguel` con contraseña `16474791@`

#### Crear las bases de datos:
```sql
CREATE DATABASE LogsAutomatizacion;
CREATE DATABASE DestinoAutomatizacion;
```

#### Verificar ODBC Driver:
```cmd
# En Windows, ejecutar
odbcad32.exe
# Verificar que aparezca "ODBC Driver 17 for SQL Server"
```

### 5. Configurar Django
```bash
# Ejecutar migraciones
python manage.py makemigrations
python manage.py migrate

# Crear superusuario (opcional)
python manage.py createsuperuser

# Crear directorio para archivos temporales
mkdir temp_files
mkdir media
```

### 6. Ejecutar el servidor
```bash
python manage.py runserver 8000
```

Ahora puedes acceder a:
- **Aplicación principal**: http://localhost:8000/
- **Panel de administración**: http://localhost:8000/admin/

## 🏗️ Estructura del proyecto

```
proyecto_automatizacion/
├── automatizacion/                 # Aplicación principal Django
│   ├── migrations/                # Migraciones de base de datos
│   ├── templates/                 # Templates HTML de la app
│   │   └── automatizacion/
│   ├── static/                    # Archivos estáticos (CSS, JS, imágenes)
│   ├── models.py                  # Modelos de datos
│   ├── views.py                   # Vistas y lógica de negocio
│   ├── urls.py                    # URLs de la aplicación
│   ├── utils.py                   # Utilidades (conectores, validadores)
│   ├── db_routers.py             # Router para múltiples bases de datos
│   └── templatetags/             # Tags personalizados para templates
├── proyecto_automatizacion/       # Configuración principal Django
│   ├── templates/                 # Templates globales
│   ├── settings.py               # Configuración principal
│   ├── urls.py                   # URLs principales
│   └── wsgi.py                   # WSGI para despliegue
├── temp_files/                    # Archivos temporales subidos
├── media/                         # Archivos de medios
├── db.sqlite3                     # Base de datos SQLite local
└── manage.py                      # Script principal de Django
```

## 🔗 Endpoints principales

### Navegación general:
- `GET /` - Página de inicio
- `GET /automatizacion/` - Dashboard principal
- `GET /automatizacion/process/list/` - Lista de procesos guardados

### Gestión de archivos Excel/CSV:
- `GET /automatizacion/excel/upload/` - Formulario de carga de archivos
- `POST /automatizacion/excel/upload/` - Procesar archivo subido
- `GET /automatizacion/excel/{id}/multi-config/` - Configuración multi-hoja
- `POST /automatizacion/api/validate-sheet-rename/` - Validar nombres de hojas

### Conexiones SQL Server:
- `GET /automatizacion/sql/connect/` - Formulario de conexión
- `POST /automatizacion/sql/connect/` - Crear nueva conexión
- `GET /automatizacion/sql/connections/` - Lista de conexiones
- `GET /automatizacion/sql/{id}/tables/` - Tablas disponibles

### Gestión de procesos:
- `GET /automatizacion/process/{id}/` - Ver detalles de proceso
- `GET /automatizacion/process/{id}/run/` - Ejecutar proceso
- `DELETE /automatizacion/process/{id}/delete/` - Eliminar proceso

## 🗄️ Modelos de datos principales

### DatabaseConnection
Almacena credenciales de conexión a SQL Server:
```python
# Campos principales:
- name: str                    # Nombre de la conexión
- server: str                  # Servidor (ej: localhost\SQLEXPRESS)
- username/password: str       # Credenciales
- port: int                    # Puerto (default: 1433)
- available_databases: JSON    # Bases disponibles
```

### DataSource
Representa un origen de datos (archivo o tabla):
```python
# Campos principales:
- source_type: str            # 'excel', 'csv', 'sql'
- name: str                   # Nombre del archivo/tabla
- file_path: str              # Ruta del archivo (si aplica)
- connection: FK              # Conexión SQL (si aplica)
```

### MigrationProcess
Proceso completo de migración:
```python
# Campos principales:
- name: str                   # Nombre del proceso
- source: FK                  # Origen de datos
- selected_columns: JSON      # Columnas seleccionadas por hoja/tabla
- column_mappings: JSON       # Mapeos de transformación
- status: str                 # Estado actual
- last_run: datetime          # Última ejecución
```

## ⚡ Flujo de trabajo típico

### Para archivos Excel/CSV:
1. **Cargar archivo** → `/automatizacion/excel/upload/`
2. **Seleccionar hojas/columnas** → `/automatizacion/excel/{id}/multi-config/`
3. **Configurar proceso** → Definir nombres, mapeos y reglas
4. **Guardar proceso** → Se crea `MigrationProcess`
5. **Ejecutar** → `/automatizacion/process/{id}/run/`

### Para SQL Server:
1. **Conectar a servidor** → `/automatizacion/sql/connect/`
2. **Seleccionar base de datos** → Lista de bases disponibles
3. **Seleccionar tablas** → `/automatizacion/sql/{id}/tables/`
4. **Configurar columnas** → Mapeos y transformaciones
5. **Guardar y ejecutar proceso**

## 🔧 Configuración avanzada

### Variables de entorno recomendadas:
```bash
# .env (crear este archivo)
DEBUG=True
SECRET_KEY=tu-clave-secreta-aqui
DB_NAME=DestinoAutomatizacion
DB_USER=miguel
DB_PASSWORD=16474791@
DB_HOST=localhost\SQLEXPRESS
```



## 🚨 Troubleshooting común

### Error: "No se pudo conectar al servidor"
**Causa**: Problemas con SQL Server o ODBC
**Solución**:
1. Verificar que SQL Server Express esté ejecutándose:
   ```cmd
   services.msc → SQL Server (SQLEXPRESS) → Iniciar
   ```
2. Verificar ODBC Driver:
   ```cmd
   odbcad32.exe → Controladores → Buscar "SQL Server"
   ```
3. Verificar credenciales en `settings.py`

### Error: "Module not found"
**Causa**: Dependencias no instaladas
**Solución**:
```bash
pip install -r requirements.txt
# Si el problema persiste:
pip install --upgrade pip
pip install --force-reinstall -r requirements.txt
```

### Error: "Port already in use"
**Causa**: Puerto 8000 ocupado
**Solución**:
```bash
# Usar puerto diferente:
python manage.py runserver 8001

# O matar proceso existente:
# Windows:
netstat -ano | findstr :8000
taskkill /PID [PID_NUMBER] /F
```

### Problemas de UI (acordeones, modales)
**Causa**: Conflictos CSS/JavaScript
**Solución**: Revisar console del navegador (F12) y verificar que se cargan todos los archivos estáticos.

### Archivos no se suben
**Causa**: Permisos de directorio
**Solución**:
```bash
# Verificar que existen los directorios:
mkdir temp_files
mkdir media
# Verificar permisos de escritura
```

## 📈 Performance y optimización

### Archivos grandes:
- Los archivos Excel >10MB pueden tardar en procesarse
- Considerar implementar procesamiento en background con Celery
- Limitar tamaño de archivos en `settings.py`:
  ```python
  FILE_UPLOAD_MAX_MEMORY_SIZE = 10485760  # 10MB
  ```

### Conexiones SQL Server:
- Usar connection pooling para múltiples consultas
- Implementar timeout en consultas largas
- Monitorear memoria con archivos con muchas columnas

## 🔄 Scripts de utilidad

El proyecto incluye varios scripts de mantenimiento:
- `clean_duplicate_connections.py` - Limpiar conexiones duplicadas
- `debug_*.py` - Scripts de debugging y diagnóstico
- `fix_*.py` - Scripts de corrección de datos

## 📝 Testing

### Ejecutar tests:
```bash
python manage.py test
```

### Tests manuales recomendados:
1. Cargar archivo Excel con múltiples hojas
2. Conectar a SQL Server local
3. Crear proceso completo y ejecutarlo
4. Verificar logs en base de datos

## 🚀 Despliegue

### Para desarrollo:
```bash
python manage.py runserver 0.0.0.0:8000
```

### Para producción (con gunicorn):
```bash
pip install gunicorn
gunicorn proyecto_automatizacion.wsgi:application --bind 0.0.0.0:8000
```

## 🆘 Soporte y contacto

- **Desarrollador original**: Miguel Angel Galeano Castañeda
- **Documentación adicional**: Revisar archivos `*.md` en el repositorio


---

