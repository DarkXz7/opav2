# Configuración SQL Server - Solución Final

## Problema Resuelto

El error `The connection 'logs' doesn't exist` se ha solucionado usando el backend correcto.

## Backend Utilizado

**`django-mssql-backend`** - Backend oficial y más moderno para SQL Server con Django
- Soporta SQL Server 2012 en adelante
- Compatible con versiones recientes (v16 y posteriores)
- Mantenimiento activo

## Instalación

### 1. Crear ambiente virtual (primera vez)
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 2. Instalar dependencias
```powershell
pip install -r requirements.txt
```

### 3. Requisitos previos en Windows
- **ODBC Driver 17 for SQL Server** instalado
- **SQL Server Express** corriendo en `localhost\SQLEXPRESS`
- Bases de datos creadas: `LogsAutomatizacion` y `DestinoAutomatizacion`

## Arquitectura

- **SQLite**: Para datos internos de Django (auth, admin, etc.)
- **SQL Server**: Para datos de negocio (LogsAutomatizacion, DestinoAutomatizacion)

## Conexiones Disponibles

| Alias | Base de Datos | Engine | Uso |
|-------|---|---|---|
| `default` | db.sqlite3 | sqlite3 | Django internals |
| `logs` | LogsAutomatizacion | mssql | Logs y seguimiento |
| `destino` | DestinoAutomatizacion | mssql | Datos procesados |
| `sqlserver` | DestinoAutomatizacion | mssql | Alias adicional |

## Uso en Código

### Usar ORM con alias

```python
from automatizacion.models import ProcesoLog

# Query en SQL Server
logs = ProcesoLog.objects.using('logs').all()

# Guardar en SQL Server  
log.save(using='logs')
```

### Acceso directo a cursores

```python
from django.db import connections

with connections['destino'].cursor() as cursor:
    cursor.execute("SELECT * FROM miTabla")
    resultados = cursor.fetchall()
```

## Verificación

```powershell
# Verificar que todo funciona
python manage.py check

# Shell de Django
python manage.py shell
```

En el shell:
```python
from django.db import connections

# Verificar conexiones
conn = connections['logs']
print(conn)  # Debería mostrar la conexión activa

# Hacer query
cursor = connections['destino'].cursor()
cursor.execute("SELECT 1")
print(cursor.fetchone())  # (1,)
```

## Cambios Realizados

1. ✅ Instalado `django-mssql-backend` - Backend moderno para SQL Server
2. ✅ Actualizado `settings.py` - Configuradas las 3 conexiones SQL Server
3. ✅ Actualizado `requirements.txt` - Incluye todas las dependencias correctas
4. ✅ Testeado - Todas las conexiones funcionan correctamente

## Ventajas

- ✅ Backend moderno y mantenido activamente
- ✅ Soporta versiones recientes de SQL Server
- ✅ Compatible con ORM de Django
- ✅ Control total sobre conexiones con cursores
- ✅ Sin hacks ni soluciones temporales

## Para otros PCs

```powershell
# 1. Crear ambiente virtual
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Verificar
python manage.py check
python manage.py runserver
```

## Notas Importantes

- El código existente que usa `using('logs')` y `connections['logs']` funciona sin cambios
- Django check no reporta errores
- Las migraciones se aplican correctamente a SQLite
- SQL Server está disponible para queries con ORM y cursores

