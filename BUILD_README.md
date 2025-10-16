# Smart DBF - Guía de Compilación y Uso del Ejecutable

## 📋 Requisitos Previos

- Python 3.7 o superior
- pip (gestor de paquetes de Python)

## 🔨 Cómo Compilar el Ejecutable

### Paso 1: Instalar Dependencias

```bash
pip install -r requirements.txt
```

### Paso 2: Compilar el Ejecutable

Simplemente ejecuta el script de build:

```bash
build.bat
```

El script automáticamente:
- ✅ Verifica las dependencias
- ✅ Limpia builds anteriores
- ✅ Compila el ejecutable con PyInstaller
- ✅ Copia los archivos de configuración externos

### Resultado

El ejecutable se generará en la carpeta `dist/`:
```
dist/
├── smart-dbf.exe          ← El ejecutable
├── .env                   ← Archivo de configuración (copiado automáticamente)
├── venue.json             ← Configuración de venue (copiado automáticamente)
└── Advantage.Data.Provider.dll  ← DLL (si existe)
```

## ⚙️ Configuración

### Archivo `.env`

Este archivo debe estar en el **mismo directorio** que el ejecutable.

```env
# Control flags
STOP_FLAG=0
DEBUG=0
DBF_CHUNKS_SIZE=500
SQL_ENABLED=1

# Date range configuration
CUSTOM_DATE_RANGE=1
# Format: YYYY-MM-DD
START_DATE=2025-08-01
END_DATE=0

# API endpoints
POST_API_BASE=http://217.154.198.75:3000
# POST_API_BASE=http://localhost:3000
GET_API_BASE=http://localhost:3000

# API operations endpoints
CREATE=items
UPDATE=items
DELETE=items

# Venue configuration file name
VENUE_FILE_NAME="rebsamen_venue.json"
# VENUE_FILE_NAME="araucarias_venue.json"

# Database name
DB_NAME=REBSA
# DB_NAME=ARAUC
```

**Campos importantes:**
- `STOP_FLAG`: Flag para detener el proceso (0=continuar, 1=detener)
- `DEBUG`: Modo debug (0=desactivado, 1=activado)
- `DBF_CHUNKS_SIZE`: Tamaño de chunks para procesar DBF
- `SQL_ENABLED`: Habilitar SQL (0=desactivado, 1=activado)
- `CUSTOM_DATE_RANGE`: Usar rango de fechas personalizado (0=no, 1=sí)
- `START_DATE`: Fecha de inicio (formato: YYYY-MM-DD)
- `END_DATE`: Fecha de fin (0=hasta hoy, o fecha en formato YYYY-MM-DD)
- `POST_API_BASE`: URL base para operaciones POST
- `GET_API_BASE`: URL base para operaciones GET
- `CREATE/UPDATE/DELETE`: Endpoints para operaciones
- `VENUE_FILE_NAME`: Nombre del archivo de configuración de venue
- `DB_NAME`: Nombre de la base de datos

### Archivo `venue.json`

Este archivo debe estar en el **mismo directorio** que el ejecutable.

```json
{
    "sucursal": "REBSA",
    "plaza": "XALAP",
    "testing_api_key": "O4TfvrDJNER",
    "config_endpoint": "www.test.com",
    "dll_path": "C:\\Users\\campo\\Documents\\projects\\DBF_Bridge\\Advantage.Data.Provider.dll",
    "data_source": "C:\\Users\\campo\\Documents\\projects\\data_sucursales\\rebsa",
    "sqlite": "C:\\Users\\campo\\Documents\\projects\\smart-dbf"
}
```

**Campos importantes:**
- `sucursal`: Nombre de la sucursal
- `plaza`: Nombre de la plaza
- `testing_api_key`: API key para autenticación
- `config_endpoint`: URL del endpoint de configuración
- `dll_path`: Ruta completa al archivo `Advantage.Data.Provider.dll`
- `data_source`: Ruta completa a los archivos DBF
- `sqlite`: Ruta donde se guardará la base de datos SQLite

## 🚀 Cómo Usar el Ejecutable

### Estructura de Archivos Necesaria

```
tu_carpeta/
├── smart-dbf.exe                    ← El ejecutable
├── .env                             ← Configuración (REQUERIDO)
├── venue.json                       ← Configuración de venue (REQUERIDO)
├── Advantage.Data.Provider.dll      ← DLL de Advantage (si usas DBF encriptados)
└── logs/                            ← Se crea automáticamente
    └── app.log                      ← Logs de ejecución
```

### Ejecutar

Simplemente haz doble clic en `smart-dbf.exe` o ejecútalo desde la línea de comandos:

```bash
smart-dbf.exe
```

### Tablas Procesadas

El programa procesa automáticamente estas tablas:
1. **XCORTE**
2. **CANOTA**
3. **CUNOTA**

Para cambiar las tablas, edita el archivo `src/test/test_controller.py` antes de compilar:

```python
# Línea 182-184
table_1 = "XCORTE"
table_2 = "CANOTA"
table_3 = "CUNOTA"
```

## 📝 Logs

Los logs se guardan automáticamente en:
```
logs/app.log
```

El archivo de log contiene:
- ✅ Información de procesamiento
- ✅ Errores y advertencias
- ✅ Estadísticas de registros procesados
- ✅ Operaciones enviadas a la API

## 🔧 Solución de Problemas

### Error: "No se encuentra .env"

**Solución:** Asegúrate de que el archivo `.env` está en el mismo directorio que `smart-dbf.exe`

### Error: "No se encuentra venue.json"

**Solución:** Asegúrate de que el archivo `venue.json` está en el mismo directorio que `smart-dbf.exe`

### Error: "No se puede conectar a la base de datos DBF"

**Solución:** 
1. Verifica que la ruta `data_source` en `venue.json` es correcta
2. Verifica que la ruta `dll_path` en `venue.json` apunta al archivo `Advantage.Data.Provider.dll`
3. Si los archivos DBF están encriptados, verifica que `ENCRYPTION_PASSWORD` en `.env` es correcto

### Error: "SQL_ENABLED not found"

**Solución:** Asegúrate de que tu archivo `.env` contiene la línea:
```env
SQL_ENABLED=1
```

## 📦 Distribución

Para distribuir el ejecutable a otros equipos:

1. Copia la carpeta `dist/` completa
2. Asegúrate de incluir:
   - ✅ `smart-dbf.exe`
   - ✅ `.env` (con configuración apropiada)
   - ✅ `venue.json` (con configuración apropiada)
   - ✅ `Advantage.Data.Provider.dll` (si es necesario)

3. **IMPORTANTE:** Cada usuario debe editar `.env` y `venue.json` con sus propias rutas y configuraciones

## 🔄 Recompilar Después de Cambios

Si haces cambios en el código fuente:

1. Modifica los archivos en `src/`
2. Ejecuta `build.bat` nuevamente
3. El nuevo ejecutable estará en `dist/`

## 📋 Archivos Internos (Empaquetados en el .exe)

Estos archivos están **dentro** del ejecutable y no necesitan estar externos:
- ✅ Todo el código en `src/`
- ✅ `src/utils/mappings.json`
- ✅ `src/utils/rules.json`
- ✅ `src/utils/data_tables_schemas.json`
- ✅ `src/utils/sql_identifiers.json`
- ✅ `src/utils/sql_mapping.json`

## 📋 Archivos Externos (Deben estar junto al .exe)

Estos archivos deben estar **fuera** del ejecutable:
- ⚠️ `.env` - Configuración general
- ⚠️ `venue.json` - Configuración de venue
- ⚠️ `Advantage.Data.Provider.dll` - DLL de Advantage (si es necesario)

---

## 💡 Consejos

1. **Backup:** Siempre haz backup de tus archivos `.env` y `venue.json` antes de actualizar el ejecutable
2. **Logs:** Revisa `logs/app.log` si algo no funciona como esperado
3. **Testing:** Usa `DEBUG=1` en `.env` para simular llamadas a la API sin enviar datos reales
4. **Fechas:** El formato de fecha en `.env` es `YYYY-MM-DD` (ejemplo: `2025-01-15`)
5. **END_DATE=0:** Cuando `END_DATE=0`, el sistema procesa hasta la fecha actual
6. **Múltiples venues:** Puedes tener varios archivos `*_venue.json` y cambiar entre ellos editando `VENUE_FILE_NAME` en `.env`

---

**Versión:** Smart DBF v1.0  
**Última actualización:** Octubre 2025
