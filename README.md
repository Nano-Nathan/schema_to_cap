# Schema to CAP - Migración de Estructura HANA a SAP CAP

Este proyecto proporciona herramientas para migrar la estructura y datos de una base de datos SAP HANA a un proyecto SAP CAP (Cloud Application Programming).

## 📋 Requisitos Previos

- Python 3.7 o superior
- Cliente SAP HANA (`hdbsql` o `hdbcli` Python)
- Proyecto CAP inicializado con `schema.cds`
- Archivo `export.tar.gz` exportado desde SAP HANA

## 📁 Estructura del Proyecto

```
proyecto/
├── cap_project/                        # Proyecto CAP (al mismo nivel que schema_to_cap)
│   └── db/
│       └── schema.cds                  # Schema CDS (se generará/actualizará)
└── schema_to_cap/                      # Este proyecto
    ├── export.tar.gz                   # Archivo exportado desde SAP HANA
    ├── data_insert_sql/                # SQL generados (se crea automáticamente)
    ├── temp_extract/                   # Archivos descomprimidos (se crea automáticamente)
    ├── logs/                           # Logs de ejecución (se crea automáticamente)
    ├── created/                        # SQL ejecutados (se crea automáticamente)
    ├── client/                         # Cliente HANA (opcional)
    ├── clone_cap_structure.py          # Script 1: Clona estructura a schema.cds
    ├── generate_sql_from_csv.py        # Script 2: Genera SQL INSERT desde CSV
    ├── execute_sql.py                  # Script 3: Ejecuta SQL en HANA
    ├── hana_config.conf                # Configuración de conexión HANA
    └── README.md
```

## 🚀 Pasos para Usar el Proyecto

### Paso 1: Preparar el Proyecto CAP

Copia tu proyecto CAP inicializado al mismo nivel que `schema_to_cap/`. El proyecto debe tener:
- Estructura básica de CAP
- Carpeta `db/` con un archivo `schema.cds` (puede estar vacío o con contenido inicial)

**Estructura esperada:**
```
proyecto/
├── cap_project/          # Tu proyecto CAP (al mismo nivel)
└── schema_to_cap/        # Este proyecto
```

**Ejemplo:**
```bash
# Si estás en el directorio padre de schema_to_cap
cp -r mi-proyecto-cap cap_project/
```

**Nota:** El nombre del proyecto CAP es configurable mediante `CAP_PROJECT_DIR` (por defecto: `cap_project`).

### Paso 2: Copiar el Archivo Export

Copia el archivo `export.tar.gz` exportado desde SAP HANA en la raíz del proyecto.

**Estructura esperada del export.tar.gz:**
```
export.tar.gz
└── index/
    └── SCHEMA_NAME/          # Nombre del schema (se auto-detecta)
        ├── TABLA1/
        │   ├── create.sql
        │   ├── data.csv
        │   ├── table.xml
        │   └── ...
        ├── TABLA2/
        │   └── ...
        └── ...
```

**Nota:** El nombre del schema se detecta automáticamente desde la estructura del `export.tar.gz`. Si necesitas especificarlo manualmente, agrega `SCHEMA=SCHEMA_NAME` en `hana_config.conf` o como variable de entorno.

**Ubicación:**
```bash
# Copiar dentro de schema_to_cap/
cp export.tar.gz schema_to_cap/
```

### Paso 3: Configurar Conexión HANA

Copia el archivo de ejemplo y configura tus credenciales:

```bash
cp hana_config.conf.example hana_config.conf
```

Edita `hana_config.conf` con tus datos de conexión:

```ini
# Host del servidor HANA
HANA_HOST=tu-host.hanacloud.ondemand.com
# Puerto (por defecto 443 para HANA Cloud)
HANA_PORT=443
# Nombre de la base de datos
HANA_DATABASE=tu_database
# Usuario
HANA_USER=TU_SCHEMA_USER
# Contraseña
HANA_PASSWORD=tu_contraseña
# Timeout en segundos (opcional, None = sin timeout)
# SQL_TIMEOUT=3600
# Nombre del schema en export.tar.gz (opcional, se auto-detecta)
# SCHEMA=SCHEMA_NAME
```

### Paso 4: Ejecutar los Scripts en Orden

#### 4.1. Clonar Estructura a schema.cds

Este script lee el `export.tar.gz`, analiza los archivos `create.sql` y `table.xml`, y genera el `schema.cds` con todas las entidades CDS.

```bash
python3 clone_cap_structure.py
```

**Qué hace:**
- Descomprime archivos necesarios del `export.tar.gz` (excepto CSV)
- Analiza `create.sql` para obtener tipos de datos y columnas
- Analiza `table.xml` para obtener claves primarias y constraints
- Genera `cap_project/db/schema.cds` con todas las entidades

**Salida:**
- `../cap_project/db/schema.cds` - Schema CDS generado (al mismo nivel que schema_to_cap)
- `../cap_project/db/schema.cds.backup` - Backup del schema anterior (si existía)

#### 4.2. Generar Archivos SQL INSERT

Este script lee los archivos `data.csv` del `export.tar.gz` y genera archivos SQL con statements INSERT.

```bash
python3 generate_sql_from_csv.py
```

**Qué hace:**
- Lee `create.sql` para obtener nombres de columnas
- Lee `data.csv` para obtener los datos
- Genera archivos `.sql` con INSERT statements en `data_insert_sql/`

**Salida:**
- `data_insert_sql/TABLA1.sql` - SQL con INSERT statements
- `data_insert_sql/TABLA2.sql`
- ...

#### 4.3. Ejecutar SQL en HANA

Este script ejecuta todos los archivos SQL generados en la base de datos HANA.

```bash
# Ejecutar todos los SQL
python3 execute_sql.py

# O ejecutar un archivo específico
python3 execute_sql.py TABLA1.sql
```

**Qué hace:**
- Lee archivos SQL de `data_insert_sql/`
- Ejecuta cada INSERT statement en HANA
- Muestra progreso en tiempo real
- Genera logs de ejecución

**Salida:**
- `logs/success.log` - Archivos ejecutados correctamente
- `logs/errors.log` - Archivos con errores
- `logs/execution.log` - Log completo
- `logs/TABLA.sql.out` - Output de cada ejecución
- `logs/TABLA.sql.err` - Errores de cada ejecución

## ⚙️ Configuración Avanzada

### Variables de Entorno

Puedes personalizar el comportamiento usando variables de entorno:

```bash
# Directorio base del proyecto (por defecto: directorio del script)
export PROJECT_BASE_DIR=/ruta/al/proyecto

# Nombre del archivo export.tar.gz (por defecto: export.tar.gz)
export EXPORT_TAR_FILE=mi_export.tar.gz

# Directorio del proyecto CAP (por defecto: cap_project)
# Debe estar al mismo nivel que schema_to_cap
export CAP_PROJECT_DIR=mi_cap_project

# Directorio de extracción temporal (por defecto: temp_extract)
export EXTRACT_DIR=temp_extract

# Directorio de salida SQL (por defecto: data_insert_sql)
export SQL_DIR=data_insert_sql

# Directorio de logs (por defecto: logs)
export LOG_DIR=logs

# Directorio de archivos ejecutados (por defecto: created)
export CREATED_DIR=created

# Timeout para ejecución SQL en segundos (por defecto: None = sin timeout)
export SQL_TIMEOUT=3600

# Ruta al cliente HANA (por defecto: client/hana_client)
export HANA_CLIENT_DIR=/ruta/al/cliente/hana
```

### Ejemplo de Uso con Variables de Entorno

```bash
export PROJECT_BASE_DIR=/home/user/mi_proyecto
export CAP_PROJECT_DIR=mi_cap
export SQL_TIMEOUT=7200  # 2 horas

python3 clone_cap_structure.py
python3 generate_sql_from_csv.py
python3 execute_sql.py
```

## 📝 Notas Importantes

### Timeout de Ejecución SQL

Por defecto, **no hay timeout** para la ejecución de SQL (útil para tablas grandes). Si necesitas un timeout, puedes configurarlo:

- En `hana_config.conf`: `SQL_TIMEOUT=3600` (1 hora)
- Como variable de entorno: `export SQL_TIMEOUT=3600`

### Estructura del export.tar.gz

El script espera que el `export.tar.gz` tenga la siguiente estructura:
- Schema en `index/SCHEMA_NAME/` (se auto-detecta automáticamente o se configura)
- Cada tabla debe tener: `create.sql`, `data.csv`, `table.xml`

### Manejo de Errores

- **Unique Constraint Violated**: Se considera éxito (datos duplicados, se omiten)
- **Timeout**: Aumenta `SQL_TIMEOUT` o déjalo en `None` para sin timeout
- **Archivos CSV vacíos**: Se omiten con advertencia

### Archivos Descomprimidos

Los archivos se descomprimen en `temp_extract/` y se reutilizan en ejecuciones posteriores. Si necesitas forzar re-descompresión, elimina la carpeta:

```bash
rm -rf temp_extract/
```

## 🔧 Solución de Problemas

### Error: "No se encontró export.tar.gz"
- Verifica que el archivo esté en la raíz del proyecto
- O configura `EXPORT_TAR_FILE` con el nombre correcto

### Error: "No se encontró schema.cds"
- Verifica que el proyecto CAP esté al mismo nivel que `schema_to_cap/`
- Verifica que `../cap_project/db/schema.cds` exista (o el nombre configurado en `CAP_PROJECT_DIR`)
- O crea un archivo vacío: `mkdir -p ../cap_project/db && touch ../cap_project/db/schema.cds`

### Error: "Timeout ejecutando hdbsql"
- Aumenta `SQL_TIMEOUT` en `hana_config.conf` o como variable de entorno
- O déjalo en `None` para sin timeout

### Error: "Connection failed"
- Verifica credenciales en `hana_config.conf`
- Verifica conectividad de red/firewall
- Para HANA Cloud, asegúrate de whitelistear tu IP

## 📚 Scripts Detallados

### clone_cap_structure.py

**Parámetros configurables:**
- `PROJECT_BASE_DIR`: Directorio base
- `EXPORT_TAR_FILE`: Nombre del archivo export
- `CAP_PROJECT_DIR`: Directorio del proyecto CAP
- `EXTRACT_DIR`: Directorio de extracción

**Funcionalidades:**
- Mapea tipos HANA a tipos CDS
- Maneja columnas IDENTITY (sin default)
- Mapea DEFAULT values correctamente
- Genera entidades CDS con claves primarias

### generate_sql_from_csv.py

**Parámetros configurables:**
- `PROJECT_BASE_DIR`: Directorio base
- `EXPORT_TAR_FILE`: Nombre del archivo export
- `SQL_DIR`: Directorio de salida SQL
- `EXTRACT_DIR`: Directorio de extracción
- `SCHEMA`: Nombre del schema (opcional, se auto-detecta)

**Funcionalidades:**
- Extrae columnas desde `create.sql`
- Genera INSERT statements desde CSV
- Escapa valores SQL correctamente
- Formato compatible con `execute_sql.py`

### execute_sql.py

**Parámetros configurables:**
- `PROJECT_BASE_DIR`: Directorio base
- `SQL_DIR`: Directorio de archivos SQL
- `LOG_DIR`: Directorio de logs
- `CREATED_DIR`: Directorio de archivos ejecutados
- `HANA_CLIENT_DIR`: Ruta al cliente HANA
- `SQL_TIMEOUT`: Timeout en segundos (None = sin timeout)

**Funcionalidades:**
- Usa `hdbsql` si está disponible (preferido)
- Fallback a `hdbcli` Python si no hay `hdbsql`
- Muestra progreso en tiempo real
- Maneja errores de constraint única
- Genera logs detallados

## 📄 Licencia

Este proyecto es de uso interno. Adapta según tus necesidades.

## 🤝 Contribuciones

Para mejorar este proyecto:
1. Haz los scripts más robustos
2. Agrega validaciones adicionales
3. Mejora el manejo de errores
4. Documenta casos especiales

---

**Última actualización:** 2026
