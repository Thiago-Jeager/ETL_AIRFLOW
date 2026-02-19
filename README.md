# 🌦️ ETL Clima - Apache Airflow + OpenWeatherMap API

Proyecto ETL (Extract, Transform, Load) automatizado que ingesta datos de clima en tiempo real desde la **OpenWeatherMap API**, aplica transformaciones de normalización (Flattening) y persiste los datos en formato JSON y Parquet con lógica de merge incremental (CDC - Change Data Capture).

## 📋 Tabla de Contenidos
- [Características](#características)
- [Arquitectura](#arquitectura)
- [Requisitos](#requisitos)
- [Instalación](#instalación)
- [Configuración](#configuración)
- [Uso](#uso)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Flujo ETL](#flujo-etl)
- [Troubleshooting](#troubleshooting)

---

## ✨ Características

✅ **Extracción Resiliente**: Reintentos automáticos con backoff exponencial (tenacity)  
✅ **Normalización (Flattening)**: Convierte JSON anidado a estructura plana  
✅ **Persistencia Dual**: JSON particionado (auditoría) + Parquet maestro (análisis)  
✅ **CDC Incremental**: Detecta UPDATEs e INSERTs, fusiona datos sin duplicados  
✅ **Orquestación**: Apache Airflow con scheduler diario  
✅ **Dockerizado**: Stack completo (Airflow + PostgreSQL + OpenWeatherMap)  

---

## 🏗️ Arquitectura

```
┌─────────────────┐
│  OpenWeatherMap │
│      API        │
└────────┬────────┘
         │
         ↓
┌─────────────────────────────────┐
│     EXTRACCIÓN (extraer_api)    │
│  - Fetch batch de 7 ciudades    │
│  - Rate limiting ético (0.2s)   │
│  - Reintentos automáticos       │
│  └─→ Retorna: Lista dicts crudos│
└────────┬────────────────────────┘
         │
         ↓
┌─────────────────────────────────┐
│   TRANSFORMACIÓN (transformar)  │
│  - Normaliza JSON anidado       │
│  - Extrae campos principales    │
│  - Agrega timestamp             │
│  └─→ Retorna: Lista dicts plana │
└────────┬────────────────────────┘
         │
         ↓
┌─────────────────────────────────┐
│      CARGA (carga)              │
│  ┌──────────────────────┐       │
│  │ JSON Particionado    │       │
│  │ /fecha=YYYY-MM-DD/   │       │
│  └──────────────────────┘       │
│              │                   │
│  ┌──────────────────────┐       │
│  │ Parquet + CDC Merge  │       │
│  │ climatico_maestro    │       │
│  │ (Upsert atómico)     │       │
│  └──────────────────────┘       │
└─────────────────────────────────┘
```

---

## 📦 Requisitos

- Docker & Docker Compose (v20.10+)
- Git
- Clave API gratuita de [OpenWeatherMap](https://openweathermap.org/api)

---

## 🚀 Instalación

### 1. Clonar el repositorio
```bash
git clone https://github.com/Thiago-Jeager/ETL_AIRFLOW.git
cd ETL_AIRFLOW
```

### 2. Crear archivo `.env` con tus credenciales
```bash
cp .env.example .env  # Si existe, o crear manualmente
```

Contenido de `.env`:
```env
# OpenWeatherMap API
TOKEN_API=your_api_key_here

# Airflow
AIRFLOW_UID=50000
AIRFLOW_GID=50000
```

**IMPORTANTE**: Nunca commiteches `.env` al repositorio (está en `.gitignore`)

### 3. Construir e iniciar los contenedores
```bash
docker compose up -d
```

Esto inicia:
- **PostgreSQL** (base de datos Airflow): `postgres:5432`
- **Airflow Webserver** (UI): `http://localhost:8080`
- **Airflow Scheduler** (motor de tareas)

### 4. Acceder a Airflow
- URL: http://localhost:8080
- Usuario: `admin`
- Contraseña: `admin`

---

## ⚙️ Configuración

### Archivo `.env`

| Variable | Descripción | Ejemplo |
|----------|-------------|---------|
| `TOKEN_API` | Clave de OpenWeatherMap API | `sk_test_123abc...` |
| `AIRFLOW_UID` | UID del usuario en contenedor | `50000` |
| `AIRFLOW_GID` | GID del grupo en contenedor | `50000` |

### Dockerfile

```dockerfile
FROM apache/airflow:2.7.1

RUN pip install --no-cache-dir \
    requests==2.31.0 \      # HTTP requests
    pandas==2.0.3 \         # Data manipulation
    pyarrow==14.0.1 \       # Parquet I/O
    python-dotenv==1.0.0 \  # Env variables
    tenacity                # Retry logic
```

**Paquetes instalados**:
- `requests`: Llamadas HTTP a OpenWeatherMap
- `pandas`: Manipulación de DataFrames
- `pyarrow`: Lectura/escritura de Parquet
- `python-dotenv`: Cargar variables de `.env`
- `tenacity`: Reintentos automáticos con backoff

### docker-compose.yaml

**Servicios**:

#### PostgreSQL
```yaml
postgres:
  image: postgres:13
  environment:
    POSTGRES_USER: airflow
    POSTGRES_PASSWORD: airflow
    POSTGRES_DB: airflow
  volumes:
    - postgres_data:/var/lib/postgresql/data
```
Base de datos que almacena metadatos de Airflow (DAGs, ejecuciones, logs).

#### Airflow Init
```yaml
airflow-init:
  command: bash -c "airflow db init && airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin"
```
Inicializa la BD y crea usuario `admin:admin`.

#### Airflow Webserver
```yaml
airflow-webserver:
  ports:
    - "8080:8080"
  command: webserver
```
Interfaz gráfica en http://localhost:8080

#### Airflow Scheduler
```yaml
airflow-scheduler:
  command: scheduler
```
Ejecuta las tareas programadas del DAG (diariamente según `schedule_interval='@daily'`).

---

## 📊 Uso

### 1. Ver el DAG en Airflow UI
1. Ir a http://localhost:8080
2. Buscar DAG: `etl_basico_clima`
3. Estado actual, ejecuciones, logs

### 2. Ejecutar manualmente
```bash
# En Airflow UI: 
# Click en el DAG → Trigger DAG → Confirm

# O por CLI:
docker exec -it taller\ etl-airflow-scheduler-1 airflow dags trigger -d etl_basico_clima execute
```

### 3. Ver logs
```bash
docker compose logs -f airflow-scheduler
```

### 4. Acceder a los datos generados

**JSON particionado**:
```bash
ls /opt/airflow/data/raw/fecha=2026-02-19/
# Output: datos_20260219_213235.json
```

**Parquet maestro**:
```bash
ls /opt/airflow/data/processed/
# Output: clima_maestro.parquet
```

---

## 📁 Estructura del Proyecto

```
etl_airflow/
├── dags/
│   ├── etl.py                    # DAG principal (Extract → Transform → Load)
│   ├── test.py                   # DAG de prueba
│   └── modules/
│       ├── __init__.py
│       └── opw_etl.py            # Cliente OpenWeatherMap + función merge
├── logs/                         # Logs de Airflow (generado automáticamente)
├── data/
│   ├── raw/                      # JSON particionado por fecha
│   │   └── fecha=YYYY-MM-DD/
│   │       └── datos_HHMMSS.json
│   └── processed/                # Parquet maestro
│       └── clima_maestro.parquet
├── plugins/                      # Plugins Airflow (vacío por defecto)
├── .env                          # Variables de entorno (NO sincronizar)
├── .env.example                  # Plantilla de .env
├── .gitignore                    # Archivos a ignorar en git
├── Dockerfile                    # Imagen personalizada de Airflow
├── docker-compose.yaml           # Orquestación de servicios
└── README.md                     # Este archivo
```

---

## 🔄 Flujo ETL Detallado

### 1. EXTRACCIÓN (`extraer_api`)

**Clase**: `OpwClient` en [opw_etl.py](dags/modules/opw_etl.py)

```python
def fetch_batch_data(self, cities):
    """
    - Itera sobre lista de ciudades
    - Llama API OpenWeatherMap para cada una
    - Reintentos automáticos (máx 5 intentos)
    - Rate limiting: 0.2s entre llamadas
    - Retorna: lista de dicts crudos (ej: 7 ciudades)
    """
```

**Entrada**: `["London", "Tokyo", "New York", "Paris", "Berlin", "Madrid", "Sydney", "Loja"]`

**Salida**:
```python
[
  {
    "id": 2643743,
    "name": "London",
    "main": {"temp": 10.5, "humidity": 72, "pressure": 1013},
    "weather": [{"main": "Clouds"}],
    ...
  },
  ...
]
```

### 2. TRANSFORMACIÓN (`transformar`)

Normaliza JSON anidado a estructura plana:

```python
def transformar(raw_data):
    """
    Para cada registro crudo:
    - Extrae: id → sensor_id
    - Extrae: name → location
    - Aplana: main.temp → temperature
    - Aplana: main.humidity → humidity
    - Aplana: main.pressure → pressure
    - Aplana: weather[0].main → weather_condition
    - Agrega: extraction_timestamp (ISO format)
    
    Retorna: lista de dicts normalizados
    """
```

**Salida**:
```python
[
  {
    "sensor_id": 2643743,
    "location": "London",
    "temperature": 10.5,
    "humidity": 72,
    "pressure": 1013,
    "weather_condition": "Clouds",
    "extraction_timestamp": "2026-02-19T21:32:35.123456"
  },
  ...
]
```

### 3. CARGA (`carga`)

Persiste en dos formatos:

#### a) JSON Particionado (estructura Hive)
```
/opt/airflow/data/raw/fecha=2026-02-19/datos_20260219_213235.json
```
- Una carpeta por fecha (facilita limpieza/archivado)
- Archivo con timestamp (evita sobrescrituras)
- Formato: Array JSON con indentación (legible)

#### b) Parquet Maestro + CDC
```
/opt/airflow/data/processed/clima_maestro.parquet
```

**Lógica `execute_iot_merge()`**:

1. **Bootstrap** (primera ejecución):
   - Si no existe el archivo
   - Crear con datos nuevos → terminar

2. **CDC - Change Data Capture**:
   - Lee estado actual del master
   - Identifica `sensor_id` en datos nuevos vs master
   
3. **Categorización**:
   - `UPDATES`: IDs que existen en master (datos se actualizarán)
   - `INSERTS`: IDs nuevos (se agregarán)

4. **Merge**:
   - Eliminar del master los IDs a actualizar
   - Concatenar: [Master Limpio] + [UPDATES] + [INSERTS]
   - Sobrescribir archivo Parquet

5. **Observabilidad**:
   ```
   ✅ MERGE COMPLETADO
   ==================================================
   🔹 Registros actualizados (UPDATES):  7
   🔹 Registros nuevos (INSERTS):       0
   📈 Total registros en Master Final:  7
   ==================================================
   ```

---

## 🔐 Seguridad

- **Variables sensibles**: Almacenadas en `.env` (no sincronizado)
- **API Key**: Enmascarada en logs: `sk_test_123a...cdef` ✅
- **Base de datos**: Credenciales en docker-compose (cambiar en producción)
- **Airflow UI**: Protegida con usuario/contraseña

---

## 🐛 Troubleshooting

### Error: "TOKEN_API not found"
```bash
# Verificar que .env exista y tenga TOKEN_API=...
cat .env

# Reiniciar contenedores para cargar .env
docker compose down
docker compose up -d
```

### Error: "Parquet file not found" en carga
```bash
# Verificar permisos de /opt/airflow/data
docker exec airflow-scheduler ls -la /opt/airflow/data/

# Crear directorio manualmente si falta
docker exec airflow-scheduler mkdir -p /opt/airflow/data/{raw,processed}
```

### Error: "CDC merge failed - column 'sensor_id' not found"
- Verificar que transformación normalice correctamente
- Ver logs en http://localhost:8080 → DAG → Task → Logs

### Limpiar datos y reiniciar
```bash
# Parar contenedores
docker compose down

# Borrar volúmenes (BD + datos)
docker volume rm taller_etl_postgres_data

# Reiniciar
docker compose up -d
```

---

## 📈 Próximas Mejoras

- [ ] Agregar validación de calidad de datos (Great Expectations)
- [ ] Alerts en Slack/Email si falla extracción
- [ ] Dashboard en Apache Superset
- [ ] Soporte para múltiples fuentes de datos
- [ ] Airflow variable: lista de ciudades dinámicas
- [ ] Compresión de Parquet (snappy)
- [ ] Auditoría: tabla de cambios (cuándo se actualizó qué)

---

## 📝 Licencia

Este proyecto es parte del curso "Diseño de procesos ETL en Data Science" - Período 2, Maestría.

## 👤 Autor

**Santiago Loachamin**  
Período: 2026-02 | Maestría en Data Science

---

## 🤝 Contribuir

Para agregar features:
1. Fork del repositorio
2. Rama feature: `git checkout -b feature/nueva-funcionalidad`
3. Commit: `git commit -am 'Add nueva funcionalidad'`
4. Push: `git push origin feature/nueva-funcionalidad`
5. Pull Request

---

## 📞 Soporte

- **Documentación Airflow**: https://airflow.apache.org/docs/
- **OpenWeatherMap API**: https://openweathermap.org/api
- **Pandas**: https://pandas.pydata.org/docs/
