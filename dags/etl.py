from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import os
import sys

# Importación del módulo personalizado
sys.path.append(os.path.join(os.path.dirname(__file__), 'modules'))
from opw_etl import OpwClient, execute_iot_merge

# Argumentos base del DAG
default_args = {
    'owner': 'Santiago_Loachamin',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='etl_basico_clima',
    default_args=default_args,
    start_date=datetime(2026, 2, 18),
    schedule_interval='@daily',
    catchup=False,
    tags=['basico', 'etl', 'weather', 'json', 'taller']
) as dag:
    # 1. EXTRACCIÓN
    @task(task_id="extraer_api")
    def extraer_api_y_explorar():
        cities = ["London", "Tokyo", "New York", "Paris", "Berlin", "Madrid", "Sydney","Loja"]
        client = OpwClient()
        raw_data = client.fetch_batch_data(cities)  # Retorna lista de dicts crudos
        print(f"\n📊 Telemetría en vivo obtenida: {len(raw_data)} registros.")
        return raw_data

    # 2. TRANSFORMACIÓN
    @task(task_id="transformar_datos")
    def transformar(raw_data):
        """Normaliza datos crudos a estructura plana.
        raw_data: lista de dicts con JSON crudo de OpenWeatherMap
        Retorna: lista de dicts normalizados
        """
        datos_normalizados = []
        
        for raw in raw_data:
            # Normalización (Flattening): Convertir JSON anidado a estructura plana
            record = {
                'sensor_id': raw.get('id'),                                    # ID único de la ciudad
                'location': raw.get('name'),                                   # Nombre de la ciudad
                'temperature': raw['main'].get('temp'),                       # Temperatura
                'humidity': raw['main'].get('humidity'),                      # Humedad
                'pressure': raw['main'].get('pressure'),                      # Presión
                'weather_condition': raw['weather'][0].get('main'),           # Condición del clima
                'extraction_timestamp': datetime.now().isoformat()            # Timestamp de extracción
            }
            datos_normalizados.append(record)
        
        print(f"\n✅ {len(datos_normalizados)} registros normalizados")
        return datos_normalizados

    # 3. CARGA (Data Lake)
    @task(task_id="cargar_parquet")
    def carga(datos_normalizados):
        """Carga datos en:
        1. JSON particionado por fecha (estructura Hive)
        2. Parquet maestro consolidado con lógica de merge (CDC)
        """
        if not datos_normalizados:
            print("No hay datos para cargar.")
            return None
        
        base_dir = '/opt/airflow/data'
        today = datetime.now().strftime("%Y-%m-%d")
        
        # ========== 1. GUARDAR JSON PARTICIONADO ==========
        json_dir = os.path.join(base_dir, 'raw')
        output_dir = os.path.join(json_dir, f"fecha={today}")
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_file = os.path.join(output_dir, f"datos_{timestamp}.json")
        
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(datos_normalizados, f, indent=2, ensure_ascii=False)
        
        print(f"✅ JSON guardado: {json_file}")
        
        # ========== 2. GUARDAR PARQUET MAESTRO CON MERGE ==========
        # Convertir a DataFrame
        client = OpwClient()

        df = pd.DataFrame(datos_normalizados)
        
        parquet_dir = os.path.join(base_dir, 'processed')
        os.makedirs(parquet_dir, exist_ok=True)
        
        parquet_file = os.path.join(parquet_dir, "clima_maestro.parquet")
        
        # Aplicar lógica de merge (CDC - Change Data Capture)
        df_master_final = execute_iot_merge(df, parquet_file)
        
        print(f"✅ Parquet maestro actualizado: {parquet_file}")
        
        return {"json": json_file, "parquet": parquet_file}

    # Instanciar el DAG - flujo de ejecución
    datos_raw = extraer_api_y_explorar()
    datos_limpios = transformar(datos_raw)
    carga(datos_limpios)

