from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import os

# Argumentos base del DAG
default_args = {
    'owner': 'Santiago_Loachamin',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}
with DAG(
    dag_id='etl_test_api',
    default_args=default_args,
    start_date=datetime(2026, 2, 18),
    schedule_interval='@daily',
    catchup=False,
    tags=['basico', 'etl', 'weather', 'json', 'taller']
) as dag:
    # 1. EXTRACCIÓN
    @task(task_id="text_api")
    def test():
        # 1. Recuperamos la clave que ingresaste
        api_key = os.getenv("TOKEN_API")

        print(f"🔑 Verificando clave: {api_key[:4]}...{api_key[-4:]} (Oculta por seguridad)")

        # 2. Hacemos una petición directa y simple
        url = f"https://api.openweathermap.org/data/2.5/weather?q=London&appid={api_key}"
        response = requests.get(url)

        print(f"📡 Estado HTTP: {response.status_code}")
        print(f"📄 Respuesta del Servidor: {response.text}")

        if response.status_code == 401:
            print("\n⚠️ DIAGNÓSTICO: Error 401 Unauthorized.")
            print("SOLUCIÓN: Tu API Key es válida pero NO ESTÁ ACTIVA todavía.")
            print("Espere 10-20 minutos y vuelva a ejecutar el notebook.")
        elif response.status_code == 200:
            print("\n✅ DIAGNÓSTICO: La clave funciona correctamente.")
            print("SOLUCIÓN: Vuelve a ejecutar la celda de extracción del Notebook.")
        else:
            print(f"\n⚠️ DIAGNÓSTICO: Error inesperado ({response.status_code}).")
            # Flujo lineal: Extraer -> Transformar -> Cargar

    datos_crudos = test()