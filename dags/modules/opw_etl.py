import os
from datetime import datetime, timedelta
import time
import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


class OpwClient:
    def __init__(self):
        self.api_key = os.getenv("TOKEN_API")
        self.base_url = "https://api.openweathermap.org/data/2.5/weather"
        print(f"🔑 Verificando clave: {self.api_key[:4]}...{self.api_key[-4:]} (Oculta por seguridad)")

    # --- DECORADOR DE RESILIENCIA ---
    # Este método "envuelve" la petición HTTP.
    # Intercepta errores de conexión y reintenta automáticamente.
    @retry(
        stop=stop_after_attempt(5),      # Máximo 5 intentos antes de rendirse
        wait=wait_exponential(multiplier=1, min=2, max=10), # Espera: 2s, 4s, 8s...
        retry=retry_if_exception_type(requests.exceptions.RequestException), # Solo reintenta errores de red
        before_sleep=lambda retry_state: print(f"⚠️ Fallo intento {retry_state.attempt_number}. Error: {retry_state.outcome.exception()}")
    )
    def _get_sensor_data(self, city_name):
        params = {
            "q": city_name,
            "appid": self.api_key,
            "units": "metric" # Sistema métrico
        }

        # Ejecución HTTP
        response = requests.get(self.base_url, params=params)

        # IMPORTANTE: requests no lanza excepción en 401/404/500 por defecto.
        # raise_for_status() convierte códigos de error HTTP en excepciones Python
        # para que 'tenacity' pueda detectarlos.
        response.raise_for_status()

        return response.json()

    def fetch_batch_data(self, cities):
        """Procesa un lote (batch) de ciudades.
        Retorna lista de dicts crudos sin procesar.
        """
        iot_data = []
        extraction_ts = datetime.now()
        print(f"📡 Iniciando ingesta de telemetría para {len(cities)} estaciones...")
        for city in cities:
            try:
                raw = self._get_sensor_data(city)
                iot_data.append(raw)  # Guardar datos CRUDOS sin normalizar
                print(f"   ✅ Dato recibido: {city}")
                # Rate Limiting manual ético (respeto a la capa gratuita)
                time.sleep(0.2)
            except Exception as e:
                print(f"   ❌ Error crítico en sensor {city}: {str(e)}")
        return iot_data  # Retornar lista de dicts, no DataFrame
    
# ============= FUNCIONES DE UTILIDAD =============
def execute_iot_merge(df_new, master_path, primary_key='sensor_id'):
    """
    Realiza un Upsert atómico sobre un archivo Parquet (CDC - Change Data Capture).
    Args:
        df_new: DataFrame con datos frescos (Staging)
        master_path: Ruta del archivo Parquet maestro
        primary_key: Campo de clave primaria para deduplicación
    Returns:
        DataFrame con estado final del master
    """
    # 1. Bootstrapping: Si no existe el master, crearlo (Full Load inicial)
    if not os.path.exists(master_path):
        print(f"🆕 Creando Data Lake maestro (primera ejecución)...")
        df_new.to_parquet(master_path, index=False)
        print(f"✅ Master creado con {len(df_new)} registros iniciales")
        return df_new
    # 2. Leer estado actual del master
    print("📖 Leyendo Data Lake existente...")
    df_master = pd.read_parquet(master_path)
    print(f"   -> Master actual: {len(df_master)} registros")
    # 3. Lógica CDC (Change Data Capture)
    master_ids = df_master[primary_key].tolist()
    # IDENTIFICAR UPDATES: Ciudades que ya teníamos, pero sus datos cambiaron
    df_updates = df_new[df_new[primary_key].isin(master_ids)]
    # IDENTIFICAR INSERTS: Ciudades nuevas
    df_inserts = df_new[~df_new[primary_key].isin(master_ids)]
    # 4. Aplicar Cambios (Merge)
    # Eliminar del Master las filas viejas que vamos a actualizar
    ids_to_update = df_updates[primary_key].tolist()
    df_master_clean = df_master[~df_master[primary_key].isin(ids_to_update)]
    # Unir las partes: (Master Limpio) + (Actualizaciones) + (Nuevos)
    df_final = pd.concat([df_master_clean, df_updates, df_inserts], ignore_index=True)
    # 5. Escritura (Sobrescribir archivo)
    df_final.to_parquet(master_path, index=False)
    # Métricas para Observabilidad
    print("\n✅ MERGE COMPLETADO")
    print("=" * 50)
    print(f"🔹 Registros actualizados (UPDATES):  {len(df_updates)}")
    print(f"🔹 Registros nuevos (INSERTS):       {len(df_inserts)}")
    print(f"📈 Total registros en Master Final:  {len(df_final)}")
    print("=" * 50)
    return df_final