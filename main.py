import requests
import pandas as pd
import csv
import json
from collections import defaultdict

class WeatherAPI:
    # Se define la url de destino 
    BASE_URL = "https://api.cabinpaq.xyz"  
    
    def __init__(self, auth_token = "rqypux9snyqljtwt4qozr1nhes1efrztpgk6k4qe92zsmyocgy37batrj3s3zry7n433kvdbk7s34ts43m9c334lkuugc2v2bk8g"):
        # Se inicializa la API con el token de autenticación que se ha proporcionado.
        self.auth_token = auth_token
        self.headers = {"Authorization": f"Bearer {self.auth_token}"} if auth_token else {}

    def get_weather_status(self, dci_id):
        """Se obtiene el estado meteorológico dado un DCI ID."""
        url = f"{self.BASE_URL}/weather/status_by_dci_id_new"
        params = {"dci_id": dci_id}
        
        response = requests.get(url, headers=self.headers, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"Error {response.status_code}: {response.text}"}

    def get_weather_history(self, dci_id, data_type, start_time, end_time):
        """Se obtiene el historial meteorológico de un DCI entre dos fechas."""
        url = f"{self.BASE_URL}/weather/history"
        params = {
            "dci_id": dci_id,
            "data_type": data_type,
            "start_time": start_time,
            "end_time": end_time
        }
        
        response = requests.get(url, headers=self.headers, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"Error {response.status_code}: {response.text}"}
    
    def save_all_weather_data(self, dci_ids, data_types, filename="todos_los_valores.json"):
        """
        Genera todos los datos históricos para los DCI dados y retorna una 
        lista de diccionarios con toda la información (ya NO escribe JSON en disco).
        """
        data_by_timestamp = defaultdict(lambda: defaultdict(dict))  

        for dci_id in dci_ids:
            print(f"Procesando datos para DCI {dci_id}...")
            for data_type in data_types:
                history = self.get_weather_history(dci_id, data_type, "2000-01-01", "2030-01-01")

                if history and "weather_history" in history:
                    for timestamp, value in history["weather_history"].items():
                        fecha, hora = timestamp.split(" ")  
                        if "dci_id" not in data_by_timestamp[timestamp][dci_id]:
                            data_by_timestamp[timestamp][dci_id]["dci_id"] = dci_id
                            data_by_timestamp[timestamp][dci_id]["fecha"] = fecha
                            data_by_timestamp[timestamp][dci_id]["hora"] = hora
                        
                        # Agregar el valor correspondiente al data_type
                        data_by_timestamp[timestamp][dci_id][data_type] = value

        # Convertir la estructura en una lista de diccionarios
        structured_data = []
        for timestamp in sorted(data_by_timestamp.keys()):
            for dci_id in data_by_timestamp[timestamp]:
                structured_data.append(data_by_timestamp[timestamp][dci_id])

        # En lugar de guardar en JSON, simplemente retornamos los datos
        return structured_data

    def list_dci_reduced(self):
        """Obtiene un listado reducido de todos los DCI (requiere autenticación)."""
        if not self.auth_token:
            return {"error": "Se requiere un token de autenticación."}
        
        url = f"{self.BASE_URL}/dci/list_reduced"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"Error {response.status_code}: {response.text}"}
        
    def save_dci_list_to_json(api, filename="dci_list.json"):
        """
        Obtiene la lista de DCIs desde la API y la guarda en un archivo JSON.
        
        :param api: Instancia de la clase WeatherAPI.
        :param filename: Nombre del archivo donde se guardarán los datos.
        """
        dci_data = api.list_dci_reduced()

        if "dci_options_list" in dci_data:
            with open(filename, "w", encoding="utf-8") as file:
                json.dump(dci_data["dci_options_list"], file, indent=4, ensure_ascii=False)
            print(f"Datos guardados correctamente en {filename}")
        else:
            print("Error al obtener los datos:", dci_data)


# Ejemplo de uso
if __name__ == "__main__":
    # Instancia de la clase con tu token
    api = WeatherAPI(auth_token="rqypux9snyqljtwt4qozr1nhes1efrztpgk6k4qe92zsmyocgy37batrj3s3zry7n433kvdbk7s34ts43m9c334lkuugc2v2bk8g")  # Reemplaza con tu token real si fuera distinto

    # Lista de DCI IDs GALAPAGAR
    
    dci_ids = [2022927251, 1761212833, 397884123, 249312727, 607730158, 1826254598, 795229630, 433355383, 1019938642, 1812543069, 527602687, 5948360]

    # Tipos de datos a recopilar
    data_types = [
        "temperature", "humidity", "atm_pressure", "noise", "uv_index",
        "wind_direction", "wind_speed", "wind_strength", "rainfall",
        "co", "no2", "co2", "o3", "ch2o", "pm1_particles",
        "pm2p5_particles", "pm10_particles"
    ]

    # 1) Obtener los datos en memoria (ya no se genera "todos_los_valores.json")
    all_data = api.save_all_weather_data(dci_ids, data_types)

    # 2) Conectar con la misma API para obtener la lista de DCIs (duplicado abajo, pero lo mantenemos)
    API_URL = "https://api.cabinpaq.xyz/dci/list_reduced"
    TOKEN = "rqypux9snyqljtwt4qozr1nhes1efrztpgk6k4qe92zsmyocgy37batrj3s3zry7n433kvdbk7s34ts43m9c334lkuugc2v2bk8g"

    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }

    response = requests.get(API_URL, headers=headers)

    if response.status_code == 200:
        data = response.json()
        print("Lista de DCI:", data)
    else:
        print("Error:", response.status_code, response.text)

    # 3) Metemos la lista de estaciones en un DF (df_dci)
    if response.status_code == 200:
        data = response.json()["dci_options_list"]
        df_dci = pd.DataFrame(data)
        print(df_dci)
    else:
        print("Error:", response.status_code, response.text)
        df_dci = pd.DataFrame()

    # 4) Convertir la data principal a DataFrame (en vez de leer "todos_los_valores.json")
    df = pd.DataFrame(all_data)
    print("Primeras filas de los datos generados en memoria:")
    print(df.head())

    # 5) Enriquecer el df con los campos de la lista dci
    if not df.empty and not df_dci.empty and "dci_id" in df.columns and "dci_id" in df_dci.columns:
        df_enriched = pd.merge(
            df,
            df_dci[['dci_id', 'name', 'dci_ip', 'postal_code', 'latitude', 'longitude']],
            on='dci_id',
            how='left'
        )
    else:
        df_enriched = df  # Si no fue posible enriquecer (por columnas ausentes)

    print("Primeras filas del DataFrame enriquecido:")
    print(df_enriched.head())

    # 6) Guardar el DataFrame enriquecido a CSV
    df_enriched.to_csv("df_enriched.csv", index=False)
    print("Archivo CSV final generado: df_enriched.csv")
