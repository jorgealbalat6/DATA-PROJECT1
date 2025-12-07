from urllib import response
import requests
import pandas as pd
import io
import os
import time
import datetime
import ast

URL_BAR = 'https://fgc.opendatasoft.com/api/explore/v2.1/catalog/datasets/calidad-del-aire-por-paradas0/records?limit=100'
INTERNAL_API_URL = os.getenv("API_URL", "http://api:5000") + "/ingest/calidad_aire"
def procesar_datos():
    print("Descargando datos de Barcelona...")
    try:
        resp = requests.get(URL_BAR)
        data = resp.json().get('results')
        ahora = datetime.datetime.now()
    except Exception as e:
        print(f"Error descargando: {e}")
        return

    print(f"Transformando {len(data)} registros.")
    
    lista_inserciones = []
    
    for datos in data:
        try:
            coords = {}
            if "stop_coordinates" in datos and datos["stop_coordinates"]:
                try:
                    coords = ast.literal_eval(datos["stop_coordinates"])
                except:
                    coords = {'lat': 0, 'lon': 0}
            datos_base = {
                "punto_muestreo": datos["id"],
                "municipio": 19,
                "estacion": datos['stop_name'],
                "ano": ahora.year,
                "mes": ahora.month,
                "dia": ahora.day,
                "hora": ahora.hour,
                "validacion": "V",            
                "lat": float(coords.get("lat", 0)),
                "lon": float(coords.get("lon", 0))
            }
            contaminantes = [
                ("NO2", datos.get("data_no2")),
                ("O3",  datos.get("data_o3")),
                ("PM10", datos.get("data_pm10"))
            ]

            for nombre_magnitud, valor in contaminantes:
                if valor is not None:
                    fila = datos_base.copy()
                    
                    fila["magnitud"] = nombre_magnitud
            
                    try:
                        fila["valor"] = float(valor)
                    except:
                        fila["valor"] = 0.0
                        
                    lista_inserciones.append(fila)
        except ValueError:
            continue

    if lista_inserciones:
        df = pd.DataFrame(lista_inserciones)
        mapeo_columnas = {
            'ano': 'ANO', 
            'mes': 'MES', 
            'dia': 'DIA', 
            'hora': 'HORA',
            'magnitud': 'MAGNITUD', 
            'valor': 'VALOR', 
            'validacion': 'VALIDACION',
            'punto_muestreo': 'PUNTO_MUESTREO',
            'estacion': 'ESTACION',   
            'municipio': 'MUNICIPIO',
            'lat': 'LAT',
            'lon': 'LON'
        }
        
        df = df.rename(columns=mapeo_columnas)
        
        if 'ESTACION' not in df.columns:
            df['ESTACION'] = 'Desconocida'

        df['VALIDACION'] = df['VALIDACION'].astype(str).str.upper()

        buffer = io.StringIO()

        columnas_finales = ['MUNICIPIO', 'ESTACION', 'MAGNITUD', 'PUNTO_MUESTREO', 
                            'ANO', 'MES', 'DIA', 'HORA', 'VALOR', 'VALIDACION', 'LAT', 'LON']
        
        df[columnas_finales].to_csv(buffer, index=False, header=False, encoding='utf-8')
        buffer.seek(0)

        print(f"Enviando {len(df)} filas procesadas a la API de Barcelona...")
        files = {'file': ('data.csv', buffer)}
        res = requests.post(INTERNAL_API_URL, files=files)

        if res.status_code == 201:
            print("Datos insertados correctamente de Barcelona.")
        else:
            print(f"Error API: {res.text}")

    else:
        print("No se generaron datos para insertar.")

if __name__ == "__main__":
    time.sleep(30) # Esperar a que los servicios estén activos
    print("Esperando inicio de servicios...")
    while True:
        procesar_datos()
        time.sleep(3600)