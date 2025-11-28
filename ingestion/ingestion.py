import requests
import pandas as pd
import io
import os
import time

# 1. Configuración de URLs
URL_MADRID_JSON = 'https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal.json?pageSize=5000'
# La URL de tu API interna (el contenedor 'api')
INTERNAL_API_URL = os.getenv("API_URL", "http://api:5000") + "/ingest/calidad_aire"

def obtener_datos_json():
    print(f"📡 Consultando API Madrid: {URL_MADRID_JSON}...")
    try:
        response = requests.get(URL_MADRID_JSON)
        response.raise_for_status()
        
        # Extraemos la lista de registros del JSON
        data = response.json().get('records')
        
        if not data:
            print("⚠️ La API de Madrid devolvió una lista vacía.")
            return None
            
        print(f"✅ Descargados {len(data)} registros en JSON.")
        return data
    except Exception as e:
        print(f"❌ Error descargando JSON: {e}")
        return None

def procesar_y_enviar(data_list):
    try:
        # 1. Convertir lista de diccionarios (JSON) a DataFrame de Pandas
        df = pd.DataFrame(data_list)

        # 2. Renombrar columnas para que coincidan con la Base de Datos
        #    (Mapeamos los nombres del JSON a los de tu tabla Postgres)
        df = df.rename(columns={
            "MUNICIPIO": "municipio_id",
            "ESTACION": "estacion_id",
            "MAGNITUD": "magnitud_id",
            "PUNTO_MUESTREO": "punto_muestreo"
            # ANO, MES, DIA, H01... ya suelen venir con el nombre correcto en el JSON
        })

        # 3. Asegurar el ORDEN de las columnas
        #    El comando COPY de Postgres es posicional, así que el orden importa mucho.
        columnas_ordenadas = [
            'municipio_id', 'estacion_id', 'magnitud_id', 'punto_muestreo', 'ANO', 'MES', 'DIA',
            'H01', 'V01', 'H02', 'V02', 'H03', 'V03', 'H04', 'V04', 'H05', 'V05', 'H06', 'V06',
            'H07', 'V07', 'H08', 'V08', 'H09', 'V09', 'H10', 'V10', 'H11', 'V11', 'H12', 'V12',
            'H13', 'V13', 'H14', 'V14', 'H15', 'V15', 'H16', 'V16', 'H17', 'V17', 'H18', 'V18',
            'H19', 'V19', 'H20', 'V20', 'H21', 'V21', 'H22', 'V22', 'H23', 'V23', 'H24', 'V24'
        ]
        
        # Filtramos y reordenamos el DataFrame. Si falta alguna columna en el JSON, esto dará error (es bueno para detectar fallos)
        df = df[columnas_ordenadas]

        # 4. Convertir a CSV en memoria (Buffer)
        buffer = io.StringIO()
        # header=False: No enviamos los nombres de columna, solo datos
        # index=False: No enviamos el índice de filas de Pandas
        df.to_csv(buffer, index=False, header=False, sep=',') 
        buffer.seek(0)

        # 5. Enviar a tu API Interna
        print(f"🚀 Enviando datos transformados a la API: {INTERNAL_API_URL}")
        files = {'file': ('realtime_json.csv', buffer)}
        
        response = requests.post(INTERNAL_API_URL, files=files)

        if response.status_code == 201:
            print("🎉 ¡ÉXITO! Datos insertados en la BD a través de la API.")
        else:
            print(f"⚠️ Error en la API Interna: {response.status_code} - {response.text}")

    except KeyError as e:
        print(f"❌ Error de formato: Falta la columna {e} en el JSON de Madrid.")
    except Exception as e:
        print(f"❌ Error procesando datos: {e}")

if __name__ == "__main__":
    # Espera inicial para asegurar que la API interna esté lista
    print("⏳ Iniciando Ingestión Tiempo Real...")
    time.sleep(5) 
    
    datos = obtener_datos_json()
    if datos:
        procesar_y_enviar(datos)