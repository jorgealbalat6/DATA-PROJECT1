import requests
import pandas as pd
import io
import os
import time

URL_MADRID = 'https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal.json?pageSize=5000'
INTERNAL_API_URL = os.getenv("API_URL", "http://api:5000") + "/ingest/calidad_aire"

def procesar_datos():
    print("Descargando datos de Madrid...")
    try:
        resp = requests.get(URL_MADRID)
        data = resp.json().get('records')
    except Exception as e:
        print(f"Error descargando: {e}")
        return

    print(f"Transformando {len(data)} registros (Wide -> Long)...")
    
    lista_inserciones = []
    
    for datos in data:
        try:
            municipio = int(datos['MUNICIPIO'])
            estacion = int(datos['ESTACION'])
            magnitud = int(datos['MAGNITUD'])
            punto = datos['PUNTO_MUESTREO']
            ano = int(datos['ANO'])
            mes = int(datos['MES'])
            dia = int(datos['DIA'])

            for hora in range(1, 25):
                clave_valor = f"H{hora:02d}"
                clave_validacion = f"V{hora:02d}"

                if clave_valor in datos:
                    valor_hora = float(datos[clave_valor])
                    validacion_hora = datos[clave_validacion]

                    fila = {
                        'MUNICIPIO': municipio,
                        'ESTACION': estacion,
                        'MAGNITUD': magnitud,
                        'PUNTO_MUESTREO': punto,
                        'ANO': ano,
                        'MES': mes,
                        'DIA': dia,
                        'HORA': hora,
                        'VALOR': valor_hora,
                        'VALIDACION': validacion_hora
                    }
                    lista_inserciones.append(fila)
        except ValueError:
            continue

    if lista_inserciones:
        df = pd.DataFrame(lista_inserciones)

        buffer = io.StringIO()

        columnas = ['MUNICIPIO', 'ESTACION', 'MAGNITUD', 'PUNTO_MUESTREO', 
                    'ANO', 'MES', 'DIA', 'HORA', 'VALOR', 'VALIDACION']
        
        df[columnas].to_csv(buffer, index=False, header=False, encoding = 'utf-8')
        buffer.seek(0)

        print(f"Enviando {len(df)} filas procesadas a la API...")
        files = {'file': ('data.csv', buffer)}
        res = requests.post(INTERNAL_API_URL, files=files)

        if res.status_code == 201:
            print("Datos insertados correctamente.")
        else:
            print(f"Error API: {res.text}")
    else:
        print("No se generaron datos para insertar.")

if __name__ == "__main__":
    print("Esperando inicio de servicios...")
    time.sleep(1)
    procesar_datos()