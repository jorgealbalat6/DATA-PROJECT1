import requests
import pandas as pd
import io
import os
import time

# Configuración
URL_MADRID = 'https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal.json?pageSize=5000'
INTERNAL_API_URL = os.getenv("API_URL", "http://api:5000") + "/ingest/calidad_aire"

def procesar_datos():
    print("📡 Descargando datos de Madrid...")
    try:
        resp = requests.get(URL_MADRID)
        data = resp.json().get('records')
    except Exception as e:
        print(f"❌ Error descargando: {e}")
        return

    print(f"🔄 Transformando {len(data)} registros (Wide -> Long)...")
    
    lista_inserciones = []

    # --- TU LÓGICA DE TRANSFORMACIÓN (ADAPTADA) ---
    for datos in data:
        # Extraemos los valores comunes
        try:
            municipio = int(datos['MUNICIPIO'])
            estacion = int(datos['ESTACION'])
            magnitud = int(datos['MAGNITUD'])
            punto = datos['PUNTO_MUESTREO']
            ano = int(datos['ANO'])
            mes = int(datos['MES'])
            dia = int(datos['DIA'])

            # Iteramos las 24 horas (Tu bucle)
            for hora in range(1, 25):
                clave_valor = f"H{hora:02d}"
                clave_validacion = f"V{hora:02d}"

                # Solo añadimos si existen las claves en el JSON
                if clave_valor in datos:
                    valor_hora = float(datos[clave_valor])
                    validacion_hora = datos[clave_validacion]

                    # Creamos la fila (Diccionario para Pandas)
                    fila = {
                        'municipio_id': municipio,
                        'estacion_id': estacion,
                        'magnitud_id': magnitud,
                        'punto_muestreo': punto,
                        'ano': ano,
                        'mes': mes,
                        'dia': dia,
                        'hora': hora,
                        'valor': valor_hora,
                        'validacion': validacion_hora
                    }
                    lista_inserciones.append(fila)
        except ValueError:
            continue # Saltar filas con errores de datos

    # --- ENVIAR A LA API ---
    if lista_inserciones:
        df = pd.DataFrame(lista_inserciones)
        
        # Buffer en memoria
        buffer = io.StringIO()
        # El orden aquí debe coincidir con el SQL de la API
        columnas = ['municipio_id', 'estacion_id', 'magnitud_id', 'punto_muestreo', 
                    'ano', 'mes', 'dia', 'hora', 'valor', 'validacion']
        
        df[columnas].to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        print(f"🚀 Enviando {len(df)} filas procesadas a la API...")
        files = {'file': ('data.csv', buffer)}
        res = requests.post(INTERNAL_API_URL, files=files)

        if res.status_code == 201:
            print("✅ ¡ÉXITO! Datos insertados correctamente.")
        else:
            print(f"⚠️ Error API: {res.text}")
    else:
        print("⚠️ No se generaron datos para insertar.")

if __name__ == "__main__":
    print("⏳ Esperando inicio de servicios...")
    time.sleep(5)
    procesar_datos()