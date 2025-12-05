from confluent_kafka import Producer
import json
import time
import os
import psycopg
from datetime import datetime, date
from decimal import Decimal

config = {
    'bootstrap.servers': 'kafka:29092',
}
productor = Producer(config)
topico = "alertas_pob_general"

url = os.getenv("DATABASE_URL")

LIMITES = {
    "Dióxido de Nitrógeno (NO2)": 400,
    "Partículas < 2.5 µm (PM2.5)": 50,
    "Partículas < 10 µm (PM10)": 80,
    "Ozono (O3)": 240,
    "Dióxido de Azufre (SO2)": 500
}

def obtener_ultima_fecha_local():
    if not os.path.exists("ultima_fecha.txt"):
        return None
    try:
        with open("ultima_fecha.txt", "r") as f:
            fecha = f.read().strip()
            if not fecha:
                return None
            return fecha
    except Exception as e:
        print(f"Error leyendo archivo local: {e}")
        return None

def guardar_ultima_fecha_local(fecha):
    try:
        with open("ultima_fecha.txt", "w") as f:
            f.write(str(fecha))
        print(f"Fecha actualizada en ultima_fecha.txt: {fecha}")
    except Exception as e:
        print(f"Error escribiendo archivo local: {e}")

def estado_envio(e, mensaje):
    if e is not None:
        print(f'Error al enviar mensaje: {e}')
    else:
        print(f'Mensaje enviado a {mensaje.topic()} [{mensaje.partition()}]')

def procesar_alertas():
    try:
        if not url:
            print("Error: DATABASE_URL no definida.")
            return
        with psycopg.connect(url) as connection:
            with connection.cursor() as cur:
                cur.execute('SELECT MAX(fecha) FROM "marts_CalidadAire" LIMIT 1')
                res = cur.fetchone()
                if not res or res[0] is None:
                    print("No se encontraron fechas en la base de datos.")
                    return
                fecha_db = res[0]
                print(f"Última fecha en DB: {fecha_db}")
                fecha_local = obtener_ultima_fecha_local()
                print(f"Última fecha local: {fecha_local}")
                if fecha_local is None or str(fecha_db) > fecha_local:
                    print(f"Nueva fecha detectada: {fecha_db}. Procesando alertas...")
                    cur.execute('SELECT * FROM "marts_CalidadAire" WHERE fecha = %s', (fecha_db,))
                    registros = cur.fetchall()
                    col_names = [desc[0] for desc in cur.description]
                    alertas_enviadas = 0
                    for fila in registros:
                        data = dict(zip(col_names, fila))
                        magnitud = data.get('magnitud')
                        valor = data.get('valor')
                        if magnitud in LIMITES and valor is not None:
                            limite = LIMITES[magnitud]
                            if valor > limite:
                                if isinstance(valor, Decimal):
                                    valor = float(valor)
                                alerta = {
                                    "fecha": str(fecha_db),
                                    "estacion": data.get('estacion'),
                                    "municipio": data.get('municipio'),
                                    "magnitud": magnitud,
                                    "valor": valor,
                                    "limite": limite,
                                    "mensaje": f"Valor de {magnitud} ({valor}) excede el límite de {limite}"
                                }
                                print(f"Generando alerta: {alerta}")
                                productor.produce(
                                    topico, 
                                    key=str(data.get('id', 'key')),
                                    value=json.dumps(alerta), 
                                    callback=estado_envio
                                )
                                alertas_enviadas += 1
                    
                    if alertas_enviadas > 0:
                        productor.flush()
                        print(f"Se enviaron {alertas_enviadas} alertas.")
                    else:
                        print("No se detectaron valores fuera de límite.")
                    
                    guardar_ultima_fecha_local(fecha_db)
                    
                else:
                    print("No hay nuevas fechas para procesar. Esperando...")

    except Exception as e:
        print(f"Error procesando alertas: {e}")

if __name__ == '__main__':
    print("Iniciando servicio de alertas de calidad del aire...")
    while True:
        procesar_alertas()
        time.sleep(60)

