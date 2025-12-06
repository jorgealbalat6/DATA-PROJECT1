print("hola")from confluent_kafka import Producer
import json
import time
import os
import psycopg
from datetime import datetime, date
from decimal import Decimal

config = {
    'bootstrap.servers': 'kafka:29092'
}
productor = Producer(config)
topico = "datos_pob_riesgo"

url = os.getenv("DATABASE_URL")

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

def procesar_datos():
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
                    print(f"Nueva fecha detectada: {fecha_db}. Enviando todos los datos...")
                    cur.execute('SELECT * FROM "marts_CalidadAire" WHERE fecha = %s', (fecha_db,))
                    registros = cur.fetchall()
                    col_names = [desc[0] for desc in cur.description]
                    datos_enviados = 0
                    for row in registros:
                        data = dict(zip(col_names, row))
                        for k, v in data.items():
                            if isinstance(v, (datetime, date)):
                                data[k] = str(v)
                            elif isinstance(v, Decimal):
                                data[k] = float(v)
                        productor.produce(
                            topico, 
                            key=str(data.get('id', 'key')),
                            value=json.dumps(data), 
                            callback=estado_envio
                        )
                        datos_enviados += 1
                        productor.poll(0) 
                    if datos_enviados > 0:
                        productor.flush()
                        print(f"Se enviaron {datos_enviados} registros a Kafka.")
                    else:
                        print("No se encontraron registros para la fecha (extraño).")
                    guardar_ultima_fecha_local(fecha_db)
                else:
                    print("No hay nuevas fechas para procesar. Esperando...")

    except Exception as e:
        print(f"Error procesando datos: {e}")

if __name__ == '__main__':
    print("Iniciando productor de datos de calidad del aire (todos los datos)...")
    while True:
        procesar_datos()
        time.sleep(60)
