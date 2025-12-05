from confluent_kafka import Producer
import json
import time
import os
import psycopg
from sqlalchemy import create_engine, text

config = {
    'bootstrap.servers': 'kafka:9092',
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

def guardar_ultima_fecha_procesada():
    try: 
        connection = psycopg.connect(url)
        cur = connection.cursor()
        cur.execute("""SELECT MAX(fecha) FROM "marts_CalidadAire_Mad";""") 
        dato = cur.fetchone()
        if dato:
            fecha = dato[0]
            with open("ultima_fecha.txt", "w") as f:
                f.write(str(fecha))
            print(f"Fecha guardada: {fecha}")
    except Exception as e:
        print(f"Error: {e}")


def obtener_ultima_fecha_procesada():
    try:
        with open("ultima_fecha.txt", "r") as f:
            fecha = f.read()
        return fecha
    except FileNotFoundError:
        return None

if __name__ == '__main__':
    guardar_ultima_fecha_procesada()