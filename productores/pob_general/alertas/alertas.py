from confluent_kafka import Producer
import json
import time
import os
import psycopg


config = {
    'bootstrap.servers': 'kafka:9092',
}
producer = Producer(config)
topic = "alertas_pob_general"

DB_URL = os.getenv("DATABASE_URL")

LIMITES = {
    "Dióxido de Nitrógeno (NO2)": 400,
    "Partículas < 2.5 µm (PM2.5)": 50,
    "Partículas < 10 µm (PM10)": 80,
    "Ozono (O3)": 240,
    "Dióxido de Azufre (SO2)": 500
}


while True:
    try:
        conn = psycopg.connect(DB_URL)
        cur = conn.cursor()
        query = """
            SELECT municipio, estacion, indicador_contaminante, fecha_hora, valor 
            FROM "int_CalidadAire_Mad"
            WHERE fecha_hora > NOW() - INTERVAL '1 hour'
            AND datos_disponibles = true
        """
        cur.execute(query)
        datos = cur.fetchall()
        for fila in datos: 
            municipio = fila[0]   
            estacion = fila[1]    
            contaminante = fila[2]
            fecha = str(fila[3])  
            valor = fila[4]   
            if valor is None: continue
            if contaminante in LIMITES and valor > LIMITES[contaminante]:
                texto_alerta = (f"Alerta de contaminación en {municipio} "
                                f"({estacion}) el {fecha}: "
                                f"{contaminante} = {valor} µg/m³, "
                                f"superando el límite de {LIMITES[contaminante]} µg/m³.")
                mensaje = {
                    "municipio": municipio,
                    "estacion": estacion,
                    "contaminante": contaminante,
                    "fecha": fecha,
                    "valor": valor,
                    "texto_alerta": texto_alerta
                }
                producer.produce(
                    topic,
                    value=json.dumps(mensaje).encode("utf-8")
                )
                print("Alerta enviada:", texto_alerta)
        
        producer.flush()
        conn.close()
    except Exception as e:
        print(f"⚠️ Error en el proceso (reintentando en 5s): {e}")
    time.sleep(5)