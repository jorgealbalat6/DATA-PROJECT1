from confluent_kafka import Producer
import json
import time
import os
import psycopg


config = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': 'datos_pob_general'
}

productor = Producer(config)
topico = "datos_pob_general"

DB_URL = os.getenv("DATABASE_URL")
ultima_fecha_procesada = None

print("Iniciando productor de datos generales...")

while True:
    try:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                if ultima_fecha_procesada is None:
                    query = """
                        SELECT municipio, estacion, indicador_contaminante, fecha_hora, valor 
                        FROM "int_CalidadAire_Mad"
                        WHERE fecha_hora > NOW() - INTERVAL '1 hour'
                        AND datos_disponibles = true
                    """
                    cur.execute(query)
                else:
                    query = """
                        SELECT municipio, estacion, indicador_contaminante, fecha_hora, valor 
                        FROM "int_CalidadAire_Mad"
                        WHERE fecha_hora > %s
                        AND datos_disponibles = true
                    """
                    cur.execute(query, (ultima_fecha_procesada,))

                datos = cur.fetchall()
                
                for fila in datos: 
                    municipio = fila[0]   
                    estacion = fila[1]    
                    contaminante = fila[2]
                    fecha_obj = fila[3]
                    fecha_str = str(fecha_obj)
                    valor = fila[4]   

                    if ultima_fecha_procesada is None or fecha_obj > ultima_fecha_procesada:
                        ultima_fecha_procesada = fecha_obj

                    if valor is None: continue
                    
                    mensaje = {
                        "municipio": municipio,
                        "estacion": estacion,
                        "contaminante": contaminante,
                        "fecha": fecha_str,
                        "valor": valor
                    }
                    
                    productor.produce(
                        topico,
                        value=json.dumps(mensaje).encode("utf-8")
                    )
                
                if datos:
                    print(f"Se han enviado {len(datos)} registros nuevos a Kafka.")
                    productor.flush()
        
    except Exception as e:
        print(f"⚠️ Error en el proceso de datos (reintentando en 5s): {e}")
    time.sleep(5)