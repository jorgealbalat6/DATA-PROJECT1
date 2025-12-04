from confluent_kafka import Producer
import json
import time
import os
import psycopg

config = {
    'bootstrap.servers': 'kafka:9092',
}
productor = Producer(config)
topico = "alertas_pob_general"

DB_URL = os.getenv("DATABASE_URL")

LIMITES = {
    "Dióxido de Nitrógeno (NO2)": 400,
    "Partículas < 2.5 µm (PM2.5)": 50,
    "Partículas < 10 µm (PM10)": 80,
    "Ozono (O3)": 240,
    "Dióxido de Azufre (SO2)": 500
}

ultima_fecha_procesada = None # Se pone esto, para que cada vez que entre en el bucle, no procese todos los datos, sino solo los nuevos

while True:
    try:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                if ultima_fecha_procesada is None:
                    query = """
                        SELECT municipio, estacion, indicador_contaminante, fecha_hora, valor 
                        FROM "marts_CalidadAire_Mad"
                        WHERE fecha_hora > NOW() - INTERVAL '1 hour'
                        AND datos_disponibles = true
                    """
                    cur.execute(query)
                else:
                    query = """
                        SELECT municipio, estacion, indicador_contaminante, fecha_hora, valor 
                        FROM "marts_CalidadAire_Mad"
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
                    
                    if contaminante in LIMITES and valor > LIMITES[contaminante]:
                        texto_alerta = (f"Alerta de contaminación en {municipio} "
                                        f"({estacion}) el {fecha_str}: "
                                        f"{contaminante} = {valor} µg/m³, "
                                        f"superando el límite de {LIMITES[contaminante]} µg/m³.")
                        mensaje = {
                            "municipio": municipio,
                            "estacion": estacion,
                            "contaminante": contaminante,
                            "fecha": fecha_str,
                            "valor": valor,
                            "texto_alerta": texto_alerta
                        }
                        productor.produce(
                            topico,
                            value=json.dumps(mensaje).encode("utf-8")
                        )
                        print("Alerta enviada:", texto_alerta)
                
                productor.flush()
        
    except Exception as e:
        print(f"⚠️ Error en el proceso (reintentando en 5s): {e}")
    
    time.sleep(5)