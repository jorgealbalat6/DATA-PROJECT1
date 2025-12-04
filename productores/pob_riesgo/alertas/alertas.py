from confluent_kafka import Producer
import json
import time
import os
import psycopg


config = {
    'bootstrap.servers': 'kafka:9092',
    
}
producer = Producer(config)
topic = "alertas_pob_riesgo"

DB_URL = os.getenv("DATABASE_URL")

LIMITES = {
    "Dióxido de Nitrógeno (NO2)": 200,
    "Partículas < 2.5 µm (PM2.5)": 25,
    "Partículas < 10 µm (PM10)": 50,
    "Ozono (O3)": 180,
    "Dióxido de Azufre (SO2)": 350
}

ultima_fecha_procesada = None # Se pone esto, para que cada vez que entre en el bucle, no procese todos los datos, sino solo los nuevos

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
    # 'fila' es: ('Madrid', 'Pza. España', 'NO2', '2025-11-30', 210)

            municipio = fila[0]   # Coge lo que hay en la 1ª posición -> 'Madrid'
            estacion = fila[1]    # Coge lo que hay en la 2ª posición -> 'Pza. España'
            contaminante = fila[2]# Coge lo que hay en la 3ª posición -> 'NO2'
            fecha_obj = fila[3] 
            fecha_str = str(fecha_obj) # Coge la fecha y conviértela a texto
            valor = fila[4]       # Coge el número del final -> 210


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



