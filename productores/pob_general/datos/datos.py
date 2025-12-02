from confluent_kafka import Producer
import time
import json

config = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': 'datos_pob_general'
}

productor = Producer(config)


def enviar_datos_pob_general(<aqui va en funcion del nombre de las columnas del dbt>):
    texto = f"Datos: {aqui va en funcion del nombre de las columnas del dbt}"
    mensaje = {
        "texto": texto
    }
    productor.produce(
        topic = "datos_pob_general",
        value = json.dumps(mensaje).encode("utf-8")
    )
    print("Datos enviados.", texto)