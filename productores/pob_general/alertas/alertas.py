from confluent_kafka import Producer
import time
import json

config = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': 'alertas_pob_general'
}

productor = Producer(config)


def enviar_alerta_pob_general(<aqui va en funcion del nombre de las columnas del dbt>):
    texto = f"Alerta: {aqui va en funcion del nombre de las columnas del dbt}"
    mensaje = {
        "texto": texto
    }
    productor.produce(
        topic = "alertas_pob_generales",
        value = json.dumps(mensaje).encode("utf-8")
    )
    print("Alerta enviada.", texto)
