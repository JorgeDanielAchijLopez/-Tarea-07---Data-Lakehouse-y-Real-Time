from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

TOPIC = "gpscamiones"
BOOTSTRAP_SERVERS = "kafka:9092"

def crear_productor():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5
            )
            print("Productor conectado a Kafka.")
            return producer
        except Exception as e:
            print(f"Esperando Kafka... {e}")
            time.sleep(5)

producer = crear_productor()

zonas = ["NORTE", "SUR", "CENTRO", "OCCIDENTE", "ORIENTE"]
estados = ["ACTIVO", "ACTIVO", "ACTIVO", "RETRASADO", "MANTENIMIENTO"]

print(f"Enviando eventos al topic: {TOPIC}")

while True:
    evento = {
        "id_camion": f"CAM-{random.randint(1, 100):03d}",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "lat": round(random.uniform(14.0, 16.0), 6),
        "lon": round(random.uniform(-91.8, -89.0), 6),
        "zona": random.choice(zonas),
        "estado": random.choice(estados),
        "velocidad": round(random.uniform(5, 90), 2),
        "toneladas": round(random.uniform(1, 25), 2),
        "capacidad_patio": random.randint(40, 100)
    }

    producer.send(TOPIC, evento)
    producer.flush()

    print(f"Evento enviado: {evento}")
    time.sleep(5)