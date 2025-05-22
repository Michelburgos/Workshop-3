import json
import time
import logging
from kafka import KafkaProducer
import os

# Crear carpeta de logs si no existe
os.makedirs("logs", exist_ok=True)

# Configurar logging
logging.basicConfig(
    filename="logs/producer.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

KAFKA_BOOTSTRAP_SERVERS = ['localhost:29092']
TOPIC_NAME = 'happiness_data'
DATA_PATH = '../data/transformed_data.json'

def kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logging.info("✅ Productor Kafka inicializado.")
        return producer
    except Exception as e:
        logging.error(f"❌ Error al inicializar productor Kafka: {e}")
        raise

def cargar_datos(path: str):
    try:
        with open(path, 'r') as f:
            data = json.load(f)
        logging.info(f"📄 Datos cargados desde {path}, total de registros: {len(data)}")
        return data
    except Exception as e:
        logging.error(f"❌ Error al leer los datos desde {path}: {e}")
        raise

def enviar_datos_a_kafka(data: list, topic: str, sleep_seconds: float = 0.1):
    try:
        producer = kafka_producer()
        logging.info("🚀 Enviando datos a Kafka...")

        for i, row in enumerate(data, start=1):
            producer.send(topic, value=row)
            logging.info(f"📤 Enviado registro {i}")
            time.sleep(sleep_seconds)

        producer.flush()
        producer.close()
        logging.info("✅ Todos los registros fueron enviados correctamente.")
    except Exception as e:
        logging.error(f"❌ Error durante el envío de datos a Kafka: {e}")
        raise

if __name__ == "__main__":
    datos = cargar_datos(DATA_PATH)
    enviar_datos_a_kafka(datos, topic=TOPIC_NAME)

