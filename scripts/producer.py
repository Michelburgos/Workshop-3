from kafka import KafkaProducer
import json
from time import sleep
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']
TOPIC_NAME = 'happiness_data'

# Inicializar productor Kafka
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logging.info("Productor Kafka inicializado exitosamente.")
except Exception as e:
    logging.error(f"Error al inicializar productor Kafka: {e}")
    exit(1)

# Leer datos transformados generados por feature_selection.py
try:
    with open('data/transformed_data.json', 'r') as f:
        data = json.load(f)
    logging.info(f"Datos cargados desde data/transformed_data.json, {len(data)} registros.")
except Exception as e:
    logging.error(f"Error al leer transformed_data.json: {e}")
    exit(1)

# Enviar cada fila al topic de Kafka
for i, row in enumerate(data):
    try:
        producer.send(TOPIC_NAME, value=row)
        logging.info(f"Enviando datos para índice {i}")
        sleep(0.1)  # Simular retraso en streaming
    except Exception as e:
        logging.warning(f"Error al enviar mensaje para índice {i}: {e}")

# Cerrar productor
try:
    producer.flush()
    producer.close()
    logging.info("Transmisión de datos completada.")
except Exception as e:
    logging.error(f"Error al cerrar productor: {e}")
