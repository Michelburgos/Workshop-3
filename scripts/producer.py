from kafka import KafkaProducer
import json
from time import sleep

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']  # Cambiar por tu servidor Kafka
TOPIC_NAME = 'happiness_data'

# Inicializar productor Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Leer datos transformados generados por feature_selection.py
with open('transformed_data.json', 'r') as f:
    data = json.load(f)

# Enviar cada fila al topic de Kafka
for row in data:
    try:
        producer.send(TOPIC_NAME, value=row)
        print(f"Enviando datos para índice {data.index(row)}")
        sleep(0.1)  # Simular retraso en streaming
    except Exception as e:
        print(f"Error al enviar mensaje: {e}")

# Cerrar productor
producer.flush()
producer.close()

print("Transmisión de datos completada.")