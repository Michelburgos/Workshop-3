from kafka import KafkaProducer
import json
import pandas as pd

def send_data_to_kafka():
    # Cargar datos transformados
    X_test = pd.read_csv('../data/processed/transformed_data.csv')
    selected_features = ['GDP_per_capita', 'Life_Expectancy', 'Social_Support', 'Freedom']

    # Configurar productor de Kafka
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Enviar datos al tópico
    for _, row in X_test.iterrows():
        data = row.to_dict()
        producer.send('datos_felicidad', value=data)
        print(f'Enviado: {data}')
    producer.flush()

    print('Todos los datos enviados a Kafka.')

if __name__ == '__main__':
    send_data_to_kafka()