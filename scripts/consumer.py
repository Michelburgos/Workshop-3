from kafka import KafkaConsumer
import json
import joblib
import pandas as pd
import sqlite3

def consume_and_predict():
    # Cargar modelo y escalador
    model = joblib.load('../models/modelo_felicidad.pkl')
    scaler = joblib.load('../models/escalador.pkl')

    # Configurar consumidor de Kafka
    consumer = KafkaConsumer(
        'datos_felicidad',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Conectar a la base de datos SQLite
    conn = sqlite3.connect('../database/predicciones_felicidad.db')
    cursor = conn.cursor()

    # Crear tabla si no existe
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS predicciones (
            GDP_per_capita FLOAT,
            Life_Expectancy FLOAT,
            Social_Support FLOAT,
            Freedom FLOAT,
            Prediccion FLOAT
        )
    ''')
    conn.commit()

    # Consumir datos y realizar predicciones
    selected_features = ['GDP_per_capita', 'Life_Expectancy', 'Social_Support', 'Freedom']
    for message in consumer:
        data = message.value
        features = pd.DataFrame([data])[selected_features]
        features_scaled = scaler.transform(features)
        prediction = model.predict(features_scaled)[0]

        # Almacenar en la base de datos
        cursor.execute('''
            INSERT INTO predicciones (GDP_per_capita, Life_Expectancy, Social_Support, Freedom, Prediccion)
            VALUES (?, ?, ?, ?, ?)
        ''', (data['GDP_per_capita'], data['Life_Expectancy'], data['Social_Support'], data['Freedom'], prediction))
        conn.commit()

        print(f'Predicción: {prediction}, Almacenado: {data}')

    conn.close()

if __name__ == '__main__':
    consume_and_predict()