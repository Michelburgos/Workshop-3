from confluent_kafka import Consumer, KafkaError
import pickle
import pandas as pd
import json
import sqlite3
from sklearn.metrics import r2_score

# Configuración del consumidor
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'happiness_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['happiness_data'])

# Cargar modelo
with open('../models/xgb_model.pkl', 'rb') as file:
    xgb_model = pickle.load(file)

# Conectar a SQLite
conn = sqlite3.connect('../data/happiness_predictions.db')
cursor = conn.cursor()

# Crear tabla
cursor.execute('''
    CREATE TABLE IF NOT EXISTS predictions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        Freedom FLOAT,
        Happiness_Rank INTEGER,
        Perceptions_of_corruption FLOAT,
        Social_support FLOAT,
        Economic_Health_Index FLOAT,
        region_Australia_and_New_Zealand INTEGER,
        region_Central_and_Eastern_Europe INTEGER,
        region_Eastern_Asia INTEGER,
        region_Latin_America_and_Caribbean INTEGER,
        region_Middle_East_and_Northern_Africa INTEGER,
        region_North_America INTEGER,
        region_Southeastern_Asia INTEGER,
        region_Southern_Asia INTEGER,
        region_Sub_Saharan_Africa INTEGER,
        region_Western_Europe INTEGER,
        Year_Year_2015 INTEGER,
        Year_Year_2016 INTEGER,
        Year_Year_2017 INTEGER,
        Year_Year_2018 INTEGER,
        Year_Year_2019 INTEGER,
        Predicted_Happiness_Score FLOAT
    )
''')
conn.commit()

# Listas para evaluación
predictions = []
true_values = []

# Cargar y_test
df = pd.read_csv('../data/clean_dataset.csv')
X = df.drop(columns=['Happiness_Score'])
y = df['Happiness_Score']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# Consumir mensajes
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break

    # Procesar mensaje
    data_json = msg.value().decode('utf-8')
    data = json.loads(data_json)
    input_data = pd.DataFrame([data])
    prediction = xgb_model.predict(input_data)[0]
    
    # Almacenar en la base de datos
    cursor.execute('''
        INSERT INTO predictions (
            Freedom, Happiness_Rank, Perceptions_of_corruption, Social_support,
            Economic_Health_Index, region_Australia_and_New_Zealand,
            region_Central_and_Eastern_Europe, region_Eastern_Asia,
            region_Latin_America_and_Caribbean, region_Middle_East_and_Northern_Africa,
            region_North_America, region_Southeastern_Asia, region_Southern_Asia,
            region_Sub_Saharan_Africa, region_Western_Europe, Year_Year_2015,
            Year_Year_2016, Year_Year_2017, Year_Year_2018, Year_Year_2019,
            Predicted_Happiness_Score
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        data['Freedom'], data['Happiness_Rank'], data['Perceptions_of_corruption'],
        data['Social_support'], data['Economic_Health_Index'],
        data['region_Australia and New Zealand'], data['region_Central and Eastern Europe'],
        data['region_Eastern Asia'], data['region_Latin America and Caribbean'],
        data['region_Middle East and Northern Africa'], data['region_North America'],
        data['region_Southeastern Asia'], data['region_Southern Asia'],
        data['region_Sub-Saharan Africa'], data['region_Western Europe'],
        data['Year_Year_2015'], data['Year_Year_2016'], data['Year_Year_2017'],
        data['Year_Year_2018'], data['Year_Year_2019'], prediction
    ))
    conn.commit()
    
    # Almacenar para evaluación
    index = input_data.index[0]
    true_value = y_test.loc[index]
    predictions.append(prediction)
    true_values.append(true_value)

# Calcular R²
r2 = r2_score(true_values, predictions)
print(f'R² Score (consumer.py): {r2}')

# Cerrar conexiones
consumer.close()
conn.close()