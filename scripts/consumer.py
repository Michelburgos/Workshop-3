from kafka import KafkaConsumer
import pandas as pd
import json
import sqlite3
import pickle
from sklearn.metrics import r2_score
import warnings
from time import sleep

# Ignorar advertencias
warnings.filterwarnings('ignore', category=UserWarning)

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']  # Cambiar por tu servidor Kafka
TOPIC_NAME = 'happiness_data'

# Cargar el modelo entrenado
with open('../models/catboost_model.pkl', 'rb') as file:
    cat_model = pickle.load(file)

# Conectar a la base de datos SQLite
conn = sqlite3.connect('happiness_predictions.db')
cursor = conn.cursor()

# Crear tabla si no existe
cursor.execute('''
    CREATE TABLE IF NOT EXISTS predictions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        Freedom REAL,
        Generosity REAL,
        Perceptions_of_corruption REAL,
        GDP_per_capita REAL,
        Healthy_life_expectancy REAL,
        Social_support REAL,
        Year INTEGER,
        Region_Australia_and_New_Zealand INTEGER,
        Region_Central_and_Eastern_Europe INTEGER,
        Region_Eastern_Asia INTEGER,
        Region_Latin_America_and_Caribbean INTEGER,
        Region_Middle_East_and_Northern_Africa INTEGER,
        Region_North_America INTEGER,
        Region_Southeastern_Asia INTEGER,
        Region_Southern_Asia INTEGER,
        Region_Sub-Saharan_Africa INTEGER,
        Region_Western_Europe INTEGER,
        Happiness_Score_Predicted REAL,
        Happiness_Score_Actual REAL
    )
''')
conn.commit()

# Inicializar consumidor Kafka
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Lista para almacenar predicciones y valores reales
predictions = []
actuals = []

# Consumir mensajes y realizar predicciones
for message in consumer:
    data = message.value
    print(f"Datos recibidos: {data}")
    
    # Extraer características (excluyendo Happiness_Score para predicción)
    features = {k: v for k, v in data.items() if k != 'Happiness_Score'}
    
    # Convertir características a DataFrame para predicción
    row_df = pd.DataFrame([features], columns=features.keys())
    
    # Realizar predicción
    pred = cat_model.predict(row_df)[0]
    
    # Obtener el valor real desde los datos recibidos
    actual = data['Happiness_Score'] if 'Happiness_Score' in data else None
    
    # Si no hay Happiness_Score (por error), usar un valor nulo o promedio (simulación)
    if actual is None:
        actual = 0.0  # Valor placeholder; en producción, manejar este caso
    
    # Almacenar en base de datos
    cursor.execute('''
        INSERT INTO predictions (
            Freedom, Generosity, Perceptions_of_corruption, GDP_per_capita,
            Healthy_life_expectancy, Social_support, Year,
            Region_Australia_and_New_Zealand, Region_Central_and_Eastern_Europe,
            Region_Eastern_Asia, Region_Latin_America_and_Caribbean,
            Region_Middle_East_and_Northern_Africa, Region_North_America,
            Region_Southeastern_Asia, Region_Southern_Asia, Region_Sub-Saharan_Africa,
            Region_Western_Europe, Happiness_Score_Predicted, Happiness_Score_Actual
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        data['Freedom'], data['Generosity'], data['Perceptions_of_corruption'],
        data['GDP_per_capita'], data['Healthy_life_expectancy'], data['Social_support'],
        data['Year'], data['Region_Australia_and_New_Zealand'],
        data['Region_Central_and_Eastern_Europe'], data['Region_Eastern_Asia'],
        data['Region_Latin_America_and_Caribbean'], data['Region_Middle_East_and_Northern_Africa'],
        data['Region_North_America'], data['Region_Southeastern_Asia'],
        data['Region_Southern_Asia'], data['Region_Sub-Saharan_Africa'],
        data['Region_Western_Europe'], pred, actual
    ))
    conn.commit()

    predictions.append(pred)
    actuals.append(actual)
    print(f"Predicción: {pred}, Valor real: {actual}")

# Calcular métrica de rendimiento
if actuals and predictions:  # Verificar que haya datos
    r2 = r2_score(actuals, predictions)
    print(f"R2 Score: {r2:.4f}")
else:
    print("No se recibieron suficientes datos para calcular R2 Score.")

# Cerrar conexión
conn.close()
consumer.close()

# Verificar datos almacenados
conn = sqlite3.connect('happiness_predictions.db')
sample_data = pd.read_sql_query("SELECT * FROM predictions LIMIT 5", conn)
print("\nPrimeras 5 filas de la base de datos:")
print(sample_data)
conn.close()