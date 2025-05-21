import pandas as pd
import json
import pickle
import warnings
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from sqlalchemy import text
from db_connection import get_connection, close_connection

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Ignorar advertencias
warnings.filterwarnings('ignore', category=UserWarning)

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092'] 
TOPIC_NAME = 'happiness_data'

# Columnas esperadas para la predicción
REQUIRED_FEATURES = [
    'Freedom', 'Generosity', 'Perceptions_of_corruption', 'GDP_per_capita',
    'Healthy_life_expectancy', 'Social_support', 'Year',
    'Region_Australia_and_New_Zealand', 'Region_Central_and_Eastern_Europe',
    'Region_Eastern_Asia', 'Region_Latin_America_and_Caribbean',
    'Region_Middle_East_and_Northern_Africa', 'Region_North_America',
    'Region_Southeastern_Asia', 'Region_Southern_Asia', 'Region_Sub-Saharan_Africa',
    'Region_Western_Europe'
]

# Cargar el modelo entrenado
try:
    with open('../models/catboost_model.pkl', 'rb') as file:
        cat_model = pickle.load(file)
    logging.info("Modelo CatBoost cargado exitosamente.")
except FileNotFoundError:
    logging.error("No se encontró el archivo del modelo '../models/catboost_model.pkl'.")
    exit(1)
except Exception as e:
    logging.error(f"Error al cargar el modelo: {e}")
    exit(1)

# Conectar a la base de datos PostgreSQL
try:
    engine = get_connection()
    # Crear tabla si no existe
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS predictions (
                id SERIAL PRIMARY KEY,
                Freedom FLOAT,
                Generosity FLOAT,
                Perceptions_of_corruption FLOAT,
                GDP_per_capita FLOAT,
                Healthy_life_expectancy FLOAT,
                Social_support FLOAT,
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
                Happiness_Score_Predicted FLOAT,
                Happiness_Score_Actual FLOAT
            )
        """))
        conn.commit()
    logging.info("Tabla 'predictions' verificada/creada exitosamente.")
except Exception as e:
    logging.error(f"Error al conectar o crear la tabla en PostgreSQL: {e}")
    close_connection(engine)
    exit(1)

# Inicializar consumidor Kafka
try:
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=10000  # Timeout de 10 segundos para salir si no hay mensajes
    )
    logging.info(f"Consumidor Kafka inicializado para el tema '{TOPIC_NAME}'.")
except KafkaError as e:
    logging.error(f"Error al conectar con Kafka: {e}")
    close_connection(engine)
    exit(1)

# Consumir mensajes y realizar predicciones
try:
    for message in consumer:
        try:
            data = message.value
            logging.info(f"Datos recibidos: {data}")

            # Verificar si todas las características requeridas están presentes
            missing_features = [feat for feat in REQUIRED_FEATURES if feat not in data]
            if missing_features:
                logging.warning(f"Faltan características en los datos: {missing_features}. Saltando mensaje.")
                continue

            # Extraer características (excluyendo Happiness_Score para predicción)
            features = {k: v for k, v in data.items() if k in REQUIRED_FEATURES}

            # Convertir características a DataFrame para predicción
            row_df = pd.DataFrame([features], columns=REQUIRED_FEATURES)

            # Realizar predicción
            pred = cat_model.predict(row_df)[0]

            # Obtener el valor real desde los datos recibidos
            actual = data.get('Happiness_Score', None)

            # Preparar datos para inserción
            insert_data = features.copy()
            insert_data['Happiness_Score_Predicted'] = pred
            insert_data['Happiness_Score_Actual'] = actual
            insert_df = pd.DataFrame([insert_data])

            # Almacenar en base de datos usando pandas.to_sql
            insert_df.to_sql('predictions', engine, if_exists='append', index=False)
            logging.info(f"Datos insertados en la base de datos: Predicción={pred}, Valor real={actual}")

        except (KeyError, ValueError, json.JSONDecodeError) as e:
            logging.warning(f"Error al procesar el mensaje: {e}. Saltando mensaje.")
            continue
        except Exception as e:
            logging.error(f"Error al insertar en la base de datos: {e}")
            continue

except KeyboardInterrupt:
    logging.info("Consumo detenido por el usuario.")

finally:
    # Cerrar recursos
    logging.info("Cerrando conexiones...")
    try:
        consumer.close()
        logging.info("Consumidor Kafka cerrado correctamente.")
    except KafkaError as e:
        logging.error(f"Error al cerrar el consumidor Kafka: {e}")
    close_connection(engine)

# Verificar datos almacenados
try:
    engine = get_connection()
    sample_data = pd.read_sql_query("SELECT * FROM predictions LIMIT 5", engine)
    logging.info("\nPrimeras 5 filas de la base de datos:")
    logging.info(f"\n{sample_data}")
except Exception as e:
    logging.error(f"Error al leer datos de la base de datos: {e}")
finally:
    close_connection(engine)