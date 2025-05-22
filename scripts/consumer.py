import pandas as pd
import json
import pickle
import warnings
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from sqlalchemy import text

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database.db_connection import get_connection, close_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
warnings.filterwarnings('ignore', category=UserWarning)

KAFKA_BOOTSTRAP_SERVERS = ['localhost:29092']
TOPIC_NAME = 'happiness_data'

COLUMN_MAP = {
    'Freedom': 'freedom',
    'Generosity': 'generosity',
    'Perceptions_of_corruption': 'perceptions_of_corruption',
    'GDP_per_capita': 'gdp_per_capita',
    'Healthy_life_expectancy': 'healthy_life_expectancy',
    'Social_support': 'social_support',
    'Year': 'year',
    'Region_Australia_and_New_Zealand': 'region_australia_and_new_zealand',
    'Region_Central_and_Eastern_Europe': 'region_central_and_eastern_europe',
    'Region_Eastern_Asia': 'region_eastern_asia',
    'Region_Latin_America_and_Caribbean': 'region_latin_america_and_caribbean',
    'Region_Middle_East_and_Northern_Africa': 'region_middle_east_and_northern_africa',
    'Region_North_America': 'region_north_america',
    'Region_Southeastern_Asia': 'region_southeastern_asia',
    'Region_Southern_Asia': 'region_southern_asia',
    'Region_Sub_Saharan_Africa': 'region_sub_saharan_africa',
    'Region_Western_Europe': 'region_western_europe',
}

REQUIRED_FEATURES = list(COLUMN_MAP.keys())

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

engine = None

try:
    engine = get_connection()
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS predictions (
                freedom DOUBLE PRECISION,
                generosity DOUBLE PRECISION,
                perceptions_of_corruption DOUBLE PRECISION,
                gdp_per_capita DOUBLE PRECISION,
                healthy_life_expectancy DOUBLE PRECISION,
                social_support DOUBLE PRECISION,
                year INTEGER,
                region_australia_and_new_zealand INTEGER,
                region_central_and_eastern_europe INTEGER,
                region_eastern_asia INTEGER,
                region_latin_america_and_caribbean INTEGER,
                region_middle_east_and_northern_africa INTEGER,
                region_north_america INTEGER,
                region_southeastern_asia INTEGER,
                region_southern_asia INTEGER,
                region_sub_saharan_africa INTEGER,
                region_western_europe INTEGER,
                happiness_score_predicted DOUBLE PRECISION,
                happiness_score_actual DOUBLE PRECISION
            )
        """))
        conn.execute(text("TRUNCATE TABLE predictions"))
    logging.info("Tabla 'predictions' verificada/creada y truncada exitosamente.")
except Exception as e:
    logging.error(f"Error al conectar, crear la tabla o truncar en PostgreSQL: {e}")
    if engine:
        close_connection(engine)
    exit(1)

try:
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=10000
    )
    logging.info(f"Consumidor Kafka inicializado para el tema '{TOPIC_NAME}'.")
except KafkaError as e:
    logging.error(f"Error al conectar con Kafka: {e}")
    if engine:
        close_connection(engine)
    exit(1)

def transform_keys(data: dict) -> dict:
    return {COLUMN_MAP.get(k, k): v for k, v in data.items() if k in COLUMN_MAP}

try:
    for message in consumer:
        try:
            data = message.value
            logging.info(f"Datos recibidos: {data}")

            missing_features = [feat for feat in REQUIRED_FEATURES if feat not in data]
            if missing_features:
                logging.warning(f"Faltan características en los datos: {missing_features}. Saltando mensaje.")
                continue

            features = {k: data[k] for k in REQUIRED_FEATURES}
            features_df = pd.DataFrame([features], columns=REQUIRED_FEATURES)

            pred = cat_model.predict(features_df)[0]

            actual = data.get('Happiness_Score', None)

            insert_data = transform_keys(features)
            insert_data['happiness_score_predicted'] = pred
            insert_data['happiness_score_actual'] = actual

            insert_df = pd.DataFrame([insert_data])

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
    logging.info("Cerrando conexiones...")
    try:
        consumer.close()
        logging.info("Consumidor Kafka cerrado correctamente.")
    except KafkaError as e:
        logging.error(f"Error al cerrar el consumidor Kafka: {e}")
    if engine:
        close_connection(engine)

try:
    engine = get_connection()
    sample_data = pd.read_sql_query("SELECT * FROM predictions LIMIT 5", engine)
    logging.info("\nPrimeras 5 filas de la base de datos:")
    logging.info(f"\n{sample_data}")
except Exception as e:
    logging.error(f"Error al leer datos de la base de datos: {e}")
finally:
    if engine:
        close_connection(engine)
