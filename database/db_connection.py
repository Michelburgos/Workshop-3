import os
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


load_dotenv()



def get_connection():
    """Crea una conexión a la base de datos PostgreSQL utilizando SQLAlchemy.

    Args:
        database_name (str, optional): Clave lógica de la base de datos (por ejemplo, 'default', 'merge').
            Por defecto, 'default'.

    Returns:
        sqlalchemy.engine.Engine: Objeto de conexión (engine) a la base de datos.

    Raises:
        EnvironmentError: Si faltan variables de entorno necesarias para la conexión.
        Exception: Si ocurre un error al crear el motor de conexión.
    """
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_name = os.getenv('DB_NAME')
    db_port = os.getenv('DB_PORT')

    if not all([db_user, db_password, db_host, db_port, db_name]):
        logging.error("Faltan variables de entorno para la conexión a la base de datos.")
        raise EnvironmentError("Variables de entorno incompletas para la conexión.")

    db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    try:
        engine = create_engine(db_url)
        logging.info(f"Conexión a la base de datos '{db_name}' creada exitosamente.")
        return engine
    except Exception as e:
        logging.error(f"Error al crear el motor de conexión para '{db_name}': {e}")
        raise


def close_connection(engine):
    """Cierra la conexión al engine de SQLAlchemy.

    Args:
        engine: Objeto de conexión (engine) de SQLAlchemy a cerrar.

    Raises:
        Exception: Si ocurre un error al cerrar la conexión.
    """
    if engine:
        try:
            engine.dispose()
            logging.info("Conexión al engine cerrada correctamente.")
        except Exception as e:
            logging.error(f"Error al cerrar la conexión: {e}")
    else:
        logging.warning("No hay engine para cerrar.")