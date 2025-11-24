# Workshop-3: Predicción del Índice de Felicidad con Machine Learning y Streaming  
Este proyecto combina análisis de datos, aprendizaje automático y streaming de datos para predecir el Happiness Score de países utilizando datos del World Happiness Report (2015-2019). A través de Jupyter Notebooks, scripts de Python, Kafka para streaming y una base de datos, exploramos, procesamos y modelamos datos para entender qué hace felices a las naciones. 

## Objetivo del Proyecto
El objetivo es construir un modelo de regresión para predecir el Happiness Score de países basado en características como el PIB per cápita, esperanza de vida, apoyo social, libertad y percepción de corrupción. El flujo de trabajo incluye:

- **Análisis Exploratorio de Datos (EDA):** Explorar y comparar las características de los datasets de 2015 a 2019, ya que las columnas pueden variar entre años.
- **ETL (Extracción, Transformación y Carga):** Extraer características relevantes de los cinco archivos CSV, limpiar y unificar los datos.
- **Entrenamiento del Modelo:** Entrenar un modelo de regresión con una división de datos 70% (entrenamiento) y 30% (prueba).
- **Streaming con Kafka:** Transmitir los datos transformados usando un producer y consumirlos con un consumer para realizar predicciones.
- **Predicciones y Almacenamiento:** Usar el modelo entrenado para predecir el Happiness Score en el conjunto de prueba y almacenar las predicciones junto con las características en una base de datos.
- **Evaluación:** Extraer métricas de rendimiento (R²) para evaluar el modelo con los datos de prueba.

## Estructura del Repositorio
```

├── Dockerfile                
├── README.md                
├── data/                  
│   ├── 2015.csv
│   ├── 2016.csv
│   ├── 2017.csv
│   ├── 2018.csv
│   ├── 2019.csv
│   ├── clean\_dataset.csv
│   └── transformed\_data.json
├── database/
│   └── db\_connection.py
├── docker-compose.yml
├── models/
│   └── catboost\_model.pkl
├── notebooks/
│   ├── 001-EDA.ipynb
│   ├── 002-training.ipynb
│   ├── 003-evaluation.ipynb
├── requirements.txt
└── scripts/
├── consumer.py
├── feature\_selection.py
└── producer.py

````

## Tecnologías Utilizadas

- Python 3.8+
- Jupyter Notebook
- Scikit-learn
- Kafka (para streaming de datos)
- Base de datos (PostgreSQL)
- Archivos CSV
- Docker y Docker Compose

## Requisitos Previos

Para ejecutar el proyecto, asegúrate de tener instalado:

- Python 3.8+ y pip
- Docker y Docker Compose (para Kafka y la base de datos)
- Jupyter Notebook
- Git
- Kafka (incluido en docker-compose.yml)

## Instalación

Sigue estos pasos para configurar el proyecto:

**Clona el repositorio:**
```bash
git clone https://github.com/Michelburgos/Workshop-3.git
cd Workshop-3
````

**Crea un entorno virtual :**

```bash
python -m venv venv
source venv/bin/activate 
```

**Instala las dependencias:**

```bash
pip install -r requirements.txt
```

**Contenido de requirements.txt:**

```
pandas
numpy
scikit-learn
matplotlib
seaborn
jupyter
catboost
psycopg2-binary
kafka-python
```

**Configura y ejecuta Docker Compose:**

```bash
docker-compose up -d --build
```

Esto inicia Kafka Y Zookeeper. Asegúrate de que los puertos (por ejemplo, 9092 para Kafka) estén libres.

**Instala y configura PostgreSQL**
   - Instala:  
     ```bash
     sudo apt update
     sudo apt install postgresql -y
     ```
   - Crea usuario y base de datos:
     ```sql
     CREATE USER 'tu_usuario' WITH PASSWORD 'tu_contraseña';
     ALTER USER 'tu_usuario' CREATEDB;
     createdb -U postgres 'la_db_del_workshop'
     ```

**Configura el archivo `.env`**
   ```
   PG_USER=<tu_usuario>
   PG_PASSWORD=<tu_contraseña>
   PG_HOST=localhost
   PG_PORT=5432
   PG_DATABASE=<la_db_de_grammys>
   PG_DATABASE_DIMENSIONAL=<la_db_de_merge>
   ```

## Cómo Ejecutar el Proyecto

1. **Análisis Exploratorio de Datos (EDA):**

   ```bash
   jupyter notebook notebooks/001-EDA.ipynb
   ```

2. **Preprocesamiento de Datos (ETL):**

   ```bash
   python scripts/feature_selection.py
   ```

3. **Entrenamiento del Modelo:**

   ```bash
   jupyter notebook notebooks/002-training.ipynb
   ```

4. **Streaming con Kafka:**

   ```bash
   python scripts/producer.py
   python scripts/consumer.py
   ```

5. **Evaluación del Modelo:**

   ```bash
   jupyter notebook notebooks/003-evaluation.ipynb
   ```

6. **Configuración de la Base de Datos:**
   Configura las credenciales en `database/db_connection.py`.

## Datos

El proyecto utiliza cinco archivos CSV del World Happiness Report (2015-2019) en `data/`, con características como:

* PIB per cápita
* Esperanza de vida
* Apoyo social
* Libertad
* Percepción de corrupción
* Happiness Score (variable objetivo)

## Notas Importantes

* Kafka: Asegúrate de que Kafka y Zookeeper estén corriendo (`docker-compose up -d`) antes de ejecutar `producer.py` o `consumer.py`.
* Base de datos: Configura las credenciales en `db_connection.py`. 
* Docker: Verifica que los puertos en `docker-compose.yml` no estén en uso.
## Autor

**Michel Dahiana Burgos Santos**  
Proyecto académico de Ingeniería de Datos e Inteligencia Artificial
