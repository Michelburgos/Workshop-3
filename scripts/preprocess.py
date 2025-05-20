import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from sklearn.model_selection import train_test_split
from confluent_kafka import Producer
import json

# Cargar datasets
files = ['2015.csv', '2016.csv', '2017.csv', '2018.csv', '2019.csv']
dataframes = []
for i, file in enumerate(files):
    df = pd.read_csv(f'../data/{file}')
    year = 2015 + i
    # Estandarizar nombres de columnas
    if year == 2015:
        df = df.rename(columns={
            'Country': 'Country_or_region', 'Happiness Rank': 'Happiness_Rank',
            'Happiness Score': 'Happiness_Score', 'Economy (GDP per Capita)': 'GDP_per_capita',
            'Family': 'Social_support', 'Health (Life Expectancy)': 'Healthy_life_expectancy',
            'Trust (Government Corruption)': 'Perceptions_of_corruption'
        })
    elif year == 2016:
        df = df.rename(columns={
            'Country': 'Country_or_region', 'Happiness Rank': 'Happiness_Rank',
            'Happiness Score': 'Happiness_Score', 'Economy (GDP per Capita)': 'GDP_per_capita',
            'Family': 'Social_support', 'Health (Life Expectancy)': 'Healthy_life_expectancy',
            'Trust (Government Corruption)': 'Perceptions_of_corruption'
        })
    elif year == 2017:
        df = df.rename(columns={
            'Country': 'Country_or_region', 'Happiness.Rank': 'Happiness_Rank',
            'Happiness.Score': 'Happiness_Score', 'Economy..GDP.per.Capita.': 'GDP_per_capita',
            'Family': 'Social_support', 'Health..Life.Expectancy.': 'Healthy_life_expectancy',
            'Trust..Government.Corruption.': 'Perceptions_of_corruption'
        })
    elif year in [2018, 2019]:
        df = df.rename(columns={
            'Country or region': 'Country_or_region', 'Overall rank': 'Happiness_Rank',
            'Score': 'Happiness_Score', 'GDP per capita': 'GDP_per_capita',
            'Social support': 'Social_support', 'Healthy life expectancy': 'Healthy_life_expectancy',
            'Perceptions of corruption': 'Perceptions_of_corruption',
            'Freedom to make life choices': 'Freedom'
        })
    df['Year'] = year
    dataframes.append(df)

# Combinar datasets
df = pd.concat(dataframes, ignore_index=True)

# Manejo de valores nulos
df['Perceptions_of_corruption'] = df['Perceptions_of_corruption'].fillna(df['Perceptions_of_corruption'].mean())

# Feature Engineering: Crear Economic_Health_Index con PCA
pca = PCA(n_components=1)
df['Economic_Health_Index'] = pca.fit_transform(df[['GDP_per_capita', 'Healthy_life_expectancy']])

# Mapeo de regiones
region_mapping = {
    'Switzerland': 'Western Europe', 'Iceland': 'Western Europe', 'Denmark': 'Western Europe',
    # Añade el mapeo completo aquí (puedes copiarlo del notebook 001-EDA (2).ipynb)
    # Por brevedad, asumo que tienes el diccionario completo
}
df['Region'] = df['Country_or_region'].map(region_mapping).fillna('Other')

# Codificación one-hot
df = pd.get_dummies(df, columns=['Region', 'Year'], prefix=['region', 'Year'])

# Eliminar columnas innecesarias
df = df.drop(columns=['Country_or_region', 'GDP_per_capita', 'Healthy_life_expectancy', 'Generosity'])

# Guardar dataset limpio
df.to_csv('../data/clean_dataset.csv', index=False)

# Dividir datos (70%-30%)
X = df.drop(columns=['Happiness_Score'])
y = df['Happiness_Score']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# Configurar productor Kafka
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Enviar X_test a Kafka
for index, row in X_test.iterrows():
    data = row.to_dict()
    data_json = json.dumps(data)
    producer.produce('happiness_data', value=data_json.encode('utf-8'), callback=delivery_report)
    producer.flush()

print("Datos de prueba enviados a Kafka.")