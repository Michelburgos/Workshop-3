import pandas as pd
import json

# Lista de archivos CSV originales
csv_files = {
    2015: "../data/2015.csv",
    2016: "../data/2016.csv",
    2017: "../data/2017.csv"
}

# Diccionario para mapear países a regiones
region_mapping = {}
for year, filepath in csv_files.items():
    df = pd.read_csv(filepath)
    if 'Region' in df.columns:
        for country, region in zip(df['Country'], df['Region']):
            region_mapping[country] = region

# Lista para almacenar los DataFrames
dfs = []

# Procesar cada archivo CSV
for year, filepath in csv_files.items():
    df = pd.read_csv(filepath)
    
    # Estandarizar nombres de columnas según el año
    if year == 2015 or year == 2016:
        df = df.rename(columns={
            'Country': 'Country_or_region',
            'Economy (GDP per Capita)': 'GDP_per_capita',
            'Family': 'Social_support',
            'Health (Life Expectancy)': 'Healthy_life_expectancy',
            'Trust (Government Corruption)': 'Perceptions_of_corruption',
            'Happiness Score': 'Happiness_Score',
            'Happiness Rank': 'Happiness_Rank'
        })
    elif year == 2017:
        df = df.rename(columns={
            'Happiness.Score': 'Happiness_Score',
            'Happiness.Rank': 'Happiness_Rank',
            'Economy..GDP.per.Capita.': 'GDP_per_capita',
            'Health..Life.Expectancy.': 'Healthy_life_expectancy',
            'Trust..Government.Corruption.': 'Perceptions_of_corruption'
        })
    
    # Agregar columna de año
    df['Year'] = year
    
    # Mapear regiones usando el diccionario
    df['Region'] = df['Country_or_region'].map(region_mapping)
    
    # Eliminar columnas innecesarias
    df = df.drop(columns=['Country_or_region', 'Happiness_Rank'])
    
    # Reemplazar espacios por guiones bajos en nombres de columnas
    df.columns = [col.replace(' ', '_') for col in df.columns]
    
    dfs.append(df)

# Unificar los DataFrames
df = pd.concat(dfs, ignore_index=True)

# Aplicar One-Hot Encoding a la columna Region
df_encoded = pd.get_dummies(df, columns=['Region'], prefix='Region')

# Convertir columnas booleanas a enteros (0/1)
for col in df_encoded.columns:
    if df_encoded[col].dtype == bool:
        df_encoded[col] = df_encoded[col].astype(int)

# Guardar datos transformados en un archivo para kafka_producer.py
with open('transformed_data.json', 'w') as f:
    json.dump(df_encoded.to_dict(orient='records'), f)

print("Datos transformados generados y listos para streaming.")