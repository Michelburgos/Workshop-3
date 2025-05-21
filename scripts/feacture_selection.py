import pandas as pd
import json

# Lista de archivos CSV originales
csv_files = {
    2015: "../data/2015.csv",
    2016: "../data/2016.csv",
    2017: "../data/2017.csv",
    2018: "../data/2018.csv",
    2019: "../data/2019.csv",
}

# Mapeo de columnas por año para estandarizar nombres
column_mappings = {
    2015: {
        'Country': 'Country_or_region',
        'Family': 'Social_support',
        'Happiness Rank': 'Happiness_Rank',
        'Happiness Score': 'Happiness_Score',
        'Economy (GDP per Capita)': 'GDP_per_capita',
        'Health (Life Expectancy)': 'Healthy_life_expectancy',
        'Trust (Government Corruption)': 'Perceptions_of_corruption'
    },
    2016: {
        'Country': 'Country_or_region',
        'Family': 'Social_support',
        'Happiness Rank': 'Happiness_Rank',
        'Happiness Score': 'Happiness_Score',
        'Economy (GDP per Capita)': 'GDP_per_capita',
        'Health (Life Expectancy)': 'Healthy_life_expectancy',
        'Trust (Government Corruption)': 'Perceptions_of_corruption'
    },
    2017: {
        'Country': 'Country_or_region',
        'Happiness.Score': 'Happiness_Score',
        'Family': 'Social_support',
        'Happiness.Rank': 'Happiness_Rank',
        'Economy..GDP.per.Capita.': 'GDP_per_capita',
        'Health..Life.Expectancy.': 'Healthy_life_expectancy',
        'Trust..Government.Corruption.': 'Perceptions_of_corruption',
        'Dystopia.Residual': 'Dystopia_Residual'
    },
    2018: {
        'Country or region': 'Country_or_region',
        'Score': 'Happiness_Score',
        'Social support': 'Social_support',
        'Overall rank': 'Happiness_Rank',
        'GDP per capita': 'GDP_per_capita',
        'Healthy life expectancy': 'Healthy_life_expectancy',
        'Freedom to make life choices': 'Freedom',
        'Perceptions of corruption': 'Perceptions_of_corruption'
    },
    2019: {
        'Country or region': 'Country_or_region',
        'Social support': 'Social_support',
        'Score': 'Happiness_Score',
        'Overall rank': 'Happiness_Rank',
        'GDP per capita': 'GDP_per_capita',
        'Healthy life expectancy': 'Healthy_life_expectancy',
        'Freedom to make life choices': 'Freedom',
        'Perceptions of corruption': 'Perceptions_of_corruption'
    }
}

# Lista para almacenar los DataFrames
dfs = []

# Procesar cada archivo CSV
for year, filepath in csv_files.items():
    df = pd.read_csv(filepath)
    
    # Estandarizar nombres de columnas según el año
    if year in column_mappings:
        df = df.rename(columns=column_mappings[year])
    
    # Agregar columna de año
    df['Year'] = year
    
    dfs.append(df)

# Unificar los DataFrames
df = pd.concat(dfs, ignore_index=True)

# Imputar valor faltante para United Arab Emirates (2018)
imputed_value = (0.32449 + 0.182) / 2  # Promedio de 2017 y 2019
df.loc[(df['Country_or_region'] == 'United Arab Emirates') & (df['Year'] == 2018), 'Perceptions_of_corruption'] = imputed_value
print(f"Valor imputado para United Arab Emirates (2018): {imputed_value}")
print(df[(df['Country_or_region'] == 'United Arab Emirates') & (df['Year'] == 2018)][['Country_or_region', 'Year', 'Perceptions_of_corruption']])

# Diccionario de mapeo de países a regiones
region_mapping = {
    'Switzerland': 'Western Europe', 'Iceland': 'Western Europe', 'Denmark': 'Western Europe', 'Norway': 'Western Europe',
    'Finland': 'Western Europe', 'Netherlands': 'Western Europe', 'Sweden': 'Western Europe', 'Austria': 'Western Europe',
    'Luxembourg': 'Western Europe', 'Ireland': 'Western Europe', 'Belgium': 'Western Europe', 'United Kingdom': 'Western Europe',
    'Germany': 'Western Europe', 'France': 'Western Europe', 'Spain': 'Western Europe', 'Malta': 'Western Europe',
    'Italy': 'Western Europe', 'Slovenia': 'Western Europe', 'Portugal': 'Western Europe', 'Greece': 'Western Europe',
    'Canada': 'North America', 'United States': 'North America', 'Puerto Rico': 'North America',
    'New Zealand': 'Australia and New Zealand', 'Australia': 'Australia and New Zealand',
    'Israel': 'Middle East and Northern Africa', 'United Arab Emirates': 'Middle East and Northern Africa',
    'Oman': 'Middle East and Northern Africa', 'Qatar': 'Middle East and Northern Africa', 'Saudi Arabia': 'Middle East and Northern Africa',
    'Kuwait': 'Middle East and Northern Africa', 'Bahrain': 'Middle East and Northern Africa', 'Libya': 'Middle East and Northern Africa',
    'Algeria': 'Middle East and Northern Africa', 'Turkey': 'Middle East and Northern Africa', 'Jordan': 'Middle East and Northern Africa',
    'Morocco': 'Middle East and Northern Africa', 'Tunisia': 'Middle East and Northern Africa', 'Palestinian Territories': 'Middle East and Northern Africa',
    'Iran': 'Middle East and Northern Africa', 'Iraq': 'Middle East and Northern Africa', 'Egypt': 'Middle East and Northern Africa',
    'Yemen': 'Middle East and Northern Africa', 'Lebanon': 'Middle East and Northern Africa', 'Syria': 'Middle East and Northern Africa',
    'Costa Rica': 'Latin America and Caribbean', 'Mexico': 'Latin America and Caribbean', 'Brazil': 'Latin America and Caribbean',
    'Venezuela': 'Latin America and Caribbean', 'Panama': 'Latin America and Caribbean', 'Chile': 'Latin America and Caribbean',
    'Argentina': 'Latin America and Caribbean', 'Uruguay': 'Latin America and Caribbean', 'Colombia': 'Latin America and Caribbean',
    'Suriname': 'Latin America and Caribbean', 'Trinidad and Tobago': 'Latin America and Caribbean', 'El Salvador': 'Latin America and Caribbean',
    'Guatemala': 'Latin America and Caribbean', 'Ecuador': 'Latin America and Caribbean', 'Bolivia': 'Latin America and Caribbean',
    'Paraguay': 'Latin America and Caribbean', 'Nicaragua': 'Latin America and Caribbean', 'Peru': 'Latin America and Caribbean',
    'Jamaica': 'Latin America and Caribbean', 'Dominican Republic': 'Latin America and Caribbean', 'Honduras': 'Latin America and Caribbean',
    'Haiti': 'Latin America and Caribbean', 'Belize': 'Latin America and Caribbean', 'Trinidad & Tobago': 'Latin America and Caribbean',
    'Singapore': 'Southeastern Asia', 'Thailand': 'Southeastern Asia', 'Malaysia': 'Southeastern Asia', 'Indonesia': 'Southeastern Asia',
    'Vietnam': 'Southeastern Asia', 'Philippines': 'Southeastern Asia', 'Laos': 'Southeastern Asia', 'Cambodia': 'Southeastern Asia',
    'Myanmar': 'Southeastern Asia',
    'Czech Republic': 'Central and Eastern Europe', 'Uzbekistan': 'Central and Eastern Europe', 'Slovakia': 'Central and Eastern Europe',
    'Kazakhstan': 'Central and Eastern Europe', 'Moldova': 'Central and Eastern Europe', 'Belarus': 'Central and Eastern Europe',
    'Poland': 'Central and Eastern Europe', 'Croatia': 'Central and Eastern Europe', 'Russia': 'Central and Eastern Europe',
    'North Cyprus': 'Central and Eastern Europe', 'Cyprus': 'Central and Eastern Europe', 'Kosovo': 'Central and Eastern Europe',
    'Turkmenistan': 'Central and Eastern Europe', 'Estonia': 'Central and Eastern Europe', 'Kyrgyzstan': 'Central and Eastern Europe',
    'Azerbaijan': 'Central and Eastern Europe', 'Montenegro': 'Central and Eastern Europe', 'Romania': 'Central and Eastern Europe',
    'Serbia': 'Central and Eastern Europe', 'Latvia': 'Central and Eastern Europe', 'Macedonia': 'Central and Eastern Europe',
    'Albania': 'Central and Eastern Europe', 'Bosnia and Herzegovina': 'Central and Eastern Europe', 'Hungary': 'Central and Eastern Europe',
    'Ukraine': 'Central and Eastern Europe', 'Bulgaria': 'Central and Eastern Europe', 'Armenia': 'Central and Eastern Europe',
    'Georgia': 'Central and Eastern Europe', 'Tajikistan': 'Central and Eastern Europe', 'Northern Cyprus': 'Central and Eastern Europe',
    'North Macedonia': 'Central and Eastern Europe',
    'Taiwan': 'Eastern Asia', 'Japan': 'Eastern Asia', 'South Korea': 'Eastern Asia', 'Hong Kong': 'Eastern Asia',
    'China': 'Eastern Asia', 'Mongolia': 'Eastern Asia', 'Taiwan Province of China': 'Eastern Asia',
    'Hong Kong S.A.R., China': 'Eastern Asia',
    'Nigeria': 'Sub-Saharan Africa', 'Zambia': 'Sub-Saharan Africa', 'Mozambique': 'Sub-Saharan Africa', 'Lesotho': 'Sub-Saharan Africa',
    'Swaziland': 'Sub-Saharan Africa', 'South Africa': 'Sub-Saharan Africa', 'Ghana': 'Sub-Saharan Africa', 'Zimbabwe': 'Sub-Saharan Africa',
    'Liberia': 'Sub-Saharan Africa', 'Sudan': 'Sub-Saharan Africa', 'Congo (Kinshasa)': 'Sub-Saharan Africa', 'Ethiopia': 'Sub-Saharan Africa',
    'Sierra Leone': 'Sub-Saharan Africa', 'Mauritania': 'Sub-Saharan Africa', 'Kenya': 'Sub-Saharan Africa', 'Djibouti': 'Sub-Saharan Africa',
    'Botswana': 'Sub-Saharan Africa', 'Malawi': 'Sub-Saharan Africa', 'Cameroon': 'Sub-Saharan Africa', 'Angola': 'Sub-Saharan Africa',
    'Mali': 'Sub-Saharan Africa', 'Congo (Brazzaville)': 'Sub-Saharan Africa', 'Comoros': 'Sub-Saharan Africa', 'Uganda': 'Sub-Saharan Africa',
    'Senegal': 'Sub-Saharan Africa', 'Gabon': 'Sub-Saharan Africa', 'Niger': 'Sub-Saharan Africa', 'Tanzania': 'Sub-Saharan Africa',
    'Madagascar': 'Sub-Saharan Africa', 'Central African Republic': 'Sub-Saharan Africa', 'Chad': 'Sub-Saharan Africa',
    'Guinea': 'Sub-Saharan Africa', 'Ivory Coast': 'Sub-Saharan Africa', 'Burkina Faso': 'Sub-Saharan Africa', 'Rwanda': 'Sub-Saharan Africa',
    'Benin': 'Sub-Saharan Africa', 'Burundi': 'Sub-Saharan Africa', 'Togo': 'Sub-Saharan Africa', 'Mauritius': 'Sub-Saharan Africa',
    'Somalia': 'Sub-Saharan Africa', 'Namibia': 'Sub-Saharan Africa', 'South Sudan': 'Sub-Saharan Africa', 'Somaliland Region': 'Sub-Saharan Africa',
    'Gambia': 'Sub-Saharan Africa',
    'Bhutan': 'Southern Asia', 'Pakistan': 'Southern Asia', 'Bangladesh': 'Southern Asia', 'India': 'Southern Asia',
    'Nepal': 'Southern Asia', 'Sri Lanka': 'Southern Asia', 'Afghanistan': 'Southern Asia', 'Lithuania': 'Central and Eastern Europe',
    'Somaliland region': 'Sub-Saharan Africa'
}

# Agregar columna Region
df['Region'] = df['Country_or_region'].map(region_mapping)

# Verificar regiones
print("Regiones únicas:", df['Region'].unique())
print("Distribución de regiones:\n", df['Region'].value_counts())

# Aplicar One-Hot Encoding a la columna Region
df_encoded = pd.get_dummies(df, columns=['Region'], prefix='Region')

# Reemplazar espacios por guiones bajos en nombres de columnas
df.columns = [col.replace(' ', '_') for col in df.columns]
df_encoded.columns = [col.replace(' ', '_') for col in df_encoded.columns]

# Eliminar columnas innecesarias
df = df.drop(columns=['Country_or_region', 'Happiness_Rank'], errors='ignore')
df_encoded = df_encoded.drop(columns=['Country_or_region', 'Happiness_Rank'], errors='ignore')

# Convertir columnas booleanas a enteros (0/1)
for col in df_encoded.columns:
    if df_encoded[col].dtype == bool:
        df_encoded[col] = df_encoded[col].astype(int)

# Guardar datos transformados en un archivo para kafka_producer.py
with open('transformed_data.json', 'w') as f:
    json.dump(df_encoded.to_dict(orient='records'), f)

print("Datos transformados generados y listos para streaming.")