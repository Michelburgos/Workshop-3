import pandas as pd
import json
import logging
from sklearn.model_selection import train_test_split

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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
    try:
        df = pd.read_csv(filepath)
        if year in column_mappings:
            df = df.rename(columns=column_mappings[year])
        df['Year'] = year
        dfs.append(df)
        logging.info(f"Procesado archivo {filepath}")
    except FileNotFoundError:
        logging.error(f"No se encontró el archivo {filepath}")
        continue

# Unificar los DataFrames
df = pd.concat(dfs, ignore_index=True)

selected_features = ['Freedom', 'Generosity', 'Happiness_Rank', 'Country_or_region',
                     'Perceptions_of_corruption', 'GDP_per_capita', 'Healthy_life_expectancy',
                     'Happiness_Score', 'Social_support', 'Year', 'Region']

df = df[selected_features]
# Imputar valor faltante
imputed_value = (0.32449 + 0.182) / 2
df.loc[(df['Country_or_region'] == 'United Arab Emirates') & (df['Year'] == 2018), 'Perceptions_of_corruption'] = imputed_value
logging.info(f"Valor imputado para United Arab Emirates (2018): {imputed_value}")
logging.info(f"Datos imputados:\n{df[(df['Country_or_region'] == 'United Arab Emirates') & (df['Year'] == 2018)][['Country_or_region', 'Year', 'Perceptions_of_corruption']]}")

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
         'Nigeria': 'Sub_Saharan Africa', 'Zambia': 'Sub_Saharan Africa', 'Mozambique': 'Sub_Saharan Africa', 'Lesotho': 'Sub_Saharan Africa',
         'Swaziland': 'Sub_Saharan Africa', 'South Africa': 'Sub_Saharan Africa', 'Ghana': 'Sub_Saharan Africa', 'Zimbabwe': 'Sub_Saharan Africa',
         'Liberia': 'Sub_Saharan Africa', 'Sudan': 'Sub_Saharan Africa', 'Congo (Kinshasa)': 'Sub_Saharan Africa', 'Ethiopia': 'Sub_Saharan Africa',
         'Sierra Leone': 'Sub_Saharan Africa', 'Mauritania': 'Sub_Saharan Africa', 'Kenya': 'Sub_Saharan Africa', 'Djibouti': 'Sub_Saharan Africa',
         'Botswana': 'Sub_Saharan Africa', 'Malawi': 'Sub_Saharan Africa', 'Cameroon': 'Sub_Saharan Africa', 'Angola': 'Sub_Saharan Africa',
         'Mali': 'Sub_Saharan Africa', 'Congo (Brazzaville)': 'Sub_Saharan Africa', 'Comoros': 'Sub_Saharan Africa', 'Uganda': 'Sub_Saharan Africa',
         'Senegal': 'Sub_Saharan Africa', 'Gabon': 'Sub_Saharan Africa', 'Niger': 'Sub_Saharan Africa', 'Tanzania': 'Sub_Saharan Africa',
         'Madagascar': 'Sub_Saharan Africa', 'Central African Republic': 'Sub_Saharan Africa', 'Chad': 'Sub_Saharan Africa',
         'Guinea': 'Sub_Saharan Africa', 'Ivory Coast': 'Sub_Saharan Africa', 'Burkina Faso': 'Sub_Saharan Africa', 'Rwanda': 'Sub_Saharan Africa',
         'Benin': 'Sub_Saharan Africa', 'Burundi': 'Sub_Saharan Africa', 'Togo': 'Sub_Saharan Africa', 'Mauritius': 'Sub_Saharan Africa',
         'Somalia': 'Sub_Saharan Africa', 'Namibia': 'Sub_Saharan Africa', 'South Sudan': 'Sub_Saharan Africa', 'Somaliland Region': 'Sub_Saharan Africa',
         'Gambia': 'Sub_Saharan Africa',
         'Bhutan': 'Southern Asia', 'Pakistan': 'Southern Asia', 'Bangladesh': 'Southern Asia', 'India': 'Southern Asia',
         'Nepal': 'Southern Asia', 'Sri Lanka': 'Southern Asia', 'Afghanistan': 'Southern Asia', 'Lithuania': 'Central and Eastern Europe',
         'Somaliland region': 'Sub_Saharan Africa'
}

# Agregar columna Region
df['Region'] = df['Country_or_region'].map(region_mapping)

# Verificar regiones
logging.info(f"Regiones únicas: {df['Region'].unique()}")
logging.info(f"Distribución de regiones:\n{df['Region'].value_counts()}")

# One-Hot Encoding
df_encoded = pd.get_dummies(df, columns=['Region'], prefix='Region')

# Reemplazar espacios en nombres de columnas
df.columns = [col.replace(' ', '_') for col in df.columns]
df_encoded.columns = [col.replace(' ', '_') for col in df_encoded.columns]

# Eliminar columnas innecesarias
df = df.drop(columns=['Country_or_region', 'Happiness_Rank'], errors='ignore')
df_encoded = df_encoded.drop(columns=['Country_or_region', 'Happiness_Rank'], errors='ignore')

# Convertir booleanos a enteros
for col in df_encoded.select_dtypes(include='bool').columns:
    df_encoded[col] = df_encoded[col].astype(int)


# Dividir los datos en entrenamiento (70%) y prueba (30%)
X = df_encoded.drop(columns=['Happiness_Score'])
y = df_encoded['Happiness_Score']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=50)
logging.info(f"Datos divididos: {len(X_train)} para entrenamiento, {len(X_test)} para prueba")

# Combinar X_test y y_test para guardar el conjunto de prueba
test_data = X_test.copy()
test_data['Happiness_Score'] = y_test


# Guardar a JSON
with open('../data/transformed_data.json', 'w') as f:
    json.dump(test_data.to_dict(orient='records'), f)

logging.info("Datos transformados generados y listos para streaming.")
