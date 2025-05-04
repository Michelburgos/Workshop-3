import pandas as pd

def preprocess_data():
    # Cargar datos
    df_2015 = pd.read_csv('../data/raw/happiness_2015.csv')
    df_2016 = pd.read_csv('../data/raw/happiness_2016.csv')
    df_2017 = pd.read_csv('../data/raw/happiness_2017.csv')
    df_2018 = pd.read_csv('../data/raw/happiness_2018.csv')
    df_2019 = pd.read_csv('../data/raw/happiness_2019.csv')

    # Seleccionar columnas comunes
    selected_features = ['GDP_per_capita', 'Life_Expectancy', 'Social_Support', 'Freedom']
    common_columns = selected_features + ['Happiness_Score']

    # Agregar columna de año
    df_2015['Year'] = 2015
    df_2016['Year'] = 2016
    df_2017['Year'] = 2017
    df_2018['Year'] = 2018
    df_2019['Year'] = 2019

    # Combinar datos
    combined_df = pd.concat([
        df_2015[common_columns + ['Year']],
        df_2016[common_columns + ['Year']],
        df_2017[common_columns + ['Year']],
        df_2018[common_columns + ['Year']],
        df_2019[common_columns + ['Year']]
    ], ignore_index=True)

    # Manejar valores faltantes
    combined_df.fillna(combined_df.mean(), inplace=True)

    # Guardar datos combinados
    combined_df.to_csv('../data/processed/combined_data.csv', index=False)
    print('Datos preprocesados y guardados en ../data/processed/combined_data.csv')

if __name__ == '__main__':
    preprocess_data()