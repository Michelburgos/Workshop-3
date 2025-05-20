import pandas as pd
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor
from sklearn.metrics import r2_score
import pickle

# Cargar dataset limpio
df = pd.read_csv('../data/clean_dataset.csv')

# Dividir datos (70%-30%)
X = df.drop(columns=['Happiness_Score'])
y = df['Happiness_Score']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# Entrenar modelo XGBoost
xgb_model = XGBRegressor(objective='reg:squarederror', random_state=42)
xgb_model.fit(X_train, y_train)

# Evaluar modelo
y_pred = xgb_model.predict(X_test)
r2 = r2_score(y_test, y_pred)
print(f'R² Score (model_prediction.py): {r2}')

# Guardar modelo
with open('../models/xgb_model.pkl', 'wb') as file:
    pickle.dump(xgb_model, file)

print("Modelo entrenado y guardado como xgb_model.pkl")