# model.py
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import joblib
import os

class AirQualityModel:
    def __init__(self, model_path='co_model.pkl', data_path='training_data.csv'):
        self.model_path = model_path
        self.data_path = data_path
        self.model = None

    def _feature_engineering(self, df):
        # Combine Date and Time into a single datetime index
        df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
        df.set_index('Datetime', inplace=True)

        # Time-based features
        df['hour'] = df.index.hour
        df['dayofweek'] = df.index.dayofweek
        df['month'] = df.index.month

        # Lagged and rolling features for pollutants
        pollutants = ['CO(GT)', 'NOx(GT)', 'NO2(GT)', 'C6H6(GT)', 'NMHC(GT)']
        for col in pollutants:
            df[f'{col}_lag1'] = df[col].shift(1)
            df[f'{col}_lag2'] = df[col].shift(2)
            df[f'{col}_rolling_mean3'] = df[col].rolling(window=3).mean()
            df[f'{col}_rolling_std3'] = df[col].rolling(window=3).std()

        df.dropna(inplace=True)
        return df

    def train_model(self):
        df = pd.read_csv(self.data_path)
        df = self._feature_engineering(df)

        target = 'CO(GT)'
        features = [col for col in df.columns if col not in ['CO(GT)', 'Date', 'Time']]

        # Chronological split
        train = df.iloc[:-24]
        test = df.iloc[-24:]

        self.model = RandomForestRegressor(n_estimators=100, random_state=42)
        self.model.fit(train[features], train[target])

        # Evaluation
        preds = self.model.predict(test[features])
        mae = mean_absolute_error(test[target], preds)
        rmse = np.sqrt(mean_squared_error(test[target], preds))

        print(f"Model Evaluation - MAE: {mae:.3f}, RMSE: {rmse:.3f}")
        joblib.dump(self.model, self.model_path)
        return self.model

    def predict(self, row):
        if self.model is None:
            self.model = joblib.load(self.model_path)

        df = pd.read_csv(self.data_path)
        df = pd.concat([df, row])
        df = self._feature_engineering(df)
        row = df.iloc[[-1]]

        features = [col for col in row.columns if col not in ['CO(GT)', 'Date', 'Time']]
        prediction = self.model.predict(row[features])[0]
        return prediction

# Usage:
# model = AirQualityModel()
# model.train_model()
# pred = model.predict(new_cleaned_row_df)
