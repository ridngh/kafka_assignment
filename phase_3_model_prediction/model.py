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


    def replace_missing_with_mean(self, df):
        # Forward fill only numeric columns
        df[df.select_dtypes(include='number').columns] = df.select_dtypes(include='number').ffill()
        return df

    def _feature_engineering(self, df):
        # Combine Date and Time into a single datetime index
        df = df.copy()
        df.loc[:,'Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
        df.set_index('Datetime', inplace=True)

        # Time-based features
        df.loc[:,'hour'] = df.index.hour
        df.loc[:,'dayofweek'] = df.index.dayofweek
        df.loc[:,'month'] = df.index.month

        # Replace missing values by forward fill
        df = self.replace_missing_with_mean(df)


        # print(df)
        # Lagged and rolling features for pollutants
        pollutants = ['CO(GT)', 'NOx(GT)', 'NO2(GT)', 'C6H6(GT)', 'NMHC(GT)']
        

        for col in pollutants:
            df.loc[:, f'{col}_lag1'] = df[col].shift(1)
            df.loc[:,f'{col}_lag2'] = df[col].shift(2)
            df.loc[:,f'{col}_rolling_mean3'] = df[col].rolling(window=3).mean()
            df.loc[:,f'{col}_rolling_std3'] = df[col].rolling(window=3).std()

        # print(df)

        return df

    def train_model(self):
        df = pd.read_csv(self.data_path)
        df = self._feature_engineering(df)

        target = 'CO(GT)'  # This is the target for prediction at t+1
        # Shift the target column for the next hour prediction (t+1)
        df.loc[:,'CO(GT)_next_hour'] = df[target].shift(-1)  # t+1 target

        # Drop the last row since it has no t+1 value
        df.dropna(subset=['CO(GT)_next_hour'], inplace=True)

        features = [col for col in df.columns if col not in [target, 'Date', 'Time', 'CO(GT)_next_hour']]

        len_df = len(df)

        # Chronological split (train on all but the last hour for prediction)
        train = df.iloc[:round(0.7*len_df)]  # Remove last row for test (we predict for the next hour)
        test = df.iloc[round(0.7*len_df):]   # Take the last row as the test case for prediction

        self.model = RandomForestRegressor(n_estimators=100, random_state=42)
        self.model.fit(train[features], train['CO(GT)_next_hour'])

        # Evaluation
        preds = self.model.predict(test[features])
        mae = mean_absolute_error(test['CO(GT)_next_hour'], preds)
        rmse = np.sqrt(mean_squared_error(test['CO(GT)_next_hour'], preds))

        print(f"Model Evaluation - MAE: {mae:.3f}, RMSE: {rmse:.3f}")
        joblib.dump(self.model, self.model_path)
        return self.model

    def predict(self, df):
        # if self.model is None:
        #     self.model = joblib.load(self.model_path)
    
        features = [col for col in df.columns if col not in ['CO(GT)', 'Date', 'Time']]

        prediction = self.model.predict(df[features])[0]
        return prediction

# Usage:
# model = AirQualityModel()
# model.train_model()
# pred = model.predict(new_cleaned_row_df)