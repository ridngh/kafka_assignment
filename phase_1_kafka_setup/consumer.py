from kafka import KafkaConsumer
import json
import pandas as pd
import os
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error
from model import AirQualityModel  # Ensure that this import is correct


# Kafka consumer setup
consumer = KafkaConsumer(
    'aq_data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

output_file = "consumed_air_quality.csv"
column_values = {}  # Store valid numeric values to compute means

def update_column_means(row_series):
    for col, val in row_series.items():
        if col not in column_values:
            column_values[col] = []
        column_values[col].append(val)

def replace_missing_with_mean(row_series):
    for col in row_series.index:
        try:
            if pd.isna(row_series[col]) and col in column_values and column_values[col]:
                mean_val = np.mean(column_values[col])
                row_series[col] = mean_val
        except Exception:
            continue
    return row_series

def save_to_csv4eda(record):
    try:
        df = pd.DataFrame([record])
        write_header = not os.path.exists(output_file) or os.stat(output_file).st_size == 0
        df.to_csv(output_file, mode='a', header=write_header, index=False)
    except Exception as e:
        print(f"Error saving to CSV: {e}")

def save_to_csv4model(record):
    try:
        # Check if the record is a DataFrame; if it's a Series, convert it into a DataFrame
        if isinstance(record, pd.Series):
            record = record.to_frame().T  # Convert to DataFrame if it's a Series

        # Save the cleaned DataFrame to CSV
        write_header = not os.path.exists('training_data.csv') or os.stat('training_data.csv').st_size == 0
        record.to_csv('training_data.csv', mode='a', header=write_header, index=False)
    except Exception as e:
        print(f"Error saving to CSV: {e}")

def save_to_csv4model_compare(comparison_data):
    comparison_df = pd.DataFrame([comparison_data])
    comparison_df.to_csv('model_comparison_results.csv', mode='a', header=not os.path.exists('model_comparison_results.csv'), index=False)

def compare_models(model_id, current_date, cleaned_row, prev_model_pred, curr_model_pred):
    # Comparison of baseline vs. current model predictions
    if model_id != 1:
        prev_model_error = mean_absolute_error([cleaned_row['CO(GT)']], [prev_model_pred])
        curr_model_error = mean_absolute_error([cleaned_row['CO(GT)']], [curr_model_pred])
        prev_model_rmse = np.sqrt(mean_squared_error([cleaned_row['CO(GT)']], [prev_model_pred]))
        curr_model_rmse = np.sqrt(mean_squared_error([cleaned_row['CO(GT)']], [curr_model_pred]))

        comparison_data = {
            'Date': current_date,
            'Previous Model id': model_id - 1,
            'Previous Model Prediction': prev_model_pred,
            'Current Model id': model_id,
            'Current Model Prediction': curr_model_pred,
            'Previous Model MAE': prev_model_error,
            'Current Model MAE': curr_model_error,
            'Previous Model RMSE': prev_model_rmse,
            'Current Model RMSE': curr_model_rmse
        }
        save_to_csv4model_compare(comparison_data)
    else:
        curr_model_error = mean_absolute_error([cleaned_row['CO(GT)']], [curr_model_pred])
        curr_model_rmse = np.sqrt(mean_squared_error([cleaned_row['CO(GT)']], [curr_model_pred]))

        comparison_data = {
            'Date': current_date,
            'Previous Model id': model_id - 1,
            'Previous Model Prediction': np.NaN,
            'Previous Model MAE': np.NaN,
            'Current Model id': model_id,
            'Current Model Prediction': curr_model_pred,
            'Previous Model MAE': np.NaN,
            'Current Model MAE': curr_model_error,
            'Previous Model RMSE': np.NaN,
            'Current Model RMSE': curr_model_rmse
        }
        save_to_csv4model_compare(comparison_data)


def consume_message(first_date):
    print("Starting Kafka consumer...")
    last_model_update_dt = first_date

    # Initialize the model once outside of the loop to avoid reloading on each iteration
    model_instance = AirQualityModel()

    for message in consumer:
        raw_data = message.value
        print(f"Raw message: {raw_data}")

        try:
            row = pd.Series(raw_data)

            update_column_means(row)
            cleaned_row = replace_missing_with_mean(row)
            print(f"Cleaned row: {cleaned_row.to_dict()}")
            save_to_csv4eda(cleaned_row)
            print('Saved for eda')

            current_date = pd.to_datetime(cleaned_row['Date'])

            # Feature engineering before prediction
            cleaned_row = model_instance._feature_engineering(pd.DataFrame([cleaned_row]))  # Feature engineering applied

            if last_model_update_dt == first_date:
                if current_date < first_date + pd.Timedelta(days=60):
                    # saving data for first iteration of model training
                    save_to_csv4model(cleaned_row)
                    print('In first 60 days, saved for model')

                elif current_date == first_date + pd.Timedelta(days=60):
                    # first trained model
                    print('--------------------------------------TRAINING FIRST MODEL----------------------------')

                    model = model_instance.train_model()
                    curr_model_pred = model.predict(pd.DataFrame([cleaned_row]))  # Prediction using the current model
                    last_model_update_dt = current_date  
                    model_id = 1
                    prev_model_pred = np.NaN
                    with open('models_file.txt', 'w') as file:
                        file.write(f"{model_id}, {current_date.strftime('%Y-%m-%d')}\n")

                    compare_models(model_id, current_date, cleaned_row, prev_model_pred, curr_model_pred)


            else:
                if current_date < last_model_update_dt + pd.Timedelta(days=60):
                    # for each cycle of 30 days, save data for re-training and predict using current model
                    save_to_csv4model(cleaned_row)
                    print('In cycle of 60 days, saved for model')
                    prev_model_pred = model.predict(pd.DataFrame([cleaned_row]))
                    curr_model_pred = model.predict(pd.DataFrame([cleaned_row]))
                    compare_models(model_id, current_date, cleaned_row, prev_model_pred, curr_model_pred)

                elif current_date == last_model_update_dt + pd.Timedelta(days=60):
                    # re-training model in cycles of 30 days
                    prev_model = model
                    model = model_instance.train_model()
                    prev_model_pred = prev_model.predict(pd.DataFrame([cleaned_row]))
                    curr_model_pred = model.predict(pd.DataFrame([cleaned_row]))
                    last_model_update_dt = current_date  # Update model refresh point
                    model_id = model_id + 1
                    with open('models_file.txt', 'a') as file:
                        file.write(f"{model_id}, {current_date.strftime('%Y-%m-%d')}\n")

                    compare_models(model_id, current_date, cleaned_row, prev_model_pred, curr_model_pred)

        except Exception as e:
            print(f"Error processing message: {e}")

if __name__ == '__main__':
    first_date = pd.to_datetime("2004-03-10")
    consume_message(first_date)
