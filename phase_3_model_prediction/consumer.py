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
# column_values = {}  # Store valid numeric values to compute means

# def update_column_means(row_series):
#     for col, val in row_series.items():
#         if col not in column_values:
#             column_values[col] = []
#         column_values[col].append(val)

# def replace_missing_with_mean(row_series):
#     for col in row_series.index:
#         try:
#             if pd.isna(row_series[col]) and col in column_values and column_values[col]:
#                 mean_val = np.mean(column_values[col])
#                 row_series[col] = mean_val
#         except Exception:
#             continue
#     return row_series

def save_to_csv4eda(record):
    try:
        df = pd.DataFrame([record])
        write_header = not os.path.exists(output_file) or os.stat(output_file).st_size == 0
        df.to_csv(output_file, mode='a', header=write_header, index=False)
    except Exception as e:
        print(f"Error saving to CSV: {e}")

def save_to_csv4model(record):
    try:
        record = pd.DataFrame([record])
        write_header = not os.path.exists('training_data.csv') or os.stat('training_data.csv').st_size == 0
        record.to_csv('training_data.csv', mode='a', header=write_header, index=False)

    except Exception as e:
        print(f"Error saving to CSV: {e}")

def save_to_csv4model_compare(comparison_data):
    print('In save to CSV')
    comparison_df = pd.DataFrame([comparison_data])
    comparison_df.to_csv('model_comparison_results.csv', mode='a', header=not os.path.exists('model_comparison_results.csv'), index=False)

def compare_models(model_id, current_date, cleaned_row, prev_model_pred, curr_model_pred):
    print('Inside compare models')
    # Comparison of baseline vs. current model predictions
    if model_id != 1:
        print('Starting Error Calculation')
        print(cleaned_row['CO(GT)'],prev_model_pred, curr_model_pred)
        prev_model_error = mean_absolute_error([cleaned_row['CO(GT)']], [prev_model_pred])
        curr_model_error = mean_absolute_error([cleaned_row['CO(GT)']], [curr_model_pred])
        prev_model_rmse = np.sqrt(mean_squared_error([cleaned_row['CO(GT)']], [prev_model_pred]))
        curr_model_rmse = np.sqrt(mean_squared_error([cleaned_row['CO(GT)']], [curr_model_pred]))
        print('Error Calculation Done')

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
        print('Saved to CSV')
    else:
        print('Starting Error Calculation')
        print(cleaned_row['CO(GT)'], curr_model_pred)
        curr_model_error = mean_absolute_error([cleaned_row['CO(GT)']], [curr_model_pred])
        curr_model_rmse = np.sqrt(mean_squared_error([cleaned_row['CO(GT)']], [curr_model_pred]))
        print('Error Calculation Done')

        comparison_data = {
            'Date': current_date,
            'Previous Model id': model_id - 1,
            'Previous Model Prediction': np.nan,
            'Previous Model MAE': np.nan,
            'Current Model id': model_id,
            'Current Model Prediction': curr_model_pred,
            'Previous Model MAE': np.nan,
            'Current Model MAE': curr_model_error,
            'Previous Model RMSE': np.nan,
            'Current Model RMSE': curr_model_rmse
        }
        save_to_csv4model_compare(comparison_data)


def consume_message(first_date):
    print("Starting Kafka consumer...")
    last_model_update_dt = first_date

    # Initialize the model once outside of the loop to avoid reloading on each iteration
    model_instance = AirQualityModel()
    buffer_df = pd.DataFrame()  # Stores rolling window of past messages
    message_id = 0



    for message in consumer:
        print(f'-----------------------------------------{message_id}----------------------------------------')
        raw_data = message.value

        try:
            row = pd.Series(raw_data)

            # update_column_means(row)
            # cleaned_row = replace_missing_with_mean(row)

            save_to_csv4eda(row)

            current_date = pd.to_datetime(row['Date'] + ' ' + row['Time'])
            
            # Append new row to buffer
            buffer_df = pd.concat([buffer_df, pd.DataFrame([row])], ignore_index=True)

            # Keep only the most recent N rows (e.g., 10 or 20 rows depending on lag window)
            buffer_df = buffer_df.tail(10).reset_index(drop=True)

            # Apply feature engineering to the whole buffer
            fe_df = AirQualityModel()._feature_engineering(buffer_df.copy())
            buffer_df = fe_df.loc[:,:'AH'].copy()
            
            print(fe_df)

            # Now get the last row (latest one, after feature engineering)
            cleaned_row_fe = fe_df.iloc[[-1]]
            print(cleaned_row_fe.iloc[:5])
            # print(cleaned_row_fe.iloc[5:9])
            # print(cleaned_row_fe.iloc[9:])

            if last_model_update_dt == first_date:
                if current_date < first_date + pd.Timedelta(days=60):
                    # saving data for first iteration of model training
                    save_to_csv4model(row)
                    print(f'{row["Date"]}: In first 60 days, saved for model')

                elif current_date == (first_date + pd.Timedelta(days=60)):
                    # first trained model
                    print('--------------------------------------TRAINING FIRST MODEL----------------------------')

                    model_instance.train_model()
                    print(f'-------------------------------------------{row["Date"]}: Trained 1st model------------------------')
                    print(f"Type of model: {type(model_instance)}")

                    print(f'-------------------------------------------{row["Date"]}: Prediction made------------------------')
                    curr_model_pred = model_instance.predict(cleaned_row_fe)  # Prediction using the current model
                    print(f'-------------------------------------------{row["Date"]}: Prediction made------------------------')
                    last_model_update_dt = current_date  
                    model_id = 1
                    prev_model_pred = np.nan
                    with open('models_file.txt', 'w') as file:
                        file.write(f"{model_id}, {current_date.strftime('%Y-%m-%d')}\n")

                    compare_models(model_id, current_date, cleaned_row_fe, prev_model_pred, curr_model_pred)
                    print(f'-------------------------------------------{row["Date"]}: Compared model------------------------')


            else:
                if current_date < last_model_update_dt + pd.Timedelta(days=60):
                    # for each cycle of 60 days, save data for re-training and predict using current model
                    save_to_csv4model(row)
                    print(f'{row["Date"]}: In cycle of 60 days, saved for model')

                    prev_model_pred = model_instance.predict(cleaned_row_fe)
                    curr_model_pred = model_instance.predict(cleaned_row_fe)
                    print('Prediction done')
                    compare_models(model_id, current_date, row, prev_model_pred, curr_model_pred)

                elif current_date == (last_model_update_dt + pd.Timedelta(days=60)):
                    # re-training model in cycles of 60 days
                    prev_model = model_instance
                    model_instance.train_model()
                    print(f'{row["Date"]}: Trained model {model_id+1}')
                    prev_model_pred = prev_model.predict(cleaned_row_fe)
                    curr_model_pred = model_instance.predict(cleaned_row_fe)
                    last_model_update_dt = current_date  # Update model refresh point
                    model_id = model_id + 1
                    with open('models_file.txt', 'a') as file:
                        file.write(f"{model_id}, {current_date.strftime('%Y-%m-%d')}\n")

                    compare_models(model_id, current_date, cleaned_row_fe, prev_model_pred, curr_model_pred)
            
            message_id = message_id + 1

        except Exception as e:
            print(f"Error processing message: {e}")

if __name__ == '__main__':
    first_date = pd.to_datetime("2004-03-10").normalize()
    consume_message(first_date)
