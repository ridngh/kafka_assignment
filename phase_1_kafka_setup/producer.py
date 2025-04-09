from kafka import KafkaProducer
import json
import time
import pandas as pd
import numpy as np

# Function to send messages to the Kafka topic
def send_message(df):
    # Iterate through the rows of the dataframe
    for index, row in df.iterrows():

        # Convert the cleaned row into a dictionary and send to Kafka
        message = row.to_dict()  # Convert row to dictionary
        producer.send('aq_data', message)  # Send the message to Kafka topic
        print(f"Sent: {message}")
        time.sleep(0.01)  # Pause for 0.1 second between messages

if __name__ == '__main__':

    
    # Initialize the Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],  # Kafka server(s)
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data to JSON
    )

    # Load the dataset
    df = pd.read_excel('AirQualityUCI.xlsx', sheet_name='AirQualityUCI')

    # Convert 'Date' to datetime type and 'Time' to time type
    # df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y').dt.date  # Convert to date
    # df['Time'] = pd.to_datetime(df['Time'], format='%H:%M:%S').dt.time  # Convert to time

    df['Date'] = df['Date'].astype('str')
    df['Time'] = df['Time'].astype('str')

    # Replace -200 with NaN
    df.replace('-200', np.nan, inplace=True)
    df.replace(-200, np.nan, inplace=True)

    # Convert the rest of the columns to numeric (ignoring 'Date' and 'Time')
    df.iloc[:, 2:] = df.iloc[:, 2:].apply(pd.to_numeric, errors='coerce')

    send_message(df)

    # Flush to ensure all messages are sent before exiting
    time.sleep(5)
    producer.flush()

    # Close the producer
    producer.close()