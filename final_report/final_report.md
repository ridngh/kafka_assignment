# Air Quality Streaming Pipeline: Final Report

This report summarizes the end-to-end strategy used to stream, preprocess, and analyze air quality data using Apache Kafka. The pipeline is designed to simulate real-time data processing and support both exploratory analysis and predictive modeling for environmental monitoring applications.

---

## Kafka Setup (Windows with Git Bash)

Kafka version 4.0 and Java 17 were installed and configured on a Windows machine using Git Bash. To avoid path length issues, Kafka was unzipped near the root directory. A standalone Kafka server was set up using KRaft mode with appropriate log directory permissions. Configuration challenges related to path resolution were handled by explicitly using `file:///` prefixes. The metadata for the standalone server was initialized using a randomly generated UUID.

---

## Data Streaming Strategy

- **Producer Side:**  
  The producer reads the air quality dataset row by row and sends each record as a JSON-encoded message to the Kafka topic `aq_data`. Missing values were replaced with `None` to reflect real-world sensor behavior without applying statistical imputation, which is more suitable on the consumer side when context is available.

- **Consumer Side:**  
  The consumer listens to the `aq_data` topic and appends the incoming records to multiple datasets:
  - `consumed_air_quality.csv` for EDA
  - `training_data.csv` for model training
  - `model_comparison_results.csv` for performance tracking

  The script also maintains a buffer of the last 10 records for context-aware prediction and feature generation. Forward fill is used as the imputation strategy just before making predictions, preserving continuity in the data stream without relying on full-dataset statistics.

---

## Exploratory Data Analysis (EDA)

The EDA focused on identifying temporal patterns, data quality issues, and relationships between pollutants. Key insights included:

- **Sensor Downtime:** Gaps in all sensor readings occurred simultaneously, suggesting systemic outages.
- **Hourly Patterns:** Pollutant concentrations peaked during morning and evening rush hours, likely due to traffic and industrial activity.
- **Daily Trends:** Levels were higher on weekdays, decreasing over weekends.
- **Correlations:** Strong relationships were observed between certain pollutants, such as CO and C6H6 (0.93), and CO and NOx (0.80), indicating shared sources.

---

## Modeling and Evaluation Strategy

A modular consumer design supports continuous model retraining using rolling 60-day windows. After each prediction, the model’s performance is compared with previous versions using MAE and RMSE metrics. These comparisons are logged to track improvements over time and support model selection decisions.

---

## Conclusion

This streaming pipeline demonstrates a scalable, modular approach to real-time air quality monitoring. By prioritizing context-aware processing and minimizing assumptions at the ingestion stage, the system remains flexible for both analysis and machine learning applications. The strategy reflects real-world constraints of streaming environments, such as handling incomplete data and needing to retrain models periodically based on recent trends.

