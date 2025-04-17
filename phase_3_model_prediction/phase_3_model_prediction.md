# Consumer Script Overview

This consumer script ingests air quality data from a Kafka topic and processes it for exploratory data analysis (EDA), model training, and model performance comparison. It is designed to handle streaming data, apply basic preprocessing, and manage datasets for various analytical and modeling tasks.

---

## Logged Datasets

- **EDA Dataset**  
  Each incoming record is appended to `consumed_air_quality.csv`. This dataset is used for exploratory data analysis and visualization of pollutant trends.

- **Model Training Dataset**  
  Records are also saved to `training_data.csv`, which is used to train air quality prediction models. Data is collected in rolling 60-day windows to support periodic model retraining.

- **Model Comparison Dataset**  
  After each prediction, model performance metrics (MAE and RMSE) for both the current and previous models are logged to `model_comparison_results.csv`. This enables systematic tracking of model improvements over time.

---

## Imputation Strategy

Before making predictions, the script uses forward fill as the imputation strategy to handle missing values. Feature engineering is then applied using the model's internal method to generate rolling features.

---

## Rolling Window Buffer

The script maintains a buffer of the last 10 records to enable context-aware feature engineering and prediction.
