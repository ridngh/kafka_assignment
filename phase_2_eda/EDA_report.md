# Air Quality EDA Analysis

This exploratory data analysis focuses on air pollutant sensor data collected over time, highlighting temporal trends and relationships among key pollutants: **CO**, **NOx**, **NO2**, **C6H6**, and **NMHC**.

---

## Key Insights

### Sensor Downtime
- All pollutant sensors show simultaneous downtime, evidenced by aligned gaps or drops in the time series plots.
- These occurrences suggest system-wide outages rather than isolated sensor malfunctions.

### Hourly Patterns
- Hourly averages of pollutants (CO, NOx, NO2, C6H6) exhibit distinct peaks:
  - Morning peak: start of the office day.
  - Evening peak: end of the office day.
- Likely driven by vehicular and industrial activity during commute hours.

### Daily Trends
- Pollutant levels are consistently higher on weekdays, with a noticeable drop on weekends.
- Indicates a strong link between human activity and pollution levels.

### Correlations Between Pollutants
- The correlation matrix shows strong positive relationships:
  - CO and C6H6: 0.93
  - CO and NOx: 0.80
- These high correlations suggest shared sources or similar emission behaviors.

---

This analysis lays the foundation for understanding real-world emission patterns and will inform downstream predictive modeling and anomaly detection tasks.

