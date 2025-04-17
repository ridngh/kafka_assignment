Air Quality EDA Analysis
This exploratory data analysis examines air pollutant sensor data, focusing on temporal trends and relationships between key pollutants: CO, NOx, NO2, C6H6, and NMHC.

Key Insights

Sensor Downtime:
All pollutant sensors experienced simultaneous downtime, as seen in the time series plots where gaps or drops in data occur at the same periods for each pollutant. This indicates system-wide outages rather than isolated sensor failures.

Hourly Patterns:
Hourly average concentrations of pollutants (CO, NOx, NO2, C6H6) show clear peaks at the start and end of the typical office day. This pattern likely reflects increased vehicular and industrial activity during morning and evening rush hours, leading to higher emissions during these periods.

Daily Trends:
Pollutant levels are generally higher on weekdays, with concentrations dropping significantly over the weekend. This further supports the link between human activity and pollutant emissions.

Correlations:
The correlation matrix reveals strong positive relationships between several pollutants, especially between CO and C6H6 (correlation: 0.93), and CO and NOx (correlation: 0.80). This suggests common sources or similar emission patterns for these pollutants.

Summary Table

Feature	Observation
Sensor Downtime	All sensors down at the same time
Hourly Peaks	Highest at office start/end (rush hours)
Weekday vs Weekend	Higher concentrations on weekdays, lower on weekends
Correlation	Strongest between CO and C6H6, and CO and NOx
This analysis highlights the influence of human activity on air pollutant concentrations and the importance of robust sensor infrastructure for reliable monitoring.
