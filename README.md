# Weather Analytics and Historical Trend Monitoring System

A Big Data Analytics platform that collects, stores, processes and visualizes
historical and simulated real-time weather data for Delhi, Mumbai, Chennai,
Bengaluru and Hyderabad.

## Architecture

Batch:     Open-Meteo API -> HDFS -> Spark (PySpark) -> Parquet -> Streamlit
Streaming: Python producer -> Kafka -> Spark Structured Streaming -> Parquet -> Streamlit

## Tech stack

| Layer | Tool |
|---|---|
| Language | Python 3.12 |
| Storage | Apache Hadoop HDFS |
| Batch processing | Apache Spark / PySpark |
| Ingestion | Apache Kafka |
| Stream processing | Spark Structured Streaming |
| Dashboard | Streamlit + Plotly |
| Infrastructure | Docker Compose |

## Setup

1. Install Java 17, Python 3.12, Docker Desktop, Git
2. `python -m venv venv` and `.\venv\Scripts\Activate.ps1`
3. `pip install -r requirements.txt`
4. `docker compose up -d`
5. On Windows, place `winutils.exe` and `hadoop.dll` in `C:\hadoop\bin` and set `HADOOP_HOME=C:\hadoop`

## Run order

1. `python src\collect_historical.py` (downloads 10 years of hourly data)
2. Upload CSVs to HDFS (`/weather/raw`)
3. `python src\spark_batch.py`
4. `python src\kafka_producer.py` (terminal 1)
5. `python src\spark_streaming.py` (terminal 2)
6. `streamlit run dashboard\app.py`

## Dataset

About 438,000 hourly records (2015-2024) with temperature, humidity,
rainfall and wind speed, from the Open-Meteo archive API.

## Features

- Yearly and monthly trends, seasonal patterns
- City-by-city comparison and temperature heatmap
- Warming trend per city (linear regression slope)
- Live view of simulated sensor data in 30-second windows

