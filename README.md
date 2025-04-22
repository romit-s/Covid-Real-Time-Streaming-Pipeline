# Real-Time COVID Data Streaming Pipeline

This project demonstrates a real-time data streaming pipeline for COVID-19 data. The pipeline reads data from a CSV file, publishes it to a Kafka topic, and processes it using a Spark Streaming application.

## 📌 Project Overview

- **Data Source**: CSV file containing COVID-19 statistics.
- **Streaming Platform**: Apache Kafka.
- **Processing Framework**: Apache Spark (Structured Streaming).
- **Goal**: Stream and process COVID data in real-time for analytics or dashboarding purposes.

## ⚙️ Architecture
CSV File → Kafka Producer → Kafka Topic → Spark Streaming → Delta Lake Table


## 📂 Project Structure

1. Read cases_deaths.csv using python and send every line as a message to imitate a real time data source.
2. Read from your Kafka topic using Spark Structured Streaming.
3. Clean data by removing cells with countries as Null
4. Perform Aggregation on your dataframe and create an OLAP Cube.
5. Store this final dataframe in a Delta lake table.

## 🔧 Prerequisites
- Python 3.8+
- Apache Kafka
- Apache Spark 3.x
- Java 8+
- pip for Python packages

Install Python dependencies:
- pip install confluent-kafka


## 🚀 Running the Pipeline

Run python snippet to send live data using kafka-producer

```bash
python kafka-message/producer.py
```
Start Spark Structured Streaming script run_pipeline.py on Databricks to read the stream and create your OLAP cube.



