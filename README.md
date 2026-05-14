# ✈️ NYC Flight Delay Prediction & Analytics Platform

An end-to-end Data Engineering project that combines **Real-Time Streaming** and **Batch Processing** to analyze and predict flight delays in New York City.

---

##  Project Overview

This project demonstrates how to build a **modern, production-style data platform** capable of handling both streaming and batch workloads.

It integrates multiple data sources such as flight data and weather conditions to:

* Predict flight delays in real-time
* Analyze historical delay patterns
* Understand the impact of weather and congestion

The system follows best practices like:

* **Medallion Architecture (Bronze → Silver → Gold)**
* **Galaxy Schema (Data Warehouse)**

---

## 🏗️ Architecture

![Architecture](https://github.com/AhmedRefat1412/Flight-Airline-Analytics-Platform/blob/main/doc/arcticture_digram/Flight%20%26%20Airline%20Analytics%20Platform.png)

---

## Streaming Pipeline (Real-Time)

###  Data Sources

* OpenSky API (Flight Data)
* Open-Meteo API (Weather Data)

###   Flow

1. Data is ingested in real-time via APIs
2. Sent to Kafka topics
3. Processed using PySpark Structured Streaming
4. XGBoost model applied for delay prediction (**72% accuracy**)
5. Results stored in TimescaleDB
6. Visualized in Grafana dashboard

### 🎯 Output

* Real-time predictions
* Live monitoring system

---

## 🔴 Real-Time Dashboard (Grafana)

![Grafana Dashboard](https://github.com/AhmedRefat1412/Flight-Airline-Analytics-Platform/blob/main/doc/dashboard%20(Grafana)/Pasted%20image.png)

Built using Grafana to monitor real-time flight data and predictions.

---

## 🗄️ Batch Pipeline (Data Warehouse)

Built using **Medallion Architecture**

### 🥉 Bronze Layer

* Raw data stored in S3 (CSV format)

  * Flights (BTS)
  * Weather
  * Airports

### 🥈 Silver Layer

* Data cleaned and transformed using PySpark
* Stored in Parquet format
* Schema standardized

### 🥇 Gold Layer

* Business-ready data modeled for analytics
* Optimized for querying

---

## ⭐ Data Warehouse (Galaxy Schema)

![Data Warehouse](https://github.com/AhmedRefat1412/Flight-Airline-Analytics-Platform/blob/main/doc/data_warehouse_schema/Flight%20%26%20Airline%20Data%20warehouse2.drawio.png)

The data warehouse is designed using a **Galaxy Schema** to support complex analytical queries.



###  Storage

* Loaded into Snowflake
* Optimized for BI tools

---

##  Analytical Dashboard (Power BI)

![Power BI Dashboard_1](https://github.com/AhmedRefat1412/Flight-Airline-Analytics-Platform/blob/main/doc/power_bi/Dashboard_1.png)

![Power BI Dashboard_2](https://github.com/AhmedRefat1412/Flight-Airline-Analytics-Platform/blob/main/doc/power_bi/Dashboard_2.png)


Built on top of Snowflake data warehouse to deliver business insights.


---

##  Orchestration

Managed using Apache Airflow

### DAG Workflow:

1. upload_bronze
2. process_flights
3. process_weather
4. process_airports (parallel execution)
5. build_gold
6. upload_snowflake

---

## 🧠 Machine Learning

* Model: XGBoost Classifier
* Use Case: Flight delay prediction
* Accuracy: **72%**

### Applied in:

* Real-time streaming predictions
* Analytical insights

---

## 🛠️ Tech Stack

* Apache Kafka
* PySpark (Batch & Streaming)
* Apache Airflow
* AWS S3
* Snowflake
* TimescaleDB
* Grafana
* Power BI
* Docker
* XGBoost

---

## 📈 Key Highlights

* Unified **Streaming + Batch Architecture**
* End-to-end pipeline (Ingestion → Processing → ML → Visualization)
* Real-time ML predictions
* Scalable and production-like design
* Data modeling using Galaxy Schema

