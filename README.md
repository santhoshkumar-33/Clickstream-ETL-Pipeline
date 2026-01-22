# Clickstream ETL Pipeline (Azure Databricks + PySpark)

An end-to-end **Clickstream Data Engineering project** that simulates realistic user events and processes them using an **industry-standard Bronze–Silver–Gold Lakehouse architecture**. The final Gold-layer datasets are designed for **Power BI analytics**.

---

## 📌 Project Overview

This project demonstrates how raw clickstream data from an e-commerce platform can be:

1. **Generated** using Python (realistic sessions & funnels)
2. **Ingested** as raw JSON into a Bronze layer
3. **Cleaned & structured** in a Silver layer using PySpark
4. **Aggregated into business KPIs** in a Gold layer
5. **Consumed by Power BI** for analytics and visualization

The architecture and data modeling closely follow **real-world data engineering practices**.

---

## 🧱 Architecture

```
Python (Data Simulation)
        ↓
Bronze Layer (Raw JSON)
        ↓
Silver Layer (Clean Parquet / Delta)
        ↓
Gold Layer (Aggregated Metrics)
        ↓
Power BI (Analytics & Dashboards)
```

---

## 🛠️ Tech Stack

* **Python** – Data simulation
* **Apache Spark (PySpark)** – Distributed data processing
* **Azure Data Lake Storage Gen2** – Cloud storage
* **Azure Databricks** – Spark execution environment
* **Delta Lake** – Reliable storage format
* **Power BI** – Analytics & dashboards

---

## 📂 Repository Structure

```
Clickstream-ETL-Pipeline/
│
├── data_simulation.py          # Generates realistic clickstream events
│
├── Spark_Jobs/
│   ├── bronze_ingestion.py     # Reads raw JSON (Bronze)
│   ├── silver_transformation.py# Cleans & structures data (Silver)
│   ├── gold_sessions.py        # Session-level KPIs (Gold)
│   ├── conversion_metrics.py   # Funnel & conversion KPIs (Gold)
│
└── README.md
```

---

## 🟤 Bronze Layer – Raw Data

**Purpose:** Preserve raw data exactly as received (append-only).

* Format: JSON (line-delimited)
* Stored in: Azure Data Lake Storage Gen2
* No schema enforcement or transformations

**Example fields:**

* event_id
* user_id
* session_id
* event_type
* product_id
* device
* country
* event_timestamp

---

## ⚪ Silver Layer – Clean & Structured Data

**Purpose:** Prepare data for analytics by cleaning and standardizing.

Transformations include:

* Removing duplicate events

* Casting timestamps

* Filtering invalid records

* Enforcing a stable schema

* Format: Parquet 

* Optimized for querying and aggregations

---

## 🟡 Gold Layer – Business Metrics

Only **Gold-layer tables** are exposed to Power BI.

### 1️⃣ Session Metrics (`gold_sessions.py`)

**Grain:** One row per session

Metrics:

* Session duration
* Events per session
* Sessions per user

Used for:

* User engagement analysis
* Session behavior insights

---

### 2️⃣ Conversion Metrics (`conversion_metrics.py`)

**Grain:** Aggregated (date / event-type level)

Metrics:

* Page views
* Product views
* Add-to-cart events
* Purchases
* Conversion rates
* Cart abandonment rate

Used for:

* Funnel analysis
* Conversion optimization

