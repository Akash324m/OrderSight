# PySpark Orders Data Pipeline & Qlik Analytics

## 📌 Project Overview
This project demonstrates an **end-to-end analytics pipeline** combining **PySpark-based data engineering** with **Qlik BI analytics**.  
Raw e-commerce order data is cleaned, validated, and modeled using PySpark, then consumed in Qlik for KPI-driven business insights.

The focus is on **data quality, proper data modeling, and meaningful metrics**, following industry best practices.

---

## 🛠️ Tech Stack
- **Python 3.10**
- **PySpark 3.5.0**
- **Apache Spark (Local Mode)**
- **Parquet**
- **Qlik Sense**
- **Star Schema Data Modeling**

---

## 🔄 Data Engineering Pipeline (PySpark)

### Data Ingestion
- Reads raw transactional data from Parquet
- Handles Spark session lifecycle and structured logging

### Data Cleaning & Feature Engineering
- Time-of-day classification for business analysis
- Text normalization (product, category)
- Removal of irrelevant product segments
- Extraction of geographic attributes from address fields
- Standardization of date fields

### Data Validation
Automated checks implemented for:
- Missing critical identifiers (order_id, order_date)
- Invalid numeric values (price and quantity)
- Null product references

Validation metrics are logged for auditability.

### Rejected Records Handling
- Invalid records are separated instead of silently dropped
- Clean and rejected datasets are written independently
- Enables traceability and debugging

### Data Export
- Clean data written in **Parquet format**
- Optimized for BI consumption

---

## 📐 Data Modeling in Qlik
- Data loaded using **Qlik Data Load Editor**
- Implemented a **Star Schema**:
  - Fact table: Orders
  - Dimension tables: Product, Category, Date, Geography
- Model optimized for KPI calculations and dashboard performance

---

## 📊 Analytics & KPIs (Qlik Dashboard)

The Qlik dashboard focuses on **decision-oriented metrics**, including:

- **Profitability**
  - Profit Margin %
  - High-margin product ratio
- **Growth**
  - Year-over-Year (YoY) revenue growth
- **Sales Behavior**
  - Average items per order
  - Business hours contribution to revenue
- **Category & Geography Insights**
  - Top revenue-contributing category
  - Revenue concentration across top-performing states

Only core KPIs were selected to maintain clarity and business relevance.

---

## 📁 Output Structure
orders_data_clean.parquet/
├── part-00000-xxxx.snappy.parquet
└── _SUCCESS

---

## 🧠 Key Concepts Demonstrated
- Production-style PySpark ETL design
- Data quality validation and rejected record management
- Star schema modeling for BI tools
- KPI-driven dashboard design
- Separation of data engineering and analytics responsibilities

---

## 🚀 Future Enhancements
- Incremental data loads
- Airflow orchestration
- Automated data quality reporting
- Performance tuning for large-scale datasets

---

## 👤 Author
Akash Mishra  
(Data Engineering & Analytics Project)

