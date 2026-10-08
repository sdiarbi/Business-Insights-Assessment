# 🚀 Business Insights Assessment: Medallion Data Pipeline

An end-to-end, automated ELT data pipeline implementing a **Medallion Architecture (Bronze → Silver → Gold)** on AWS. This pipeline processes raw e-commerce/restaurant operational data, cleans and optimizes it into columnar Parquet storage, and materializes 7 core analytical metric tables for business intelligence and stakeholder reporting.

---

## 🏗️ Architecture & Tech Stack

* **Storage:** Amazon S3 (`business-insights-assessment-bucket`) — Raw CSVs, Parquet files, and query results.
* **Metadata Catalog:** AWS Glue Data Catalog — Schema registration for databases and tables (`bia_db`).
* **Compute / SQL Engine:** Amazon Athena — Serverless SQL query execution for transformations.
* **Orchestration:** Apache Airflow (via Custom DAGs using `AthenaOperator`) — Automated workflow management.

---

## 📊 Medallion Pipeline Layers

### 1. Bronze Layer (Raw Ingestion)
Registers raw CSV files sitting in S3 as external SQL tables without modifying underlying source files. Handles type safety and null conversion for messy incoming fields.
* **Tables Created:**
  * `bronze_order_items`
  * `bronze_order_item_options`
  * `bronze_date_dim`

### 2. Silver Layer (Cleaned & Optimized)
Cleans, casts, strips whitespace, filters out corrupt rows/null primary keys, and converts data into compressed, columnar **Parquet** format for high-performance analytics.
* **Tables Created:**
  * `silver_order_items`
  * `silver_order_item_options`
  * `silver_date_dim`

### 3. Gold Layer (Business Intelligence & Metrics)
Specialized aggregate tables designed to answer key business questions and drive executive reporting.
* **Tables Created (7):**
  1. `gold_customer_lifetime_value` — Estimated CLV per customer with N-Tile spending tiers.
  2. `gold_customer_segmentation` — RFM scoring and behavior tiers (VIP, New Customer, Regular).
  3. `gold_churn_indicators` — Recency tracking, order gaps, and "At-Risk" status tagging.
  4. `gold_sales_trends` — Time-series aggregations grouped by date, restaurant location, and category.
  5. `gold_loyalty_program_impact` — Comparative engagement analysis of loyalty members vs. non-members.
  6. `gold_top_locations` — Store-level revenue performance and ranking.
  7. `gold_pricing_discount_analysis` — Impact of option discounts on order volume and revenue.

---

