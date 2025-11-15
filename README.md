# Developed-a-scalable-ETL-pipeline-for-E-commerce-analytics
A scalable ETL pipeline for e-commerce analytics that integrates multiple data sources, applies transformation logic, and produces a clean analytics dataset for reporting and dashboards.

# 📁 Files
---
etl_pipeline.py – Core pipeline script for extract, transform, and load steps.

config.yaml – Configuration for input sources, database credentials, and processing rules.

requirements.txt – Dependency list.

README.md – Project overview.

# 🛠️ Techniques Used
----
Batch ingestion from CSV and JSON files

Data cleaning, deduplication, and normalization

Transformations with Pandas and SQL

Workflow scheduling with Airflow or Cron

Loading into a warehouse or relational database

Logging, validation checks, and basic monitoring

# 📊 Result
----
Processed more than 5 million transaction records

Reduced manual data prep time by 80%

Achieved end-to-end pipeline runtime under 15 minutes per batch

Delivered a consistent dataset supporting 10+ analytics dashboards

# 🔗 Data Source
----
E-commerce transaction logs, product catalog data, and user activity exports depending on the environment.
