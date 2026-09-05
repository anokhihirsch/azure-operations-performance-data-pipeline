# Azure Operations Analytics Data Platform

A cloud-based lakehouse data engineering project built on Azure to ingest, process, transform, and model operational order data for analytics and reporting.

This project implements a medallion architecture using Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, Unity Catalog, Apache Spark, PySpark, Auto Loader, Delta Lake, Databricks Workflows, and Delta Live Tables.

---

## Overview

This project demonstrates an end-to-end data pipeline for operational analytics. The pipeline ingests raw order fulfillment data, lands it in Azure Data Lake Storage, processes it through Bronze and Silver layers using Databricks and PySpark, and creates Gold reporting tables for downstream BI consumption.

The use case focuses on order fulfillment and delivery performance, including order volume, revenue, shipment status, late orders, customer activity, and operational KPI tracking.

---

## Business Use Case

Operations teams often need reliable visibility into order fulfillment, shipping performance, and process bottlenecks. Directly connecting BI reports to operational systems can work for simple dashboards, but it becomes harder to maintain when reporting requires historical tracking, reusable transformations, standardized business logic, and multiple curated datasets.

This project addresses that by introducing a scalable lakehouse pipeline that separates raw ingestion, data cleaning, transformation, and reporting layers.

The final curated datasets are designed to support metrics such as:

- Order volume
- Revenue
- Shipped order count
- Late order count
- Average days to ship
- Average days late against promise date
- Delivery performance by shipping organization
- Customer-level order summary
- Late order follow-up detail

---

## Architecture

```text
Source CSV Files
        |
        v
Azure Data Factory
        |
        v
ADLS Gen2 - Bronze Container
        |
        v
Azure Databricks / Auto Loader
        |
        v
Bronze Delta Layer
        |
        v
PySpark Transformations
        |
        v
Silver Delta Layer
        |
        v
Delta Live Tables
        |
        v
Gold Reporting Tables
        |
        v
Power BI
```

---

## Technology Stack

| Layer | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Orchestration / Ingestion | Azure Data Factory |
| Storage | Azure Data Lake Storage Gen2 |
| Compute | Azure Databricks |
| Processing Engine | Apache Spark |
| Development Language | PySpark, Python, SQL |
| Governance | Unity Catalog |
| Incremental Ingestion | Databricks Auto Loader |
| Streaming Framework | Spark Structured Streaming |
| Storage Format | Delta Lake |
| Workflow Orchestration | Databricks Workflows |
| Declarative Pipelines | Delta Live Tables |
| Reporting | Power BI |

---

## Data Sources

The project uses synthetic operational datasets modeled around order fulfillment and delivery performance.

| Dataset | Description |
|---|---|
| `orders.csv` | Order-level transaction data |
| `order_items.csv` | Line-level order item data |
| `payments.csv` | Payment and transaction data |
| `reviews.csv` | Customer review and satisfaction data |
| `customers.csv` | Customer reference data |
| `products.csv` | Product reference data |
| `sellers.csv` | Seller reference data |

---

## Medallion Architecture

The pipeline follows a Bronze, Silver, and Gold design pattern.

```text
Bronze = Raw ingested data
Silver = Cleaned and transformed data
Gold = Reporting-ready business tables
```

---

## Bronze Layer

The Bronze layer stores raw ingested data with minimal transformation.

Responsibilities:

- Land source data in ADLS Gen2
- Preserve raw file structure
- Support incremental ingestion
- Maintain checkpoint and schema tracking metadata
- Provide traceability back to source files

Implemented using:

- Azure Data Factory
- ADLS Gen2
- Databricks Auto Loader
- Spark Structured Streaming
- Delta Lake

---

## Silver Layer

The Silver layer applies data cleaning, type casting, standardization, and business-level transformations.

Responsibilities:

- Cast date fields to proper date types
- Cast numeric fields to appropriate numeric types
- Standardize text columns
- Handle null values
- Remove duplicate lookup records
- Create reusable cleaned datasets
- Add operational business logic columns

Example derived fields:

| Column | Description |
|---|---|
| `days_to_ship` | Difference between order date and actual ship date |
| `days_late_vs_promise` | Difference between actual ship date and promised date |
| `is_late` | Late shipment flag |
| `is_shipped` | Shipped order flag |
| `silver_processed_timestamp` | Transformation processing timestamp |

---

## Gold Layer

The Gold layer contains curated reporting tables designed for analytics and Power BI consumption.

Responsibilities:

- Aggregate operational KPIs
- Apply reporting-level business logic
- Create clean, reusable analytical tables
- Support dashboard-ready datasets
- Enforce data quality expectations

Gold tables:

| Table | Description |
|---|---|
| `gold_orders_clean` | Clean order-level reporting table |
| `gold_daily_operations_kpi` | Daily operational KPI summary |
| `gold_delivery_performance` | Delivery performance by shipping organization and order status |
| `gold_customer_order_summary` | Customer-level revenue and late order summary |
| `gold_late_order_detail` | Detailed late order records for follow-up |

Implemented using:

- Delta Live Tables
- DLT expectations
- Delta Lake
- Spark aggregations

---

## Pipeline Components

### 1. Azure Data Factory Ingestion

Azure Data Factory is used to copy source files into ADLS Gen2.

Implemented features:

- Parameterized datasets
- Dynamic source and sink paths
- ForEach activity for multiple file ingestion
- Validation activity before copy execution
- Pipeline monitoring through ADF Monitor

### 2. Databricks Auto Loader

Databricks Auto Loader is used for incremental file ingestion from ADLS.

Implemented features:

- `cloudFiles` source format
- Schema inference
- Schema location tracking
- Checkpoint location tracking
- Spark Structured Streaming read
- Incremental file processing

Example pattern:

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("header", "true")
    .load(source_path)
)
```

### 3. Parameterized Notebooks

Databricks notebooks are parameterized using `dbutils.widgets`.

This allows the same notebook logic to process multiple source and target folders without duplicating code.

Example:

```python
dbutils.widgets.text("sourcefolder", "customers")
dbutils.widgets.text("targetfolder", "customers")

sourcefolder = dbutils.widgets.get("sourcefolder")
targetfolder = dbutils.widgets.get("targetfolder")
```

### 4. Workflow Task Values

Dynamic values are passed between Databricks Workflow tasks using `dbutils.jobs.taskValues`.

Example:

```python
files = [
    {"sourcefolder": "customers", "targetfolder": "customers"},
    {"sourcefolder": "products", "targetfolder": "products"},
    {"sourcefolder": "sellers", "targetfolder": "sellers"}
]

dbutils.jobs.taskValues.set(
    key="silver_lookup_files",
    value=files
)
```
### 5. Databricks Workflows

Databricks Workflows orchestrate notebook execution across the pipeline.

Workflow features:

- Task dependencies
- Multi-notebook execution
- Dynamic parameter passing
- Iterative task execution
- Conditional IF/ELSE logic
- Skip/fallback notebook execution

Example workflow structure:

```text
Lookup Silver Files
        |
        v
Parameterized Silver Lookup Load
        |
        v
Silver Orders Transformation
        |
        v
Weekday Condition Check
        |
        v
Gold DLT Pipeline
```

### 6. Conditional Execution

A weekday lookup notebook returns a weekday value to the workflow. The workflow condition determines whether the main processing path should continue or a skip notebook should run.

This pattern simulates production scheduling logic, such as running operational pipelines only on business days.

Example:

```python
from datetime import datetime

weekday = datetime.today().isoweekday()

dbutils.jobs.taskValues.set(
    key="weekoutput",
    value=weekday
)
```

### 7. Delta Live Tables

Delta Live Tables is used to define and manage Gold layer tables.

DLT provides a declarative approach for creating tables, applying expectations, and managing pipeline dependencies.

Example:

```python
@dlt.table(
    name="gold_daily_operations_kpi",
    comment="Daily operational KPI summary."
)
def gold_daily_operations_kpi():
    df = dlt.read("gold_orders_clean")

    return (
        df.groupBy("order_date")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.sum("order_amount"), 2).alias("total_revenue"),
            F.sum("is_shipped").alias("shipped_orders"),
            F.sum("is_late").alias("late_orders")
        )
    )
```

---

## Data Quality

The Gold layer includes Delta Live Tables expectations to validate key business fields before records are used for reporting.

Example rules:

```python
orders_rules = {
    "order_id_is_not_null": "order_id IS NOT NULL",
    "customer_id_is_not_null": "customer_id IS NOT NULL",
    "order_date_is_not_null": "order_date IS NOT NULL",
    "order_amount_is_not_null": "order_amount IS NOT NULL"
}
```

Applied with:

```python
@dlt.expect_all_or_drop(orders_rules)
```

This prevents incomplete records from entering the curated reporting layer.

---

## Repository Structure

```text
azure-operations-analytics-platform/
│
├── README.md
│
├── data/
│   ├── orders.csv
│   ├── order_items.csv
│   ├── payments.csv
│   ├── reviews.csv
│   ├── customers.csv
│   ├── products.csv
│   └── sellers.csv
│
├── notebooks/
│   ├── 01_autoloader_bronze_ingestion.py
│   ├── 02_silver_parameterized_lookup.py
│   ├── 03_lookup_silver_files.py
│   ├── 04_silver_orders_transformation.py
│   ├── 05_weekday_lookup.py
│   ├── 05_weekend_skip_notebook.py
│   └── 06_dlt_gold_layer.py
│
├── docs/
│   ├── data_dictionary.csv
│   └── project_notes.md
│
├── architecture/
│   └── architecture_diagram.png
│
└── screenshots/
    ├── adf_pipeline.png
    ├── databricks_workflow.png
    ├── unity_catalog.png
    └── powerbi_dashboard.png
```

---

## Notebooks

| Notebook | Purpose |
|---|---|
| `01_autoloader_bronze_ingestion.py` | Incrementally ingests raw files using Auto Loader |
| `02_silver_parameterized_lookup.py` | Loads lookup/reference files into Silver using parameters |
| `03_lookup_silver_files.py` | Returns dynamic source/target folder mappings |
| `04_silver_orders_transformation.py` | Cleans and transforms order data into Silver |
| `05_weekday_lookup.py` | Returns weekday value for conditional workflow logic |
| `05_weekend_skip_notebook.py` | Handles false/skip workflow path |
| `06_dlt_gold_layer.py` | Creates Gold reporting tables using Delta Live Tables |

## Project Outcome

This project demonstrates how raw operational data can be ingested, processed, governed, transformed, and modeled into analytics-ready datasets using a modern Azure lakehouse architecture.

The final Gold layer is designed to support Power BI reporting for operational performance, order fulfillment, shipping delays, customer activity, and business KPI tracking.

