# Everlytics Data Engineering Assessment

## Overview

This project is a hands-on Data Engineering practice assignment using **Apache Airflow, MinIO, and Apache Doris**. It covers ETL pipelines, data warehouse design, data quality validation, data lake concepts, and incremental data ingestion.

The project uses customer and order CSV files as source data and processes them through Airflow into Doris tables for analytics.

## Technology Stack

- **Apache Airflow** — Workflow orchestration
- **MinIO** — Object storage for raw CSV files
- **Apache Doris** — Staging, target tables, and analytical queries
- **Python** — ETL logic and validation
- **SQL** — Table creation, transformations, and data validation
- **DBeaver** — SQL execution and database verification

## Project Structure

```text
everlytics-data-engineering-assessment/
├── TASK 1 AND 2/
│   ├── dags/
│   │   └── assessment/
│   │       ├── ingestion_dag.py
│   │       ├── task1_config.py
│   │       ├── task1_functions.py
│   │       └── task1_tasks.py
│   ├── minio/
│   │   └── test/mohammedhyder/raw/
│   │       ├── customers/customers.csv
│   │       └── orders/orders.csv
│   └── output results/
│       ├── DBeaver SQL Scripts/
│       └── Additional/
├── TASK 3/
│   ├── Notes.txt
│   └── Airflow-Data Quality Testing.png
├── TASK 4/
│   ├── Notes.txt
│   └── MinIO screenshots
└── TASK 6/
    ├── dags/
    ├── sql/
    ├── sample_files/
    └── attachments/
```

## Tasks Implemented

### Task 1 — ETL Pipeline

- Stored customer and order CSV files in MinIO.
- Checked whether the input files exist.
- Loaded source data into Doris staging tables.
- Cleaned customer names, cities, and email addresses.
- Calculated order total amounts using quantity and unit price.
- Loaded transformed data into target tables.
- Validated the final data load.

**Airflow DAG:** `mohammedhyder_task1_etl`

### Task 2 — Data Warehouse Design

- Created a customer dimension table (`dim_customer`).
- Created an order fact table (`fact_order`).
- Used a surrogate key for customer dimension records.
- Defined the fact-table grain as one row per order.
- Wrote SQL queries for monthly sales, sales by city, and sales by customer.

### Task 3 — Data Quality

Implemented and documented checks for:

- Missing mandatory fields and IDs.
- Duplicate customer and order IDs.
- Quantity greater than zero.
- Non-negative unit prices.
- Missing dates.
- Referential integrity between orders and customers.
- Correct order total calculations.
- Non-empty target tables.

Tested invalid input data and observed Airflow failures. Further work is needed to ensure explicit date validation runs before loading target tables and to verify invalid-data handling.

### Task 4 — Data Lake with MinIO

- Used the `test` bucket to store raw customer and order CSV files.
- Organized files under customer and order prefixes.
- Retained the original raw files after processing.

### Task 6 — Incremental ETL

- Created a Doris ingestion tracker table.
- Tracked file names, processing status, processing time, record counts, and error messages.
- Checked whether files had already been processed successfully.
- Skipped files already marked as successful.
- Added validation for input data.
- Included sample files for testing valid and invalid data.

**Airflow DAG:** `mohammedhyder_task6_incremental_etl`

The incremental workflow is designed to process new files without repeatedly processing previously successful files.

## Architecture

```text
Customer and Order CSV Files
            |
            v
       MinIO Storage
            |
            v
     Apache Airflow
            |
            v
      Doris Staging
            |
            v
 Transform and Validate
            |
            v
     Doris Target Tables
            |
            v
   SQL Analytics and Reports
```

For incremental ingestion, the tracker table records file-processing status and helps prevent repeated processing of successfully ingested files.

## Setup and Execution

1. Set up Apache Airflow, MinIO, and Apache Doris.
2. Create the required database and tables by executing the SQL scripts using DBeaver.
3. Configure the MinIO and Doris connections in Airflow.
4. Upload the customer and order CSV files to the configured MinIO bucket and prefixes.
5. Place the DAG and supporting Python files in the appropriate Airflow DAG directory.
6. Enable the relevant DAG in the Airflow UI.
7. Trigger the DAG manually.
8. Verify task status in Airflow and inspect the loaded records using SQL queries.

Use the connection IDs and file paths defined in the corresponding configuration files.

## Current Progress

- **Task 1:** ETL pipeline implementation
- **Task 2:** Data warehouse design and analytical SQL
- **Task 3:** Data quality checks and testing
- **Task 4:** MinIO raw-data storage
- **Task 6:** Incremental ingestion and file tracking

Tasks 5 (Iceberg lakehouse), 7 (CDC), and 8 (end-to-end integration) remain to be implemented or demonstrated.

## Learning Objectives

- Understand ETL pipeline design and orchestration.
- Learn how MinIO stores raw source data.
- Practise staging tables and dimensional modeling in Doris.
- Implement data quality checks and failure handling.
- Understand incremental ingestion and safe reruns.
- Build practical experience with data engineering tools.

This repository is a learning project created to practise the concepts in the Everlytics Data Engineering Practical Assignment.
