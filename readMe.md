# Lakehouse ETL Pipeline – AWS Orchestrated

## Overview

This project implements an **automated, event-driven data ingestion and transformation pipeline** for a Lakehouse architecture using AWS services:

* **S3** for raw and processed data storage
* **Lambda** for format conversion, dataset classification, and orchestration steps
* **Glue ETL Jobs** for schema-enforced data processing and Delta Lake output
* **Glue Crawler** for updating the AWS Glue Data Catalog
* **Step Functions** for orchestrating the workflow end to end

**All processing is automatically triggered** whenever new files are uploaded to S3.

---

## Architecture

### S3 Structure

Your S3 bucket (e.g., `lakehouse-datastore`) is organized as follows:

```
lakehouse-datastore
├── staging/                     # S3 Event triggers pipeline
│   ├── orders/
│   ├── order_items/
│   └── products/
│
├── raw/                         # Converted CSVs (partitioned by dataset)
│   ├── orders/
│   ├── order_items/
│   └── products/
│
├── processed/                   # Delta Lake outputs
│   ├── orders/
│   ├── order_items/
│   └── products/
│
├── archived/                    # Raw files archived after processing
│   ├── orders/
│   ├── order_items/
│   └── products/
│
└── athena-results/              # Athena query results (if used)
```

---

### Orchestration Flow

The pipeline follows this **event-driven orchestration:**

```
1️.  S3 Upload (staging/)
   ⬇ EventBridge Rule triggers Step Functions

2️. Step Function Execution:
   - Lambda: Convert XLSX to CSV
   - Lambda: Determine dataset(s) present
   - Choice: Route to correct Glue job(s)
   - Glue ETL: Process datasets
   - Lambda: Start Glue Crawler
   - Lambda: Archive processed files
```

**Only the Glue jobs corresponding to the datasets that arrived are executed.**

---

## Components

### Lambda Functions

| Lambda Function            | Purpose                                    |
| -------------------------- | ------------------------------------------ |
| `xlsx_to_csv_lambda`       | Converts `.xlsx` files to `.csv` in `raw/` |
| `EvaluateDatasetChoice`    | Inspects S3 key, flags datasets present    |
| `start_crawlers`           | Starts the Glue Crawler                    |
| `archive_raw_files_lambda` | Moves processed files to `archived/`       |

---

### Glue Jobs

| Job Name                  | Purpose                                        |
| ------------------------- | ---------------------------------------------- |
| `process_orders_ETL`      | Cleans, deduplicates Orders, writes Delta      |
| `process_order_items_ETL` | Cleans, deduplicates Order Items, writes Delta |
| `process_products_ETL`    | Cleans, deduplicates Products, writes Delta    |

---

### Glue Crawler

| Crawler Name        | Purpose                                                        |
| ------------------- | -------------------------------------------------------------- |
| `lakehouse-crawler` | Updates Glue Data Catalog so Athena can query processed tables |

---

### Step Functions

Your **Step Function** is the core orchestrator.
It does:

* File conversion
* Dataset classification
* Conditional ETL job execution
* Catalog updates
* Archival of processed data

---

## User Guide

### Uploading Data

1. **Place your source files in `staging/` directories**:

   * Orders data: `staging/orders/`
   * Order Items data: `staging/order_items/`
   * Products data: `staging/products/`

2. The pipeline **automatically starts** when a file is uploaded.

Example:

```
aws s3 cp orders_2025-06-30.xlsx s3://lakehouse-datastore/staging/orders/
```

---

### What Happens Next

**Step Functions Flow:**

1️. **ConvertFilesLambda**

* Converts `.xlsx` to `.csv`

2️. **EvaluateDatasetLambda**

* Determines which dataset(s) were uploaded

3️. **DatasetChoice**

* Branches to the appropriate Glue job(s)

4️. **Glue Jobs**

* Each dataset gets processed into `processed/`

5️. **RunGlueCrawler**

* Glue Data Catalog updated automatically

6️. **ArchiveRawFiles**

* Processed raw files moved to `archived/`

---

**If no datasets are detected**, the pipeline fails gracefully with:

```
NoDatasetError: No datasets were flagged for processing.
```

---

### Querying the Data

After the Glue Crawler finishes, you can query data via Athena:

**Example:**

```sql
SELECT *
FROM "lakehouse_dwh"."orders"
LIMIT 10;
```

The crawler ensures partitions are up to date.

---

## Operational Notes

**Retries:**
Each Lambda and Glue job has 3 retries with exponential backoff.

**Waits:**
Wait states are used to ensure S3 consistency before next steps.

**Logging:**
All steps log to CloudWatch Logs:

* Lambda logs are in `/aws/lambda/<function>`
* Step Function logs show full execution traces

**Error Handling:**
Failures are surfaced clearly in Step Functions console.

---

## Advanced Extensions (Optional)

This pipeline can be extended with:

* Athena validation Lambda to confirm table row counts
* SNS notifications on success/failure
* Custom CloudWatch metrics for observability
* Incremental partition processing

---

## Development Workflow

**To update the pipeline:**

1. Modify any Lambda code.
2. Redeploy the Lambda(s).
3. Update Step Functions JSON definition if necessary.
4. Re-upload files to `staging/` to trigger a new run.

Always monitor execution in **Step Functions Console** to confirm behavior.

---

## Example S3 Layout After One Run

```
lakehouse-datastore/
├── staging/
│   └── orders/                   (empty after move to archive)
│
├── raw/
│   └── orders/
│       └── orders_2025-06-30_Sheet1.csv
│
├── processed/
│   └── orders/
│       └── <Delta Lake partitions>
│
├── archived/
│   └── orders/
│       └── orders_2025-06-30.xlsx
```

---

## How to Monitor

* **Step Functions Console:**
  Shows current executions, status, and history.

* **CloudWatch Logs:**
  For each Lambda and Glue job.

* **Athena Console:**
  To verify tables are refreshed and queryable.

---

## User Steps Recap

1️. Upload `.xlsx` or `.csv` to `staging/`
2️. Wait for pipeline completion (\~5–10 min)
3️. Query processed data via Athena
4️. Confirm archived raw files in `archived/`

* No manual steps required in normal operation.

---

## Questions?

If you have any issues or want to extend this pipeline (validation, notifications, metrics), please contact the project maintainer or your data engineering team.

---


