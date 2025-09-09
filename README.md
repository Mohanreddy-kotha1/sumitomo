# Lakehouse Repo Scaffold (Bronze / Silver / Gold)

This repository is a lightweight framework to:
- Ingest **source Excel files** into `data/raw_excel_files/`
- Convert Excel → CSV into **Bronze** (`data/bronze_csv_files/` locally or `s3://.../bronze/csv/` in AWS)
- Create **Bronze Delta** tables, then **Silver** (joins, cleansing), and **Gold** (reporting-ready) using Spark SQL
- Keep **Spark SQL** and **schemas** separate from job code
- Use **YAML** configs by environment and per-table
- Provide a **custom logger** and modular utilities for Delta and S3 I/O

> Run locally first, then deploy the same code to AWS Glue/S3 with environment-specific configs.

## Quick start (Local)

```bash
python -m venv .venv && source .venv/bin/activate   # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt

# 1) Put your Excel files here:
#    data/raw_excel_files/<your files>.xlsx

# 2) Convert Excel -> CSV (Bronze CSV)
python jobs/bronze/convert_excel_to_csv.py --env local

# 3) Land Bronze CSV into Bronze Delta (example for dim_product)
python jobs/bronze/raw_to_bronze_generic.py --env local --table dim_product

# 4) Build Silver from Bronze (uses sql/silver/<table>_merge.sql)
python jobs/silver/bronze_to_silver_generic.py --env local --table dim_product

# 5) Build Gold from Silver (uses sql/gold/<table>.sql)
python jobs/gold/silver_to_gold_generic.py --env local --table sales_summary
```

## Deploy to AWS

1. Set values in `conf/environments/aws-dev.yaml` (bucket, region, Glue db, IAM role).
2. Package dependencies if needed (see `scripts/build_layer_or_wheel.md`). Upload the repo (or a build artifact zip/wheel) to S3.
3. Create Glue jobs that call the same entry points, e.g.:
   - `jobs/bronze/raw_to_bronze_generic.py --env aws-dev --table dim_product`
   - `jobs/silver/bronze_to_silver_generic.py --env aws-dev --table dim_product`
   - `jobs/gold/silver_to_gold_generic.py --env aws-dev --table sales_summary`
4. Ensure job has Delta + Hadoop AWS JARs and Python libs available (Glue 5.0 recommended).

## Layout

- `conf/environments/*.yaml` — environment-level paths, Glue settings
- `conf/tables/*.yaml` — per-table schema (English names), keys, options
- `sql/{bronze|silver|gold}/...` — Spark SQL kept outside code
- `jobs/{bronze|silver|gold}/...` — thin drivers that call framework utilities
- `framework/*` — reusable Spark session, logging, config loader, Delta helpers

## Notes
- The provided YAML/schema and SQL samples are illustrative—adjust to your project.
- For Bronze CSV creation we use **pandas** locally for Excel parsing. In AWS, you can use a Python Shell job or any existing Glue script you tested to perform Excel→CSV into `s3://.../bronze/csv/`.
