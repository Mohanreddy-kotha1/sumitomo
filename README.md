# Glue ETL Framework (Bronze / Silver / Gold)

Lightweight framework to:

- Ingest **source Excel files** into `data/raw_excel_sheet/`
- Convert Excel → CSV into **Bronze CSV** (`data/bronze_csv_files/` locally or `s3://.../bronze/csv/` in AWS)
- Build **Bronze Delta** tables, then **Silver** (joins/cleansing), and **Gold** (reporting‑ready) with Spark SQL
- Keep **Spark SQL** and **schemas** separate from code
- Use **YAML** config per **environment** and **per‑table**
- Emit **JSON logs** to a timestamped file (local or S3)

> Develop and test **locally** first (pure Python for Excel→CSV), then run the **same jobs** in AWS Glue/S3 using environment configs.

---

## Table of contents

1. [Repository layout](#repository-layout)  
2. [Environments & configuration](#environments--configuration)  
3. [Local quick start](#local-quick-start)  
4. [Bronze Excel → CSV (details)](#bronze-excel--csv-details)  
5. [Bronze/Silver/Gold jobs](#bronzesilvergold-jobs)  
6. [Deploy to AWS (Glue + S3)](#deploy-to-aws-glue--s3)  
7. [Schema definitions (English)](#schema-definitions-english)  
8. [Logging](#logging)  
9. [Troubleshooting](#troubleshooting)

---

## Repository layout

```
glue-etl-framework/
│
├── data/                                   # Raw & processed data files
│   ├── raw_excel_sheet/
│   │   ├── Bulgaria/
│   │   ├── Slovakia/
│   │   └── Slovenia/
│   ├── bronze_csv_files/
│   ├── bronze_delta/
│   ├── silver_delta/
│   └── gold_delta/
│
├── conf/                                   # Config (env + table)
│   ├── environments/
│   │   ├── local.yaml
│   │   ├── aws-dev.yaml
│   │   └── aws-prod.yaml
│   └── tables/
│       ├── dim_product.yaml
│       ├── fact_sales.yaml
│       └── sales_summary.yaml
│
├── sql/                                    # DDLs and transformation SQLs
│   ├── bronze/
│   │   └── bronze_<Table_A>.sql
│   ├── silver/
│   │   └── silver_<Table_A>_merge.sql
│   └── gold/
│       └── gold_<Table_A>.sql
│
├── jobs/                                   # ETL Job scripts
│   ├── bronze/
│   │   ├── convert_excel_to_csv.py
│   │   └── raw_to_bronze_generic.py
│   ├── silver/
│   │   └── bronze_to_silver_generic.py
│   └── gold/
│       └── silver_to_gold_generic.py
│
├── framework/                              # Common reusable modules
│   ├── __init__.py
│   ├── spark_session.py
│   ├── config.py
│   ├── utils.py
│   ├── transformations.py
│   ├── delta_writer.py
│   ├── logger.py
│   ├── io.py
│   └── sql_runner.py
│
├── logs/                                   # Local log files (JSON; gitignored)
├── requirements.txt
├── README.md
└── .gitignore
```

---

## Environments & configuration

### `conf/environments/local.yaml`
```yaml
paths:
  raw_excel: data/raw_excel_sheet
  bronze_csv: data/bronze_csv_files
  bronze_delta: data/bronze_delta
  silver_delta: data/silver_delta
  gold_delta: data/gold_delta
  logs: logs
excel:
  first_sheet_only: false
  encoding: "utf-8-sig"
spark:
  master: "local[*]"
  app_name: "glue-etl-local"
```

### `conf/environments/aws-dev.yaml`
```yaml
paths:
  raw_excel: s3://my-bucket/raw/excel
  bronze_csv: s3://my-bucket/bronze/csv
  bronze_delta: s3://my-bucket/bronze/delta
  silver_delta: s3://my-bucket/silver/delta
  gold_delta: s3://my-bucket/gold/delta
  logs: s3://my-bucket/logs
excel:
  first_sheet_only: false
  encoding: "utf-8-sig"
glue:
  region: ap-south-1
  role: arn:aws:iam::<account-id>:role/MyGlueRole
  database: my_lakehouse_db
  version: "5.0"
```

---

## Local quick start

```bash
python -m venv .venv && . .venv/Scripts/activate
pip install -r requirements.txt

# 1) Place Excel files under data/raw_excel_sheet/<Country>/
# 2) Convert Excel → CSV
python -m jobs.bronze.convert_excel_to_csv --env local

# 3) Bronze CSV → Bronze Delta
python -m jobs.bronze.raw_to_bronze_generic --env local --table dim_product

# 4) Build Silver
python -m jobs.silver.bronze_to_silver_generic --env local --table dim_product

# 5) Build Gold
python -m jobs.gold.silver_to_gold_generic --env local --table sales_summary
```

---

## Bronze Excel → CSV (details)

- Handles: `.xlsx`, `.xlsm`, `.xls`, `.xlx`, `.xlsb`  
- Fallback: if misnamed Excel is actually text/CSV, auto‑sniffs delimiter/encoding  
- Output names: `Workbook__Sheet.csv` (or `Workbook.csv` if first‑sheet only)  
- CLI options: `--env`, or explicit `--src`, `--dst`, `--log-dir`

---

## Bronze/Silver/Gold jobs

- **Bronze:** CSV → Delta, schema mapping, audit cols, dedupe, upsert/merge  
- **Silver:** run SQL in `sql/silver/` with templating; joins & cleansing  
- **Gold:** run SQL in `sql/gold/`; reporting-ready tables  

---

## Deploy to AWS (Glue + S3)

- **Excel → CSV:** Glue Python Shell job  
  - Script: `jobs/bronze/convert_excel_to_csv.py`  
  - Args: `--env aws-dev`  
  - Extra modules: `pandas,openpyxl,PyYAML,s3fs,xlrd,pyxlsb,chardet`  

- **Bronze/Silver/Gold:** Glue Spark jobs  
  - Bronze: `jobs/bronze/raw_to_bronze_generic.py --env aws-dev --table ...`  
  - Silver: `jobs/silver/bronze_to_silver_generic.py --env aws-dev --table ...`  
  - Gold:   `jobs/gold/silver_to_gold_generic.py --env aws-dev --table ...`  

---

## Schema definitions (English)

Example `conf/tables/dim_product.yaml`:

```yaml
table: dim_product
primary_keys: [product_id]
write_mode: merge
source:
  file_pattern: "dim_product*.csv"
columns:
  product_id: { type: long, source: "商品ID" }
  product_name: { type: string, source: "商品名" }
  category: { type: string, source: "カテゴリ" }
  active_flag: { type: boolean, default: true }
options:
  audit_columns: true
  deduplicate_on_keys: true
```

---

## Logging

- Each run writes JSON logs to `logs/<process>__YYYYMMDD_HHMMSS.log` (or S3).  
- Logs are JSON lines (one event per line).  
- `paths.logs` controls log location.  

---

## Troubleshooting

- `ModuleNotFoundError: framework` → run from repo root with `-m`  
- `No module named 'yaml'` → `pip install PyYAML`  
- `.xls/.xlx` parse errors → ensure `xlrd==2.0.1` installed  
- `.xlsb` parse errors → install `pyxlsb`  
