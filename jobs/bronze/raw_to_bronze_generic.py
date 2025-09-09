import argparse, os
from pathlib import Path
from pyspark.sql import functions as F

from framework.logger import get_logger
from framework.config import load_env_config, load_table_config, resolve_path
from framework.spark_session import get_spark
from framework.transformations import rename_and_cast, add_audit_columns
from framework.delta_writer import merge_upsert

def main(env: str, table: str):
    log = get_logger(f"bronze:{table}")
    env_cfg = load_env_config(env)
    tbl_cfg = load_table_config(table)
    app_name = env_cfg["spark"]["app_name"]
    spark = get_spark(app_name, env_cfg)

    csv_root = resolve_path(env_cfg, "bronze_csv")
    bronze_delta_root = resolve_path(env_cfg, "bronze_delta")

    glob_pat = tbl_cfg["source"].get("filename_glob", "*.csv")
    input_path = os.path.join(csv_root, glob_pat)
    log.info(f"Reading CSV(s): {input_path}")
    df = (spark.read
          .option("header", "true")
          .csv(input_path))

    df = rename_and_cast(df, tbl_cfg["schema"])
    df = add_audit_columns(df)

    target_path = os.path.join(bronze_delta_root, table)
    log.info(f"Upserting to Delta: {target_path}")
    keys = tbl_cfg.get("keys", [])
    merge_upsert(spark, df, target_path, keys)
    log.info("Done.")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--env", default="local")
    p.add_argument("--table", required=True)
    args = p.parse_args()
    main(args.env, args.table)
