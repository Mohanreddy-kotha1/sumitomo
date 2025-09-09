import argparse
from framework.logger import get_logger
from framework.config import load_env_config, resolve_path
from framework.spark_session import get_spark
from framework.sql_runner import render_sql, run_sql

def main(env: str, table: str):
    log = get_logger(f"gold:{table}")
    env_cfg = load_env_config(env)
    spark = get_spark(env_cfg["spark"]["app_name"], env_cfg)

    sql_path = f"sql/gold/gold_{table}.sql"
    rendered = render_sql(
        sql_path,
        silver_delta_path=resolve_path(env_cfg, "silver_delta"),
        gold_delta_path=resolve_path(env_cfg, "gold_delta"),
    )
    log.info(f"Running SQL from {sql_path}")
    run_sql(spark, rendered)
    log.info("Done.")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--env", default="local")
    p.add_argument("--table", required=True)
    args = p.parse_args()
    main(args.env, args.table)
