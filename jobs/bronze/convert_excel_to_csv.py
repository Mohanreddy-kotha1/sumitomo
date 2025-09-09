import argparse
from framework.logger import get_logger
from framework.config import load_env_config, resolve_path
from framework.io import excel_to_csv_dir

def main(env: str):
    log = get_logger("excel2csv")
    env_cfg = load_env_config(env)
    src = resolve_path(env_cfg, "raw_excel")
    dst = resolve_path(env_cfg, "bronze_csv")
    log.info(f"Converting Excel from {src} -> CSV into {dst}")
    outs = excel_to_csv_dir(src, dst)
    log.info(f"Wrote {len(outs)} CSV files")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--env", default="local", help="Environment key (local, aws-dev, aws-prod)")
    args = p.parse_args()
    main(args.env)
