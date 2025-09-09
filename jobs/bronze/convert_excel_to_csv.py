# Allow direct execution without PYTHONPATH tweaks
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

import argparse
from framework.logger import get_logger, build_log_uri, add_log_destination, close_logger
from framework.io import excel_to_csv_dir

# Config is optional (you can pass --src/--dst without PyYAML installed)
try:
    from framework.config import load_env_config, resolve_path
    HAVE_CFG = True
    CFG_ERR = None
except Exception as _e:
    HAVE_CFG = False
    CFG_ERR = _e

PROCESS_NAME = "excel2csv"

def main():
    ap = argparse.ArgumentParser(description="Convert Excel (raw) to CSV (bronze)")
    ap.add_argument("--env", default="local", help="Environment (local/aws-dev/aws-prod)")
    ap.add_argument("--src", help="Source dir/URI (override config)")
    ap.add_argument("--dst", help="Destination dir/URI (override config)")
    ap.add_argument("--log-dir", help="Directory/URI for logs (override config)")
    ap.add_argument("--first-sheet-only", action="store_true", help="Export only the first sheet")
    ap.add_argument("--encoding", default=None, help="CSV encoding (default from config or utf-8-sig)")
    args = ap.parse_args()

    log = get_logger(PROCESS_NAME)

    if args.src and args.dst:
        src = pathlib.Path(args.src)
        dst = pathlib.Path(args.dst)
        log_dir = pathlib.Path(args.log_dir) if args.log_dir else pathlib.Path("logs")
        first_only = args.first_sheet_only
        encoding = args.encoding or "utf-8-sig"
    else:
        if not HAVE_CFG:
            log.error(
                "Config not available (likely PyYAML missing). "
                "Install deps or pass --src, --dst, and optionally --log-dir."
            )
            if CFG_ERR:
                log.error(f"Root cause: {CFG_ERR}")
            sys.exit(2)
        env_cfg = load_env_config(args.env)
        src = resolve_path(env_cfg, "raw_excel")
        dst = resolve_path(env_cfg, "bronze_csv")
        log_dir = resolve_path(env_cfg, "logs")
        first_only = env_cfg.get("excel", {}).get("first_sheet_only", args.first_sheet_only)
        encoding = args.encoding or env_cfg.get("excel", {}).get("encoding", "utf-8-sig")

    # Build and attach file/S3 log destination
    log_uri = build_log_uri(log_dir, PROCESS_NAME)
    add_log_destination(log, str(log_uri))
    log.info(f"Log file: {log_uri}")
    log.info(f"Converting from {src} -> {dst}; first_sheet_only={first_only}; encoding={encoding}")

    try:
        outs = excel_to_csv_dir(src, dst, first_sheet_only=first_only, encoding=encoding)
        log.info(f"DONE. Wrote {len(outs)} CSV file(s).")
    finally:
        # ensure file/S3 handler flushes
        close_logger(log)

if __name__ == "__main__":
    main()
