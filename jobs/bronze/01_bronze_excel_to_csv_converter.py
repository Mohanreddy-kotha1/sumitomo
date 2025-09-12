# =========================
# 1) IMPORTS
# =========================
import os
import sys
import re
import json
import logging
import pathlib
import argparse
import traceback
from datetime import datetime
from typing import List

import pandas as pd

# Optional framework imports (graceful if missing)
try:
    # Resolve repo root if __file__ exists
    if '__file__' in globals():
        sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))
    from framework.logger import get_logger as fw_get_logger, build_log_uri, add_log_destination, close_logger
    from framework.config import load_env_config, resolve_path
    from framework.io import excel_to_csv_dir as fw_excel_to_csv_dir  # might not accept sep=
    HAVE_FRAMEWORK = True
except Exception:
    HAVE_FRAMEWORK = False
    fw_get_logger = build_log_uri = add_log_destination = close_logger = None  # type: ignore
    fw_excel_to_csv_dir = None  # type: ignore

# =========================
# 2) PARAMETERS (EDIT HERE FOR LOCAL)
# =========================
PROCESS_NAME = "excel2csv"
UNIT_SEP = "\u001F"               # ASCII Unit Separator (non-printable)
DEFAULT_ENCODING = "utf-8"        # CSV encoding
FIRST_SHEET_ONLY = True           # Legacy default; dynamic behavior overrides this (see AUTO_ALL_SHEETS)
AUTO_ALL_SHEETS = True            # If a workbook has >1 sheet, export each sheet to its own CSV

# Local (Windows) defaults — adjust if needed
LOCAL_SRC = pathlib.Path(r"C:\Users\mohanreddykotha\sumitomo_datalake\sumitomo\data\raw_excel_files")
LOCAL_DST = pathlib.Path(r"C:\Users\mohanreddykotha\sumitomo_datalake\sumitomo\data\bronze_csv_files")
LOCAL_LOG_DIR = pathlib.Path(r"C:\Users\mohanreddykotha\sumitomo_datalake\sumitomo\logs")

# =========================
# 3) HELPER FUNCTIONS
# =========================
def is_glue_env() -> bool:
    """Heuristic: detect AWS Glue Python Shell."""
    return "AWS_EXECUTION_ENV" in os.environ or "AWS_REGION" in os.environ

def ensure_dir(path: pathlib.Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def safe_sheet_name(name: str) -> str:
    return re.sub(r'[\\/:\*\?"<>\|]+', "_", str(name))

def setup_local_logger(log_dir: pathlib.Path) -> logging.Logger:
    """File + console logger for local runs."""
    ensure_dir(log_dir)
    log_path = log_dir / f"{PROCESS_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logger = logging.getLogger(PROCESS_NAME)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)

    logger.info(f"Log file: {log_path}")
    return logger

def setup_glue_logger() -> logging.Logger:
    """Console (CloudWatch) logger for Glue. Optional S3 upload handled after run."""
    logger = logging.getLogger(PROCESS_NAME)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger

def upload_text_to_s3(s3_uri: str, content: str, logger: logging.Logger) -> None:
    """Upload text content to S3 key."""
    try:
        import boto3
        from urllib.parse import urlparse

        parsed = urlparse(s3_uri)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))
        logger.info(f"Uploaded log to s3://{bucket}/{key}")
    except Exception as e:
        logger.warning(f"Failed to upload log to {s3_uri}: {e}")

def workbook_sheet_names(xls_path: pathlib.Path) -> List[str]:
    """Return sheet names safely; falls back to empty list on error."""
    try:
        xf = pd.ExcelFile(xls_path)
        return list(xf.sheet_names)
    except Exception:
        return []

def read_sheet(xls_path: pathlib.Path, sheet_name, logger: logging.Logger) -> pd.DataFrame:
    """Read a single sheet as object dtype (no type coercion surprises)."""
    try:
        return pd.read_excel(xls_path, sheet_name=sheet_name, dtype=object)
    except Exception as e:
        logger.error(f"Read failed for {xls_path} [sheet={sheet_name}]: {e}")
        logger.debug("Traceback:\n" + traceback.format_exc())
        raise

def write_csv(df: pd.DataFrame, out_path: pathlib.Path, sep: str, encoding: str):
    df.to_csv(out_path, sep=sep, index=False, encoding=encoding)

def fallback_excel_to_csv_dir(
    src_dir: pathlib.Path,
    dst_dir: pathlib.Path,
    *,
    first_sheet_only: bool = FIRST_SHEET_ONLY,
    encoding: str = DEFAULT_ENCODING,
    sep: str = UNIT_SEP,
    auto_all_sheets: bool = AUTO_ALL_SHEETS,
    logger: logging.Logger,
) -> List[pathlib.Path]:
    """
    Pure-pandas converter for .xls/.xlsx → CSV using custom separator.
    - If auto_all_sheets=True and workbook has >1 sheet: writes one CSV per sheet.
    - If a sheet is empty (no columns/rows), it is skipped.
    - Writes no data content to logs; only file-level progress.
    """
    outs: List[pathlib.Path] = []
    ensure_dir(dst_dir)

    excel_exts = {".xls", ".xlsx"}
    files = [p for p in src_dir.rglob("*") if p.is_file() and p.suffix.lower() in excel_exts]
    logger.info(f"Found {len(files)} Excel file(s) under: {src_dir}")

    for idx, xls_path in enumerate(files, 1):
        rel_parent = xls_path.parent.relative_to(src_dir)
        out_parent = dst_dir / rel_parent
        ensure_dir(out_parent)
        logger.info(f"[{idx}/{len(files)}] Converting: {xls_path}")

        try:
            sheets = workbook_sheet_names(xls_path)
            # Dynamic behavior: if workbook has >1 sheet, export all sheets
            export_all = (auto_all_sheets and len(sheets) > 1)

            if export_all or not first_sheet_only:
                if not sheets:
                    logger.warning(f"No sheets detected: {xls_path}")
                    continue
                for sname in sheets:
                    df = read_sheet(xls_path, sname, logger)
                    if df.empty or len(df.columns) == 0:
                        logger.info(f"  - Skipped empty sheet: {sname}")
                        continue
                    out_csv = out_parent / f"{xls_path.stem}__{safe_sheet_name(sname)}.csv"
                    write_csv(df, out_csv, sep=sep, encoding=encoding)
                    outs.append(out_csv)
                    logger.info(f"  - Wrote: {out_csv}")
            else:
                # Single-sheet behavior
                df = read_sheet(xls_path, 0, logger)
                if df.empty or len(df.columns) == 0:
                    logger.info("  - Skipped (first sheet empty).")
                    continue
                out_csv = out_parent / (xls_path.stem + ".csv")
                write_csv(df, out_csv, sep=sep, encoding=encoding)
                outs.append(out_csv)
                logger.info(f"  - Wrote: {out_csv}")

        except Exception as e:
            logger.error(f"FAILED: {xls_path} -> {e}")
            logger.debug("Traceback:\n" + traceback.format_exc())
            # Continue to next file

    return outs

def try_framework_convert(
    src: pathlib.Path,
    dst: pathlib.Path,
    *,
    first_sheet_only: bool,
    encoding: str,
    sep: str,
    auto_all_sheets: bool,
    logger: logging.Logger,
) -> List[pathlib.Path]:
    """
    Conversion entrypoint. We now **always** use the fallback to guarantee:
      - per-sheet exports when multiple sheets exist,
      - skipping empty sheets,
      - and the U+001F separator.
    The framework helper is retained for compatibility but not used for conversion.
    """
    logger.info("Using built-in converter to ensure per-sheet CSV generation and custom separator.")
    return fallback_excel_to_csv_dir(
        src, dst,
        first_sheet_only=first_sheet_only,
        encoding=encoding,
        sep=sep,
        auto_all_sheets=auto_all_sheets,
        logger=logger,
    )

# =========================
# 4) MAIN
# =========================
def main(argv: List[str] = None) -> int:
    argv = argv or sys.argv[1:]

    parser = argparse.ArgumentParser(description="Excel (.xls/.xlsx) → CSV with ASCII Unit Separator (U+001F).")
    parser.add_argument("--mode", choices=["local", "glue"], default="local", help="Run mode. In Glue, logs go to CloudWatch.")
    parser.add_argument("--src", type=str, help="Source folder (Excel files). Required in Glue mode.")
    parser.add_argument("--dst", type=str, help="Destination folder (CSV output). Required in Glue mode.")
    parser.add_argument("--log-s3-uri", type=str, default=None, help="Optional S3 URI to upload a copy of the log (Glue mode).")
    parser.add_argument("--first-only", action="store_true", help="Convert only the first sheet from each workbook (overridden by auto-all when multiple sheets).")
    parser.add_argument("--all-sheets", action="store_true", help="Force convert all sheets (one CSV per sheet) for every workbook.")
    parser.add_argument("--encoding", type=str, default=DEFAULT_ENCODING, help="CSV encoding (default utf-8).")
    parser.add_argument("--sep", type=str, default=UNIT_SEP, help="CSV separator (default U+001F).")
    parser.add_argument("--no-auto-all", action="store_true", help="Disable auto multi-sheet export; obey --first-only unless --all-sheets is set.")

    args = parser.parse_args(argv)

    # Resolve mode defaults
    detected_glue = is_glue_env()
    mode = args.mode
    if detected_glue and mode != "glue":
        mode = "glue"  # force glue if running inside Glue container

    # Logger
    if mode == "glue":
        logger = setup_glue_logger()
    else:
        logger = setup_local_logger(LOCAL_LOG_DIR)

    # Sheet selection
    # Base choice from flags
    first_sheet_only = True if args.first_only else (False if args.all_sheets else FIRST_SHEET_ONLY)
    # Auto behavior: if not disabled, auto_all_sheets True means "export all sheets whenever a workbook has >1 sheet"
    auto_all_sheets = AUTO_ALL_SHEETS and (not args.no_auto_all)
    # If user forces --all-sheets, that overrides any auto logic (always export all)
    if args.all_sheets:
        auto_all_sheets = True
        first_sheet_only = False

    encoding = args.encoding or DEFAULT_ENCODING
    sep = args.sep or UNIT_SEP

    # Paths
    if mode == "glue":
        if not args.src or not args.dst:
            logger.error("In Glue mode, --src and --dst are required (S3 prefixes or mounted paths).")
            return 2
        src = pathlib.Path(args.src)
        dst = pathlib.Path(args.dst)
    else:
        src = LOCAL_SRC
        dst = LOCAL_DST

    # Banner
    print(f"Starting {PROCESS_NAME} (mode={mode}) …")
    logger.info(
        json.dumps(
            {
                "process": PROCESS_NAME,
                "mode": mode,
                "src": str(src),
                "dst": str(dst),
                "first_sheet_only": first_sheet_only,
                "auto_all_sheets": auto_all_sheets,
                "encoding": encoding,
                "sep": "U+001F",
                "framework_available": HAVE_FRAMEWORK,
            }
        )
    )

    # Convert
    outs = try_framework_convert(
        src=src,
        dst=dst,
        first_sheet_only=first_sheet_only,
        encoding=encoding,
        sep=sep,
        auto_all_sheets=auto_all_sheets,
        logger=logger,
    )

    print(f"Completed: wrote {len(outs)} CSV file(s).")
    logger.info(f"DONE. Wrote {len(outs)} CSV file(s).")

    # Glue optional log upload
    if mode == "glue" and args.log_s3_uri:
        # Build a small text log summary; CloudWatch has full logs already
        summary = f"{datetime.utcnow().isoformat()}Z | {PROCESS_NAME} | outputs={len(outs)}\n"
        upload_text_to_s3(args.log_s3_uri, summary, logger)

    return 0

if __name__ == "__main__":
    sys.exit(main())
