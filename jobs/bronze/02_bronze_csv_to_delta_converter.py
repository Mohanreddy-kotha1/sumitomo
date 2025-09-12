import json
import logging
import re
import sys
import traceback
from pathlib import Path
from typing import Dict, List, Tuple, Iterable, Optional

import pandas as pd
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

# ------------------------------------------------------------------------------
# Defaults (LOCAL PATHS + Delimiter/Encoding)
# ------------------------------------------------------------------------------
SRC_ROOT = Path(r"C:\Users\mohanreddykotha\sumitomo_datalake\sumitomo\data\bronze_csv_files")
DELTA_ROOT = Path(r"C:\Users\mohanreddykotha\sumitomo_datalake\sumitomo\data\bronze_delta")
DEFAULT_FRAMEWORK_DIR = Path(r"C:\Users\mohanreddykotha\sumitomo_datalake\sumitomo\framework")
DEFAULT_LOG_DIR = Path(r"C:\Users\mohanreddykotha\sumitomo_datalake\sumitomo\logs")

# CSVs produced earlier use ASCII Unit Separator (non-printable) as the delimiter
UNIT_SEP = "\u001F"
DEFAULT_ENCODING = "utf-8"

SAFE_COL_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def is_perfect_headers(cols: List[str]) -> bool:
    """All columns valid identifiers and unique?"""
    seen = set()
    for c in cols:
        if c is None:
            return False
        name = str(c).strip().lstrip("\ufeff")
        if not SAFE_COL_RE.match(name) or name in seen:
            return False
        seen.add(name)
    return True


def normalize_one_header(col: str, make_lower: bool = True) -> str:
    """Normalize a single header to Delta/Spark-safe identifier."""
    s = (col or "").strip().lstrip("\ufeff")
    s = re.sub(r"[\r\n\t]+", " ", s)       # collapse control whitespace
    s = re.sub(r"\s+", "_", s)             # spaces -> underscores
    s = re.sub(r"[^A-Za-z0-9_]", "_", s)   # non-word -> underscore
    s = re.sub(r"_+", "_", s).strip("_")   # dedupe underscores
    if s == "" or s[0].isdigit():
        s = f"c_{s}" if s else "c_col"
    if make_lower:
        s = s.lower()
    return s


def normalize_headers(cols: List[str], make_lower: bool = True) -> Tuple[List[str], Dict[str, str]]:
    """Return normalized column list and mapping {orig -> normalized}, ensuring uniqueness."""
    mapping: Dict[str, str] = {}
    new_cols: List[str] = []
    seen: set = set()
    counts: Dict[str, int] = {}

    for c in cols:
        base = normalize_one_header(str(c), make_lower=make_lower)
        name = base
        while name in seen:
            counts[base] = counts.get(base, 1) + 1
            name = f"{base}_{counts[base]}"
        seen.add(name)
        mapping[str(c)] = name
        new_cols.append(name)
    return new_cols, mapping


def safe_table_dir_name(stem: str, case: str = "as-is") -> str:
    """Derive a safe folder/table name from filename stem."""
    name = stem.lstrip("\ufeff").strip()
    name = re.sub(r"^\d+[\.\)\-_ ]+\s*", "", name)        # drop leading ordinals like "01 - "
    name = re.sub(r"[^A-Za-z0-9_\-]+", "_", name).strip("_")
    if not name:
        name = "table"
    if case == "lower":
        name = name.lower()
    return name


def read_csv_iter(csv_path: Path, encoding: str, chunksize: Optional[int]) -> Iterable[pd.DataFrame]:
    """
    Read CSV with explicit UNIT_SEP delimiter.
    - We keep dtype=str and disable NA parsing to preserve raw text.
    - First row is header by default (pandas default header=0).
    """
    read_kwargs = dict(
        dtype=str,
        keep_default_na=False,
        na_values=[],
        encoding=encoding,
        sep=UNIT_SEP,          # <<< IMPORTANT: our custom delimiter
        engine="python",       # robust for unusual delimiters
    )
    if chunksize and chunksize > 0:
        reader = pd.read_csv(csv_path, chunksize=chunksize, **read_kwargs)
        for chunk in reader:
            yield chunk
    else:
        yield pd.read_csv(csv_path, **read_kwargs)


def align_columns(df: pd.DataFrame, target_cols: List[str]) -> pd.DataFrame:
    """Ensure chunk has all target columns in order; add missing as empty strings."""
    for c in target_cols:
        if c not in df.columns:
            df[c] = ""
    return df[target_cols]


def setup_logger(framework_dir: Path, log_dir: Path, process_name: str) -> logging.Logger:
    """Use your framework logger (file destination), no data values printed."""
    if str(framework_dir) not in sys.path:
        sys.path.insert(0, str(framework_dir))
    from logger import get_logger, add_log_destination, build_log_uri  # type: ignore

    logger = get_logger(name=process_name, level=logging.INFO)
    log_uri = build_log_uri(str(log_dir), process_name=process_name)
    add_log_destination(logger, log_uri)
    logger.info(json.dumps({"event": "logger_initialized", "log_uri": log_uri}))
    return logger


def process_single_csv(
    csv_path: Path,
    src_root: Path,
    delta_root: Path,
    logger: logging.Logger,
    normalize: str = "auto",          # "auto" | "yes" | "no"
    case: str = "as-is",              # "as-is" | "lower"
    encoding: str = DEFAULT_ENCODING, # your CSVs are utf-8
    chunksize: Optional[int] = None,  # set e.g., 100_000 for big files
) -> int:
    """
    Read a single UNIT_SEP-delimited CSV and write to a Delta table directory.
    - First chunk drives header normalization and target schema.
    - Subsequent chunks are conformed and appended.
    """
    rel_parent = csv_path.parent.relative_to(src_root)
    table_name = safe_table_dir_name(csv_path.stem, case=case)
    delta_dir = delta_root / rel_parent / table_name
    delta_dir.mkdir(parents=True, exist_ok=True)

    logger.info(json.dumps({"event": "start_file", "csv": str(csv_path), "delta_dir": str(delta_dir)}))

    rowcount = 0
    first_chunk = True
    header_mapping: Dict[str, str] = {}
    target_cols: List[str] = []

    for chunk in read_csv_iter(csv_path, encoding=encoding, chunksize=chunksize):
        if first_chunk:
            cols = list(chunk.columns)
            if normalize == "yes":
                new_cols, mapping = normalize_headers(cols, make_lower=(case == "lower"))
                chunk = chunk.rename(columns=mapping)
                header_mapping = mapping
                target_cols = new_cols
            elif normalize == "no":
                header_mapping = {c: c for c in cols}
                target_cols = cols
            else:
                # auto: keep if perfect; otherwise normalize
                if is_perfect_headers(cols):
                    header_mapping = {c: c for c in cols}
                    target_cols = cols
                else:
                    new_cols, mapping = normalize_headers(cols, make_lower=(case == "lower"))
                    chunk = chunk.rename(columns=mapping)
                    header_mapping = mapping
                    target_cols = new_cols
        else:
            # conform subsequent chunks
            chunk = chunk.rename(columns=header_mapping)
            chunk = align_columns(chunk, target_cols)

        write_mode = "overwrite" if first_chunk else "append"
        write_deltalake(str(delta_dir), chunk, mode=write_mode)
        rowcount += len(chunk)
        first_chunk = False

    logger.info(json.dumps({"event": "end_file", "csv": str(csv_path), "delta_dir": str(delta_dir), "rows": rowcount}))
    return rowcount


def main():
    logger = setup_logger(DEFAULT_FRAMEWORK_DIR, DEFAULT_LOG_DIR, process_name="raw_to_bronze_generic")

    if not SRC_ROOT.exists():
        logger.error(json.dumps({"event": "src_root_missing", "path": str(SRC_ROOT)}))
        sys.exit(2)

    # discover CSV files written by the excel→csv job (could include __Sheet suffix)
    files = sorted(SRC_ROOT.glob("**/*.csv"))
    files = [f for f in files if f.is_file()]
    if not files:
        logger.warning(json.dumps({"event": "no_files_found"}))
        return

    total_rows = 0
    successes = 0
    for f in files:
        try:
            rows = process_single_csv(
                csv_path=f,
                src_root=SRC_ROOT,
                delta_root=DELTA_ROOT,
                logger=logger,
                # normalize="auto" keeps headers if already safe; otherwise normalizes
                # case="as-is" (set to "lower" if you want forced lowercase)
                encoding=DEFAULT_ENCODING,
                chunksize=None,  # set a number for large files if needed
            )
            total_rows += rows
            successes += 1
        except Exception as e:
            logger.error(json.dumps({
                "event": "file_failed",
                "csv": str(f),
                "error": str(e),
                "trace": traceback.format_exc()
            }, ensure_ascii=False))

    logger.info(json.dumps({"event": "run_complete", "files_total": len(files), "converted": successes, "total_rows": total_rows}))


if __name__ == "__main__":
    main()
