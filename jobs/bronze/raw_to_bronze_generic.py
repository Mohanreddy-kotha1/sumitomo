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

# Defaults
SRC_ROOT = Path(r"C:\Users\mohanreddykotha\sumitomo_datalake\sumitomo\data\bronze_csv_files")
DELTA_ROOT = Path(r"C:\Users\mohanreddykotha\sumitomo_datalake\sumitomo\data\bronze_delta")
DEFAULT_FRAMEWORK_DIR = Path(r"C:\Users\mohanreddykotha\sumitomo_datalake\sumitomo\framework")
DEFAULT_LOG_DIR = Path(r"C:\Users\mohanreddykotha\sumitomo_datalake\sumitomo\logs")

SAFE_COL_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def is_perfect_headers(cols: List[str]) -> bool:
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
    s = (col or "").strip().lstrip("\ufeff")
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_]", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if s == "" or s[0].isdigit():
        s = f"c_{s}" if s else "c_col"
    if make_lower:
        s = s.lower()
    return s


def normalize_headers(cols: List[str], make_lower: bool = True) -> Tuple[List[str], Dict[str, str]]:
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
    name = stem.lstrip("\ufeff").strip()
    name = re.sub(r"^\d+[\.\)\-_ ]+\s*", "", name)
    name = re.sub(r"[^A-Za-z0-9_\-]+", "_", name).strip("_")
    if not name:
        name = "table"
    if case == "lower":
        name = name.lower()
    return name


def read_csv_iter(csv_path: Path, encoding: str, chunksize: Optional[int]) -> Iterable[pd.DataFrame]:
    read_kwargs = dict(
        dtype=str,
        keep_default_na=False,
        na_values=[],
        encoding=encoding,
        sep=None,
        engine="python",
    )
    if chunksize and chunksize > 0:
        reader = pd.read_csv(csv_path, chunksize=chunksize, **read_kwargs)
        for chunk in reader:
            yield chunk
    else:
        yield pd.read_csv(csv_path, **read_kwargs)


def align_columns(df: pd.DataFrame, target_cols: List[str]) -> pd.DataFrame:
    for c in target_cols:
        if c not in df.columns:
            df[c] = ""
    return df[target_cols]


def setup_logger(framework_dir: Path, log_dir: Path, process_name: str) -> logging.Logger:
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
    normalize: str = "auto",
    case: str = "as-is",
    encoding: str = "utf-8-sig",
    chunksize: Optional[int] = None,
) -> int:
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
                if is_perfect_headers(cols):
                    header_mapping = {c: c for c in cols}
                    target_cols = cols
                else:
                    new_cols, mapping = normalize_headers(cols, make_lower=(case == "lower"))
                    chunk = chunk.rename(columns=mapping)
                    header_mapping = mapping
                    target_cols = new_cols
        else:
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
