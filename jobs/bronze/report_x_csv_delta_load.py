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

# Accept either a folder or a single file
INPUT_PATH = Path(r"C:\Users\mohanreddykotha\sumitomo_datalake\sumitomo\data\bronze_csv_files\Bulgaria\REPORT_X_BULGARIA.csv")
DELTA_ROOT = Path(r"C:\Users\mohanreddykotha\sumitomo_datalake\sumitomo\data\bronze_delta\Bulgaria\REPORT_X_BULGARIA")

UNIT_SEP = "\u001F"
SAFE_COL_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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


def align_columns(df: pd.DataFrame, target_cols: List[str]) -> pd.DataFrame:
    for c in target_cols:
        if c not in df.columns:
            df[c] = ""
    return df[target_cols]


def read_csv_iter(csv_path: Path, encoding: str = "utf-8", chunksize: Optional[int] = None) -> Iterable[pd.DataFrame]:
    read_kwargs = dict(
        dtype=str,
        keep_default_na=False,
        na_values=[],
        encoding=encoding,
        sep=UNIT_SEP, 
        engine="python",
        header=0
    )
    if chunksize and chunksize > 0:
        reader = pd.read_csv(csv_path, chunksize=chunksize, **read_kwargs)
        for chunk in reader:
            yield chunk
    else:
        yield pd.read_csv(csv_path, **read_kwargs)


def is_delta_table(path: Path) -> bool:
    log_dir = path / "_delta_log"
    return log_dir.exists() and any(log_dir.glob("*.json"))


def process_single_csv(csv_path: Path, delta_root: Path) -> int:
    table_name = csv_path.stem
    delta_dir = delta_root / table_name
    delta_dir.mkdir(parents=True, exist_ok=True)

    rowcount = 0
    first_chunk = True
    header_mapping: Dict[str, str] = {}
    target_cols: List[str] = []

    for chunk in read_csv_iter(csv_path):
        if first_chunk:
            cols = list(chunk.columns)
            new_cols, mapping = normalize_headers(cols, make_lower=True)
            chunk = chunk.rename(columns=mapping)
            header_mapping = mapping
            target_cols = new_cols

            if "order_no" not in chunk.columns:
                for col in list(chunk.columns):
                    if col.lower() == "order_no":
                        chunk.rename(columns={col: "order_no"}, inplace=True)
                        break

            if is_delta_table(delta_dir):
                existing_df = DeltaTable(str(delta_dir)).to_pandas()
                chunk = pd.concat([existing_df, chunk], ignore_index=True)
                if "order_no" in chunk.columns:
                    chunk = chunk.drop_duplicates(subset=["order_no"])
                else:
                    chunk = chunk.drop_duplicates(ignore_index=True)

        else:
            chunk = chunk.rename(columns=header_mapping)
            chunk = align_columns(chunk, target_cols)

        write_mode = "overwrite" if first_chunk else "append"
        write_deltalake(str(delta_dir), chunk, mode=write_mode)
        rowcount += len(chunk)
        first_chunk = False

    print(f"Processed {csv_path.name} -> {rowcount} rows -> {delta_dir}")
    return rowcount


if INPUT_PATH.is_file():
    process_single_csv(INPUT_PATH, DELTA_ROOT)
elif INPUT_PATH.is_dir():
    files = sorted(INPUT_PATH.glob("**/*.csv"))
    for f in files:
        process_single_csv(f, DELTA_ROOT)
else:
    print(f"Invalid input path: {INPUT_PATH}")
