# =========================
# 1) IMPORTS
# =========================
import sys
import re
import json
import traceback
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# =========================
# 2) PARAMETERS
# =========================
SRC_ROOT = Path(r"C:\Users\mohanreddykotha\sumitomo_datalake\sumitomo\data\bronze_csv_files")
DELTA_ROOT = Path(r"C:\Users\mohanreddykotha\sumitomo_datalake\sumitomo\data\bronze_delta")
LOG_DIR = Path(r"C:\Users\mohanreddykotha\sumitomo_datalake\sumitomo\logs")

UNIT_SEP = "\u001F"
DEFAULT_ENCODING = "utf-8"
PROCESS_NAME = "raw_to_bronze_spark"

# =========================
# 3) HELPER FUNCTIONS
# =========================
SAFE_COL_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def is_perfect_headers(cols):
    seen = set()
    for c in cols:
        if not c or not SAFE_COL_RE.match(c) or c in seen:
            return False
        seen.add(c)
    return True

def normalize_one(col):
    s = (col or "").strip().lstrip("\ufeff")
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_]", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s or s[0].isdigit():
        s = f"c_{s}" if s else "c_col"
    return s.lower()

def normalize_headers(cols):
    mapping = {}
    seen = set()
    counts = {}
    new_cols = []
    for c in cols:
        base = normalize_one(c)
        name = base
        while name in seen:
            counts[base] = counts.get(base, 1) + 1
            name = f"{base}_{counts[base]}"
        seen.add(name)
        mapping[c] = name
        new_cols.append(name)
    return new_cols, mapping

def safe_table_dir_name(stem):
    name = stem.lstrip("\ufeff").strip()
    name = re.sub(r"^\d+[\.\)\-_ ]+\s*", "", name)
    name = re.sub(r"[^A-Za-z0-9_\-]+", "_", name).strip("_")
    return name.lower() if name else "table"

def setup_logger():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{PROCESS_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    return log_path.open("w", encoding="utf-8")

# =========================
# 4) MAIN
# =========================
def main():
    logf = setup_logger()
    spark = SparkSession.builder.appName(PROCESS_NAME).getOrCreate()

    if not SRC_ROOT.exists():
        msg = f"Source root not found: {SRC_ROOT}"
        print(msg)
        logf.write(msg + "\n")
        sys.exit(2)

    files = sorted(SRC_ROOT.glob("**/*.csv"))
    files = [f for f in files if f.is_file()]
    if not files:
        msg = "No CSV files found"
        print(msg)
        logf.write(msg + "\n")
        return

    total_rows = 0
    successes = 0

    for f in files:
        try:
            print(f"Processing {f}")
            logf.write(f"START {f}\n")

            df = spark.read.option("sep", UNIT_SEP).option("header", True).option("encoding", DEFAULT_ENCODING).csv(str(f))
            cols = df.columns
            if not is_perfect_headers(cols):
                new_cols, _ = normalize_headers(cols)
                for old, new in zip(cols, new_cols):
                    df = df.withColumnRenamed(old, new)

            rel_parent = f.parent.relative_to(SRC_ROOT)
            table_name = safe_table_dir_name(f.stem)
            delta_dir = DELTA_ROOT / rel_parent / table_name

            df.write.format("delta").mode("overwrite").save(str(delta_dir))

            count = df.count()
            total_rows += count
            successes += 1

            msg = f"END {f} -> {delta_dir} rows={count}"
            print(msg)
            logf.write(msg + "\n")

        except Exception as e:
            err = f"FAILED {f}: {e}"
            print(err)
            logf.write(err + "\n")
            logf.write(traceback.format_exc() + "\n")

    summary = f"COMPLETE files={len(files)} success={successes} total_rows={total_rows}"
    print(summary)
    logf.write(summary + "\n")
    logf.close()
    spark.stop()

if __name__ == "__main__":
    main()
