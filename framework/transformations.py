from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

def add_audit_columns(df: DataFrame) -> DataFrame:
    return df.withColumn("ingest_ts", F.current_timestamp())

def rename_and_cast(df: DataFrame, schema_cfg: dict) -> DataFrame:
    # Map source column names (non-English) to English and cast types
    renames = {c["source"]: c["name"] for c in schema_cfg["columns"]}
    casts = {c["name"]: c["type"] for c in schema_cfg["columns"]}
    for src, dst in renames.items():
        if src != dst and src in df.columns:
            df = df.withColumnRenamed(src, dst)

    for col, typ in casts.items():
        if col in df.columns:
            if typ == "string":
                df = df.withColumn(col, F.col(col).cast(StringType()))
            elif typ in ("int", "integer"):
                df = df.withColumn(col, F.col(col).cast(IntegerType()))
            elif typ in ("double", "float"):
                df = df.withColumn(col, F.col(col).cast(DoubleType()))
            elif typ == "date":
                df = df.withColumn(col, F.to_date(F.col(col)))
            else:
                # leave as-is if unknown type
                pass
    return df
