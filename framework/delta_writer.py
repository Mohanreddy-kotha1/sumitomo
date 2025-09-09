from pyspark.sql import DataFrame
from delta.tables import DeltaTable

def write_delta_overwrite(df: DataFrame, path: str, partitionBy: list[str] | None = None):
    writer = df.write.format("delta").mode("overwrite")
    if partitionBy:
        writer = writer.partitionBy(partitionBy)
    writer.save(path)

def merge_upsert(spark, source_df: DataFrame, target_path: str, keys: list[str]):
    if not DeltaTable.isDeltaTable(spark, target_path):
        # First-time write
        source_df.write.format("delta").mode("overwrite").save(target_path)
        return

    tgt = DeltaTable.forPath(spark, target_path)
    cond = " AND ".join([f"t.{k} = s.{k}" for k in keys])
    updates = {c: f"s.{c}" for c in source_df.columns}
    (tgt.alias("t")
        .merge(source_df.alias("s"), cond)
        .whenMatchedUpdate(set=updates)
        .whenNotMatchedInsert(values=updates)
        .execute())
