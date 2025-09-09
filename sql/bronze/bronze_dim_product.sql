-- Optional: Keep DDLs for first-time Delta table creation (local or AWS)
-- In many cases Bronze Delta is created by write() from the job. This is illustrative.
CREATE TABLE IF NOT EXISTS delta.`{bronze_delta_path}/dim_product` (
  product_id STRING,
  product_name STRING,
  category STRING,
  brand STRING,
  ingest_ts TIMESTAMP
) USING DELTA;
