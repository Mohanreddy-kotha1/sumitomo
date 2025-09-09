-- Build/merge Silver dim_product from Bronze Delta
MERGE INTO delta.`{silver_delta_path}/dim_product` AS tgt
USING (
  SELECT product_id, product_name, category, brand, ingest_ts
  FROM delta.`{bronze_delta_path}/dim_product`
) AS src
ON tgt.product_id = src.product_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
