-- Reporting-ready Gold: join fact_sales with dim_product
CREATE OR REPLACE TABLE delta.`{gold_delta_path}/sales_summary`
USING DELTA
AS
SELECT
  f.sales_date,
  f.product_id,
  d.product_name,
  SUM(f.quantity) AS total_qty,
  SUM(f.quantity * f.price) AS gross_revenue
FROM delta.`{silver_delta_path}/fact_sales` f
LEFT JOIN delta.`{silver_delta_path}/dim_product` d
  ON f.product_id = d.product_id
GROUP BY f.sales_date, f.product_id, d.product_name;
