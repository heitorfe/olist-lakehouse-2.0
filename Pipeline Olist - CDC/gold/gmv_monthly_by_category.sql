CREATE OR REFRESH MATERIALIZED VIEW gold_gmv_monthly_by_category
COMMENT "GMV mensal por categoria (itens + produtos)."
CLUSTER BY AUTO
AS
SELECT
  DATE_TRUNC('month', o.purchase_timestamp) AS order_month,
  p.product_category_name AS category,
  COUNT(*) AS items,
  SUM(i.price) AS items_gmv,
  SUM(i.freight_value) AS freight_total
FROM silver_orders o
JOIN silver_order_items i
  ON o.order_id = i.order_id
JOIN silver_products p
  ON i.product_id = p.product_id
GROUP BY DATE_TRUNC('month', o.purchase_timestamp), p.product_category_name;
