CREATE OR REFRESH MATERIALIZED VIEW gold_gmv_daily_by_seller
COMMENT "GMV diário por seller (itens + pedidos)."
CLUSTER BY AUTO
AS
SELECT
  DATE(o.purchase_timestamp) AS order_date,
  i.seller_id,
  COUNT(*) AS items,
  SUM(i.price) AS items_gmv,
  SUM(i.freight_value) AS freight_total
FROM silver_orders o
JOIN silver_order_items i
  ON o.order_id = i.order_id
GROUP BY DATE(o.purchase_timestamp), i.seller_id;
