CREATE OR REFRESH MATERIALIZED VIEW gold_orders_daily
COMMENT "KPIs diários de pedidos (base: silver_orders_enriched)."
CLUSTER BY AUTO
AS
SELECT
  DATE(purchase_timestamp) AS order_date,
  COUNT(*) AS orders,
  SUM(order_total_expected) AS gmv_expected,
  SUM(paid_total) AS gmv_paid,
  AVG(items_count) AS avg_items_per_order,
  AVG(avg_review_score) AS avg_review_score,
  SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) / COUNT(*) AS delivered_rate,
  SUM(CASE WHEN status = 'canceled' THEN 1 ELSE 0 END) / COUNT(*) AS canceled_rate
FROM silver_orders_full
GROUP BY DATE(purchase_timestamp);
