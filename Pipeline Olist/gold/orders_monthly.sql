CREATE OR REFRESH MATERIALIZED VIEW gold_orders_monthly
COMMENT "KPIs mensais de pedidos."
CLUSTER BY AUTO
AS
SELECT
  DATE_TRUNC('month', purchase_timestamp) AS order_month,
  COUNT(*) AS orders,
  SUM(order_total_expected) AS gmv_expected,
  SUM(paid_total) AS gmv_paid,
  AVG(paid_total) AS aov_paid,
  AVG(order_total_expected) AS aov_expected
FROM silver_orders_full
GROUP BY DATE_TRUNC('month', purchase_timestamp);
