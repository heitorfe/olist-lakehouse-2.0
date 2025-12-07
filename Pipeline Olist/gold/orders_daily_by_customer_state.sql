CREATE OR REFRESH MATERIALIZED VIEW gold_orders_daily_by_customer_state
COMMENT "KPIs diários por UF do cliente."
CLUSTER BY AUTO
AS
SELECT
  DATE(o.purchase_timestamp) AS order_date,
  c.customer_state AS customer_state,
  COUNT(*) AS orders,
  SUM(o.order_total_expected) AS gmv_expected,
  SUM(o.paid_total) AS gmv_paid,
  AVG(o.avg_review_score) AS avg_review_score
FROM silver_orders_full o
JOIN silver_customers c
  ON o.customer_id = c.customer_id
GROUP BY DATE(o.purchase_timestamp), c.customer_state;
