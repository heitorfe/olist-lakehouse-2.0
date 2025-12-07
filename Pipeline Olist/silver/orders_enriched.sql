CREATE OR REFRESH MATERIALIZED VIEW mv_order_items_agg
AS
SELECT
  order_id,
  COUNT(*)              AS items_count,
  SUM(price)            AS items_gmv,
  SUM(freight_value)    AS freight_total
FROM silver_order_items
WHERE order_id IS NOT NULL
GROUP BY order_id;

CREATE OR REFRESH MATERIALIZED VIEW mv_order_payments_agg
AS
SELECT
  order_id,
  COUNT(*)                   AS payments_count,
  SUM(payment_value)         AS paid_total,
  MAX(payment_installments)  AS max_installments
FROM silver_order_payments
WHERE order_id IS NOT NULL
GROUP BY order_id;

CREATE OR REFRESH MATERIALIZED VIEW mv_order_reviews_agg
AS
SELECT
  order_id,
  AVG(review_score)           AS avg_review_score,
  MAX(review_creation_date)   AS last_review_creation_date,
  MAX(review_answer_timestamp) AS last_review_answer_ts
FROM silver_order_reviews
WHERE order_id IS NOT NULL
GROUP BY order_id;

-- =========================================================
-- 2) Tabela enriquecida final (materialized view)
-- =========================================================

CREATE OR REFRESH MATERIALIZED VIEW silver_orders_full
AS
SELECT
  o.order_id,
  o.customer_id,
  o.status,
  o.purchase_timestamp,
  o.approved_at,
  o.delivered_carrier_date,
  o.delivered_customer_date,
  o.estimated_delivery_date,

  COALESCE(i.items_count, 0)     AS items_count,
  COALESCE(i.items_gmv, 0.0)     AS items_gmv,
  COALESCE(i.freight_total, 0.0) AS freight_total,

  COALESCE(p.payments_count, 0)  AS payments_count,
  COALESCE(p.paid_total, 0.0)    AS paid_total,
  p.max_installments,

  r.avg_review_score,
  r.last_review_creation_date,
  r.last_review_answer_ts,

  (COALESCE(i.items_gmv, 0.0) + COALESCE(i.freight_total, 0.0)) AS order_total_expected,
  (COALESCE(p.paid_total, 0.0) -
    (COALESCE(i.items_gmv, 0.0) + COALESCE(i.freight_total, 0.0))
  ) AS paid_minus_expected,

  CURRENT_TIMESTAMP() AS processed_time,
  '1.0' AS silver_layer_version
FROM silver_orders o
LEFT JOIN mv_order_items_agg    i ON o.order_id = i.order_id
LEFT JOIN mv_order_payments_agg p ON o.order_id = p.order_id
LEFT JOIN mv_order_reviews_agg  r ON o.order_id = r.order_id
WHERE o.order_id IS NOT NULL;