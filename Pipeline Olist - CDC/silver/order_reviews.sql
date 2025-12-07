-- =========================================================
-- silver_order_reviews (1 linha por review_id)
-- =========================================================
CREATE OR REFRESH STREAMING TABLE silver_order_reviews (
  CONSTRAINT or_review_id_not_null EXPECT (review_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT or_order_id_not_null EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT or_score_range EXPECT (CAST(review_score AS INT) BETWEEN 1 AND 5) ON VIOLATION DROP ROW
)
AS
SELECT
  TRIM(review_id) AS review_id,
  TRIM(order_id) AS order_id,
  CAST(TRIM(review_score) AS INT) AS review_score,
  TRIM(review_comment_title) AS review_comment_title,
  TRIM(review_comment_message) AS review_comment_message,
  TO_DATE(TRIM(review_creation_date), 'yyyy-MM-dd HH:mm:ss') AS review_creation_date,
  TO_TIMESTAMP(TRIM(review_answer_timestamp), 'yyyy-MM-dd HH:mm:ss') AS review_answer_timestamp,

  CURRENT_TIMESTAMP() AS processed_time,
  '1.0' AS silver_layer_version
FROM STREAM bronze_order_reviews;