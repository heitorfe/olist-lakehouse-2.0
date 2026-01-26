-- =============================================================================
-- Silver Layer: Order Reviews (Data Quality & Transformation)
-- =============================================================================
-- Description: Cleanses and validates review data from bronze layer
-- Source: bronze_order_reviews
-- Data Quality: DROP rows with invalid IDs or out-of-range scores
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE silver_order_reviews (
    -- Data Quality Constraints (DROP invalid rows)
    CONSTRAINT valid_review_id
        EXPECT (review_id IS NOT NULL AND LENGTH(TRIM(review_id)) = 32)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_order_id
        EXPECT (order_id IS NOT NULL AND LENGTH(TRIM(order_id)) = 32)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_review_score
        EXPECT (review_score IS NOT NULL AND review_score BETWEEN 1 AND 5)
        ON VIOLATION DROP ROW
)
COMMENT 'Cleansed review data with validated scores and sentiment classification'
TBLPROPERTIES (
    'quality' = 'silver',
    'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
    -- Primary identifiers
    TRIM(review_id) AS review_id,
    TRIM(order_id) AS order_id,

    -- Review data
    CAST(review_score AS INT) AS review_score,
    TRIM(review_comment_title) AS review_comment_title,
    TRIM(review_comment_message) AS review_comment_message,

    -- Timestamps
    TO_TIMESTAMP(review_creation_date) AS review_creation_date,
    TO_TIMESTAMP(review_answer_timestamp) AS review_answer_timestamp,

    -- Derived fields: Sentiment classification based on score
    CASE
        WHEN CAST(review_score AS INT) >= 4 THEN 'positive'
        WHEN CAST(review_score AS INT) = 3 THEN 'neutral'
        ELSE 'negative'
    END AS sentiment,

    -- Response time calculation
    CASE
        WHEN review_answer_timestamp IS NOT NULL AND review_creation_date IS NOT NULL
        THEN DATEDIFF(TO_TIMESTAMP(review_answer_timestamp), TO_TIMESTAMP(review_creation_date))
        ELSE NULL
    END AS response_days,

    -- Has comment flag
    CASE
        WHEN review_comment_message IS NOT NULL AND LENGTH(TRIM(review_comment_message)) > 0
        THEN TRUE
        ELSE FALSE
    END AS has_comment,

    -- Audit columns
    _source_file,
    _file_modified_at,
    _ingested_at,
    current_timestamp() AS _processed_at
FROM STREAM(bronze_order_reviews);
