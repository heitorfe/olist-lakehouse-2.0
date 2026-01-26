-- =============================================================================
-- Silver Layer: Order Payments (Data Quality & Transformation)
-- =============================================================================
-- Description: Cleanses and validates payment data from bronze layer
-- Source: bronze_order_payments
-- Data Quality: DROP rows with invalid IDs or negative values
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE silver_order_payments (
    -- Data Quality Constraints (DROP invalid rows)
    CONSTRAINT valid_order_id
        EXPECT (order_id IS NOT NULL AND LENGTH(TRIM(order_id)) = 32)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_payment_sequential
        EXPECT (payment_sequential IS NOT NULL AND payment_sequential > 0)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_payment_type
        EXPECT (payment_type IN ('credit_card', 'boleto', 'voucher', 'debit_card', 'not_defined'))
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_payment_value
        EXPECT (payment_value IS NOT NULL AND payment_value >= 0)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_installments
        EXPECT (payment_installments IS NOT NULL AND payment_installments >= 0)
        ON VIOLATION DROP ROW,
    -- Warning constraints
    CONSTRAINT no_rescued_data
        EXPECT (_rescued_data IS NULL)
)
COMMENT 'Cleansed payment data with validated payment types and amounts'
TBLPROPERTIES (
    'quality' = 'silver',
    'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
    -- Primary identifiers
    TRIM(order_id) AS order_id,
    CAST(payment_sequential AS INT) AS payment_sequential,

    -- Payment details (normalized)
    LOWER(TRIM(payment_type)) AS payment_type,
    CAST(payment_installments AS INT) AS payment_installments,
    CAST(payment_value AS DECIMAL(10,2)) AS payment_value,

    -- Payment category (derived)
    CASE
        WHEN LOWER(TRIM(payment_type)) = 'credit_card' THEN 'card'
        WHEN LOWER(TRIM(payment_type)) = 'debit_card' THEN 'card'
        WHEN LOWER(TRIM(payment_type)) = 'boleto' THEN 'bank_slip'
        WHEN LOWER(TRIM(payment_type)) = 'voucher' THEN 'voucher'
        ELSE 'other'
    END AS payment_category,

    -- Audit columns
    _source_file,
    _file_modified_at,
    _ingested_at,
    current_timestamp() AS _processed_at
FROM STREAM(bronze_order_payments);
