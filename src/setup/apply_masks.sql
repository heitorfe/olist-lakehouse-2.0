-- =============================================================================
-- Apply PII Masks to Silver Tables
-- =============================================================================
-- Description: Applies column masks and row filters to silver_customers table
--
-- IMPORTANT: Run this AFTER the Lakeflow pipeline creates the tables
--
-- Prerequisites:
--   1. Tables must exist (run pipeline first)
--   2. Masking functions must be created (run pii_masking.sql first)
--   3. Security groups must exist in Unity Catalog
--
-- Security Groups Required:
--   - pii-readers: Full access to unmasked PII
--   - admin: Full access to all data
--   - region-southeast, region-south, etc.: Regional access
-- =============================================================================

-- =============================================================================
-- Step 1: Apply Column Masks to silver_customers
-- =============================================================================

-- Mask customer name column
ALTER TABLE olist_dev.silver.silver_customers
ALTER COLUMN customer_name SET MASK olist_dev.silver.mask_customer_name;

-- Mask customer email column
ALTER TABLE olist_dev.silver.silver_customers
ALTER COLUMN customer_email SET MASK olist_dev.silver.mask_customer_email;

-- Mask customer phone column
ALTER TABLE olist_dev.silver.silver_customers
ALTER COLUMN customer_phone SET MASK olist_dev.silver.mask_customer_phone;

-- =============================================================================
-- Step 2: Apply Row Filter to silver_customers
-- =============================================================================
-- This restricts which rows users can see based on their regional group

ALTER TABLE olist_dev.silver.silver_customers
SET ROW FILTER olist_dev.silver.filter_customer_by_region ON (customer_state);

-- =============================================================================
-- Verification Queries
-- =============================================================================
-- Run these to verify masks are applied correctly:
--
-- 1. Check table properties:
--    DESCRIBE TABLE EXTENDED olist_dev.silver.silver_customers;
--
-- 2. Test as different users (admin sees all):
--    SELECT customer_name, customer_email, customer_phone, customer_state
--    FROM olist_dev.silver.silver_customers
--    LIMIT 10;
--
-- 3. Check mask function details:
--    DESCRIBE FUNCTION olist_dev.silver.mask_customer_name;
-- =============================================================================
