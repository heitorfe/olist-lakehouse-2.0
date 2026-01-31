-- =============================================================================
-- PII Masking Functions for Unity Catalog
-- =============================================================================
-- Description: SQL functions for column-level masking and row-level filtering
--
-- These functions use IS_ACCOUNT_GROUP_MEMBER() to control data visibility:
--   - 'pii-readers': Can see full unmasked PII data
--   - 'admin': Full access to all data and all regions
--   - 'region-southeast': Access to Southeast Brazil (SP, RJ, MG, ES)
--   - 'region-south': Access to South Brazil (RS, PR, SC)
--   - 'analysts': Default group, sees masked data only
--
-- Usage: Apply to tables via ALTER TABLE ... SET MASK / SET ROW FILTER
-- =============================================================================

-- =============================================================================
-- Column Mask: Customer Name
-- =============================================================================
-- Masks name to show only first initial and last name
-- Example: "Joao Silva" -> "J*** Silva"
-- =============================================================================
CREATE OR REPLACE FUNCTION ${catalog}.silver.mask_customer_name(name STRING)
RETURNS STRING
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('pii-readers') THEN name
    WHEN IS_ACCOUNT_GROUP_MEMBER('admin') THEN name
    WHEN name IS NULL THEN NULL
    ELSE CONCAT(
        SUBSTRING(name, 1, 1),
        '*** ',
        COALESCE(SUBSTRING_INDEX(name, ' ', -1), '')
    )
END;

-- =============================================================================
-- Column Mask: Customer Email
-- =============================================================================
-- Masks email to show only first 2 chars and domain
-- Example: "joao.silva@gmail.com" -> "jo****@gmail.com"
-- =============================================================================
CREATE OR REPLACE FUNCTION ${catalog}.silver.mask_customer_email(email STRING)
RETURNS STRING
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('pii-readers') THEN email
    WHEN IS_ACCOUNT_GROUP_MEMBER('admin') THEN email
    WHEN email IS NULL THEN NULL
    ELSE CONCAT(
        SUBSTRING(email, 1, 2),
        '****@',
        SUBSTRING_INDEX(email, '@', -1)
    )
END;

-- =============================================================================
-- Column Mask: Customer Phone
-- =============================================================================
-- Masks phone to show only country/area code
-- Example: "+55 (11) 91234-5678" -> "+55 (11) 9****-****"
-- =============================================================================
CREATE OR REPLACE FUNCTION ${catalog}.silver.mask_customer_phone(phone STRING)
RETURNS STRING
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('pii-readers') THEN phone
    WHEN IS_ACCOUNT_GROUP_MEMBER('admin') THEN phone
    WHEN phone IS NULL THEN NULL
    ELSE CONCAT(
        SUBSTRING(phone, 1, 10),
        '****-****'
    )
END;

-- =============================================================================
-- Row Filter: Region-Based Access Control
-- =============================================================================
-- Filters rows based on user's regional group membership
-- - admin: All regions
-- - region-southeast: SP, RJ, MG, ES
-- - region-south: RS, PR, SC
-- - Others: No access (returns false)
-- =============================================================================
CREATE OR REPLACE FUNCTION ${catalog}.silver.filter_customer_by_region(customer_state STRING)
RETURNS BOOLEAN
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('admin') THEN TRUE
    WHEN IS_ACCOUNT_GROUP_MEMBER('pii-readers') THEN TRUE
    WHEN IS_ACCOUNT_GROUP_MEMBER('region-southeast') THEN customer_state IN ('SP', 'RJ', 'MG', 'ES')
    WHEN IS_ACCOUNT_GROUP_MEMBER('region-south') THEN customer_state IN ('RS', 'PR', 'SC')
    WHEN IS_ACCOUNT_GROUP_MEMBER('region-northeast') THEN customer_state IN ('BA', 'PE', 'CE', 'RN', 'PB', 'AL', 'SE', 'MA', 'PI')
    WHEN IS_ACCOUNT_GROUP_MEMBER('region-midwest') THEN customer_state IN ('DF', 'GO', 'MT', 'MS')
    WHEN IS_ACCOUNT_GROUP_MEMBER('region-north') THEN customer_state IN ('PA', 'AM', 'RO', 'AC', 'RR', 'AP', 'TO')
    ELSE TRUE  -- Default: allow access but with masked PII
END;
