# PII Masking Guide

## Overview

This guide explains the implementation of PII (Personally Identifiable Information) masking and row filtering in the Olist Lakehouse project using **Unity Catalog** column masks and row filters.

### Key Features

- **Column Masks**: Dynamically mask sensitive fields based on user group membership
- **Row Filters**: Restrict data access by Brazilian region based on user groups
- **Unity Catalog Integration**: Leverages `IS_ACCOUNT_GROUP_MEMBER()` for fine-grained access control
- **Transparent to Applications**: Masking is applied automatically at query time

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          PII MASKING ARCHITECTURE                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────────────┐   │
│  │ Data Gen     │───▶│ Bronze Layer │───▶│       Silver Layer           │   │
│  │ (with PII)   │    │ (Raw PII)    │    │  ┌─────────────────────────┐ │   │
│  └──────────────┘    └──────────────┘    │  │  silver_customers       │ │   │
│                                          │  │  ├── customer_name ────┐│ │   │
│  PII Fields:                             │  │  │     MASK: mask_name ││ │   │
│  • customer_name                         │  │  ├── customer_email ───┐│ │   │
│  • customer_email                        │  │  │     MASK: mask_email││ │   │
│  • customer_phone                        │  │  ├── customer_phone ───┐│ │   │
│                                          │  │  │     MASK: mask_phone││ │   │
│                                          │  │  ├── customer_state ───┐│ │   │
│                                          │  │  │     ROW FILTER      ││ │   │
│                                          │  └──┴─────────────────────┴┘ │   │
│                                          └──────────────────────────────┘   │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                     SECURITY GROUPS                                  │   │
│  │  ┌──────────┐  ┌──────────┐  ┌─────────────────┐  ┌──────────────┐  │   │
│  │  │  admin   │  │pii-reader│  │region-southeast │  │ region-south │  │   │
│  │  │Full View │  │Full PII  │  │ SP,RJ,MG,ES     │  │  RS,PR,SC    │  │   │
│  │  └──────────┘  └──────────┘  └─────────────────┘  └──────────────┘  │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## PII Fields

The following fields are generated in the customer data and protected via masking:

| Field | Description | Example | Masked Example |
|-------|-------------|---------|----------------|
| `customer_name` | Full Brazilian name | `Joao Silva` | `J*** Silva` |
| `customer_email` | Email address | `joao.silva@gmail.com` | `jo****@gmail.com` |
| `customer_phone` | Brazilian mobile | `+55 (11) 91234-5678` | `+55 (11) 9****-****` |

---

## Security Groups

Users are granted access based on Unity Catalog group membership:

| Group | PII Access | Region Access | Use Case |
|-------|------------|---------------|----------|
| `admin` | Full | All Brazil | Data administrators |
| `pii-readers` | Full | All Brazil | Authorized PII users |
| `region-southeast` | Masked | SP, RJ, MG, ES | Southeast analysts |
| `region-south` | Masked | RS, PR, SC | South analysts |
| `region-northeast` | Masked | BA, PE, CE, etc. | Northeast analysts |
| `region-midwest` | Masked | DF, GO, MT, MS | Midwest analysts |
| `region-north` | Masked | PA, AM, etc. | North analysts |
| `analysts` | Masked | All Brazil | General analysts |

---

## Column Masking Functions

Located in: `src/setup/pii_masking.sql`

### mask_customer_name

```sql
CREATE OR REPLACE FUNCTION ${catalog}.silver.mask_customer_name(name STRING)
RETURNS STRING
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('pii-readers') THEN name
    WHEN IS_ACCOUNT_GROUP_MEMBER('admin') THEN name
    WHEN name IS NULL THEN NULL
    ELSE CONCAT(SUBSTRING(name, 1, 1), '*** ', SUBSTRING_INDEX(name, ' ', -1))
END;
```

### mask_customer_email

```sql
CREATE OR REPLACE FUNCTION ${catalog}.silver.mask_customer_email(email STRING)
RETURNS STRING
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('pii-readers') THEN email
    WHEN IS_ACCOUNT_GROUP_MEMBER('admin') THEN email
    WHEN email IS NULL THEN NULL
    ELSE CONCAT(SUBSTRING(email, 1, 2), '****@', SUBSTRING_INDEX(email, '@', -1))
END;
```

### mask_customer_phone

```sql
CREATE OR REPLACE FUNCTION ${catalog}.silver.mask_customer_phone(phone STRING)
RETURNS STRING
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('pii-readers') THEN phone
    WHEN IS_ACCOUNT_GROUP_MEMBER('admin') THEN phone
    WHEN phone IS NULL THEN NULL
    ELSE CONCAT(SUBSTRING(phone, 1, 10), '****-****')
END;
```

---

## Row Filter Function

The row filter restricts which customers users can see based on Brazilian region:

```sql
CREATE OR REPLACE FUNCTION ${catalog}.silver.filter_customer_by_region(customer_state STRING)
RETURNS BOOLEAN
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('admin') THEN TRUE
    WHEN IS_ACCOUNT_GROUP_MEMBER('pii-readers') THEN TRUE
    WHEN IS_ACCOUNT_GROUP_MEMBER('region-southeast') THEN customer_state IN ('SP', 'RJ', 'MG', 'ES')
    WHEN IS_ACCOUNT_GROUP_MEMBER('region-south') THEN customer_state IN ('RS', 'PR', 'SC')
    -- Additional regions...
    ELSE TRUE  -- Default: allow access with masked PII
END;
```

---

## Applying Masks to Tables

Located in: `src/setup/apply_masks.sql`

Run this **after** the Lakeflow pipeline creates the tables:

```sql
-- Apply column masks
ALTER TABLE ${catalog}.silver.silver_customers
ALTER COLUMN customer_name SET MASK ${catalog}.silver.mask_customer_name;

ALTER TABLE ${catalog}.silver.silver_customers
ALTER COLUMN customer_email SET MASK ${catalog}.silver.mask_customer_email;

ALTER TABLE ${catalog}.silver.silver_customers
ALTER COLUMN customer_phone SET MASK ${catalog}.silver.mask_customer_phone;

-- Apply row filter
ALTER TABLE ${catalog}.silver.silver_customers
SET ROW FILTER ${catalog}.silver.filter_customer_by_region ON (customer_state);
```

---

## Deployment Steps

### 1. Deploy the Lakeflow Pipeline

```bash
databricks bundle deploy -t dev
databricks bundle run olist_main_pipeline -t dev
```

### 2. Create Masking Functions

Run `pii_masking.sql` in Databricks SQL:

```bash
# In Databricks UI or via CLI
databricks workspace import src/setup/pii_masking.sql
```

### 3. Apply Masks to Tables

Run `apply_masks.sql` after tables are created:

```sql
-- Execute in Databricks SQL Editor
-- Replace ${catalog} with your catalog name (e.g., olist_dev)
```

### 4. Create Security Groups

```sql
-- In Unity Catalog
CREATE GROUP IF NOT EXISTS pii_readers;
CREATE GROUP IF NOT EXISTS region_southeast;
CREATE GROUP IF NOT EXISTS region_south;

-- Add users to groups
ALTER GROUP pii_readers ADD USER `user@company.com`;
```

---

## Testing Masking

### Test as Regular User (Masked)

```sql
-- User NOT in pii-readers group sees masked data
SELECT customer_name, customer_email, customer_phone
FROM ${catalog}.silver.silver_customers
LIMIT 5;

-- Results:
-- | customer_name | customer_email      | customer_phone        |
-- |---------------|--------------------|-----------------------|
-- | J*** Silva    | jo****@gmail.com   | +55 (11) 9****-****   |
```

### Test as Admin User (Unmasked)

```sql
-- User in admin or pii-readers group sees full data
SELECT customer_name, customer_email, customer_phone
FROM ${catalog}.silver.silver_customers
LIMIT 5;

-- Results:
-- | customer_name  | customer_email           | customer_phone        |
-- |----------------|-------------------------|-----------------------|
-- | Joao Silva     | joao.silva@gmail.com    | +55 (11) 91234-5678   |
```

### Test Row Filter

```sql
-- User in region-southeast only sees SP, RJ, MG, ES
SELECT customer_state, COUNT(*) as count
FROM ${catalog}.silver.silver_customers
GROUP BY customer_state;

-- Results for region-southeast user:
-- | customer_state | count |
-- |----------------|-------|
-- | SP             | 250   |
-- | RJ             | 150   |
-- | MG             | 100   |
-- | ES             | 50    |
```

---

## Troubleshooting

### Check Applied Masks

```sql
DESCRIBE TABLE EXTENDED ${catalog}.silver.silver_customers;
```

### Check Function Details

```sql
DESCRIBE FUNCTION ${catalog}.silver.mask_customer_name;
```

### Verify Group Membership

```sql
SELECT IS_ACCOUNT_GROUP_MEMBER('pii-readers') AS is_pii_reader;
```

### Remove Mask (if needed)

```sql
ALTER TABLE ${catalog}.silver.silver_customers
ALTER COLUMN customer_name DROP MASK;
```

---

## References

- [Databricks - Row Filters and Column Masks](https://docs.databricks.com/en/data-governance/unity-catalog/row-filters.html)
- [Databricks - Column Mask Syntax](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-column-mask.html)
- [IS_ACCOUNT_GROUP_MEMBER Function](https://docs.databricks.com/en/sql/language-manual/functions/is_account_group_member.html)
