-- Generic string redaction — names, document IDs, special category data
CREATE OR REPLACE FUNCTION admin.shared.mask_name(val STRING)
RETURNS STRING
RETURN '[REDACTED]';

-- Email: mask local part and domain label, preserve TLD for structural context
-- julian@redkic.co.uk → ******@******.co.uk
CREATE OR REPLACE FUNCTION admin.shared.mask_email(val STRING)
RETURNS STRING
RETURN CONCAT(
  REGEXP_REPLACE(SPLIT(val, '@')[0], '.', '*'),
  '@',
  REGEXP_REPLACE(SPLIT(SPLIT(val, '@')[1], '\\.')[0], '.', '*'),
  '.',
  REGEXP_EXTRACT(SPLIT(val, '@')[1], '^[^.]+\\.(.+)$', 1)
);

-- Date of birth: truncate to year, preserving DATE type
-- 1985-07-23 → 1985-01-01
CREATE OR REPLACE FUNCTION admin.shared.mask_dob(val DATE)
RETURNS DATE
RETURN MAKE_DATE(YEAR(val), 1, 1);

-- Age: 10-year bracket. VARIANT input accepts both STRING and INT columns.
-- Returns '[REDACTED]' if value cannot be parsed as a number.
-- 35 → "30-39"
CREATE OR REPLACE FUNCTION admin.shared.mask_age(val VARIANT)
RETURNS VARIANT
RETURN CASE
  WHEN TRY_CAST(val::STRING AS BIGINT) IS NOT NULL
    THEN CONCAT(
           CAST(FLOOR(TRY_CAST(val::STRING AS BIGINT) / 10) * 10 AS STRING),
           '-',
           CAST(FLOOR(TRY_CAST(val::STRING AS BIGINT) / 10) * 10 + 9 AS STRING)
         )::VARIANT
  ELSE '[REDACTED]'::VARIANT
END;

-- IP address: first two octets for IPv4; [REDACTED] for IPv6 or unrecognised format
-- 192.168.1.100 → 192.168.*.*
CREATE OR REPLACE FUNCTION admin.shared.mask_ip(val STRING)
RETURNS STRING
RETURN CASE
  WHEN TRIM(val) RLIKE '^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$'
    THEN CONCAT(SPLIT(TRIM(val), '\\.')[0], '.', SPLIT(TRIM(val), '\\.')[1], '.*.*')
  ELSE '[REDACTED]'
END;

-- Credit card: last 4 digits only, strips all non-numeric characters first (PCI convention)
-- 4111-1111-1111-1234 → **** **** **** 1234
CREATE OR REPLACE FUNCTION admin.shared.mask_credit_card(val STRING)
RETURNS STRING
RETURN CONCAT('**** **** **** ', RIGHT(REGEXP_REPLACE(val, '[^0-9]', ''), 4));

-- Phone: preserve country code only; [REDACTED] if no E.164 country code detected
-- +44 7911 123456 → +44 *** *** ****
CREATE OR REPLACE FUNCTION admin.shared.mask_phone(val STRING)
RETURNS STRING
RETURN CASE
  WHEN TRIM(val) RLIKE '^\\+[0-9]'
    THEN CONCAT(REGEXP_EXTRACT(TRIM(val), '^(\\+[0-9]{1,3})', 1), ' *** *** ****')
  ELSE '[REDACTED]'
END;

-- Location: UK postcode outward code if detectable; [REDACTED] for everything else.
-- City names cannot be distinguished from street names without geocoding.
-- SW1A 1AA → SW1A   |   123 High Street, London, SW1A 1AA → SW1A   |   London → [REDACTED]
CREATE OR REPLACE FUNCTION admin.shared.mask_location(val STRING)
RETURNS STRING
RETURN CASE
  WHEN UPPER(TRIM(val)) RLIKE '[A-Z]{1,2}[0-9][0-9A-Z]? [0-9][A-Z]{2}'
    THEN REGEXP_EXTRACT(UPPER(TRIM(val)), '([A-Z]{1,2}[0-9][0-9A-Z]?) [0-9][A-Z]{2}', 1)
  ELSE '[REDACTED]'
END;

-- Catch-all for any class.* tag not covered by an explicit policy.
-- VARIANT input handles unknown column types; Databricks auto-casts output back to column type.
-- NOTE: if the target column is numeric this cast will fail — add an explicit policy for that tag.
CREATE OR REPLACE FUNCTION admin.shared.mask_sensitive(val VARIANT)
RETURNS VARIANT
RETURN '[REDACTED]'::VARIANT;
