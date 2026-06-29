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

-- Date of birth: truncate to decade of birth, consistent with mask_age 10-year bracket.
-- Truncating to year would allow exact age derivation, defeating the age generalisation.
-- 1985-07-23 → 1980-01-01
CREATE OR REPLACE FUNCTION admin.shared.mask_dob(val DATE)
RETURNS DATE
RETURN MAKE_DATE(CAST(FLOOR(YEAR(val) / 10) * 10 AS INT), 1, 1);

-- Age: generalise to decade. VARIANT input accepts both STRING and INT columns.
-- INT columns get a numeric decade floor so the value casts back cleanly (35 → 30).
-- STRING columns get a readable bracket (35 → "30-39").
-- Returns '[REDACTED]' if value cannot be parsed as a number.
CREATE OR REPLACE FUNCTION admin.shared.mask_age(val VARIANT)
RETURNS VARIANT
RETURN CASE
  WHEN TRY_CAST(val::STRING AS BIGINT) IS NULL
    THEN '[REDACTED]'::VARIANT
  WHEN schema_of_variant(val) = 'STRING'
    THEN CONCAT(
           CAST(FLOOR(TRY_CAST(val::STRING AS BIGINT) / 10) * 10 AS STRING),
           '-',
           CAST(FLOOR(TRY_CAST(val::STRING AS BIGINT) / 10) * 10 + 9 AS STRING)
         )::VARIANT
  ELSE
    CAST(FLOOR(TRY_CAST(val::STRING AS BIGINT) / 10) * 10 AS BIGINT)::VARIANT
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

-- Phone: preserve country code only; [REDACTED] if no country code detected.
-- Handles E.164 (+44) and international dialling prefix (0044) formats.
-- Note: without a space after the country code the greedy match may include
-- 1-2 extra digits (e.g. +447... → +447); acceptable in a masking context.
-- +44 7911 123456 → +44 *** *** ****
-- 0044 7911 123456 → +44 *** *** ****
CREATE OR REPLACE FUNCTION admin.shared.mask_phone(val STRING)
RETURNS STRING
RETURN CASE
  WHEN TRIM(val) RLIKE '^\\+[0-9]'
    THEN CONCAT(REGEXP_EXTRACT(TRIM(val), '^(\\+[0-9]{1,3})', 1), ' *** *** ****')
  WHEN TRIM(val) RLIKE '^00[0-9]'
    THEN CONCAT('+', REGEXP_EXTRACT(TRIM(val), '^00([0-9]{1,3})', 1), ' *** *** ****')
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
