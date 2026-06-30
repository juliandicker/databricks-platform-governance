-- ABAC column mask policies — applied to silver and gold catalogs.
-- Each policy fires when a column carries the matching governed tag(s).
-- Exceptions: pii_readers and data_stewards see unmasked data; team SPs are exempt
-- via {{job.parameters.exempt_sps}} (substituted at runtime by the governance job).
--
-- Multiple policies must not match the same column for the same user — Databricks
-- returns an error rather than picking one. Tags are partitioned across policies
-- so each class.* tag appears in exactly one MATCH COLUMNS condition.

-- ── silver ────────────────────────────────────────────────────────────────────

CREATE OR REPLACE POLICY mask_email_columns
ON CATALOG silver
COLUMN MASK admin.shared.mask_email
TO `account users` EXCEPT `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, {{job.parameters.exempt_sps}}
FOR TABLES MATCH COLUMNS has_tag('class.email_address') AS c ON COLUMN c;

CREATE OR REPLACE POLICY mask_dob_columns
ON CATALOG silver
COLUMN MASK admin.shared.mask_dob
TO `account users` EXCEPT `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, {{job.parameters.exempt_sps}}
FOR TABLES MATCH COLUMNS has_tag('class.date_of_birth') AS c ON COLUMN c;

CREATE OR REPLACE POLICY mask_age_columns
ON CATALOG silver
COLUMN MASK admin.shared.mask_age
TO `account users` EXCEPT `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, {{job.parameters.exempt_sps}}
FOR TABLES MATCH COLUMNS has_tag('class.age') AS c ON COLUMN c;

CREATE OR REPLACE POLICY mask_ip_columns
ON CATALOG silver
COLUMN MASK admin.shared.mask_ip
TO `account users` EXCEPT `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, {{job.parameters.exempt_sps}}
FOR TABLES MATCH COLUMNS has_tag('class.ip_address') AS c ON COLUMN c;

CREATE OR REPLACE POLICY mask_credit_card_columns
ON CATALOG silver
COLUMN MASK admin.shared.mask_credit_card
TO `account users` EXCEPT `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, {{job.parameters.exempt_sps}}
FOR TABLES MATCH COLUMNS has_tag('class.credit_card') AS c ON COLUMN c;

CREATE OR REPLACE POLICY mask_phone_columns
ON CATALOG silver
COLUMN MASK admin.shared.mask_phone
TO `account users` EXCEPT `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, {{job.parameters.exempt_sps}}
FOR TABLES MATCH COLUMNS has_tag('class.phone_number') AS c ON COLUMN c;

CREATE OR REPLACE POLICY mask_location_columns
ON CATALOG silver
COLUMN MASK admin.shared.mask_location
TO `account users` EXCEPT `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, {{job.parameters.exempt_sps}}
FOR TABLES MATCH COLUMNS has_tag('class.location') AS c ON COLUMN c;

-- Catch-all for any class.* tag not covered above.
-- has_tag('class') without an attribute should match any column carrying any class.* tag.
-- If this syntax is unsupported by the current runtime, this policy silently matches nothing —
-- newly detected tags will be unprotected until an explicit policy is added for them.
CREATE OR REPLACE POLICY mask_unknown_sensitive_columns
ON CATALOG silver
COLUMN MASK admin.shared.mask_sensitive
TO `account users` EXCEPT `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, {{job.parameters.exempt_sps}}
FOR TABLES MATCH COLUMNS (
  has_tag('class') AND NOT (
    has_tag('class.name')               OR has_tag('class.vin')                    OR
    has_tag('class.driver_license')     OR has_tag('class.us_driver_license')      OR
    has_tag('class.passport')           OR has_tag('class.us_passport')            OR
    has_tag('class.us_ssn')             OR
    has_tag('class.uk_nino')            OR has_tag('class.uk_nhs')                 OR
    has_tag('class.de_id_card')         OR has_tag('class.de_svnr')                OR has_tag('class.de_tax_id') OR
    has_tag('class.iban_code')          OR has_tag('class.us_bank_number')         OR
    has_tag('class.ethnicity')          OR has_tag('class.marital_status')         OR
    has_tag('class.sexual_orientation') OR has_tag('class.criminal_background')    OR
    has_tag('class.email_address')      OR has_tag('class.date_of_birth')          OR
    has_tag('class.age')                OR has_tag('class.ip_address')             OR
    has_tag('class.credit_card')        OR has_tag('class.phone_number')           OR
    has_tag('class.location')
  )
) AS c ON COLUMN c;

-- ── gold ──────────────────────────────────────────────────────────────────────

CREATE OR REPLACE POLICY mask_email_columns
ON CATALOG gold
COLUMN MASK admin.shared.mask_email
TO `account users` EXCEPT `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, {{job.parameters.exempt_sps}}
FOR TABLES MATCH COLUMNS has_tag('class.email_address') AS c ON COLUMN c;

CREATE OR REPLACE POLICY mask_dob_columns
ON CATALOG gold
COLUMN MASK admin.shared.mask_dob
TO `account users` EXCEPT `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, {{job.parameters.exempt_sps}}
FOR TABLES MATCH COLUMNS has_tag('class.date_of_birth') AS c ON COLUMN c;

CREATE OR REPLACE POLICY mask_age_columns
ON CATALOG gold
COLUMN MASK admin.shared.mask_age
TO `account users` EXCEPT `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, {{job.parameters.exempt_sps}}
FOR TABLES MATCH COLUMNS has_tag('class.age') AS c ON COLUMN c;

CREATE OR REPLACE POLICY mask_ip_columns
ON CATALOG gold
COLUMN MASK admin.shared.mask_ip
TO `account users` EXCEPT `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, {{job.parameters.exempt_sps}}
FOR TABLES MATCH COLUMNS has_tag('class.ip_address') AS c ON COLUMN c;

CREATE OR REPLACE POLICY mask_credit_card_columns
ON CATALOG gold
COLUMN MASK admin.shared.mask_credit_card
TO `account users` EXCEPT `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, {{job.parameters.exempt_sps}}
FOR TABLES MATCH COLUMNS has_tag('class.credit_card') AS c ON COLUMN c;

CREATE OR REPLACE POLICY mask_phone_columns
ON CATALOG gold
COLUMN MASK admin.shared.mask_phone
TO `account users` EXCEPT `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, {{job.parameters.exempt_sps}}
FOR TABLES MATCH COLUMNS has_tag('class.phone_number') AS c ON COLUMN c;

CREATE OR REPLACE POLICY mask_location_columns
ON CATALOG gold
COLUMN MASK admin.shared.mask_location
TO `account users` EXCEPT `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, {{job.parameters.exempt_sps}}
FOR TABLES MATCH COLUMNS has_tag('class.location') AS c ON COLUMN c;

CREATE OR REPLACE POLICY mask_unknown_sensitive_columns
ON CATALOG gold
COLUMN MASK admin.shared.mask_sensitive
TO `account users` EXCEPT `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, {{job.parameters.exempt_sps}}
FOR TABLES MATCH COLUMNS (
  has_tag('class') AND NOT (
    has_tag('class.name')               OR has_tag('class.vin')                    OR
    has_tag('class.driver_license')     OR has_tag('class.us_driver_license')      OR
    has_tag('class.passport')           OR has_tag('class.us_passport')            OR
    has_tag('class.us_ssn')             OR
    has_tag('class.uk_nino')            OR has_tag('class.uk_nhs')                 OR
    has_tag('class.de_id_card')         OR has_tag('class.de_svnr')                OR has_tag('class.de_tax_id') OR
    has_tag('class.iban_code')          OR has_tag('class.us_bank_number')         OR
    has_tag('class.ethnicity')          OR has_tag('class.marital_status')         OR
    has_tag('class.sexual_orientation') OR has_tag('class.criminal_background')    OR
    has_tag('class.email_address')      OR has_tag('class.date_of_birth')          OR
    has_tag('class.age')                OR has_tag('class.ip_address')             OR
    has_tag('class.credit_card')        OR has_tag('class.phone_number')           OR
    has_tag('class.location')
  )
) AS c ON COLUMN c;
