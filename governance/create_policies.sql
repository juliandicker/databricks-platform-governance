CREATE OR REPLACE POLICY mask_name_columns
ON CATALOG silver
COLUMN MASK admin.shared.mask_name
TO `account users` EXCEPT `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, {{job.parameters.exempt_sps}}
FOR TABLES MATCH COLUMNS has_tag('class.name') AS c ON COLUMN c;

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

CREATE OR REPLACE POLICY mask_location_columns
ON CATALOG silver
COLUMN MASK admin.shared.mask_location
TO `account users` EXCEPT `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, {{job.parameters.exempt_sps}}
FOR TABLES MATCH COLUMNS has_tag('class.location') AS c ON COLUMN c;

CREATE OR REPLACE POLICY mask_name_columns
ON CATALOG gold
COLUMN MASK admin.shared.mask_name
TO `account users` EXCEPT `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, {{job.parameters.exempt_sps}}
FOR TABLES MATCH COLUMNS has_tag('class.name') AS c ON COLUMN c;

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

CREATE OR REPLACE POLICY mask_location_columns
ON CATALOG gold
COLUMN MASK admin.shared.mask_location
TO `account users` EXCEPT `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, {{job.parameters.exempt_sps}}
FOR TABLES MATCH COLUMNS has_tag('class.location') AS c ON COLUMN c;
