# Governed tag ASSIGN grants — manual setup

Databricks governed tag permissions cannot be configured via the REST API or Terraform. After every fresh `terraform apply` (which creates a new Unity Catalog metastore), you must grant `ASSIGN` on each of the 25 GDPR + PCI DSS governed tags to two principals.

## When to run this

After each `terraform apply` that creates or recreates the metastore. If you destroy and redeploy (the normal cycle for this repo), the metastore is new and all tag permissions are reset — run these steps again.

Data Classification must already be enabled on silver and gold before the tags exist. That happens automatically via the CI `Enable Data Classification` step, which runs before governance bundle deploy. Wait for the CI apply job to complete before starting here.

## Principals to grant

| Principal | Type |
|---|---|
| `sg-dbplat-data-product-sps` | Entra security group — contains all domain team SPs |
| `sg-dbplat-data-stewards` | Entra security group — data quality owners, exempt from ABAC masks |

## Tags to grant (25 total)

All in the `class` namespace:

| Tag | Tag |
|---|---|
| `class.name` | `class.email_address` |
| `class.phone_number` | `class.ip_address` |
| `class.location` | `class.date_of_birth` |
| `class.age` | `class.iban_code` |
| `class.credit_card` | `class.us_bank_number` |
| `class.vin` | `class.driver_license` |
| `class.us_driver_license` | `class.passport` |
| `class.us_passport` | `class.us_ssn` |
| `class.uk_nino` | `class.uk_nhs` |
| `class.de_id_card` | `class.de_svnr` |
| `class.de_tax_id` | `class.ethnicity` |
| `class.marital_status` | `class.sexual_orientation` |
| `class.criminal_background` | |

## Steps

1. Open the Databricks workspace and go to **Catalog** (left nav).
2. In the Catalog Explorer, open the **`class`** catalog (this is the Data Classification governed tag catalog — it appears after Data Classification is enabled).
3. Select a tag (e.g. `name`).
4. Click the **Permissions** tab.
5. Click **Grant**.
6. In the principal field, type `sg-dbplat-data-product-sps`, select it, check **ASSIGN**, and confirm.
7. Repeat for `sg-dbplat-data-stewards`.
8. Repeat steps 3–7 for each of the remaining 24 tags.

> **Tip**: Open the Permissions tab for the first tag, grant both principals, then use the browser back button and click the next tag — you don't need to navigate the full tree each time.

## Why only two principals

Each domain team SP is a member of `sg-dbplat-data-product-sps` (managed by Terraform in `data-product-teams.tf`). Granting `ASSIGN` to the group covers all current and future team SPs. When a new team is added and `terraform apply` runs, the new SP is automatically added to this group — no additional tag grants are needed.

`sg-dbplat-data-stewards` is granted `ASSIGN` separately so data stewards can manually apply governed tags outside of automated Data Classification scans.

## Why this cannot be automated

The Unity Catalog REST API endpoint for governed tag permissions (`PATCH /api/2.1/unity-catalog/permissions/tag/{name}`) does not accept OIDC-derived tokens. The grants must be applied interactively by a user with metastore admin rights.
