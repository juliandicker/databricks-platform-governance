# Governed tag ASSIGN grants — manual setup

Databricks governed tag permissions cannot be configured via the REST API or Terraform. After every fresh `terraform apply` (which creates a new Unity Catalog metastore), you must grant `ASSIGN` on each of the 18 governed tags to one principal.

## When to run this

After each `terraform apply` that creates or recreates the metastore. If you destroy and redeploy (the normal cycle for this repo), the metastore is new and all tag permissions are reset — run these steps again.

Data Classification must already be enabled on silver and gold before the tags exist. That happens automatically via the CI `Enable Data Classification` step. Wait for the CI apply job to complete before starting here.

## Principal to grant

| Principal | Type | Covers |
|---|---|---|
| `sg-dbplat-governed-tags` | Entra security group | Nests `sg-dbplat-data-product-sps` (all domain team SPs) and `sg-dbplat-data-stewards` |

That's one grant per tag — 18 total.

## Tags to grant (18)

All in the `class` namespace:

| | | |
|---|---|---|
| `class.name` | `class.email_address` | `class.phone_number` |
| `class.ip_address` | `class.location` | `class.date_of_birth` |
| `class.age` | `class.iban_code` | `class.credit_card` |
| `class.vin` | `class.driver_license` | `class.passport` |
| `class.uk_nino` | `class.uk_nhs` | `class.ethnicity` |
| `class.marital_status` | `class.sexual_orientation` | `class.criminal_background` |

US-specific (`class.us_*`) and DE-specific (`class.de_*`) tags are out of scope for this deployment.

## Steps

1. Open the Databricks workspace and go to **Catalog** (left nav).
2. In the Catalog Explorer, open the **`class`** catalog (the Data Classification governed tag catalog — appears after Data Classification is enabled on at least one catalog).
3. Select a tag (e.g. `name`).
4. Click the **Permissions** tab.
5. Click **Grant**.
6. Type `sg-dbplat-governed-tags`, select it, check **ASSIGN**, and confirm.
7. Repeat steps 3–6 for each of the remaining 17 tags.

> **Tip**: After granting the first tag, use the browser back button and click the next tag — you don't need to navigate the full tree each time.

## Why one principal covers everyone

`sg-dbplat-governed-tags` nests two groups managed by Terraform:

- `sg-dbplat-data-product-sps` — all domain team SPs, added automatically when a new team is created via `terraform apply`
- `sg-dbplat-data-stewards` — data quality owners who need to manually apply tags outside of automated Data Classification scans

When a new team is added and `terraform apply` runs, the new SP joins `sg-dbplat-data-product-sps`, which is already nested inside `sg-dbplat-governed-tags` — no re-grant on any tag is needed.

## Why this cannot be automated

The Unity Catalog REST API endpoint for governed tag permissions (`PATCH /api/2.1/unity-catalog/permissions/tag/{name}`) does not accept OIDC-derived tokens. The grants must be applied interactively by a user with metastore admin rights.
