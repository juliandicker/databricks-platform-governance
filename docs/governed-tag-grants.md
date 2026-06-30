# Governed tag ASSIGN grants — manual setup

Databricks governed tag permissions cannot be configured via the REST API or Terraform. After every fresh `terraform apply` (which creates a new Unity Catalog metastore), you must grant `ASSIGN` at the account level — one grant that covers all governed tags.

## When to run this

After each `terraform apply` that creates or recreates the metastore. If you destroy and redeploy (the normal cycle for this repo), the metastore is new and all tag permissions are reset — run this step again.

Data Classification must already be enabled on silver and gold before the governed tags exist. That happens automatically via the CI `Enable Data Classification` step. Wait for the CI apply job to complete before starting here.

## Steps

1. In the Databricks workspace, click **Catalog** (left nav).
2. Click the **Govern** button (shield icon) and select **Governed Tags**.
3. Click the **Account Permissions** tab.
4. Click **Grant permissions**.
5. Type `sg-dbplat-governed-tags`, select it, check **Assign**, and confirm.

That's it — one grant at account level applies `ASSIGN` across all governed tags.

## Principal

| Principal | Type | Covers |
|---|---|---|
| `sg-dbplat-governed-tags` | Entra security group | Nests `sg-dbplat-data-product-sps` (all domain team SPs) and `sg-dbplat-data-stewards` |

When a new team is added and `terraform apply` runs, the new SP joins `sg-dbplat-data-product-sps`, which is already nested inside `sg-dbplat-governed-tags` — no re-grant is needed.

## Why this cannot be automated

Databricks has not implemented governed tag permission management in either the REST API or the SDK. There is no programmatic way to grant or revoke `ASSIGN` — it must be applied manually by a user with metastore admin rights.
