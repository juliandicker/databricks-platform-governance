# Data product teams & cost governance

How teams onboard, get isolated compute/storage, and get cost-attributed.

## Data product teams

Teams follow a data mesh model — each domain team owns one service principal and one or more data products (schemas + landing volumes). Adding a data product or a new team requires only a `terraform.tfvars` change:

```hcl
data_product_teams = {
  travel = {
    display_name          = "sp-travel-data-products"
    sp_github_repo        = "juliandicker/tfl-disruption-data-pipeline"
    sp_github_environment = "dev"
    landing_sources       = ["tfl"]   # /Volumes/landing/raw/tfl/
    schemas               = ["tfl"]   # bronze.tfl, silver.tfl, gold.tfl
  }
  # music = { ... }  # add a second domain team here — gets its own SP and isolation
}
```

Each team also gets a SQL warehouse named `<team>-sql-warehouse` (serverless 2X-Small, 10-min auto-stop by default), and an optional per-team cost centre override and budget alert:

```hcl
  travel = {
    display_name = "sp-travel-data-products"
    cost_centre  = "CC-210"   # overrides top-level cost_centre on budget policy tags
    budget = {
      enabled             = true
      alert_threshold_usd = 200
      alert_email         = "alerts@example.com"
    }
  }
```

## Serverless usage policies and cost governance

Every data product team gets a Databricks serverless usage policy (`databricks_budget_policy`) that automatically stamps all their serverless compute activity — notebooks, jobs, pipelines, model serving — with `team` and `cost_centre` tags. These flow into `system.billing.usage.custom_tags`, enabling per-team cost attribution and chargeback without any manual tagging by the team.

A separate platform policy covers serverless compute run directly by members of `sg-dbplat-data-platform-admins`.

Since each team SP is assigned to exactly one policy, Databricks auto-applies it to every new serverless resource that SP creates — no user action needed.

Optional monthly spend alerts (`databricks_budget`) can be enabled per team or at workspace level via tfvars (see above). Alerts fire by email when list-price USD spend exceeds the configured threshold and are off by default.

> **CI prerequisite**: the `dbplat-simple-github-actions` SP must have the **Billing admin** role in the Databricks Account Console (User Management → Service principals → Roles) in addition to Account admin. Billing admin is required to create budget policies via API.

## Landing zone

Landing is a raw file drop zone — CSV, JSON, Parquet, etc. Files are purged automatically after 30 days by an Azure lifecycle policy. No Delta tables are created here.

Each team source gets its own Unity Catalog external volume at `/Volumes/landing/raw/<source>/` with access locked to the team SP.
