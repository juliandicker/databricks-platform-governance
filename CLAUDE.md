# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo does

This is the **governance** half of a two-repo split. It deploys Databricks Unity Catalog governance — ABAC column-masking policies and their UDFs, GDPR audit tables (erasure, access requests, lineage cache), the SAR (Subject Access Request) Streamlit app, and the Databricks Asset Bundle (DABs) jobs/dashboards that maintain all of it. **This repo has no Terraform of its own** — everything here is expressible as DABs + SQL, deployed by `databricks bundle deploy`/`bundle run`.

The **infra** half — the Azure resource group, storage, networking, the Databricks workspace, the Unity Catalog metastore, catalogs/schemas/grants, groups, and data-mesh team service principals — lives in [`juliandicker/simple-databricks-deployment`](https://github.com/juliandicker/simple-databricks-deployment), which enables this repo (see "How this repo is enabled" below). This split exists so a future, differently-architected infra project (e.g. VNet-injected) can reuse this repo unchanged.

## How this repo is enabled

This repo has no Terraform, no `terraform init`, no state. It's enabled by the infra repo exactly the way `tfl-disruption-data-pipeline` is enabled there:

1. The infra repo's Terraform creates the `sp-data-platform` service principal and gives it a GitHub OIDC federated credential scoped to this repo (`data_product_teams.data_platform_admins.sp_github_repo` in the infra repo's `terraform.tfvars`).
2. After every `terraform apply`, the infra repo's CI pushes two secrets here: `AZURE_CLIENT_ID` (= `sp-data-platform`'s application ID) and `DATABRICKS_HOST` (= the workspace URL) — via the same `dbplat-deployment-bot` GitHub App used for the pipeline repo.
3. This repo's own `.github/workflows/deploy.yml` authenticates directly as `sp-data-platform` via Azure OIDC using those two secrets, exchanges the Azure token for a Databricks token, and runs `databricks bundle deploy --force` + `databricks bundle run governance_setup`.

Nothing in the infra repo ever triggers a deploy here (its GitHub App is scoped to `secrets:write` only, not `actions:write`) — this repo's own push triggers (on `governance/**`, `resources/**`, `apps/**`, `databricks.yml`) or a manual `workflow_dispatch` handle that. This works because this repo's config never needs a fresh value from infra beyond the two secrets above: `warehouse_id` and `platform_sp_id` (`databricks.yml`'s variables) resolve fresh via DABs `lookup:` variables against the live workspace on every deploy, by name — not via anything infra has to hand off.

**One thing that does need a manual infra-side update**: if `sp-data-platform`'s federated credential, `sg-dbplat-*` group names, or catalog/schema names ever change in the infra repo, the hardcoded references to them in `governance/create_policies.sql` (group names in `EXCEPT` clauses) and throughout `apps/sar_app/` (catalog names `bronze`/`silver`/`gold`, `admin.shared`/`admin.erasure`/`admin.access`/`admin.lineage_cache` schema names, `class.*` tag namespace) need updating here too — this is a naming *contract* between the two repos, not something either automatically discovers from the other.

## Layout

| Path | Responsibility |
|---|---|
| `databricks.yml` | DABs bundle config. Two `lookup:` variables resolve infra-created objects by name at deploy time: `warehouse_id` (`data_platform_admins-sql-warehouse`) and `platform_sp_id` (`sp-data-platform`, used to pin `run_as` on the governance jobs) |
| `governance/*.sql` | Masking UDFs (`create_udfs.sql`), ABAC column-mask policies (`create_policies.sql`), audit table DDL (`create_erasure_tables.sql`, `create_access_tables.sql`, `create_lineage_cache_tables.sql`), the incremental lineage-cache MERGE (`refresh_lineage_cache.sql`), retention/freshness views |
| `governance/*.py` | Notebook-style tasks. `apply_auto_ttl`/`compute_freshness_metrics` run by `governance_daily`; `grant_sar_app_access` runs by `governance_setup` — grants the SAR app's own SP access to bronze/silver/gold and admin.* by resolving its id via the Apps API (`w.apps.get(name="platform-sar-app")`), not from the infra repo |
| `resources/jobs/governance.yml` | `governance_setup` (runs on every deploy: UDFs → policies → erasure/access/lineage-cache tables → grant_sar_app_access) and `governance_daily` (scheduled, paused by default) — both pin `run_as: sp-data-platform` |
| `resources/jobs/lineage_cache_refresh.yml` | Standalone job refreshing `admin.lineage_cache` — triggered by `governance_daily`'s schedule *and* on-demand by the SAR app's "Refresh lineage cache now" button (via `CAN_MANAGE_RUN`), so the MERGE logic lives in exactly one SQL file |
| `resources/apps/sar.yml`, `apps/sar_app/` | The SAR Streamlit app — see `docs/sar-app.md` |
| `resources/dashboards/*.yml`, `dashboards/*.lvdash.json` | Platform Data Governance and Access Audit dashboards |
| `docs/sar-app.md` | Full SAR app documentation, including local dev via `databricks apps run-local` |
| `docs/governed-tag-grants.md` | Manual procedure for governed-tag `ASSIGN` grants (not API-manageable — see below) |
| `docs/access-and-pii-governance.md` | Catalog grants, ABAC column masking, governed tags, Entra groups/AIM, Access Audit dashboard |
| `docs/data-lifecycle-governance.md` | Platform metadata columns, freshness SLAs, Auto TTL/retention, governance jobs, Data Governance dashboard |

`docs/data-product-teams.md` lives in the infra repo, not here — data mesh teams are entirely Terraform's concern, not something this repo creates or deploys. The two docs above stay here despite touching infra-repo resources (catalog grants, Entra groups/AIM) because they're intertwined with governance content that does belong here. Some cross-references inside them point at specific `terraform/*.tf` resources in the infra repo, called out explicitly where they occur.

## Key constraints inherited from the infra repo's naming contract

- **Catalogs**: `bronze`, `silver`, `gold` (data), `admin` (governance: `shared`, `erasure`, `access`, `lineage_cache` schemas) — hardcoded throughout `apps/sar_app/*.py` and `governance/*.sql`, not configurable.
- **Governed tag namespace**: `class.*` (e.g. `class.email_address`) — hardcoded in `apps/sar_app/app.py`'s `TAG_MAP` and `database.py`'s tag queries, and in every `has_tag('class....')` in `create_policies.sql`.
- **Exempt principals**: ABAC policies' `EXCEPT` clauses reference `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, `sg-dbplat-data-product-sps` by name — all three are Databricks account groups the infra repo manages. `sg-dbplat-data-product-sps` specifically nests every team SP, `sp-data-platform`, and the SAR app's own SP (the last two via dynamic Terraform blocks keyed on `var.sar_app_sp_id`) — this is how the SAR app SP gets to see unmasked data to execute an erasure it already found via the calling user's own search.
- **Only one policy may match a column per user** — Databricks errors if two policies match the same column for the same user. Each `class.*` tag appears in exactly one policy's `MATCH COLUMNS` condition in `create_policies.sql`.
- **Governed tag `ASSIGN` grants are not API-manageable** (Databricks provider/REST limitation) — must be applied manually via Catalog → Govern → Governed Tags → Account Permissions after every fresh deploy. See `docs/governed-tag-grants.md`.
- **Policy renames need a manual `DROP POLICY`** — `CREATE OR REPLACE POLICY` only replaces a policy under its exact current name; an old name from a prior rename keeps silently existing with its stale exempt list. `DROP POLICY` doesn't support `IF EXISTS`, so this can't be folded into the idempotent job.

## Local development

The SAR app runs locally via `databricks apps run-local` against the real deployed workspace (not a mock) — see `docs/sar-app.md`'s "Local development" section for the full walkthrough, including the convenience script pattern for resolving `DATABRICKS_WAREHOUSE_ID` and `LINEAGE_CACHE_REFRESH_JOB_ID` (both `valueFrom` bindings in `apps/sar_app/app.yaml` that only resolve inside a deployed app, not under `run-local`).
