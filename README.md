# databricks-platform-governance

Unity Catalog governance for the [`simple-databricks-deployment`](https://github.com/juliandicker/simple-databricks-deployment) lakehouse: ABAC column-masking policies and their UDFs, GDPR audit tables (erasure, access requests, lineage cache), the SAR (Subject Access Request) Streamlit app, and the Databricks Asset Bundle (DABs) jobs/dashboards that maintain all of it.

**This repo has no Terraform.** Everything here is DABs + SQL, deployed with `databricks bundle deploy` / `databricks bundle run`. This is deliberate: it's the **governance** half of a two-repo split, designed to be reusable unchanged on top of a future, differently-architected infra project (e.g. a VNet-injected one) — only the infra repo needs to change for that, not this one. The boundary holds because nothing here reads infra state directly: the two values it needs (`warehouse_id`, `platform_sp_id`) resolve by name against the live workspace at deploy time via DABs `lookup:` variables, so swapping the infra project underneath doesn't require touching a single file here.

## Documentation

| Doc | Covers |
|---|---|
| [`docs/sar-app.md`](docs/sar-app.md) | The SAR Streamlit app end to end: lineage-aware search across bronze/silver/gold, GDPR erasure (Article 17, with time-travel restore) and access reports (Article 15), local dev |
| [`docs/access-and-pii-governance.md`](docs/access-and-pii-governance.md) | Catalog grants, ABAC column masking, governed tags, Entra groups/AIM, Access Audit dashboard |
| [`docs/data-lifecycle-governance.md`](docs/data-lifecycle-governance.md) | Platform metadata columns, freshness SLAs, Auto TTL/retention, governance jobs, Data Governance dashboard |
| [`docs/governed-tag-grants.md`](docs/governed-tag-grants.md) | Manual governed-tag `ASSIGN` grant procedure |

`docs/data-product-teams.md` moved back to the infra repo — data mesh teams (SPs, schemas, warehouses, budgets) are entirely Terraform's concern, not something this repo creates or deploys. The docs above stay here despite touching some infra-repo resources (catalog grants, Entra groups/AIM) because they're intertwined with governance content that does belong here. A few cross-references to specific `terraform/*.tf` resources point at the infra repo, called out explicitly where they occur.

## How this repo is enabled

There's no Terraform state, no `terraform init`, nothing to bootstrap here. The infra repo enables this one the same way it enables a downstream data pipeline repo:

1. Its Terraform creates the `sp-data-platform` service principal and gives it a GitHub OIDC federated credential scoped to this repo.
2. After every `terraform apply`, its CI pushes two secrets here: `AZURE_CLIENT_ID` (`sp-data-platform`'s application ID) and `DATABRICKS_HOST` (the workspace URL).
3. This repo's own `.github/workflows/deploy.yml` authenticates directly as `sp-data-platform` via Azure OIDC using those secrets, exchanges the token for a Databricks token, and runs `databricks bundle deploy --force` + `databricks bundle run governance_setup`.

Nothing in the infra repo ever triggers a deploy here — its GitHub App is scoped to `secrets:write` only. This repo's own push triggers (on `governance/**`, `resources/**`, `apps/**`, `databricks.yml`) or a manual `workflow_dispatch` handle deploys instead. That's fine because this repo's config always resolves fresh against the live workspace at deploy time via `databricks.yml`'s `lookup:` variables (`warehouse_id`, `platform_sp_id`) — it never needs a value handed to it beyond the two secrets above.

## Layout

| Path | Responsibility |
|---|---|
| `databricks.yml` | DABs bundle config — `warehouse_id` and `platform_sp_id` resolve by name against the live workspace, no Terraform coupling |
| `governance/*.sql` | Masking UDFs, ABAC column-mask policies, audit table DDL (erasure/access/lineage-cache), the lineage-cache incremental MERGE, retention/freshness views |
| `governance/*.py` | Notebook tasks run by `governance_daily` (Auto TTL, freshness metrics) |
| `resources/jobs/governance.yml` | `governance_setup` (every deploy: UDFs → policies → audit tables) and `governance_daily` (scheduled, paused by default) — both pinned `run_as: sp-data-platform` |
| `resources/jobs/lineage_cache_refresh.yml` | Standalone job refreshing `admin.lineage_cache`, triggerable by schedule or on-demand from the SAR app |
| `resources/apps/sar.yml`, `apps/sar_app/` | The SAR Streamlit app |
| `resources/dashboards/*.yml`, `dashboards/*.lvdash.json` | Platform Data Governance and Access Audit dashboards |
| `docs/` | See "Documentation" above |
| `scripts/` | Local-dev helper scripts — see [`docs/sar-app.md`](docs/sar-app.md) |

## Naming contract with the infra repo

None of this is configurable from here — it's a naming contract the infra repo's Terraform must uphold:

- **Catalogs**: `bronze`, `silver`, `gold` (data), `admin` (`shared`/`erasure`/`access`/`lineage_cache` schemas) — hardcoded throughout `apps/sar_app/*.py` and `governance/*.sql`.
- **Governed tag namespace**: `class.*` (e.g. `class.email_address`) — hardcoded in `apps/sar_app/app.py`'s `TAG_MAP` and every `has_tag('class....')` in `create_policies.sql`.
- **Exempt principals**: ABAC `EXCEPT` clauses reference three Databricks account groups by name — `sg-dbplat-pii-readers`, `sg-dbplat-data-stewards`, `sg-dbplat-data-product-sps`. The last one nests every team SP, `sp-data-platform`, and the SAR app's own SP (the latter two via dynamic Terraform blocks in the infra repo keyed on `var.sar_app_sp_id`) — this is how the SAR app SP gets to see unmasked data to execute an erasure it already found via the calling user's own search.

If any of these names change on the infra side, the corresponding references here need updating too — nothing here discovers them automatically.

## Key constraints

- **Only one ABAC policy may match a column per user** — Databricks errors if two policies match the same column for the same user. Each `class.*` tag appears in exactly one policy's `MATCH COLUMNS` condition in `create_policies.sql`.
- **Governed tag `ASSIGN` grants aren't API-manageable** (Databricks provider/REST limitation) — apply manually via Catalog → Govern → Governed Tags → Account Permissions after every fresh deploy. See [`docs/governed-tag-grants.md`](docs/governed-tag-grants.md).
- **Policy renames need a manual `DROP POLICY`** — `CREATE OR REPLACE POLICY` only replaces a policy under its exact current name; an old name from a prior rename keeps silently existing with its stale exempt list, and `DROP POLICY` doesn't support `IF EXISTS` so this can't be folded into the idempotent job.
- **The SAR app gets a new service principal** whenever it's deleted and redeployed (a fresh workspace, or a `bundle deploy` that doesn't recognize a pre-existing app object under its deployment state) — the infra repo's `var.sar_app_sp_id` needs updating after that, to restore the app's bronze/silver/gold grants and `sg-dbplat-data-product-sps` membership.

## Deploying manually

Actions → Deploy Governance Bundle → Run workflow. Requires `AZURE_CLIENT_ID`/`AZURE_TENANT_ID`/`DATABRICKS_HOST` already set in the `dev` environment (the first two by the infra repo's cross-repo secret push and a one-time manual `AZURE_TENANT_ID`; see the infra repo's README for the GitHub App setup that makes the push work).
