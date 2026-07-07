<#
.SYNOPSIS
    Runs the SAR app locally against the real deployed workspace.
.DESCRIPTION
    Wraps `databricks apps run-local` per docs/sar-app.md's "Local development"
    section:
      - resolves the workspace's platform SQL warehouse ID via a direct
        `databricks warehouses list` name lookup, not Terraform outputs —
        this repo has no Terraform of its own (that lives in the infra repo,
        simple-databricks-deployment); the warehouse itself is created there,
        but this script only needs to find it by its known name
      - starts that warehouse if it's stopped - a stopped warehouse's cold
        start isn't reliably handled by the SQL connector's own request
        timeout, so searching while it's still starting fails with a bare
        `databricks.sql.exc.RequestError`
      - looks up the deployed lineage_cache_refresh job's numeric ID (a
        Databricks Asset Bundle resource — `bundle summary` doesn't expose
        job IDs directly, so this greps `databricks jobs list` by name
        instead) and passes it as LINEAGE_CACHE_REFRESH_JOB_ID, since
        app.yaml's `valueFrom` binding for it only resolves inside a
        deployed app, same as the warehouse ID
      - passes PURPOSE_DRAFT_ENDPOINT as a fixed literal (unlike the
        warehouse/job IDs, the Foundation Model endpoint name isn't a
        per-workspace dynamic ID to look up — it's the same literal as
        resources/apps/sar.yml's serving_endpoint.name)
      - fetches a fresh OAuth token (tokens last about an hour; a
        long-running `run-local` process keeps using the token it launched
        with, so restart via this script rather than reusing an old window)
      - launches the app

    Requires a one-time `databricks auth login --host <workspace-host> -p <Profile>`
    before first use, and again whenever the cached token is invalid (check
    with `databricks auth profiles`).
.PARAMETER Profile
    The ~/.databrickscfg profile name for the deployed workspace.
.PARAMETER InstallDeps
    Also run `pip install -r requirements.txt` first (skipped by default -
    only needed once, or after requirements.txt changes).
#>

param(
    [string]$Profile = "adb-7405619162316939",
    [switch]$InstallDeps
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$appDir = Join-Path $repoRoot "apps\sar_app"

Write-Host "Checking auth profile '$Profile'..."
$profiles = (databricks auth profiles -o json | ConvertFrom-Json).profiles
$current = $profiles | Where-Object { $_.name -eq $Profile }
if (-not $current -or -not $current.valid) {
    Write-Error "Profile '$Profile' is missing or its cached token is invalid. Run:`n  databricks auth login --host <workspace-host> -p $Profile`nthen re-run this script."
    exit 1
}

if ($InstallDeps) {
    Write-Host "Installing Python dependencies..."
    Push-Location $appDir
    try {
        pip install -r requirements.txt
    } finally {
        Pop-Location
    }
}

Write-Host "Reading the platform SQL warehouse ID..."
$warehouses = databricks warehouses list -p $Profile -o json | ConvertFrom-Json
$platformWarehouse = $warehouses | Where-Object { $_.name -eq "data_platform_admins-sql-warehouse" }
if (-not $platformWarehouse) {
    Write-Error "Could not find the 'data_platform_admins-sql-warehouse' warehouse. Confirm the infra repo (simple-databricks-deployment) has been applied."
    exit 1
}
$warehouseId = $platformWarehouse.id

Write-Host "Ensuring SQL warehouse '$warehouseId' is running..."
$warehouse = databricks warehouses get $warehouseId -p $Profile -o json | ConvertFrom-Json
if ($warehouse.state -ne "RUNNING") {
    Write-Host "Warehouse is '$($warehouse.state)' - starting it (this blocks until ready, can take a minute or two)..."
    databricks warehouses start $warehouseId -p $Profile | Out-Null
}

Write-Host "Reading the lineage-cache-refresh job ID..."
$jobs = databricks jobs list -p $Profile -o json | ConvertFrom-Json
$lineageJob = $jobs | Where-Object { $_.settings.name -eq "platform-lineage-cache-refresh" }
if (-not $lineageJob) {
    Write-Error "Could not find the 'platform-lineage-cache-refresh' job. Run 'databricks bundle deploy' first."
    exit 1
}
$lineageJobId = $lineageJob.job_id

Write-Host "Fetching a fresh access token..."
$token = (databricks auth token -p $Profile | ConvertFrom-Json).access_token

Write-Host "Launching the app - open the printed proxy URL (not the raw Streamlit port) once it's ready..."
Push-Location $appDir
try {
    databricks apps run-local -p $Profile `
        --env DATABRICKS_WAREHOUSE_ID=$warehouseId `
        --env LINEAGE_CACHE_REFRESH_JOB_ID=$lineageJobId `
        --env PURPOSE_DRAFT_ENDPOINT=databricks-claude-3-7-sonnet `
        --env DATABRICKS_TOKEN=$token
} finally {
    Pop-Location
}
