# =============================================================================
# Day-13 cleanup script (PowerShell)
# =============================================================================
# Run from the repo root. Idempotent: safe to run multiple times.
#
#   .\scripts\day13_cleanup.ps1
#
# What it does:
#   1. Removes ephemeral files that should never be committed:
#        __pycache__, *.pyc, dbt/target/, dbt/logs/, dbt/dbt_packages/,
#        logs/, .ipynb_checkpoints, *.pbix.tmp.
#   2. Verifies .env has never been committed (checks git log AND the
#      working tree). Aborts loudly if it ever was.
#   3. Verifies no other secret-bearing file is staged.
#   4. Confirms LICENSE and CONTRIBUTORS.md exist.
# =============================================================================

$ErrorActionPreference = 'Stop'

Write-Host ""
Write-Host "==> Day-13 cleanup starting" -ForegroundColor Cyan
Write-Host ""

# ---------------------------------------------------------------------------
# Step 1: remove ephemeral files from the working tree
# ---------------------------------------------------------------------------
Write-Host "[1/4] Removing ephemeral files..." -ForegroundColor Yellow

$pathsToClean = @(
    "dbt/target",
    "dbt/logs",
    "dbt/dbt_packages",
    "logs",
    ".pytest_cache",
    ".ipynb_checkpoints"
)
foreach ($p in $pathsToClean) {
    if (Test-Path $p) {
        Remove-Item -Recurse -Force $p
        Write-Host "    removed: $p"
    }
}

# __pycache__ and *.pyc recursively
Get-ChildItem -Path . -Recurse -Force -Directory -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -eq "__pycache__" } |
    ForEach-Object {
        Remove-Item -Recurse -Force $_.FullName
        Write-Host "    removed: $($_.FullName)"
    }

Get-ChildItem -Path . -Recurse -Force -Include "*.pyc","*.pbix.tmp" -ErrorAction SilentlyContinue |
    ForEach-Object {
        Remove-Item -Force $_.FullName
        Write-Host "    removed: $($_.FullName)"
    }

Write-Host "    Done." -ForegroundColor Green
Write-Host ""

# ---------------------------------------------------------------------------
# Step 2: confirm .env was NEVER committed
# ---------------------------------------------------------------------------
Write-Host "[2/4] Verifying .env has never been committed..." -ForegroundColor Yellow

# git log over ALL history, ALL refs, for the file `.env`
$envHistory = git log --all --full-history --source -- .env 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "    git log failed: $envHistory" -ForegroundColor Red
    exit 1
}

if ([string]::IsNullOrWhiteSpace($envHistory)) {
    Write-Host "    OK: .env has no history in this repo." -ForegroundColor Green
} else {
    Write-Host "    !! .env IS in git history. Rotate secrets and rewrite history:" -ForegroundColor Red
    Write-Host $envHistory
    Write-Host ""
    Write-Host "    Suggested remediation:" -ForegroundColor Red
    Write-Host "      1. Rotate POSTGRES_PASSWORD, AIRFLOW secrets, etc."
    Write-Host "      2. Use BFG Repo-Cleaner or 'git filter-repo' to remove .env from history."
    Write-Host "      3. Force-push, then notify reviewers."
    exit 1
}

# Also check it's NOT currently staged
$stagedEnv = git ls-files --error-unmatch .env 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "    !! .env is tracked. Run: git rm --cached .env" -ForegroundColor Red
    exit 1
}

Write-Host ""

# ---------------------------------------------------------------------------
# Step 3: scan for other secret-bearing patterns
# ---------------------------------------------------------------------------
Write-Host "[3/4] Scanning the working tree for secret-bearing patterns..." -ForegroundColor Yellow

$suspectFiles = git ls-files |
    Where-Object {
        # Match anything that looks like a credentials file
        $_ -match '\.env(\..+)?$' -or
        $_ -match 'secret' -or
        $_ -match 'credentials' -or
        $_ -match 'id_rsa' -or
        $_ -match '\.pem$' -or
        $_ -match '\.p12$' -or
        $_ -match '\.pfx$' -or
        $_ -match 'standalone_admin_password\.txt$'
    } |
    Where-Object {
        # Allowed:
        $_ -notmatch '^env\.example$' -and
        $_ -notmatch 'scripts/day13_cleanup\.ps1$'   # this file talks about secrets but contains none
    }

if ($suspectFiles) {
    Write-Host "    !! Suspect tracked files found:" -ForegroundColor Red
    $suspectFiles | ForEach-Object { Write-Host "       - $_" }
    Write-Host "    Review each and 'git rm --cached <file>' if the contents are sensitive." -ForegroundColor Red
    exit 1
} else {
    Write-Host "    OK: no obvious secret-bearing files tracked." -ForegroundColor Green
}
Write-Host ""

# ---------------------------------------------------------------------------
# Step 4: confirm LICENSE and CONTRIBUTORS exist
# ---------------------------------------------------------------------------
Write-Host "[4/4] Verifying required submission files..." -ForegroundColor Yellow

$required = @("README.md", "LICENSE", "CONTRIBUTORS.md", "env.example", ".gitignore",
              "docker-compose.yml", "requirements.txt", "docs/diagrams/erd_silver_gold.png")
$missing  = $required | Where-Object { -not (Test-Path $_) }
if ($missing) {
    Write-Host "    !! Missing:" -ForegroundColor Red
    $missing | ForEach-Object { Write-Host "       - $_" }
    exit 1
} else {
    Write-Host "    OK: all required files present." -ForegroundColor Green
}

Write-Host ""
Write-Host "==> Day-13 cleanup complete. Status:" -ForegroundColor Cyan
git status --short
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  git add ."
Write-Host "  git commit -m 'chore: cleanup temporary files and verify .env not committed'"
