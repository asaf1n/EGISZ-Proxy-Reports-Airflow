param(
    [ValidateSet("All", "Airflow", "Metabase", "Dwh")]
    [string]$Target = "All",
    [switch]$Zip
)

# Сборка самодостаточных бандлов для импорта настроек во внешнюю инфраструктуру
# (см. deploy/README.md). Бандлы всегда собираются из канонических исходников
# репозитория — копии кода в git не хранятся, чтобы исключить их дрейф.

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$DistRoot = Join-Path $RepoRoot "dist\external"

function New-CleanDirectory {
    param([string]$Path)

    if (Test-Path $Path) {
        Remove-Item -Recurse -Force $Path
    }
    New-Item -ItemType Directory -Force $Path | Out-Null
}

function Copy-BundleItem {
    param(
        [string]$Source,
        [string]$Destination
    )

    $parent = Split-Path -Parent $Destination
    if (-not (Test-Path $parent)) {
        New-Item -ItemType Directory -Force $parent | Out-Null
    }
    Copy-Item -Recurse -Force $Source $Destination
}

function Get-ProjectDependencies {
    # requirements.txt бандла генерируется из [project].dependencies pyproject.toml —
    # единственный источник версий, ручная копия неизбежно разошлась бы.
    $pyproject = Get-Content (Join-Path $RepoRoot "pyproject.toml") -Raw
    if ($pyproject -notmatch '(?s)dependencies\s*=\s*\[(.*?)\]') {
        throw "cannot find [project].dependencies in pyproject.toml"
    }
    $deps = @()
    foreach ($match in [regex]::Matches($Matches[1], '"([^"]+)"')) {
        $deps += $match.Groups[1].Value
    }
    if ($deps.Count -eq 0) {
        throw "no dependencies parsed from pyproject.toml"
    }
    return $deps
}

function Write-BuildInfo {
    param([string]$BundleRoot)

    $ErrorActionPreference = 'Continue'
    $sha = git -C $RepoRoot rev-parse --short HEAD
    if ($LASTEXITCODE -ne 0) {
        throw "git rev-parse failed with exit code ${LASTEXITCODE}"
    }
    $ErrorActionPreference = 'Stop'
    $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss K"
    "commit: ${sha}`nbuilt: ${stamp}" |
        Out-File -Encoding utf8 (Join-Path $BundleRoot "BUILD_INFO.txt")
}

function Complete-Bundle {
    param(
        [string]$Name,
        [string]$BundleRoot,
        [string]$ReadmeSource
    )

    Copy-BundleItem (Join-Path $RepoRoot $ReadmeSource) (Join-Path $BundleRoot "README.md")
    Write-BuildInfo $BundleRoot
    if ($Zip) {
        $archive = Join-Path $DistRoot "egisz-external-${Name}.zip"
        Compress-Archive -Path (Join-Path $BundleRoot "*") -DestinationPath $archive -Force
    }
    $count = (Get-ChildItem -Recurse -File $BundleRoot | Measure-Object).Count
    Write-Host "[bundle] ${Name}: ${count} files -> ${BundleRoot}"
}

function Build-AirflowBundle {
    $bundle = Join-Path $DistRoot "airflow"
    New-CleanDirectory $bundle

    New-Item -ItemType Directory -Force (Join-Path $bundle "dags") | Out-Null
    Get-ChildItem (Join-Path $RepoRoot "airflow\dags") -Filter "*.py" -File |
        ForEach-Object { Copy-Item $_.FullName (Join-Path $bundle "dags") }

    New-Item -ItemType Directory -Force (Join-Path $bundle "egisz_elt") | Out-Null
    Get-ChildItem (Join-Path $RepoRoot "src\egisz_elt") -Filter "*.py" -File |
        ForEach-Object { Copy-Item $_.FullName (Join-Path $bundle "egisz_elt") }

    Copy-BundleItem (Join-Path $RepoRoot "pyproject.toml") (Join-Path $bundle "pyproject.toml")
    $header = "# DAG runtime dependencies, generated from pyproject.toml" +
        " (Apache Airflow 2.x / Python 3.11 provided by the target)."
    @($header) + (Get-ProjectDependencies) |
        Out-File -Encoding ascii (Join-Path $bundle "requirements.txt")

    Complete-Bundle "airflow" $bundle "deploy\external-airflow\README.md"
}

function Build-MetabaseBundle {
    $bundle = Join-Path $DistRoot "metabase"
    New-CleanDirectory $bundle

    Copy-BundleItem (Join-Path $RepoRoot "metabase\setup-dashboards.sh") (Join-Path $bundle "setup-dashboards.sh")
    Copy-BundleItem (Join-Path $RepoRoot "metabase\sync-models.sh") (Join-Path $bundle "sync-models.sh")
    Copy-BundleItem (Join-Path $RepoRoot "metabase\include\mb_list.sh") (Join-Path $bundle "include\mb_list.sh")
    Copy-BundleItem (Join-Path $RepoRoot "metabase_dashboards") (Join-Path $bundle "metabase_dashboards")
    Copy-BundleItem (Join-Path $RepoRoot "metabase_models") (Join-Path $bundle "metabase_models")

    # Манифест пересчитывается импортёром на месте; запечённый из образа неуместен.
    $baked = Join-Path $bundle "metabase_dashboards\.manifest.sha256"
    if (Test-Path $baked) {
        Remove-Item -Force $baked
    }

    Complete-Bundle "metabase" $bundle "deploy\external-metabase\README.md"
}

function Build-DwhBundle {
    $bundle = Join-Path $DistRoot "dwh"
    New-CleanDirectory $bundle

    # Раскладка db/dwh_init.sql + db/parts/ обязана сохраниться: точка входа
    # подключает части относительными \i db/parts/*.sql.
    Copy-BundleItem (Join-Path $RepoRoot "db\dwh_init.sql") (Join-Path $bundle "db\dwh_init.sql")
    Copy-BundleItem (Join-Path $RepoRoot "db\parts") (Join-Path $bundle "db\parts")

    Complete-Bundle "dwh" $bundle "deploy\external-dwh\README.md"
}

if (-not (Test-Path $DistRoot)) {
    New-Item -ItemType Directory -Force $DistRoot | Out-Null
}

switch ($Target) {
    "All" {
        Build-DwhBundle
        Build-AirflowBundle
        Build-MetabaseBundle
    }
    "Airflow" { Build-AirflowBundle }
    "Metabase" { Build-MetabaseBundle }
    "Dwh" { Build-DwhBundle }
}

Write-Host "[bundle] done: ${DistRoot}"
