param(
    [switch]$Zip
)

# Сборка пакета настроек для переноса во внешнюю инфраструктуру (см. deploy/README.md).
# Пакет собирается из канонических исходников репозитория — копии кода в git не хранятся,
# чтобы исключить их дрейф.

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$DistRoot = Join-Path $RepoRoot "dist"
$BundleName = "egisz-bi"
$BundleRoot = Join-Path $DistRoot $BundleName

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

function Add-Dags {
    # DAG-файлы самодостаточны: копируются как есть, целевому Airflow не нужны
    # ни PYTHONPATH, ни установка пакета.
    $dagFiles = Get-ChildItem (Join-Path $RepoRoot "dags") -Filter "egisz_*_dag.py"
    if ($dagFiles.Count -eq 0) {
        throw "no egisz_*_dag.py files found in dags"
    }
    foreach ($dagFile in $dagFiles) {
        Copy-BundleItem $dagFile.FullName (Join-Path $BundleRoot "dags\$($dagFile.Name)")
    }
}

function Add-Schema {
    # Раскладка db/ обязана сохраниться: точка входа подключает модули
    # относительными \i db/*.sql, поэтому psql запускается из корня пакета.
    Copy-BundleItem (Join-Path $RepoRoot "db\dwh_init.sql") (Join-Path $BundleRoot "db\dwh_init.sql")
    foreach ($module in Get-ChildItem (Join-Path $RepoRoot "db") -Filter "0*.sql" | Sort-Object Name) {
        Copy-BundleItem $module.FullName (Join-Path $BundleRoot "db\$($module.Name)")
    }
}

function Add-Metabase {
    $metabase = Join-Path $BundleRoot "metabase"
    Copy-BundleItem (Join-Path $RepoRoot "metabase\setup-dashboards.sh") (Join-Path $metabase "setup-dashboards.sh")
    Copy-BundleItem (Join-Path $RepoRoot "metabase\sync-models.sh") (Join-Path $metabase "sync-models.sh")
    # Импортёр подключает общие функции только из include/ рядом с собой.
    Copy-BundleItem (Join-Path $RepoRoot "metabase\include\mb_list.sh") (Join-Path $metabase "include\mb_list.sh")
    # Список выведенных из обращения объектов лежит рядом с импортёром: без него
    # переименованные карточки и модели остались бы в коллекции дублями.
    Copy-BundleItem (Join-Path $RepoRoot "metabase\retired-objects.json") (Join-Path $metabase "retired-objects.json")
    Copy-BundleItem (Join-Path $RepoRoot "metabase_dashboards") (Join-Path $metabase "dashboards")
    Copy-BundleItem (Join-Path $RepoRoot "metabase_models") (Join-Path $metabase "models")

    # Манифест пересчитывается импортёром на месте; включённый в образ неуместен.
    $baked = Join-Path $metabase "dashboards\.manifest.sha256"
    if (Test-Path $baked) {
        Remove-Item -Force $baked
    }
}

New-CleanDirectory $BundleRoot

Add-Schema
Add-Dags
Add-Metabase
Copy-BundleItem (Join-Path $RepoRoot "deploy\README.md") (Join-Path $BundleRoot "README.md")

if ($Zip) {
    $archive = Join-Path $DistRoot "${BundleName}.zip"
    Compress-Archive -Path (Join-Path $BundleRoot "*") -DestinationPath $archive -Force
    Write-Host "[bundle] archive -> ${archive}"
}

$count = (Get-ChildItem -Recurse -File $BundleRoot | Measure-Object).Count
Write-Host "[bundle] ${BundleName}: ${count} files -> ${BundleRoot}"
