<#
.SYNOPSIS
Импортирует дашборды и модели в сторонний Metabase (идемпотентно).

.DESCRIPTION
Сценарий рабочего места оператора (в переносимый пакет не входит, см. deploy/README.md §4):
`setup-dashboards.sh` написан на bash и требует `bash`, `curl`, `jq`, `psql`, `sha256sum`,
поэтому запускается в одноразовом контейнере Linux. Ключ API и пароль роли DWH берутся из
переменных окружения и передаются в контейнер по имени — значения не попадают ни в
командную строку, ни в список процессов.

Источник — собранный пакет `dist/egisz-bi`, при его отсутствии рабочая копия репозитория.

.EXAMPLE
.\deploy\import-metabase.ps1

.EXAMPLE
.\deploy\import-metabase.ps1 -Force
#>
param(
    [string]$MetabaseUrl = "https://bi.sdsys.ru",
    [string]$ApiKeyVariable = "EGISZ_BI_API_KEY",
    [string]$PgHost = "dwh-egisz-rw.bi.sdsys.svc",
    [int]$PgPort = 5432,
    [string]$Database = "dwh_egisz",
    [string]$User = "egisz",
    [string]$PasswordVariable = "EGISZ_BI_DWH_PASSWORD",
    # Адрес DWH для контейнера: по умолчанию имя резолвится средствами хоста — DNS
    # внутреннего контура недоступен из сети Docker.
    [string]$PgAddress = "",
    [string]$Image = "alpine:3.20",
    # Переимпорт при неизменных JSON (иначе отрабатывает sha256-манифест).
    [switch]$Force,
    # Глобальные настройки инстанса (локаль, часовой пояс, кеш, public sharing).
    # На общем Metabase они меняют поведение чужих сервисов — по умолчанию не трогаем.
    [switch]$ManageInstanceSettings
)

$ErrorActionPreference = "Stop"

$ContainerPackages = "bash curl jq postgresql16-client coreutils util-linux ca-certificates"

function Get-BundleLayout {
    # Собранный пакет приоритетнее рабочей копии: импортируется ровно то, что передаётся
    # на целевой контур. Каталоги дашбордов и моделей в пакете лежат внутри metabase/.
    $repoRoot = Split-Path -Parent $PSScriptRoot
    $packageRoot = Join-Path $repoRoot "dist\egisz-bi"
    if (Test-Path (Join-Path $packageRoot "metabase\setup-dashboards.sh")) {
        return @{
            Root          = $packageRoot
            WorkDir       = "/bundle/metabase"
            DashboardsDir = "/bundle/metabase/dashboards"
            ModelsDir     = "/bundle/metabase/models"
        }
    }
    if (Test-Path (Join-Path $repoRoot "metabase\setup-dashboards.sh")) {
        return @{
            Root          = $repoRoot
            WorkDir       = "/bundle/metabase"
            DashboardsDir = "/bundle/metabase_dashboards"
            ModelsDir     = "/bundle/metabase_models"
        }
    }
    throw "setup-dashboards.sh не найден ни в '${packageRoot}\metabase', ни в '${repoRoot}\metabase'"
}

function Get-EnvironmentSecret {
    param([string]$Name)

    foreach ($scope in @("Process", "User", "Machine")) {
        $value = [Environment]::GetEnvironmentVariable($Name, $scope)
        if (-not [string]::IsNullOrWhiteSpace($value)) {
            return $value
        }
    }
    throw "переменная окружения ${Name} не задана: setx ${Name} `"<значение>`" и открыть новый сеанс PowerShell"
}

function Resolve-PgAddress {
    if ($PgAddress) {
        return $PgAddress
    }
    $parsed = [System.Net.IPAddress]::Any
    if ([System.Net.IPAddress]::TryParse($PgHost, [ref]$parsed)) {
        return ""
    }
    try {
        $address = [System.Net.Dns]::GetHostAddresses($PgHost) |
            Where-Object { $_.AddressFamily -eq "InterNetwork" } |
            Select-Object -First 1
        if ($address) {
            return $address.IPAddressToString
        }
    }
    catch {
        Write-Warning "имя ${PgHost} не разрешилось на хосте; контейнер будет резолвить его сам"
    }
    return ""
}

$layout = Get-BundleLayout
$apiKey = Get-EnvironmentSecret $ApiKeyVariable
$dbPassword = Get-EnvironmentSecret $PasswordVariable

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "docker не найден в PATH"
}

Write-Host "[metabase] цель: ${MetabaseUrl}"
Write-Host "[metabase] источник: $($layout.Root)"

$dockerArgs = @(
    "run", "--rm",
    "-v", "$($layout.Root):/bundle:ro",
    "-w", $layout.WorkDir
)

$pgAddressResolved = Resolve-PgAddress
if ($pgAddressResolved) {
    $dockerArgs += @("--add-host", "${PgHost}:${pgAddressResolved}")
    Write-Host "[metabase] DWH для контейнера: ${PgHost} → ${pgAddressResolved}"
}

$dockerArgs += @(
    "-e", "METABASE_API_KEY",
    "-e", "APP_DB_PASSWORD",
    "-e", "METABASE_URL=${MetabaseUrl}",
    "-e", "METABASE_DASHBOARDS_DIR=$($layout.DashboardsDir)",
    "-e", "METABASE_MODELS_DIR=$($layout.ModelsDir)",
    "-e", "APP_DB_HOST=${PgHost}",
    "-e", "APP_DB_PORT=${PgPort}",
    "-e", "APP_DB_NAME=${Database}",
    "-e", "APP_DB_USER=${User}",
    "-e", "METABASE_FORCE_PROVISION=$(if ($Force) { 'always' } else { 'auto' })",
    "-e", "METABASE_MANAGE_INSTANCE_SETTINGS=$($ManageInstanceSettings.IsPresent.ToString().ToLower())",
    "-e", "METABASE_PUBLIC_CLIENT_DASHBOARD=false",
    $Image,
    "sh", "-c", "apk add -q ${ContainerPackages}; bash ./setup-dashboards.sh"
)

$previousApiKey = $env:METABASE_API_KEY
$previousDbPassword = $env:APP_DB_PASSWORD
$env:METABASE_API_KEY = $apiKey
$env:APP_DB_PASSWORD = $dbPassword
try {
    & docker @dockerArgs
    if ($LASTEXITCODE -ne 0) {
        throw "импорт завершился с кодом ${LASTEXITCODE}"
    }
    Write-Host "[metabase] готово"
}
finally {
    $env:METABASE_API_KEY = $previousApiKey
    $env:APP_DB_PASSWORD = $previousDbPassword
}
