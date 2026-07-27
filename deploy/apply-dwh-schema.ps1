<#
.SYNOPSIS
Применяет схему DWH `dwh_egisz` во внешнем PostgreSQL (идемпотентно).

.DESCRIPTION
Сценарий рабочего места оператора (в переносимый пакет не входит, см. deploy/README.md §4):
проверяет доступность psql, забирает пароль роли из переменной окружения, запускает
`db/dwh_init.sql` из корня пакета и, по запросу, обновляет витрины.

Источник схемы — собранный пакет `dist/egisz-bi`, при его отсутствии рабочая копия
репозитория.

.EXAMPLE
.\deploy\apply-dwh-schema.ps1

.EXAMPLE
.\deploy\apply-dwh-schema.ps1 -RefreshMarts
#>
param(
    [string]$PgHost = "dwh-egisz-rw.bi.sdsys.svc",
    [int]$PgPort = 5432,
    [string]$Database = "dwh_egisz",
    [string]$User = "egisz",
    [string]$PasswordVariable = "EGISZ_BI_DWH_PASSWORD",
    [switch]$RefreshMarts,
    [switch]$SkipSchema,
    [switch]$Force
)

$ErrorActionPreference = "Stop"

# Порядок обязателен: разбивка ошибок раньше периодических витрин — недельный
# и месячный слои строятся поверх неё.
$ReportMarts = @(
    "public.rpt_error_breakdown",
    "public.rpt_documents_weekly",
    "public.rpt_error_breakdown_weekly",
    "public.rpt_documents_monthly",
    "public.rpt_error_breakdown_monthly"
)

function Get-BundleRoot {
    # Собранный пакет приоритетнее рабочей копии: применяется ровно то, что передаётся
    # на целевой контур.
    $repoRoot = Split-Path -Parent $PSScriptRoot
    $candidates = @((Join-Path $repoRoot "dist\egisz-bi"), $repoRoot)
    foreach ($candidate in $candidates) {
        if (Test-Path (Join-Path $candidate "db\dwh_init.sql")) {
            return $candidate
        }
    }
    throw "db\dwh_init.sql не найден ни в '$($candidates -join "', '")'"
}

function Get-EnvironmentSecret {
    param([string]$Name)

    foreach ($scope in @("Process", "User", "Machine")) {
        $value = [Environment]::GetEnvironmentVariable($Name, $scope)
        if (-not [string]::IsNullOrWhiteSpace($value)) {
            return $value
        }
    }
    throw "переменная окружения ${Name} не задана: setx ${Name} `"<пароль роли ${User}>`" и открыть новый сеанс PowerShell"
}

function Invoke-PsqlScalar {
    param([string]$Sql)

    $value = & psql -h $PgHost -p $PgPort -U $User -d $Database -AtqX -v ON_ERROR_STOP=1 -c $Sql
    if ($LASTEXITCODE -ne 0) {
        throw "psql завершился с кодом ${LASTEXITCODE} на запросе: ${Sql}"
    }
    return ($value | Select-Object -Last 1)
}

function Assert-PipelineIdle {
    # Схема пересобирает отчётный слой целиком; параллельный приём фактов даёт
    # взаимоблокировку. Учитываются только сессии записи — чтение Metabase не мешает.
    $sql = @'
SELECT count(*) FROM pg_stat_activity
WHERE datname = current_database()
  AND pid <> pg_backend_pid()
  AND state <> 'idle'
  AND (query ILIKE '%transform_raw_to_facts%'
    OR query ILIKE '%recompute_document_%'
    OR query ILIKE '%REFRESH MATERIALIZED VIEW%'
    OR query ILIKE '%exchangelog_raw%')
'@
    $busy = Invoke-PsqlScalar $sql
    if ([int]$busy -gt 0) {
        throw "в ${Database} активны сессии конвейера (${busy}): поставить DAG-и на паузу и дождаться завершения задач, либо запустить с -Force"
    }
}

function Invoke-SchemaScript {
    param([string]$Root)

    # Точка входа подключает модули относительными путями (\i db/...), поэтому
    # psql запускается строго из корня бандла.
    Push-Location $Root
    try {
        & psql -h $PgHost -p $PgPort -U $User -d $Database -v ON_ERROR_STOP=1 -f "db/dwh_init.sql"
        if ($LASTEXITCODE -ne 0) {
            throw "применение схемы завершилось с кодом ${LASTEXITCODE}"
        }
    }
    finally {
        Pop-Location
    }
}

function Update-ReportMarts {
    foreach ($mart in $ReportMarts) {
        $started = Get-Date
        Invoke-PsqlScalar "REFRESH MATERIALIZED VIEW CONCURRENTLY ${mart}" | Out-Null
        $elapsed = [int]((Get-Date) - $started).TotalSeconds
        Write-Host "[dwh] обновлена витрина ${mart} (${elapsed} c)"
    }
}

$root = Get-BundleRoot
$password = Get-EnvironmentSecret $PasswordVariable

Write-Host "[dwh] цель: ${User}@${PgHost}:${PgPort}/${Database}"
Write-Host "[dwh] источник схемы: ${root}"

$previousPassword = $env:PGPASSWORD
$previousEncoding = $env:PGCLIENTENCODING
$env:PGPASSWORD = $password
$env:PGCLIENTENCODING = "UTF8"
try {
    if (-not (Get-Command psql -ErrorAction SilentlyContinue)) {
        throw "psql не найден в PATH (нужен клиент PostgreSQL 14+)"
    }

    if (-not $Force) {
        Assert-PipelineIdle
    }

    if (-not $SkipSchema) {
        $started = Get-Date
        Invoke-SchemaScript $root
        Write-Host "[dwh] схема применена за $([int]((Get-Date) - $started).TotalMinutes) мин"
    }

    if ($RefreshMarts) {
        Update-ReportMarts
    }

    Write-Host "[dwh] готово"
}
finally {
    $env:PGPASSWORD = $previousPassword
    $env:PGCLIENTENCODING = $previousEncoding
}
