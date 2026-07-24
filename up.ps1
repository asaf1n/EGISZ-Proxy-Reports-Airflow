param(
    [ValidateSet("Start", "Stop", "Airflow", "Metabase", "Metabase-Provisioning", "Stop-Airflow", "Stop-Metabase")]
    [string]$Action = "Start"
)

$ErrorActionPreference = "Stop"
$Namespace = "egisz-bi"

$DwhPoolName = "dwh_postgres"
$DwhPoolSlots = 1
$DwhPoolDescription = "Exclusive DWH transform / reconcile / enriched mart maintenance"
$AirflowImage = "egisz-airflow-worker:latest"
$AirflowChartVersion = "1.22.0"
$MetabaseImage = "egisz-metabase:latest"
$MetabaseDeployStateFile = Join-Path $PSScriptRoot ".cache/metabase-deployed-manifest"

function Invoke-Checked {
    param(
        [string]$Description,
        [scriptblock]$Command
    )

    # Local override: native-команды (docker buildx, kubectl) пишут прогресс/info
    # в stderr. Глобальный $ErrorActionPreference='Stop' трактует это как
    # terminating error ДО проверки exit-кода — даже если команда отработала
    # успешно. Переходим на Continue в пределах функции и проверяем $LASTEXITCODE
    # сами; это и есть смысл этой обёртки.
    $ErrorActionPreference = 'Continue'
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "${Description} failed with exit code ${LASTEXITCODE}"
    }
}

function Test-KubectlTransientError {
    param([string]$Message)

    return $Message -match 'TLS handshake timeout|connection refused|i/o timeout|temporary failure|the server is currently unable|dial tcp|EOF|client connection lost|unable to decode an event|object has been deleted'
}

function Invoke-Kubectl {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,
        [int]$MaxAttempts = 5,
        [int]$InitialDelaySeconds = 3
    )

    $ErrorActionPreference = 'Continue'
    $delay = $InitialDelaySeconds
    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        $output = & kubectl @Arguments 2>&1
        $exitCode = $LASTEXITCODE
        if ($exitCode -eq 0) {
            return ,$output
        }

        $message = ($output | Out-String).Trim()
        if (-not (Test-KubectlTransientError $message) -or $attempt -eq $MaxAttempts) {
            if ($message) {
                Write-Host $message
            }
            $global:LASTEXITCODE = $exitCode
            return ,$output
        }

        Write-Host "kubectl transient error (attempt ${attempt}/${MaxAttempts}): retrying in ${delay}s..."
        Start-Sleep -Seconds $delay
        $delay = [Math]::Min($delay * 2, 30)
    }
}

function Get-ReadyPodCount {
    param(
        [Parameter(Mandatory = $true)]
        [string]$LabelSelector
    )

    $jsonOutput = Invoke-Kubectl -Arguments @(
        'get', 'pods', '-n', $Namespace, '-l', $LabelSelector, '-o', 'json'
    )
    if ($LASTEXITCODE -ne 0) {
        return -1
    }

    try {
        $payload = ($jsonOutput | Out-String) | ConvertFrom-Json
    } catch {
        return -1
    }

    $readyCount = 0
    foreach ($pod in @($payload.items)) {
        if ($pod.metadata.deletionTimestamp) {
            continue
        }
        if ($pod.status.phase -ne 'Running') {
            continue
        }
        $containerStatuses = @($pod.status.containerStatuses)
        if ($containerStatuses.Count -eq 0) {
            continue
        }
        $allReady = $true
        foreach ($container in $containerStatuses) {
            if (-not $container.ready) {
                $allReady = $false
                break
            }
        }
        if ($allReady) {
            $readyCount++
        }
    }

    return $readyCount
}

function Clear-StuckTerminatingPods {
    param(
        [string]$LabelSelector = "",
        [int]$GraceSeconds = 120
    )

    $getArgs = @('get', 'pods', '-n', $Namespace, '-o', 'json')
    if (-not [string]::IsNullOrWhiteSpace($LabelSelector)) {
        $getArgs += @('-l', $LabelSelector)
    }

    $jsonOutput = Invoke-Kubectl -Arguments $getArgs
    if ($LASTEXITCODE -ne 0) {
        return
    }

    try {
        $payload = ($jsonOutput | Out-String) | ConvertFrom-Json
    } catch {
        return
    }

    $now = Get-Date
    foreach ($pod in @($payload.items)) {
        $deletionTimestamp = $pod.metadata.deletionTimestamp
        if ([string]::IsNullOrWhiteSpace($deletionTimestamp)) {
            continue
        }

        $deletedAt = [datetime]::Parse($deletionTimestamp).ToUniversalTime()
        $ageSeconds = ($now.ToUniversalTime() - $deletedAt).TotalSeconds
        if ($ageSeconds -lt $GraceSeconds) {
            continue
        }

        $podName = $pod.metadata.name
        Write-Host "Force-deleting stuck terminating pod ${podName} (${ageSeconds:N0}s)..."
        Invoke-Kubectl -Arguments @(
            'delete', 'pod', $podName, '-n', $Namespace, '--force', '--grace-period=0'
        ) | Out-Null
    }
}

function Wait-ComponentPodsReady {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Description,
        [Parameter(Mandatory = $true)]
        [string]$LabelSelector,
        [int]$ExpectedPods = 1,
        [int]$TotalTimeoutSeconds = 300,
        [int]$PollIntervalSeconds = 20
    )

    $deadline = (Get-Date).AddSeconds($TotalTimeoutSeconds)
    Write-Host "Waiting for ${Description}..."

    while ((Get-Date) -lt $deadline) {
        $readyCount = Get-ReadyPodCount -LabelSelector $LabelSelector
        if ($readyCount -ge $ExpectedPods) {
            Write-Host "${Description} ready (${readyCount}/${ExpectedPods})."
            return
        }

        if ($readyCount -lt 0) {
            $readyCount = 0
        }

        $remaining = [Math]::Max(0, [int]($deadline - (Get-Date)).TotalSeconds)
        Write-Host "${Description}: ${readyCount}/${ExpectedPods} ready pods (${remaining}s left)..."
        Start-Sleep -Seconds $PollIntervalSeconds
    }

    throw "${Description} timed out after ${TotalTimeoutSeconds}s"
}

function Initialize-KubernetesContext {
    if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
        throw @"
kubectl не найден в PATH.

Установите kubectl и включите Kubernetes в Docker Desktop:
Settings -> Kubernetes -> Enable Kubernetes -> Apply & Restart.
"@
    }

    $context = ""
    try {
        $context = (kubectl config current-context 2>$null).Trim()
    } catch {
        $context = ""
    }

    if (-not [string]::IsNullOrWhiteSpace($context)) {
        return $context
    }

    $availableContexts = @()
    try {
        $availableContexts = @(
            kubectl config get-contexts -o name 2>$null |
                ForEach-Object { $_.Trim() } |
                Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
        )
    } catch {
        $availableContexts = @()
    }

    $preferredContexts = @(
        "docker-desktop",
        "docker-for-desktop"
    )

    foreach ($preferred in $preferredContexts) {
        if ($availableContexts -contains $preferred) {
            Write-Host "Selecting Kubernetes context '$preferred'..."
            Invoke-Checked "Select Kubernetes context $preferred" {
                kubectl config use-context $preferred
            }
            return $preferred
        }
    }

    if ($availableContexts.Count -eq 1) {
        $onlyContext = $availableContexts[0]
        Write-Host "Selecting Kubernetes context '$onlyContext'..."
        Invoke-Checked "Select Kubernetes context $onlyContext" {
            kubectl config use-context $onlyContext
        }
        return $onlyContext
    }

    if ($availableContexts.Count -eq 0) {
        throw @"
kubeconfig не содержит контекстов ($env:USERPROFILE\.kube\config пуст или не синхронизирован с Docker Desktop).

Если pod'ы видны в Docker Desktop, но kubectl не работает:
1. Settings -> Kubernetes -> дождитесь статуса Running.
2. Settings -> Kubernetes -> Reset Kubernetes Cluster (пересоздаст kubeconfig).
3. Проверка: kubectl config get-contexts
4. kubectl config use-context docker-desktop
"@
    }

    throw @"
kubectl current-context не задан, подходящий контекст не найден.

Доступные контексты: $($availableContexts -join ', ')

Перед запуском up.ps1:
1. Запустите Docker Desktop и дождитесь готовности Linux engine.
2. Включите Kubernetes: Settings -> Kubernetes -> Enable Kubernetes -> Apply & Restart.
3. kubectl config use-context docker-desktop
"@
}

function Test-KubernetesConnection {
    Write-Host "Checking Kubernetes context..."
    $context = Initialize-KubernetesContext

    try {
        Invoke-Kubectl -Arguments @('cluster-info', '--request-timeout=10s') | Out-Null
    } catch {
        throw "Kubernetes cluster '${context}' is not reachable. Check Docker Desktop Kubernetes or kubeconfig."
    }
    if ($LASTEXITCODE -ne 0) {
        throw "Kubernetes cluster '${context}' is not reachable. Check Docker Desktop Kubernetes or kubeconfig."
    }
}

function Test-DockerConnection {
    Write-Host "Checking Docker Desktop Linux engine..."
    $context = ""
    try {
        $context = docker context show 2>$null
    } catch {
        $context = ""
    }
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($context)) {
        throw "Docker CLI cannot read the active context. Start Docker Desktop and select the Linux engine before running up.ps1."
    }

    $deadline = (Get-Date).AddMinutes(3)
    $serverOs = ""
    while ((Get-Date) -lt $deadline) {
        try {
            $serverOs = docker info --format '{{.OSType}}' 2>$null
        } catch {
            $serverOs = ""
        }
        if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($serverOs)) {
            break
        }
        Write-Host "Docker engine not ready yet, retrying in 10s..."
        Start-Sleep -Seconds 10
    }

    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($serverOs)) {
        throw "Docker Desktop Linux engine is not reachable for context '${context}'. Start/restart Docker Desktop and wait until 'docker version' shows a Server section."
    }
    if ($serverOs.Trim() -ne "linux") {
        throw "Docker context '${context}' is connected to a '${serverOs}' engine, but this project builds Linux images. Switch Docker Desktop to Linux containers."
    }
}

function Initialize-SecretFiles {
    Write-Host "Preparing Kubernetes secrets from examples..."
    $secretPairs = @(
        @{
            Target = "k8s/airflow/egisz-connections.json"
            Example = "k8s/airflow/egisz-connections.example.json"
        },
        @{
            Target = "k8s/metabase/metabase-connections-secret.yaml"
            Example = "k8s/metabase/metabase-connections-secret.example.yaml"
        }
    )
    foreach ($pair in $secretPairs) {
        if (-not (Test-Path $pair.Target)) {
            Copy-Item $pair.Example $pair.Target
            Write-Host "Created $($pair.Target) from example."
        }
    }
}

function Initialize-AirflowInternalMetadataDatabase {
    Write-Host "Ensuring Airflow internal metadata database airflow_db exists..."
    $postgresPodOutput = Invoke-Kubectl -Arguments @(
        'get', 'pods', '-n', $Namespace,
        '-l', 'app.kubernetes.io/name=postgresql,app.kubernetes.io/instance=airflow',
        '--field-selector=status.phase=Running',
        '-o', 'jsonpath={.items[0].metadata.name}'
    )
    $postgresPod = ($postgresPodOutput | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($postgresPod)) {
        Write-Host "Airflow PostgreSQL pod is not running yet; Helm will initialize airflow_db on first startup."
        return
    }

    Invoke-Checked "Ensure airflow_db in internal Airflow PostgreSQL" {
        $exists = kubectl exec -n $Namespace -c postgresql $postgresPod -- env PGPASSWORD=postgres psql -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname='airflow_db'" 2>$null
        if ($LASTEXITCODE -ne 0) {
            throw "Cannot query Airflow PostgreSQL pod '$postgresPod'."
        }
        if ($exists.Trim() -ne '1') {
            kubectl exec -n $Namespace -c postgresql $postgresPod -- env PGPASSWORD=postgres psql -U postgres -c "CREATE DATABASE airflow_db"
            if ($LASTEXITCODE -ne 0) {
                throw "CREATE DATABASE airflow_db failed in pod '$postgresPod'."
            }
        }
        Write-Host "Airflow internal metadata database airflow_db is present."
    }
}

function Initialize-EgiszEltNamespace {
    Write-Host "Ensuring namespace $Namespace exists..."
    # 2>$null + Continue: на свежем кластере namespace отсутствует, и stderr «NotFound» от kubectl
    # под $ErrorActionPreference='Stop' иначе обрывает бутстрап до создания namespace.
    $existing = & { $ErrorActionPreference = 'Continue'; kubectl get namespace $Namespace -o name 2>$null }
    if ([string]::IsNullOrWhiteSpace($existing)) {
        Invoke-Checked "Create namespace $Namespace" {
            kubectl create namespace $Namespace
        }
    } else {
        Write-Host "Namespace $Namespace already exists, skipping."
    }
}

function Test-LoadBalancerEndpoint {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Url,
        [Parameter(Mandatory = $true)]
        [string]$Description,
        [int]$TotalTimeoutSeconds = 180,
        [int]$PollIntervalSeconds = 5
    )

    $uri = [Uri]$Url
    $port = if ($uri.IsDefaultPort) {
        if ($uri.Scheme -eq 'https') { 443 } else { 80 }
    } else {
        $uri.Port
    }

    $deadline = (Get-Date).AddSeconds($TotalTimeoutSeconds)
    $lastError = ""
    while ((Get-Date) -lt $deadline) {
        $ErrorActionPreference = 'Continue'
        try {
            $response = Invoke-WebRequest -Uri $Url -TimeoutSec 5 -UseBasicParsing
            if ($response.StatusCode -eq 200) {
                Write-Host "${Description} is reachable at ${Url} (HTTP 200)."
                return
            }
            $lastError = "HTTP $($response.StatusCode)"
        } catch {
            $lastError = $_.Exception.Message
        }
        Start-Sleep -Seconds $PollIntervalSeconds
    }

    $portForwardConflict = Get-LoadBalancerPortForwardConflict -Port $port
    $listener = (netstat -ano 2>$null | Select-String ":${port}\b" | Out-String)
    if ($portForwardConflict) {
        throw "${Description} is not reachable at ${Url} (last error: ${lastError}).`n" +
            "Port ${port} is occupied by $($portForwardConflict) (kubectl port-forward). " +
            "Either stop the port-forward so Docker Desktop LoadBalancer can bind localhost:${port}, " +
            "or keep port-forward running and ensure it targets the Metabase/Airflow service.`n" +
            "Listeners: ${listener}"
    }

    throw "${Description} is not reachable at ${Url} (last error: ${lastError}).`n" +
        "Docker Desktop LoadBalancer must serve localhost:${port} directly.`n" +
        "If com.docker.backend listens but HTTP times out, restart Docker Desktop (API server may be overloaded).`n" +
        "Listeners: ${listener}"
}

function Get-LoadBalancerPortForwardConflict {
    param(
        [Parameter(Mandatory = $true)]
        [int]$Port
    )

    $ErrorActionPreference = 'Continue'
    $netstat = (netstat -ano 2>$null | Out-String)
    if ($netstat -match "127\.0\.0\.1:${Port}\s+.*LISTENING\s+(\d+)") {
        $listenerPid = $Matches[1]
        $proc = Get-Process -Id $listenerPid -ErrorAction SilentlyContinue
        $name = if ($proc) { $proc.ProcessName } else { "pid=${listenerPid}" }
        if ($name -in @('wslrelay', 'kubectl')) {
            return $name
        }
    }
    return $null
}

function Assert-NoLoadBalancerPortConflicts {
    param(
        [int[]]$Ports = @(8080, 3000)
    )

    $ErrorActionPreference = 'Continue'
    foreach ($port in $Ports) {
        $conflict = Get-LoadBalancerPortForwardConflict -Port $port
        if ($conflict) {
            throw "Port ${port} is occupied by ${conflict} (kubectl port-forward). Stop it so Docker Desktop LoadBalancer can bind localhost:${port}."
        }
    }
}

function Get-AirflowSchedulerPod {
    $podName = (
        kubectl get pods -n $Namespace -l "component=scheduler,release=airflow" `
            --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>$null
    )
    return ($podName | Out-String).Trim()
}

function Invoke-AirflowSchedulerCli {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,
        [switch]$AllowFailure
    )

    $schedulerPod = Get-AirflowSchedulerPod
    if ([string]::IsNullOrWhiteSpace($schedulerPod)) {
        throw "Cannot find running Airflow scheduler pod in namespace '$Namespace'."
    }

    $ErrorActionPreference = 'Continue'
    $output = & kubectl exec -n $Namespace --request-timeout=120s "pod/$schedulerPod" -c scheduler -- airflow @Arguments 2>&1
    if ($LASTEXITCODE -ne 0 -and -not $AllowFailure) {
        throw "airflow $($Arguments -join ' ') failed with exit code $LASTEXITCODE`n$output"
    }
    return ,$output
}

function Initialize-AirflowDwhPool {
    Write-Host "Ensuring Airflow pool ${DwhPoolName} (${DwhPoolSlots} slot)..."
    Invoke-Checked "Ensure Airflow pool ${DwhPoolName}" {
        Invoke-AirflowSchedulerCli -Arguments @(
            'pools', 'set', $DwhPoolName, "$DwhPoolSlots", $DwhPoolDescription
        )
    }
    $poolList = Invoke-AirflowSchedulerCli -Arguments @('pools', 'list')
    if (($poolList | Out-String) -notmatch [regex]::Escape($DwhPoolName)) {
        throw "Airflow pool ${DwhPoolName} was not found after pools set."
    }
    Write-Host "Airflow pool ${DwhPoolName} is ready."
}

function Initialize-AirflowEgiszVariables {
    $varsFile = Join-Path $PSScriptRoot "k8s\airflow\egisz-variables.json"
    if (-not (Test-Path $varsFile)) {
        throw "Airflow variables file not found: ${varsFile}"
    }

    Write-Host "Ensuring default EGISZ Airflow Variables (skip existing)..."
    $defaults = Get-Content $varsFile -Raw | ConvertFrom-Json
    foreach ($prop in $defaults.PSObject.Properties) {
        $name = $prop.Name
        $value = [string]$prop.Value
        Invoke-AirflowSchedulerCli -Arguments @('variables', 'get', $name) -AllowFailure | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Invoke-Checked "Set Airflow variable ${name}" {
                Invoke-AirflowSchedulerCli -Arguments @('variables', 'set', $name, $value)
            }
        }
    }
    Write-Host "EGISZ Airflow Variables are ready (Admin -> Variables in the UI)."
}

function Initialize-AirflowEgiszConnections {
    # Подключения живут в метабазе Airflow (Admin -> Connections), как на внешнем
    # контуре: env-переменные из секрета перекрывали бы их и расходились с UI.
    $connectionsFile = Join-Path $PSScriptRoot "k8s\airflow\egisz-connections.json"
    if (-not (Test-Path $connectionsFile)) {
        throw "Airflow connections file not found: ${connectionsFile}"
    }

    Write-Host "Provisioning Airflow connections (Admin -> Connections)..."
    $connections = Get-Content $connectionsFile -Raw | ConvertFrom-Json
    foreach ($prop in $connections.PSObject.Properties) {
        $connId = $prop.Name
        $connUri = [string]$prop.Value
        if ([string]::IsNullOrWhiteSpace($connUri)) {
            throw "Connection URI for ${connId} is empty in ${connectionsFile}."
        }
        # delete + add вместо `connections import --overwrite`: работает на всех 2.x
        # и делает провижининг идемпотентным при смене URI.
        Invoke-AirflowSchedulerCli -Arguments @('connections', 'delete', $connId) -AllowFailure | Out-Null
        Invoke-Checked "Add Airflow connection ${connId}" {
            Invoke-AirflowSchedulerCli -Arguments @('connections', 'add', $connId, '--conn-uri', $connUri)
        }
    }

    foreach ($prop in $connections.PSObject.Properties) {
        $stored = Invoke-AirflowSchedulerCli -Arguments @('connections', 'get', $prop.Name) -AllowFailure
        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace(($stored | Out-String))) {
            throw "Airflow connection $($prop.Name) is missing after provisioning. Check ${connectionsFile}."
        }
    }

    Write-Host "Airflow connections dwh_egisz_pg and proxy_egisz_fb are stored in the Airflow metadata database."
}

function Initialize-HelmAirflowRepo {
    if (-not (Get-Command helm -ErrorAction SilentlyContinue)) {
        throw "helm не найден в PATH. Установите Helm: https://helm.sh/docs/intro/install/"
    }

    $repoName = "apache-airflow"
    $repoUrl = "https://airflow.apache.org/charts"
    $existingRepos = @()
    try {
        $existingRepos = @(
            helm repo list -o json 2>$null | ConvertFrom-Json |
                ForEach-Object { $_.name }
        )
    } catch {
        $existingRepos = @()
    }

    if ($existingRepos -notcontains $repoName) {
        Write-Host "Adding Helm repo $repoName..."
        Invoke-Checked "Add Helm repo $repoName" {
            helm repo add $repoName $repoUrl
        }
    }

    Write-Host "Updating Helm repo index..."
    $ErrorActionPreference = 'Continue'
    helm repo update $repoName 2>&1 | ForEach-Object { "$_" }
    $updateFailed = $LASTEXITCODE -ne 0
    $ErrorActionPreference = 'Stop'

    if (-not $updateFailed) {
        return
    }

    # Индекс чарта кешируется локально, а версия закреплена ($AirflowChartVersion),
    # поэтому обрыв связи с airflow.apache.org не должен валить накат: он фатален
    # только если нужной версии нет и в кеше.
    Write-Host "Helm repo update failed; checking the local chart cache for ${repoName}/airflow ${AirflowChartVersion}..."
    $ErrorActionPreference = 'Continue'
    $cached = helm search repo "${repoName}/airflow" --version $AirflowChartVersion -o json 2>$null
    $ErrorActionPreference = 'Stop'
    if ([string]::IsNullOrWhiteSpace($cached) -or ($cached | Out-String).Trim() -eq '[]') {
        throw "Cannot refresh the Helm repo index and chart ${repoName}/airflow ${AirflowChartVersion} is not cached locally. Check network access to ${repoUrl}."
    }
    Write-Host "Using cached chart index for ${repoName}/airflow ${AirflowChartVersion}."
}

function Ensure-AirflowStatefulSetReplicas {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [int]$Replicas = 1
    )

    $currentOutput = Invoke-Kubectl -Arguments @(
        'get', 'statefulset', $Name, '-n', $Namespace,
        '-o', 'jsonpath={.spec.replicas}'
    )
    if ($LASTEXITCODE -ne 0) {
        return
    }

    $current = 0
    [void][int]::TryParse(($currentOutput | Out-String).Trim(), [ref]$current)
    if ($current -eq $Replicas) {
        return
    }

    Write-Host "Scaling ${Name} from ${current} to ${Replicas}..."
    Invoke-Kubectl -Arguments @(
        'scale', "statefulset/${Name}", '-n', $Namespace, "--replicas=$Replicas"
    ) | Out-Null
}

function Restore-AirflowStatefulSetsAfterStop {
    # Stop-Airflow scales StatefulSets to 0 via kubectl; Helm upgrade does not
    # always restore .spec.replicas, leaving Redis/worker stuck at 0 until scaled up.
    Ensure-AirflowStatefulSetReplicas -Name airflow-postgresql -Replicas 1
    Ensure-AirflowStatefulSetReplicas -Name airflow-redis -Replicas 1
    Ensure-AirflowStatefulSetReplicas -Name airflow-worker -Replicas 1
    Ensure-AirflowStatefulSetReplicas -Name airflow-triggerer -Replicas 1
}

function Clear-AirflowStuckTerminatingPods {
    param(
        [int]$GraceSeconds = 30
    )

    Clear-StuckTerminatingPods -LabelSelector "app.kubernetes.io/name=postgresql,app.kubernetes.io/instance=airflow" -GraceSeconds $GraceSeconds
    Clear-StuckTerminatingPods -LabelSelector "component=redis,release=airflow" -GraceSeconds $GraceSeconds
    Clear-StuckTerminatingPods -LabelSelector "component=worker,release=airflow" -GraceSeconds $GraceSeconds
    Clear-StuckTerminatingPods -LabelSelector "component=triggerer,release=airflow" -GraceSeconds $GraceSeconds
}

function Get-DagSourcesHash {
    # DAG-файлы самодостаточны: тег образа зависит только от них.
    $hash = python -c @"
import hashlib
from pathlib import Path
h = hashlib.sha256()
for path in sorted(Path('airflow/dags').glob('*.py')):
    h.update(path.read_bytes())
print(h.hexdigest())
"@
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($hash)) {
        throw "Cannot compute DAG sources hash."
    }
    return $hash.Trim()
}

function Install-Airflow {
    Initialize-SecretFiles
    Test-KubernetesConnection
    Test-DockerConnection
    Initialize-EgiszEltNamespace

    # Подключения переехали в метабазу Airflow (Initialize-AirflowEgiszConnections);
    # оставшийся секрет подмешивал бы AIRFLOW_CONN_* и перекрывал записи из UI.
    Invoke-Kubectl -Arguments @(
        'delete', 'secret', 'airflow-connections', '-n', $Namespace, '--ignore-not-found'
    ) | Out-Null

    $dagHash = Get-DagSourcesHash
    $airflowTag = $dagHash.Substring(0, 12)
    $script:AirflowImage = "egisz-airflow-worker:${airflowTag}"

    Write-Host "Building Airflow image ${AirflowImage} (DAG sources ${dagHash})..."
    Invoke-Checked "Build Airflow image" {
        # docker buildx прогресс летит в stderr; под $ErrorActionPreference='Stop'
        # это валит скрипт ещё до проверки exit-кода. Сливаем потоки и пускаем
        # через ForEach-Object, чтобы каждая строка стала обычным stdout.
        docker build -t $AirflowImage -t egisz-airflow-worker:latest -f airflow/Dockerfile . 2>&1 | ForEach-Object {
            if ($_ -is [System.Management.Automation.ErrorRecord]) { $_.Exception.Message } else { "$_" }
        }
    }

    Initialize-HelmAirflowRepo

    Write-Host "Installing Airflow..."
    Invoke-Checked "Install Airflow Helm release" {
        # Версия чарта закреплена: 1.22.0 несёт Airflow 3.2.2 (api-server + dag-processor),
        # на который рассчитаны values.yaml и ожидания up.ps1.
        helm upgrade --install airflow apache-airflow/airflow -n $Namespace `
            --version $AirflowChartVersion `
            -f k8s/airflow/values.yaml `
            --timeout 15m `
            --set images.airflow.tag=$airflowTag `
            --set workers.replicas=1 `
            --set workers.celery.replicas=1
    }

    Restore-AirflowStatefulSetsAfterStop
    Clear-AirflowStuckTerminatingPods

    Wait-ComponentPodsReady -Description "Airflow PostgreSQL" `
        -LabelSelector "app.kubernetes.io/name=postgresql,app.kubernetes.io/instance=airflow" `
        -TotalTimeoutSeconds 300
    Initialize-AirflowInternalMetadataDatabase

    Wait-ComponentPodsReady -Description "Airflow Redis" `
        -LabelSelector "component=redis,release=airflow" `
        -TotalTimeoutSeconds 300
    Wait-ComponentPodsReady -Description "Airflow scheduler" `
        -LabelSelector "component=scheduler,release=airflow" `
        -TotalTimeoutSeconds 300
    Clear-AirflowStuckTerminatingPods
    Wait-ComponentPodsReady -Description "Airflow worker" `
        -LabelSelector "component=worker,release=airflow" `
        -ExpectedPods 1 `
        -TotalTimeoutSeconds 600
    Wait-ComponentPodsReady -Description "Airflow triggerer" `
        -LabelSelector "component=triggerer,release=airflow" `
        -TotalTimeoutSeconds 300
    # Airflow 3: DAG-и парсит выделенный dag-processor, UI и REST отдаёт api-server
    # (компонента webserver в чарте больше нет).
    Wait-ComponentPodsReady -Description "Airflow DAG processor" `
        -LabelSelector "component=dag-processor,release=airflow" `
        -TotalTimeoutSeconds 300
    Wait-ComponentPodsReady -Description "Airflow API server" `
        -LabelSelector "component=api-server,release=airflow" `
        -TotalTimeoutSeconds 600

    Write-Host "Waiting for Celery worker to connect to the Redis broker..."
    Invoke-Checked "Wait for Celery worker broker connection" {
        @"
import json
import subprocess
import time

deadline = time.time() + 300
namespace = "$Namespace"
pod = "airflow-worker-0"
markers = ("Connected to redis", " ready.")

while time.time() < deadline:
    status = subprocess.run(
        ["kubectl", "get", "pod", pod, "-n", namespace, "-o", "json"],
        check=False,
        capture_output=True,
        text=True,
    )
    if status.returncode != 0:
        time.sleep(10)
        continue

    pod_obj = json.loads(status.stdout)
    if pod_obj.get("metadata", {}).get("deletionTimestamp"):
        time.sleep(10)
        continue
    if pod_obj.get("status", {}).get("phase") != "Running":
        time.sleep(10)
        continue

    logs = subprocess.run(
        ["kubectl", "logs", pod, "-n", namespace, "-c", "worker", "--tail=200"],
        check=False,
        capture_output=True,
        text=True,
    )
    if logs.returncode == 0 and all(marker in logs.stdout for marker in markers):
        print("Celery worker is connected to Redis and ready.")
        raise SystemExit(0)
    time.sleep(10)

raise SystemExit("Timed out waiting for Celery worker readiness marker in logs.")
"@ | py -
    }

    Initialize-AirflowEgiszConnections
    Initialize-AirflowDwhPool
    Initialize-AirflowEgiszVariables

    Test-LoadBalancerEndpoint -Url 'http://localhost:8080/api/v2/monitor/health' -Description 'Airflow API server LoadBalancer'

    Write-Host "Airflow is ready (pool ${DwhPoolName}, Variables in UI; DAGs paused at creation). Run 'psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql' if the DWH schema changed."
}

function Sync-MetabaseDashboardArtifacts {
    Write-Host "Applying dashboard JSON from repo..."
    Invoke-Checked "apply_dashboard_plan.py" {
        python scripts/apply_dashboard_plan.py
    }
    Invoke-Checked "layout_operational_tab.py" {
        python scripts/layout_operational_tab.py
    }
}

function Get-DashboardsManifestHash {
    $hash = python -c @"
import hashlib
from pathlib import Path
h = hashlib.sha256()
for path in sorted(Path('metabase_dashboards').glob('*.json')):
    h.update(path.read_bytes())
for path in sorted(Path('metabase').glob('*.sh')):
    h.update(path.read_bytes())
print(h.hexdigest())
"@
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($hash)) {
        throw "Cannot compute metabase_dashboards manifest hash."
    }
    return $hash.Trim()
}

function Get-MetabaseDeployState {
    if (-not (Test-Path $MetabaseDeployStateFile)) {
        return $null
    }
    return (Get-Content $MetabaseDeployStateFile -Raw).Trim()
}

function Set-MetabaseDeployState {
    param(
        [string]$Hash
    )

    $dir = Split-Path $MetabaseDeployStateFile -Parent
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
    Set-Content -Path $MetabaseDeployStateFile -Value $Hash -NoNewline
}

function Test-MetabaseManifestUnchanged {
    $current = Get-DashboardsManifestHash
    $deployed = Get-MetabaseDeployState
    return (-not [string]::IsNullOrWhiteSpace($deployed)) -and ($deployed -eq $current)
}

function Test-DockerImageExists {
    param(
        [string]$Image
    )

    $ErrorActionPreference = 'Continue'
    docker image inspect $Image 2>$null | Out-Null
    return $LASTEXITCODE -eq 0
}

function Get-MetabaseDeploymentImage {
    $ErrorActionPreference = 'Continue'
    $image = kubectl get deployment metabase -n $Namespace -o jsonpath='{.spec.template.spec.containers[0].image}' 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($image)) {
        return $null
    }
    return $image.Trim()
}

function Invoke-MetabaseDashboardProvision {
    param(
        [string]$Namespace,
        [string]$ForceProvision = "auto",
        [bool]$SkipImportIfPresent = $false
    )

    $podName = kubectl get pods -n $Namespace -l app.kubernetes.io/name=metabase -o jsonpath='{.items[0].metadata.name}' 2>$null
    if (-not [string]::IsNullOrWhiteSpace($podName)) {
        Write-Host "Syncing dashboard JSON into Metabase pod ${podName}..."
        $dashboardsDir = Join-Path $PSScriptRoot "metabase_dashboards"
        Invoke-Checked "Copy metabase_dashboards into pod" {
            Push-Location $PSScriptRoot
            try {
                foreach ($file in Get-ChildItem -Path $dashboardsDir -Filter "*.json" -File) {
                    $localPath = "metabase_dashboards/$($file.Name)"
                    $remotePath = "${Namespace}/${podName}:/app/metabase_dashboards/$($file.Name)"
                    kubectl cp $localPath $remotePath 2>&1 | ForEach-Object { Write-Host $_ }
                    if ($LASTEXITCODE -ne 0) {
                        throw "kubectl cp failed for $($file.Name)"
                    }
                }
            } finally {
                Pop-Location
            }
        }
    }

    if ($ForceProvision -eq "auto") {
        Write-Host "Running setup-dashboards.sh (manifest-aware import)..."
    } else {
        Write-Host "Running setup-dashboards.sh (forced import)..."
    }
    $skipFlag = if ($SkipImportIfPresent) { "true" } else { "false" }
    $execOutput = Invoke-Kubectl -MaxAttempts 8 -Arguments @(
        'exec', '-n', $Namespace, 'deploy/metabase', '--',
        'env',
        "METABASE_FORCE_PROVISION=$ForceProvision",
        "METABASE_SKIP_IMPORT_IF_PRESENT=$skipFlag",
        '/bin/bash', '/app/setup-dashboards.sh'
    )
    if ($LASTEXITCODE -ne 0) {
        $message = ($execOutput | Out-String).Trim()
        if ($message) {
            Write-Host $message
        }
        throw "Metabase dashboard provisioning failed with exit code ${LASTEXITCODE}"
    }
}

function Wait-MetabaseApi {
    param(
        [string]$Namespace
    )

    $deadline = (Get-Date).AddMinutes(5)
    while ((Get-Date) -lt $deadline) {
        $ready = Get-ReadyPodCount -LabelSelector 'app.kubernetes.io/name=metabase'
        if ($ready -ge 1) {
            break
        }
        Start-Sleep -Seconds 3
    }

    Test-LoadBalancerEndpoint -Url 'http://localhost:3000/api/health' -Description 'Metabase API'
}

function Invoke-MetabaseProvisioning {
    param(
        [string]$Namespace
    )

    $manifestHash = Get-DashboardsManifestHash
    $manifestUnchanged = Test-MetabaseManifestUnchanged
    $forceProvision = if ($manifestUnchanged) { "auto" } else { "always" }
    if ($manifestUnchanged) {
        Write-Host "Dashboard manifest unchanged; using fast Metabase provisioning path."
    }

    Write-Host "Provisioning Metabase dashboards and models from repo..."
    Invoke-Checked "Wait for Metabase API" {
        Wait-MetabaseApi -Namespace $Namespace
    }
    $skipImport = if ($manifestUnchanged) { "true" } else { "false" }
    Invoke-Checked "Metabase dashboard provisioning" {
        Invoke-MetabaseDashboardProvision -Namespace $Namespace -ForceProvision $forceProvision -SkipImportIfPresent:$manifestUnchanged
    }

    $verifyCardQueries = -not $manifestUnchanged
    try {
        Test-MetabaseIntegrationDashboard -VerifyCardQueries:$verifyCardQueries
    } catch {
        if (-not $manifestUnchanged) {
            throw
        }
        Write-Host "Fast path verification failed; re-running forced dashboard import..."
        Invoke-Checked "Metabase dashboard reprovisioning" {
            Invoke-MetabaseDashboardProvision -Namespace $Namespace -ForceProvision "always"
        }
        Test-MetabaseIntegrationDashboard -VerifyCardQueries:$true
        $manifestUnchanged = $false
    }

    if (-not $manifestUnchanged) {
        Set-MetabaseDeployState $manifestHash
    }
}

function Ensure-MetabaseRunning {
    if (-not (Test-NamespaceExists)) {
        throw "Namespace ${Namespace} does not exist. Run '.\up.ps1 -Action Metabase' first."
    }

    Write-Host "Ensuring Metabase components are running..."
    Invoke-Checked "Scale Metabase PostgreSQL to 1" {
        Invoke-Kubectl -Arguments @('scale', '-n', $Namespace, 'statefulset/metabase-postgres', '--replicas=1') | Out-Null
    }
    Invoke-Checked "Scale Metabase deployment to 1" {
        Invoke-Kubectl -Arguments @('scale', '-n', $Namespace, 'deployment/metabase', '--replicas=1') | Out-Null
    }
    Invoke-Checked "Wait for Metabase PostgreSQL" {
        Invoke-Kubectl -Arguments @('rollout', 'status', '-n', $Namespace, 'statefulset/metabase-postgres', '--timeout=120s') | Out-Null
    }
    Invoke-Checked "Wait for Metabase deployment" {
        Invoke-Kubectl -Arguments @('rollout', 'status', '-n', $Namespace, 'deployment/metabase', '--timeout=300s') | Out-Null
    }
}

function Invoke-MetabaseProvisioningOnly {
    Initialize-SecretFiles
    Test-KubernetesConnection
    Ensure-MetabaseRunning
    Sync-MetabaseDashboardArtifacts
    Invoke-MetabaseProvisioning -Namespace $Namespace
}

function Test-MetabaseIntegrationDashboard {
    param(
        [bool]$VerifyCardQueries = $true
    )

    Write-Host "Verifying integration dashboard matches repo JSON..."
    Invoke-Checked "Verify Metabase integration dashboard" {
        python scripts/verify_metabase_integration.py
    }
    if ($VerifyCardQueries) {
        Write-Host "Verifying all dashboard card queries..."
        Invoke-Checked "Verify Metabase dashboard cards" {
            python scripts/verify_metabase_cards.py
        }
    } else {
        Write-Host "Skipping card query verification (dashboard manifest unchanged)."
    }
}

function Install-Metabase {
    Initialize-SecretFiles
    Test-KubernetesConnection
    Test-DockerConnection
    Initialize-EgiszEltNamespace
    Sync-MetabaseDashboardArtifacts
    $dashboardsHash = Get-DashboardsManifestHash
    $metabaseTag = $dashboardsHash.Substring(0, 12)
    $script:MetabaseImage = "egisz-metabase:${metabaseTag}"

    Write-Host "Applying Metabase connection secrets..."
    Invoke-Checked "Apply Metabase connection secrets" {
        kubectl apply -n $Namespace -f k8s/metabase/metabase-connections-secret.yaml
    }

    Write-Host "Building Metabase image ${MetabaseImage} (manifest ${dashboardsHash})..."
    if (Test-DockerImageExists $MetabaseImage) {
        Write-Host "Metabase image ${MetabaseImage} already exists, skipping build."
    } else {
        Invoke-Checked "Build Metabase image" {
            docker build --build-arg "DASHBOARDS_CACHE_BUST=$dashboardsHash" -t $MetabaseImage -t egisz-metabase:latest -f metabase/Dockerfile . 2>&1 | ForEach-Object {
                if ($_ -is [System.Management.Automation.ErrorRecord]) { $_.Exception.Message } else { "$_" }
            }
        }
    }

    Write-Host "Starting Metabase PostgreSQL and Metabase..."
    Invoke-Checked "Apply Metabase deployment" {
        kubectl apply -n $Namespace -f k8s/metabase/metabase.yaml
    }

    $currentImage = Get-MetabaseDeploymentImage
    if ($currentImage -eq $MetabaseImage) {
        Write-Host "Metabase deployment already uses ${MetabaseImage}; skipping rollout restart."
    } else {
        Write-Host "Pointing Metabase deployment to image ${MetabaseImage}..."
        Invoke-Checked "Set Metabase deployment image" {
            kubectl set image -n $Namespace deployment/metabase metabase=$MetabaseImage
            Invoke-Kubectl -Arguments @('rollout', 'status', '-n', $Namespace, 'deployment/metabase', '--timeout=300s') | Out-Null
        }
    }

    Write-Host "Restoring Metabase replicas after any previous scale-to-zero stop..."
    Invoke-Checked "Scale Metabase PostgreSQL to 1" {
        Invoke-Kubectl -Arguments @('scale', '-n', $Namespace, 'statefulset/metabase-postgres', '--replicas=1') | Out-Null
    }
    Invoke-Checked "Scale Metabase deployment to 1" {
        Invoke-Kubectl -Arguments @('scale', '-n', $Namespace, 'deployment/metabase', '--replicas=1') | Out-Null
    }

    Write-Host "Waiting for Metabase PostgreSQL to be ready..."
    Invoke-Checked "Wait for Metabase PostgreSQL" {
        Invoke-Kubectl -Arguments @('rollout', 'status', '-n', $Namespace, 'statefulset/metabase-postgres', '--timeout=120s') | Out-Null
    }

    Write-Host "Waiting for Metabase pod to be ready..."
    Invoke-Checked "Wait for Metabase deployment" {
        Invoke-Kubectl -Arguments @('rollout', 'status', '-n', $Namespace, 'deployment/metabase', '--timeout=300s') | Out-Null
    }

    Invoke-MetabaseProvisioning -Namespace $Namespace
}

function Test-NamespaceExists {
    $ErrorActionPreference = 'Continue'
    $existing = kubectl get namespace $Namespace -o name 2>$null
    if ($LASTEXITCODE -ne 0) {
        return $false
    }
    return -not [string]::IsNullOrWhiteSpace(($existing | Out-String).Trim())
}

function Invoke-ScaleIfExists {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Description,
        [Parameter(Mandatory = $true)]
        [string]$Kind,
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [int]$Replicas = 0
    )

    $ErrorActionPreference = 'Continue'
    $exists = kubectl get "${Kind}/${Name}" -n $Namespace -o name 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace(($exists | Out-String).Trim())) {
        Write-Host "${Kind}/${Name} not found in namespace ${Namespace}, skipping."
        return
    }

    Invoke-Checked $Description {
        kubectl scale -n $Namespace "${Kind}/${Name}" --replicas=$Replicas
    }
}

function Wait-ForNamespaceDeleted {
    param(
        [int]$TotalTimeoutSeconds = 600,
        [int]$PollIntervalSeconds = 10,
        [int]$StuckPodGraceSeconds = 60
    )

    Write-Host "Waiting for namespace $Namespace to be deleted..."
    $deadline = (Get-Date).AddSeconds($TotalTimeoutSeconds)

    while ((Get-Date) -lt $deadline) {
        $ErrorActionPreference = 'Continue'
        kubectl get namespace $Namespace -o name 2>$null | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Namespace $Namespace deleted."
            return $true
        }

        $phase = (kubectl get namespace $Namespace -o jsonpath='{.status.phase}' 2>$null | Out-String).Trim()
        Clear-StuckTerminatingPods -GraceSeconds $StuckPodGraceSeconds

        $remaining = [Math]::Max(0, [int]($deadline - (Get-Date)).TotalSeconds)
        if ($phase -eq 'Terminating') {
            Write-Host "Namespace $Namespace is Terminating (${remaining}s left)..."
        } else {
            Write-Host "Namespace $Namespace phase=${phase} (${remaining}s left)..."
        }
        Start-Sleep -Seconds $PollIntervalSeconds
    }

    $ErrorActionPreference = 'Continue'
    kubectl get namespace $Namespace -o name 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Namespace $Namespace deleted."
        return $true
    }

    $phase = (kubectl get namespace $Namespace -o jsonpath='{.status.phase}' 2>$null | Out-String).Trim()
    if ($phase -eq 'Terminating') {
        Write-Host "Warning: namespace $Namespace is still Terminating after ${TotalTimeoutSeconds}s; deletion continues in the background."
        return $true
    }

    throw "Namespace $Namespace deletion did not complete after ${TotalTimeoutSeconds}s (phase=${phase})."
}

function Stop-All {
    Test-KubernetesConnection

    if (-not (Test-NamespaceExists)) {
        Write-Host "Namespace $Namespace does not exist; nothing to stop."
        $global:LASTEXITCODE = 0
        return $false
    }

    Write-Host "Scaling down workloads before teardown..."
    Invoke-ScaleIfExists -Description "Scale Metabase deployment to 0" -Kind deployment -Name metabase
    Invoke-ScaleIfExists -Description "Scale Metabase PostgreSQL to 0" -Kind statefulset -Name metabase-postgres
    Invoke-ScaleIfExists -Description "Scale Airflow API server to 0" -Kind deployment -Name airflow-api-server
    Invoke-ScaleIfExists -Description "Scale Airflow DAG processor to 0" -Kind deployment -Name airflow-dag-processor
    Invoke-ScaleIfExists -Description "Scale Airflow scheduler to 0" -Kind deployment -Name airflow-scheduler
    Invoke-ScaleIfExists -Description "Scale Airflow statsd to 0" -Kind deployment -Name airflow-statsd
    Invoke-ScaleIfExists -Description "Scale Airflow worker to 0" -Kind statefulset -Name airflow-worker
    Invoke-ScaleIfExists -Description "Scale Airflow triggerer to 0" -Kind statefulset -Name airflow-triggerer
    Invoke-ScaleIfExists -Description "Scale Airflow Redis to 0" -Kind statefulset -Name airflow-redis
    Invoke-ScaleIfExists -Description "Scale Airflow PostgreSQL to 0" -Kind statefulset -Name airflow-postgresql
    Clear-StuckTerminatingPods -GraceSeconds 60

    if (Get-Command helm -ErrorAction SilentlyContinue) {
        $helmReleases = @()
        try {
            $helmReleases = @(
                helm list -n $Namespace -q 2>$null |
                    ForEach-Object { $_.Trim() } |
                    Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
            )
        } catch {
            $helmReleases = @()
        }

        if ($helmReleases -contains 'airflow') {
            Write-Host "Uninstalling Helm release airflow..."
            Invoke-Checked "Uninstall Airflow Helm release" {
                helm uninstall airflow -n $Namespace --wait --timeout 10m
            }
            Clear-StuckTerminatingPods -GraceSeconds 60
        }
    }

    Write-Host "Deleting namespace $Namespace..."
    Invoke-Checked "Delete namespace $Namespace" {
        kubectl delete namespace $Namespace --wait=false
    }
    Wait-ForNamespaceDeleted | Out-Null
    return $true
}

function Stop-Airflow {
    Test-KubernetesConnection

    if (-not (Test-NamespaceExists)) {
        Write-Host "Namespace $Namespace does not exist; nothing to stop."
        return
    }

    Write-Host "Scaling down Airflow components without deleting releases or PVCs..."
    Invoke-ScaleIfExists -Description "Scale Airflow API server to 0" -Kind deployment -Name airflow-api-server
    Invoke-ScaleIfExists -Description "Scale Airflow DAG processor to 0" -Kind deployment -Name airflow-dag-processor
    Invoke-ScaleIfExists -Description "Scale Airflow scheduler to 0" -Kind deployment -Name airflow-scheduler
    Invoke-ScaleIfExists -Description "Scale Airflow statsd to 0" -Kind deployment -Name airflow-statsd
    Invoke-ScaleIfExists -Description "Scale Airflow worker to 0" -Kind statefulset -Name airflow-worker
    Invoke-ScaleIfExists -Description "Scale Airflow triggerer to 0" -Kind statefulset -Name airflow-triggerer
    Invoke-ScaleIfExists -Description "Scale Airflow Redis to 0" -Kind statefulset -Name airflow-redis
    Invoke-ScaleIfExists -Description "Scale Airflow PostgreSQL to 0" -Kind statefulset -Name airflow-postgresql
}

function Stop-Metabase {
    Test-KubernetesConnection

    if (-not (Test-NamespaceExists)) {
        Write-Host "Namespace $Namespace does not exist; nothing to stop."
        return
    }

    Write-Host "Scaling down Metabase components without deleting the metabase_app database PVC..."
    Invoke-ScaleIfExists -Description "Scale Metabase deployment to 0" -Kind deployment -Name metabase
    Invoke-ScaleIfExists -Description "Scale Metabase PostgreSQL to 0" -Kind statefulset -Name metabase-postgres
}

if ($Action -in @("Start", "Airflow")) {
    Install-Airflow
}

if ($Action -in @("Start", "Metabase")) {
    Install-Metabase
}

if ($Action -eq "Metabase-Provisioning") {
    Invoke-MetabaseProvisioningOnly
}

$namespaceDeleted = $false

if ($Action -eq "Stop") {
    $namespaceDeleted = Stop-All
} elseif ($Action -eq "Stop-Airflow") {
    Stop-Airflow
} elseif ($Action -eq "Stop-Metabase") {
    Stop-Metabase
}

if ($Action -eq "Start") {
    Write-Host "Done. Airflow: http://localhost:8080, Metabase: http://localhost:3000"
} elseif ($Action -eq "Metabase-Provisioning") {
    Write-Host "Done. Metabase dashboards and models reprovisioned."
} elseif ($Action -in @("Airflow", "Metabase")) {
    Write-Host "Done. Selected Kubernetes components are running."
} elseif ($Action -eq "Stop") {
    if ($namespaceDeleted) {
        Write-Host "Done. Namespace $Namespace was deleted."
    } else {
        Write-Host "Done. Namespace $Namespace was not found."
    }
} else {
    Write-Host "Done. Selected Kubernetes components were scaled down to zero; PVC-backed data was preserved."
}

$global:LASTEXITCODE = 0
