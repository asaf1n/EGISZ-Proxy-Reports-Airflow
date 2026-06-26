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
$MetabaseImage = "egisz-metabase:latest"

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

    return $Message -match 'TLS handshake timeout|connection refused|i/o timeout|temporary failure|the server is currently unable|dial tcp|EOF|client connection lost|unable to decode an event'
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
        [Parameter(Mandatory = $true)]
        [string]$LabelSelector,
        [int]$GraceSeconds = 120
    )

    $jsonOutput = Invoke-Kubectl -Arguments @(
        'get', 'pods', '-n', $Namespace, '-l', $LabelSelector, '-o', 'json'
    )
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
            Target = "k8s/airflow/airflow-connections-secret.yaml"
            Example = "k8s/airflow/airflow-connections-secret.example.yaml"
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
    $existing = kubectl get namespace $Namespace -o name
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
        [string[]]$Arguments
    )

    $schedulerPod = Get-AirflowSchedulerPod
    if ([string]::IsNullOrWhiteSpace($schedulerPod)) {
        throw "Cannot find running Airflow scheduler pod in namespace '$Namespace'."
    }

    $ErrorActionPreference = 'Continue'
    $output = & kubectl exec -n $Namespace --request-timeout=120s "pod/$schedulerPod" -c scheduler -- airflow @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
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

function Test-AirflowConnectionsFromSecret {
    Write-Host "Verifying Airflow connections from Kubernetes secret..."
    $connectionEnvVars = @(
        "AIRFLOW_CONN_DWH_EGISZ_PG",
        "AIRFLOW_CONN_PROXY_EGISZ_FB"
    )
    $podsToCheck = @(
        @{
            Label = "scheduler"
            Selector = "component=scheduler,release=airflow"
            Container = "scheduler"
        },
        @{
            Label = "worker"
            Selector = "component=worker,release=airflow"
            Container = "worker"
        }
    )

    foreach ($podSpec in $podsToCheck) {
        $podName = (
            kubectl get pods -n $Namespace -l $podSpec.Selector `
                --field-selector=status.phase=Running -o name 2>$null |
            Select-Object -First 1
        )
        $podName = ($podName -replace '^pod/', '').Trim()
        if ([string]::IsNullOrWhiteSpace($podName)) {
            throw "Cannot find running Airflow $($podSpec.Label) pod in namespace '$Namespace' to verify connections."
        }

        foreach ($envVar in $connectionEnvVars) {
            $value = kubectl exec -n $Namespace --request-timeout=30s pod/$podName -c $podSpec.Container -- printenv $envVar 2>$null
            if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($value)) {
                throw "Airflow connection env $envVar is not set in $($podSpec.Label) pod '$podName'. Check k8s/airflow/airflow-connections-secret.yaml and Helm values extraEnvFrom."
            }
        }
    }

    Write-Host "Airflow connections dwh_egisz_pg and proxy_egisz_fb are available via airflow-connections secret."
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
    Invoke-Checked "Update Helm repos" {
        helm repo update
    }
}

function Sync-AirflowWorkerReplicas {
    param(
        [int]$Replicas = 1
    )

    $currentOutput = Invoke-Kubectl -Arguments @(
        'get', 'statefulset', 'airflow-worker', '-n', $Namespace,
        '-o', 'jsonpath={.spec.replicas}'
    )
    if ($LASTEXITCODE -ne 0) {
        return
    }

    $current = 0
    [void][int]::TryParse(($currentOutput | Out-String).Trim(), [ref]$current)
    if ($current -le $Replicas) {
        return
    }

    Write-Host "Scaling airflow-worker from ${current} to ${Replicas}..."
    Invoke-Kubectl -Arguments @(
        'scale', 'statefulset/airflow-worker', '-n', $Namespace, "--replicas=$Replicas"
    ) | Out-Null
}

function Get-EltPackageHash {
    $hash = python -c @"
import hashlib
from pathlib import Path
h = hashlib.sha256()
for path in sorted(Path('src').rglob('*.py')):
    h.update(path.read_bytes())
for path in sorted(Path('airflow/dags').glob('*.py')):
    h.update(path.read_bytes())
print(h.hexdigest())
"@
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($hash)) {
        throw "Cannot compute egisz_elt package hash."
    }
    return $hash.Trim()
}

function Install-Airflow {
    Initialize-SecretFiles
    Test-KubernetesConnection
    Test-DockerConnection
    Initialize-EgiszEltNamespace

    Write-Host "Applying Airflow connection secrets..."
    Invoke-Checked "Apply Airflow connection secrets" {
        kubectl apply -n $Namespace -f k8s/airflow/airflow-connections-secret.yaml
    }

    $eltHash = Get-EltPackageHash
    $airflowTag = $eltHash.Substring(0, 12)
    $script:AirflowImage = "egisz-airflow-worker:${airflowTag}"

    Write-Host "Building Airflow image ${AirflowImage} (package ${eltHash})..."
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
        helm upgrade --install airflow apache-airflow/airflow -n $Namespace `
            -f k8s/airflow/values.yaml `
            --timeout 15m `
            --set images.airflow.tag=$airflowTag `
            --set workers.replicas=1 `
            --set workers.celery.replicas=1
    }

    Sync-AirflowWorkerReplicas -Replicas 1

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
    Clear-StuckTerminatingPods -LabelSelector "component=worker,release=airflow"
    Wait-ComponentPodsReady -Description "Airflow worker" `
        -LabelSelector "component=worker,release=airflow" `
        -ExpectedPods 1 `
        -TotalTimeoutSeconds 300
    Wait-ComponentPodsReady -Description "Airflow triggerer" `
        -LabelSelector "component=triggerer,release=airflow" `
        -TotalTimeoutSeconds 300
    Wait-ComponentPodsReady -Description "Airflow webserver" `
        -LabelSelector "component=webserver,release=airflow" `
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

    Test-AirflowConnectionsFromSecret
    Initialize-AirflowDwhPool

    Test-LoadBalancerEndpoint -Url 'http://localhost:8080/health' -Description 'Airflow webserver LoadBalancer'

    Write-Host "Airflow is ready (pool ${DwhPoolName}; DAGs paused at creation). Run 'psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql' if the DWH schema changed."
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

function Invoke-MetabaseDashboardProvision {
    param(
        [string]$Namespace
    )

    Write-Host "Running setup-dashboards.sh (forced import)..."
    $execOutput = Invoke-Kubectl -MaxAttempts 8 -Arguments @(
        'exec', '-n', $Namespace, 'deploy/metabase', '--',
        'env', 'METABASE_FORCE_PROVISION=always', '/bin/bash', '/app/setup-dashboards.sh'
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
        $ready = Get-ReadyPodCount -LabelSelector 'app=metabase'
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

    Write-Host "Provisioning Metabase dashboards and models from repo..."
    Invoke-Checked "Wait for Metabase API" {
        Wait-MetabaseApi -Namespace $Namespace
    }
    Invoke-Checked "Metabase dashboard provisioning" {
        Invoke-MetabaseDashboardProvision -Namespace $Namespace
    }
    Test-MetabaseIntegrationDashboard
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
    Write-Host "Verifying integration dashboard matches repo JSON..."
    Invoke-Checked "Verify Metabase integration dashboard" {
        python scripts/verify_metabase_integration.py
    }
    Write-Host "Verifying all dashboard card queries..."
    Invoke-Checked "Verify Metabase dashboard cards" {
        python scripts/verify_metabase_cards.py
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
    Invoke-Checked "Build Metabase image" {
        docker build --build-arg "DASHBOARDS_CACHE_BUST=$dashboardsHash" -t $MetabaseImage -t egisz-metabase:latest -f metabase/Dockerfile . 2>&1 | ForEach-Object {
            if ($_ -is [System.Management.Automation.ErrorRecord]) { $_.Exception.Message } else { "$_" }
        }
    }

    Write-Host "Starting Metabase PostgreSQL and Metabase..."
    Invoke-Checked "Apply Metabase deployment" {
        kubectl apply -n $Namespace -f k8s/metabase/metabase.yaml
    }

    Write-Host "Pointing Metabase deployment to image ${MetabaseImage}..."
    Invoke-Checked "Set Metabase deployment image" {
        kubectl set image -n $Namespace deployment/metabase metabase=$MetabaseImage
        Invoke-Kubectl -Arguments @('rollout', 'restart', '-n', $Namespace, 'deployment/metabase') | Out-Null
        Invoke-Kubectl -Arguments @('rollout', 'status', '-n', $Namespace, 'deployment/metabase', '--timeout=300s') | Out-Null
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

function Stop-All {
    Test-KubernetesConnection

    if (-not (Test-NamespaceExists)) {
        Write-Host "Namespace $Namespace does not exist; nothing to stop."
        $global:LASTEXITCODE = 0
        return $false
    }

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
                helm uninstall airflow -n $Namespace --wait --timeout 5m
            }
        }
    }

    Write-Host "Deleting namespace $Namespace..."
    Invoke-Checked "Delete namespace $Namespace" {
        kubectl delete namespace $Namespace --wait --timeout=300s
    }
    return $true
}

function Stop-Airflow {
    Test-KubernetesConnection

    if (-not (Test-NamespaceExists)) {
        Write-Host "Namespace $Namespace does not exist; nothing to stop."
        return
    }

    Write-Host "Scaling down Airflow components without deleting releases or PVCs..."
    Invoke-ScaleIfExists -Description "Scale Airflow webserver to 0" -Kind deployment -Name airflow-webserver
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
