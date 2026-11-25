#Requires -Version 7.3
<#
.SYNOPSIS
  Deploys the passwordless Event Hubs PoC environment (tickets 01-02) from an empty resource group
  to a running app, and prints the endpoint. Safe to re-run (idempotent).

.DESCRIPTION
  1. Detects the caller's public IP once (or uses -CallerIp).
  2. Creates the resource group and deploys infra/main.bicep (VNet, ACR, AKS, private Event Hubs, workload
     identity). The IP becomes the AKS API server authorized range.
  3. Builds the app image remotely with `az acr build` (no local Java/Maven/Docker). The tag is a hash of
     the app sources, so an unchanged app is not rebuilt.
  4. Fetches kubeconfig into .deploy-state/ (your ~/.kube is not touched) and applies k8s/ with these
     placeholders rendered (all non-secret): ${IMAGE}, ${CALLER_IP} (Service source range),
     ${IDENTITY_CLIENT_ID} (service account annotation), ${EVENTHUBS_NAMESPACE_FQDN} and ${EVENTHUBS_TOPIC}
     (container env vars consumed by the app).
  5. Waits for the rollout, the load balancer IP and a healthy /actuator/health, then prints the endpoint.
  6. Verifies the passwordless producer (skip with -SkipVerify): waits for readiness, checks that
     /api/diagnostics reports Spring Cloud Azure's passwordless auto-configuration as applied, then POSTs one
     probe event to /api/events, retrying with backoff for up to -VerifyTimeoutSeconds (default 360) because
     Azure role assignments can take up to about five minutes to take effect.

  Requires only the Azure CLI (logged in: `az login`) and PowerShell 7.3+. kubectl is installed through
  the Azure CLI (`az aks install-cli`) if it is missing.
#>
[CmdletBinding()]
param(
    [string] $ResourceGroup = 'rg-eh-passwordless-poc',
    [string] $Location = 'australiaeast',
    [string] $NamePrefix = 'ehpoc',
    # Override the auto-detected public IPv4 address (e.g. when behind a proxy).
    [string] $CallerIp,
    # Rebuild the image even if a tag for the current sources already exists in ACR.
    [switch] $ForceBuild,
    # Skip the post-deploy verification (readiness, diagnostics and one probe event through Event Hubs).
    [switch] $SkipVerify,
    # How long the verification keeps retrying the probe event. Role assignments can take up to ~5 minutes to
    # take effect, so the default leaves a margin above that.
    [ValidateRange(60, 3600)]
    [int] $VerifyTimeoutSeconds = 360
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
# A non-zero exit code from any native command (az, kubectl) becomes a terminating error.
$PSNativeCommandUseErrorActionPreference = $true
# `az acr build` streams unicode build logs; avoid cp1252 console encoding crashes on Windows.
$env:PYTHONIOENCODING = 'utf-8'

$repoRoot = Split-Path -Parent $PSScriptRoot
$stateDir = Join-Path $repoRoot '.deploy-state'
$namespace = 'poc'
$imageRepo = 'ehpoc'

function Write-Step([string] $Message) { Write-Host "`n==> $Message" -ForegroundColor Cyan }

function Test-Command([string] $Name) { [bool](Get-Command $Name -ErrorAction SilentlyContinue) }

function Get-PublicIp {
    # Plain HTTPS GET to well-known IP echo services; nothing identifying is sent.
    $services = 'https://api.ipify.org', 'https://checkip.amazonaws.com', 'https://ifconfig.me/ip'
    foreach ($url in $services) {
        try {
            $ip = (Invoke-RestMethod -Uri $url -UserAgent 'ehpoc-deploy' -TimeoutSec 15).ToString().Trim()
            if ($ip -match '^(\d{1,3}\.){3}\d{1,3}$') { return $ip }
        }
        catch { Write-Verbose "IP lookup via $url failed: $($_.Exception.Message)" }
    }
    throw 'Could not detect the public IPv4 address. Re-run with -CallerIp <address>.'
}

function Get-AppHash {
    # Deterministic hash of the app sources: same sources => same image tag => no rebuild.
    $appDir = Join-Path $repoRoot 'app'
    $files = Get-ChildItem -Path $appDir -Recurse -File |
        Where-Object { $_.FullName -notmatch '[\\/](target|\.idea)[\\/]' } |
        Sort-Object FullName
    $sha = [System.Security.Cryptography.SHA256]::Create()
    foreach ($f in $files) {
        $rel = [System.IO.Path]::GetRelativePath($appDir, $f.FullName).Replace('\', '/')
        # Normalise line endings so a CRLF checkout hashes the same as an LF one.
        $text = (Get-Content -Raw -Path $f.FullName) -replace "`r`n", "`n"
        $bytes = [System.Text.Encoding]::UTF8.GetBytes("$rel`n$text`n")
        $null = $sha.TransformBlock($bytes, 0, $bytes.Length, $null, 0)
    }
    $null = $sha.TransformFinalBlock([byte[]]::new(0), 0, 0)
    return ([BitConverter]::ToString($sha.Hash) -replace '-', '').Substring(0, 12).ToLowerInvariant()
}

function Expand-Manifest([string] $Path, [System.Collections.IDictionary] $Values) {
    # Replaces ${NAME} placeholders and fails if any placeholder is left unfilled or unknown.
    $text = Get-Content -Raw -Path $Path
    foreach ($key in $Values.Keys) {
        if ([string]::IsNullOrWhiteSpace([string]$Values[$key])) { throw "Empty value for manifest placeholder $key." }
        $text = $text.Replace('${' + $key + '}', [string]$Values[$key])
    }
    $left = [regex]::Matches($text, '\$\{[A-Z_]+\}') | ForEach-Object { $_.Value } | Sort-Object -Unique
    if ($left) { throw "Unfilled placeholder(s) in $(Split-Path -Leaf $Path): $($left -join ', ')" }
    return $text
}

function Invoke-WithRetry([scriptblock] $Action, [string] $What, [int] $Attempts = 12, [int] $DelaySeconds = 15) {
    for ($i = 1; $i -le $Attempts; $i++) {
        try { return & $Action }
        catch {
            if ($i -eq $Attempts) { throw }
            Write-Host "  $What failed (attempt $i/$Attempts): $($_.Exception.Message). Retrying in ${DelaySeconds}s..." -ForegroundColor Yellow
            Start-Sleep -Seconds $DelaySeconds
        }
    }
}

function Test-PasswordlessProducer([string] $Endpoint, [int] $TimeoutSeconds) {
    # Bounded post-deploy verification of the passwordless producer. Throws with a clear message on failure.

    Write-Step 'Verifying: readiness'
    $deadline = [DateTime]::UtcNow.AddSeconds(120)
    $ready = $false
    while (-not $ready -and [DateTime]::UtcNow -lt $deadline) {
        try { $ready = ((Invoke-RestMethod -Uri "$Endpoint/actuator/health/readiness" -TimeoutSec 10).status -eq 'UP') }
        catch { Write-Verbose "readiness probe failed: $($_.Exception.Message)" }
        if (-not $ready) { Start-Sleep -Seconds 5 }
    }
    if (-not $ready) { throw "Verification failed: $Endpoint/actuator/health/readiness was not UP within 2 minutes." }
    Write-Host '  readiness: UP'

    Write-Step 'Verifying: effective Kafka client configuration (/api/diagnostics)'
    $diag = Invoke-RestMethod -Uri "$Endpoint/api/diagnostics" -TimeoutSec 30
    $kafka = $diag.kafka
    Write-Host ("  securityProtocol={0}  saslMechanism={1}" -f $kafka.securityProtocol, $kafka.saslMechanism)
    Write-Host ("  loginCallbackHandlerClass={0}" -f $kafka.loginCallbackHandlerClass)
    Write-Host ("  passwordlessAutoConfigApplied={0}  credentialPropertiesConfigured={1}" -f $kafka.passwordlessAutoConfigApplied, $kafka.credentialPropertiesConfigured)
    if (-not $kafka.passwordlessAutoConfigApplied) {
        # A configuration problem, not role propagation: retrying cannot fix it.
        throw "Verification failed: Spring Cloud Azure's passwordless auto-configuration is NOT applied. Problems: $($kafka.problems -join '; ')"
    }
    if ($kafka.credentialPropertiesConfigured) {
        throw "Verification failed: credential properties are configured ($($kafka.credentialPropertyNames -join ', ')); the PoC must be passwordless."
    }

    Write-Step "Verifying: probe event through Event Hubs (retrying up to $TimeoutSeconds s for role-assignment propagation)"
    $body = @{ message = 'deploy-probe'; source = 'api' } | ConvertTo-Json -Compress
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $delay = 10
    $attempt = 0
    $last = 'no response'
    while ($true) {
        $attempt++
        $status = 0
        $resp = $null
        try {
            $resp = Invoke-RestMethod -Method Post -Uri "$Endpoint/api/events" -ContentType 'application/json' -Body $body `
                -TimeoutSec 60 -SkipHttpErrorCheck -StatusCodeVariable status
        }
        catch { $last = "request failed: $($_.Exception.Message)" }

        if ($status -ge 200 -and $status -lt 300) {
            Write-Host ("  probe event {0} acknowledged by Event Hubs: topic={1} partition={2} offset={3} (attempt {4})" -f `
                $resp.event.id, $resp.topic, $resp.partition, $resp.offset, $attempt) -ForegroundColor Green
            return
        }
        if ($status -ge 400 -and $status -lt 500) {
            throw "Verification failed: the app rejected the probe event with HTTP $status ($($resp | ConvertTo-Json -Compress)). This is not role propagation."
        }
        if ($status -ne 0) { $last = "HTTP $status $($resp | ConvertTo-Json -Compress)" }

        $remaining = [int]($deadline - [DateTime]::UtcNow).TotalSeconds
        if ($remaining -le 0) { break }
        $wait = [Math]::Min($delay, [Math]::Max($remaining, 1))
        Write-Host "  attempt $attempt failed ($last). Retrying in ${wait}s ($remaining s left)..." -ForegroundColor Yellow
        Start-Sleep -Seconds $wait
        $delay = [Math]::Min($delay * 2, 30)
    }
    throw ("Verification failed: no probe event was acknowledged within $TimeoutSeconds s. Last result: $last. " +
        'Role assignments for the workload identity can take up to five minutes to take effect, so re-run with a longer ' +
        '-VerifyTimeoutSeconds, or inspect: kubectl -n poc logs deploy/ehpoc, and ' + "$Endpoint/api/diagnostics.")
}

# ---------------------------------------------------------------- preflight
Write-Step 'Preflight'
if (-not (Test-Command 'az')) { throw 'Azure CLI (az) not found on PATH.' }
try { $null = az account show -o none } catch { throw 'Not logged in to Azure. Run `az login` first.' }
Write-Host "Subscription: $(az account show --query name -o tsv)"

# Resource providers a fresh subscription may not have registered yet (no-op when already registered).
foreach ($ns in 'Microsoft.ContainerService', 'Microsoft.ContainerRegistry', 'Microsoft.Network', 'Microsoft.Compute', 'Microsoft.EventHub', 'Microsoft.ManagedIdentity') {
    if ((az provider show --namespace $ns --query registrationState -o tsv) -ne 'Registered') {
        Write-Host "Registering resource provider $ns (waits until done)..."
        az provider register --namespace $ns --wait
    }
}

if (-not (Test-Command 'kubectl')) {
    Write-Host 'kubectl not found; installing it through the Azure CLI...'
    az aks install-cli
    # `az aks install-cli` puts kubectl in ~/.azure-kubectl; make it visible to this script only.
    $azKubectl = Join-Path $HOME '.azure-kubectl'
    if (Test-Path $azKubectl) { $env:PATH = "$azKubectl$([IO.Path]::PathSeparator)$env:PATH" }
    if (-not (Test-Command 'kubectl')) { throw 'kubectl is still not on PATH after `az aks install-cli`; add its install folder to PATH and re-run.' }
}

# ---------------------------------------------------------------- caller IP (detected once)
Write-Step 'Caller public IP'
if (-not $CallerIp) { $CallerIp = Get-PublicIp }
if ($CallerIp -notmatch '^(\d{1,3}\.){3}\d{1,3}$') { throw "Invalid -CallerIp '$CallerIp' (IPv4 address expected)." }
Write-Host "Using $CallerIp for the AKS API server authorized range and the Service source range."

# ---------------------------------------------------------------- infrastructure
Write-Step "Resource group $ResourceGroup ($Location)"
az group create --name $ResourceGroup --location $Location --tags purpose=eventhubs-passwordless-poc -o none

Write-Step 'Deploying infra/main.bicep (VNet, ACR, AKS, private Event Hubs, workload identity). AKS creation takes several minutes on first run.'
$outputsJson = az deployment group create `
    --resource-group $ResourceGroup `
    --name 'ehpoc-base' `
    --template-file (Join-Path $repoRoot 'infra/main.bicep') `
    --parameters location=$Location namePrefix=$NamePrefix callerIp=$CallerIp `
    --query properties.outputs -o json
$out = $outputsJson | ConvertFrom-Json
$acrName = $out.acrName.value
$acrLoginServer = $out.acrLoginServer.value
$aksName = $out.aksName.value
$eventHubsNamespace = $out.eventHubsNamespaceName.value
$eventHubsFqdn = $out.eventHubsFqdn.value
$eventHubName = $out.eventHubName.value
$identityClientId = $out.identityClientId.value
$tenantId = $out.tenantId.value
$identityPrincipalId = $out.identityPrincipalId.value
Write-Host "ACR: $acrLoginServer   AKS: $aksName"
Write-Host "Event Hubs: $eventHubsFqdn (event hub '$eventHubName')   Identity client ID: $identityClientId"

# ---------------------------------------------------------------- image (remote build)
Write-Step 'Application image'
$tag = Get-AppHash
$image = "$acrLoginServer/${imageRepo}:$tag"
$exists = $false
if (-not $ForceBuild) {
    try {
        $tags = az acr repository show-tags --name $acrName --repository $imageRepo -o tsv 2>$null
        $exists = [bool]($tags -split "`n" | Where-Object { $_.Trim() -eq $tag })
    }
    catch { $exists = $false }
}
if ($exists) {
    Write-Host "Image $image already in ACR for the current sources; skipping build (use -ForceBuild to rebuild)."
}
else {
    Write-Host "Building $image in ACR (compiles the app inside the image; no local toolchain)..."
    az acr build --registry $acrName --image "${imageRepo}:$tag" --file (Join-Path $repoRoot 'app/Dockerfile') (Join-Path $repoRoot 'app')
}

# ---------------------------------------------------------------- Kubernetes
Write-Step 'Cluster credentials'
New-Item -ItemType Directory -Force -Path $stateDir | Out-Null
$kubeconfig = Join-Path $stateDir 'kubeconfig'
$env:KUBECONFIG = $kubeconfig
# The API-server IP allow-list can take a moment to apply after a changed IP, so retry.
Invoke-WithRetry -What 'get-credentials' -Action {
    az aks get-credentials --resource-group $ResourceGroup --name $aksName --file $kubeconfig --overwrite-existing -o none
}
Write-Host "kubeconfig: $kubeconfig  (use: `$env:KUBECONFIG = '$kubeconfig')"

Write-Step 'Applying manifests'
# Placeholders in k8s/*.yaml. Only non-secret values: no secret exists anywhere in the manifests.
$manifestValues = [ordered]@{
    IMAGE                    = $image
    CALLER_IP                = $CallerIp
    IDENTITY_CLIENT_ID       = $identityClientId
    EVENTHUBS_NAMESPACE_FQDN = $eventHubsFqdn
    EVENTHUBS_TOPIC          = $eventHubName
}
$manifestFiles = Get-ChildItem -Path (Join-Path $repoRoot 'k8s') -Filter '*.yaml' | Sort-Object Name
foreach ($m in $manifestFiles) {
    $rendered = Expand-Manifest -Path $m.FullName -Values $manifestValues
    Invoke-WithRetry -What "kubectl apply $($m.Name)" -Action { $rendered | kubectl apply -f - }
}

Write-Step 'Waiting for rollout'
kubectl -n $namespace rollout status deployment/ehpoc --timeout=300s

Write-Step 'Waiting for the load balancer IP'
$lbIp = $null
for ($i = 1; $i -le 60 -and -not $lbIp; $i++) {
    $lbIp = (kubectl -n $namespace get service ehpoc -o 'jsonpath={.status.loadBalancer.ingress[0].ip}')
    if (-not $lbIp) { Start-Sleep -Seconds 10 }
}
if (-not $lbIp) { throw 'The Service did not get an external IP within 10 minutes. Check: kubectl -n poc describe service ehpoc' }

$endpoint = "http://$lbIp"
Write-Step "Waiting for $endpoint/actuator/health"
$healthy = $false
for ($i = 1; $i -le 30 -and -not $healthy; $i++) {
    try {
        $h = Invoke-RestMethod -Uri "$endpoint/actuator/health" -TimeoutSec 10
        $healthy = ($h.status -eq 'UP')
    }
    catch { Start-Sleep -Seconds 10 }
}
if (-not $healthy) { throw "Health endpoint at $endpoint did not report UP within 5 minutes (is your IP still $CallerIp?)." }

# State for later scripts (smoke test etc.). Gitignored; contains your IP, no secrets.
[ordered]@{
    resourceGroup = $ResourceGroup
    location = $Location
    acrName = $acrName
    aksName = $aksName
    image = $image
    callerIp = $CallerIp
    endpoint = $endpoint
    kubeconfig = $kubeconfig
    namespace = $namespace
    eventHubsNamespace = $eventHubsNamespace
    eventHubsFqdn = $eventHubsFqdn
    eventHubName = $eventHubName
    identityName = $out.identityName.value
    identityClientId = $identityClientId
    identityPrincipalId = $identityPrincipalId
    tenantId = $tenantId
    serviceAccount = 'ehpoc'
} | ConvertTo-Json | Set-Content -Path (Join-Path $stateDir 'deploy-outputs.json')

Write-Host "`nDeployed. Endpoint (reachable only from $CallerIp):" -ForegroundColor Green
Write-Host "  $endpoint/actuator/health"
Write-Host "  $endpoint/api/diagnostics"
Write-Host "  POST $endpoint/api/events   {`"message`":`"hello`"}"

if ($SkipVerify) {
    Write-Host "`nSkipping verification (-SkipVerify)." -ForegroundColor Yellow
}
else {
    Test-PasswordlessProducer -Endpoint $endpoint -TimeoutSeconds $VerifyTimeoutSeconds
    Write-Host "`nVerified: the app published an event to Event Hubs with no secret." -ForegroundColor Green
}
