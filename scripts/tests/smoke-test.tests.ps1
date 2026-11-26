#Requires -Version 7.3
<#
.SYNOPSIS
  Local harness for scripts/smoke-test.ps1. Runs the REAL script against a fake app and fake az/kubectl, with no
  Azure access, so every assertion can be proven to pass on healthy data and to fail, by name, when violated.

.DESCRIPTION
  - FakeEventsApp.cs: an in-process HTTP server that speaks the app's documented API contract (canned data).
  - shims/: fake `az` and `kubectl` placed first on PATH for the child process only; they serve canned JSON from
    a per-scenario directory and log every call.
  - fixtures/: the healthy canned data. Each scenario copies it, applies one mutation and runs the script.

  Usage:   pwsh -File scripts/tests/smoke-test.tests.ps1 [-Only '<wildcard on scenario name>'] [-ShowOutput]
  Exit code is 0 only if every scenario behaved as expected. Needs only pwsh. Never calls Azure or a cluster.

  Adding a scenario (e.g. for ticket 06): add an entry to $scenarios with a Mutate scriptblock that edits the
  fixture data in $d (Diag, Namespace, Roles, Federated, Secrets, Server options, ScriptArgs) and ExpectFail =
  the assertion ids that must FAIL (empty = must pass everything). Keep $allIds in step with the script.
#>
[CmdletBinding()]
param(
    [string] $Only = '*',
    # Print the full smoke-test output of every scenario (default: only on a mismatch).
    [switch] $ShowOutput
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$here = $PSScriptRoot
$smokeScript = Join-Path (Split-Path -Parent $here) 'smoke-test.ps1'
$fixtures = Join-Path $here 'fixtures'
$pwsh = (Get-Process -Id $PID).Path

# Every assertion id the script must report on a healthy run. Written here independently of the script on purpose,
# so a dropped assertion is caught.
$allIds = @(
    'app-ready', 'consumer-ready', 'send-accepted', 'list-roundtrip', 'sse-delivery',
    'sasl-mechanism', 'login-callback-handler', 'passwordless-autoconfig', 'no-credential-properties',
    'private-endpoint-ip', 'workload-identity-env', 'clear-view-only', 'clear-empties-view',
    'local-auth-disabled', 'public-network-disabled', 'roles-sender-receiver-only', 'roles-scoped-to-event-hub',
    'federated-subject', 'no-user-secrets'
)
$httpIds = $allIds[0..12]

# ------------------------------------------------------------------ environment: fake app, shims
Add-Type -Path (Join-Path $here 'FakeEventsApp.cs')
$port = & {
    $l = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    $l.Start(); $p = $l.LocalEndpoint.Port; $l.Stop(); $p
}
$server = [FakeEventsApp]::new($port)
$server.Start()

$work = Join-Path ([IO.Path]::GetTempPath()) ("smoke-harness-" + [guid]::NewGuid().ToString('N').Substring(0, 8))
$shimDir = Join-Path $work 'shims'
New-Item -ItemType Directory -Force -Path $shimDir | Out-Null
foreach ($tool in 'az', 'kubectl') {
    $impl = Join-Path (Join-Path $here 'shims') "fake-$tool.ps1"
    if ($IsWindows) {
        Set-Content -Path (Join-Path $shimDir "$tool.cmd") -Encoding ascii -Value "@echo off`r`n`"$pwsh`" -NoProfile -File `"$impl`" %*`r`nexit /b %ERRORLEVEL%"
    }
    else {
        $sh = Join-Path $shimDir $tool
        Set-Content -Path $sh -Value "#!/bin/sh`nexec `"$pwsh`" -NoProfile -File `"$impl`" `"`$@`""
        chmod +x $sh
    }
}

function Read-Fixture([string] $Name) { Get-Content -Raw -Path (Join-Path $fixtures $Name) | ConvertFrom-Json }
function ConvertTo-FixtureJson($Value) { ConvertTo-Json -InputObject $Value -Depth 20 }

# ------------------------------------------------------------------ scenario runner
function Invoke-Scenario($Scenario) {
    $name = $Scenario.Name
    $dir = Join-Path $work ($name -replace '[^a-zA-Z0-9]+', '-')
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    $kubeconfig = Join-Path $dir 'kubeconfig'
    Set-Content -Path $kubeconfig -Value 'fake'

    # Healthy canned data, then this scenario's single mutation.
    $d = @{
        Diag      = Read-Fixture 'diagnostics.json'
        Namespace = Read-Fixture 'namespace.json'
        Roles     = @(Read-Fixture 'role-assignments.json')
        Federated = @(Read-Fixture 'federated-credentials.json')
        Secrets   = Read-Fixture 'secrets.json'
        State     = Read-Fixture 'deploy-outputs.json'
        Server    = @{}                 # FakeEventsApp property overrides
        ScriptArgs = @{}                 # extra smoke-test.ps1 parameters
        RemoveFiles = @()                # canned files to delete (simulates a failing az call)
        ServiceIp = '127.0.0.1'
    }
    $d.State.kubeconfig = $kubeconfig
    if ($Scenario.Mutate) { & $Scenario.Mutate $d }

    Set-Content -Path (Join-Path $dir 'diagnostics.json') -Value (ConvertTo-FixtureJson $d.Diag)
    Set-Content -Path (Join-Path $dir 'namespace.json') -Value (ConvertTo-FixtureJson $d.Namespace)
    Set-Content -Path (Join-Path $dir 'role-assignments.json') -Value (ConvertTo-FixtureJson $d.Roles)
    Set-Content -Path (Join-Path $dir 'federated-credentials.json') -Value (ConvertTo-FixtureJson $d.Federated)
    Set-Content -Path (Join-Path $dir 'secrets.json') -Value (ConvertTo-FixtureJson $d.Secrets)
    Set-Content -Path (Join-Path $dir 'service.json') -Value (ConvertTo-FixtureJson ([pscustomobject]@{ status = [pscustomobject]@{ loadBalancer = [pscustomobject]@{ ingress = @([pscustomobject]@{ ip = $d.ServiceIp }) } } }))
    Set-Content -Path (Join-Path $dir 'deploy-outputs.json') -Value (ConvertTo-FixtureJson $d.State)
    foreach ($f in $d.RemoveFiles) { Remove-Item -Path (Join-Path $dir $f) }

    $server.Reset()
    $server.DiagnosticsJson = ConvertTo-FixtureJson $d.Diag
    foreach ($k in $d.Server.Keys) { $server.$k = $d.Server[$k] }

    $argList = [ordered]@{
        StateFile = (Join-Path $dir 'deploy-outputs.json')
        ReadyTimeoutSeconds = 5
        PollTimeoutSeconds = 3
        PollIntervalSeconds = 1
    }
    if (-not $Scenario.UseServiceEndpoint) { $argList.Endpoint = $server.BaseUrl }
    foreach ($k in $d.ScriptArgs.Keys) { $argList[$k] = $d.ScriptArgs[$k] }
    $childArgs = @('-NoProfile', '-File', $smokeScript)
    foreach ($k in $argList.Keys) { $childArgs += "-$k"; $childArgs += [string]$argList[$k] }

    # The shims are on PATH for this child process only (restored right after).
    $oldPath = $env:PATH
    $env:PATH = $shimDir + [IO.Path]::PathSeparator + $env:PATH
    $env:SMOKE_FAKE_DIR = $dir
    try { $output = & $pwsh @childArgs 2>&1 | ForEach-Object { "$_" }; $exit = $LASTEXITCODE }
    finally { $env:PATH = $oldPath; Remove-Item Env:\SMOKE_FAKE_DIR -ErrorAction SilentlyContinue }

    $text = $output -join "`n"
    $passed = @([regex]::Matches($text, '(?m)^PASS \[([a-z0-9-]+)\]') | ForEach-Object { $_.Groups[1].Value })
    $failed = @([regex]::Matches($text, '(?m)^FAIL \[([a-z0-9-]+)\]') | ForEach-Object { $_.Groups[1].Value })
    $calls = if (Test-Path (Join-Path $dir 'calls.log')) { Get-Content -Raw (Join-Path $dir 'calls.log') } else { '' }

    $problems = @()
    $expected = @($Scenario.ExpectFail)
    if ($expected.Count -eq 0) {
        if ($exit -ne 0) { $problems += "expected exit 0, got $exit" }
        $missing = @($allIds | Where-Object { $_ -notin $passed })
        if ($missing) { $problems += "assertions not reported as PASS: $($missing -join ', ')" }
        if ($text -notmatch "Summary: $($allIds.Count) passed, 0 failed") { $problems += 'summary line missing or wrong' }
    }
    else {
        if ($exit -eq 0) { $problems += 'expected non-zero exit, got 0' }
        $unexpected = @($failed | Where-Object { $_ -notin $expected })
        $notFailed = @($expected | Where-Object { $_ -notin $failed })
        if ($unexpected) { $problems += "unexpected FAIL: $($unexpected -join ', ')" }
        if ($notFailed) { $problems += "expected FAIL but not reported: $($notFailed -join ', ')" }
        if ($text -notmatch "Summary: \d+ passed, $($expected.Count) failed") { $problems += "summary does not say $($expected.Count) failed" }
        # Every assertion must still be accounted for (PASS or FAIL), whatever failed.
        $unaccounted = @($allIds | Where-Object { $_ -notin $passed -and $_ -notin $failed })
        if ($unaccounted) { $problems += "assertions neither PASS nor FAIL: $($unaccounted -join ', ')" }
    }
    if ($Scenario.Check) {
        $extra = & $Scenario.Check @{ Text = $text; Calls = $calls; Server = $server; Dir = $dir; Exit = $exit }
        if ($extra) { $problems += $extra }
    }

    [pscustomobject]@{ Name = $name; Problems = $problems; Output = $text; Exit = $exit; Failed = $failed }
}

# ------------------------------------------------------------------ scenarios
function S($Name, $ExpectFail, $Mutate, $Check = $null, [switch] $UseServiceEndpoint) {
    [pscustomobject]@{ Name = $Name; ExpectFail = $ExpectFail; Mutate = $Mutate; Check = $Check; UseServiceEndpoint = [bool]$UseServiceEndpoint }
}

$privateIpPattern = '(?m)^PASS \[private-endpoint-ip\].*10\.20\.2\.4'
$scenarios = @(
    # ---- healthy
    S 'healthy' @() $null {
        param($r)
        $e = @()
        if ($r.Text -notmatch $privateIpPattern) { $e += 'resolved private IP not reported in the evidence line' }
        foreach ($needle in 'rg-test', 'ehpoc-ns-test', 'ehpoc-id-test', '00000000-0000-0000-0000-0000000000aa', "--kubeconfig $($r.Dir)", '-n poc') {
            if ($r.Calls -notlike "*$needle*") { $e += "az/kubectl never called with '$needle' (state not used?)" }
        }
        if ($r.Server.RequestLog -notcontains 'POST /api/events/clear') { $e += 'clear was never called' }
        $e
    }
    S 'healthy-with-sa-token-secret' @() { param($d) $d.Secrets.items = @([pscustomobject]@{ type = 'kubernetes.io/service-account-token'; metadata = [pscustomobject]@{ name = 'sa-token' } }) }
    S 'healthy-after-propagation-delay' @() {
        param($d)
        # Readiness down once, consumer not assigned for two polls, first two sends refused (role propagation).
        $warm = Read-Fixture 'diagnostics.json'; $warm.consumer.listenerRunning = $false; $warm.consumer.assignedPartitions = 0
        $d.Server = @{ ReadinessDownCalls = 1; DiagnosticsWarmupCalls = 2; WarmupDiagnosticsJson = (ConvertTo-FixtureJson $warm); PostFailFirst = 2 }
        $d.ScriptArgs = @{ ReadyTimeoutSeconds = 30 }
    }
    S -Name 'endpoint-from-service-ip' -ExpectFail $httpIds -UseServiceEndpoint -Mutate {
        param($d) $d.State.PSObject.Properties.Remove('endpoint'); $d.ScriptArgs = @{ ReadyTimeoutSeconds = 2 }
    } -Check {
        param($r)
        $e = @()
        if ($r.Calls -notlike '* get service *') { $e += 'did not ask kubectl for the Service external IP' }
        if ($r.Text -notlike '*http://127.0.0.1*') { $e += 'did not use the Service IP as the endpoint' }
        $e
    }

    # ---- readiness and consumer
    S 'app-never-ready' $httpIds { param($d) $d.Server = @{ ReadinessDownCalls = 100000 } }
    S 'consumer-no-partitions' @('consumer-ready', 'send-accepted', 'list-roundtrip', 'sse-delivery') { param($d) $d.Diag.consumer.assignedPartitions = 0 }
    S 'consumer-not-running' @('consumer-ready', 'send-accepted', 'list-roundtrip', 'sse-delivery') { param($d) $d.Diag.consumer.listenerRunning = $false }

    # ---- HTTP round trip
    S 'send-rejected-400' @('send-accepted', 'list-roundtrip', 'sse-delivery') { param($d) $d.Server = @{ PostStatus = 400 } }
    S 'send-never-authorized-503' @('send-accepted', 'list-roundtrip', 'sse-delivery') { param($d) $d.Server = @{ PostFailFirst = 100000 }; $d.ScriptArgs = @{ ReadyTimeoutSeconds = 4 } }
    S 'send-without-offset' @('send-accepted') { param($d) $d.Server = @{ PostOmitOffset = $true } }
    S 'list-missing-event' @('list-roundtrip') { param($d) $d.Server = @{ ListDropsEvent = $true } }
    S 'list-different-message' @('list-roundtrip') { param($d) $d.Server = @{ ListMutates = 'message' } }
    S 'list-different-key' @('list-roundtrip') { param($d) $d.Server = @{ ListMutates = 'key' } }
    S 'list-different-offset' @('list-roundtrip') { param($d) $d.Server = @{ ListMutates = 'offset' } }
    S 'sse-missing-event' @('sse-delivery') { param($d) $d.Server = @{ SseDropsEvent = $true } }
    S 'sse-different-message' @('sse-delivery') { param($d) $d.Server = @{ SseTampersMessage = $true } }

    # ---- diagnostics
    S 'diag-sasl-plain' @('sasl-mechanism') { param($d) $d.Diag.kafka.saslMechanism = 'PLAIN' }
    S 'diag-wrong-handler' @('login-callback-handler') { param($d) $d.Diag.kafka.loginCallbackHandlerClass = 'org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler' }
    S 'diag-autoconfig-not-applied' @('passwordless-autoconfig') { param($d) $d.Diag.kafka.passwordlessAutoConfigApplied = $false }
    S 'diag-credential-properties' @('no-credential-properties') { param($d) $d.Diag.kafka.credentialPropertiesConfigured = $true; $d.Diag.kafka.credentialPropertyNames = @('spring.kafka.properties.sasl.jaas.config') }
    S 'diag-public-ip-flag' @('private-endpoint-ip') { param($d) $d.Diag.eventHubs.resolvedIpIsPrivate = $false; $d.Diag.eventHubs.resolvedIp = '52.1.2.3' }
    S 'diag-public-ip-despite-flag' @('private-endpoint-ip') { param($d) $d.Diag.eventHubs.resolvedIp = '52.1.2.3' }
    S 'diag-unresolved' @('private-endpoint-ip') { param($d) $d.Diag.eventHubs.resolvedIp = $null; $d.Diag.eventHubs.resolvedIpIsPrivate = $null; $d.Diag.eventHubs.resolutionError = 'cannot resolve host' }
    S 'diag-no-federated-token' @('workload-identity-env') { param($d) $d.Diag.workloadIdentity.AZURE_FEDERATED_TOKEN_FILE = $false }
    S 'diag-no-client-id' @('workload-identity-env') { param($d) $d.Diag.workloadIdentity.AZURE_CLIENT_ID = $false }

    # ---- clear
    S 'clear-purges-hub' @('clear-view-only') { param($d) $d.Server = @{ ClearPurgesHub = $true } }
    S 'clear-leaves-events' @('clear-empties-view') { param($d) $d.Server = @{ ClearIsNoop = $true } }

    # ---- Azure CLI assertions
    S 'ns-local-auth-enabled' @('local-auth-disabled') { param($d) $d.Namespace.disableLocalAuth = $false }
    S 'ns-public-access-enabled' @('public-network-disabled') { param($d) $d.Namespace.publicNetworkAccess = 'Enabled' }
    S 'ns-az-call-fails' @('local-auth-disabled', 'public-network-disabled') { param($d) $d.RemoveFiles = @('namespace.json') }
    S 'roles-extra-contributor' @('roles-sender-receiver-only') {
        param($d)
        $extra = [pscustomobject]@{ principalId = $d.Roles[0].principalId; roleDefinitionName = 'Contributor'; scope = $d.Roles[0].scope }
        $d.Roles = @($d.Roles + $extra)
    }
    S 'roles-missing-receiver' @('roles-sender-receiver-only') { param($d) $d.Roles = @($d.Roles[0]) }
    S 'roles-sender-at-namespace' @('roles-scoped-to-event-hub') { param($d) $d.Roles[0].scope = $d.Roles[0].scope -replace '/eventhubs/events$', '' }
    S 'roles-receiver-at-resource-group' @('roles-scoped-to-event-hub') { param($d) $d.Roles[1].scope = '/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-test' }
    S 'roles-on-other-event-hub' @('roles-scoped-to-event-hub') { param($d) $d.Roles[1].scope = $d.Roles[1].scope -replace '/eventhubs/events$', '/eventhubs/events-other' }
    S 'roles-none' @('roles-sender-receiver-only', 'roles-scoped-to-event-hub') { param($d) $d.Roles = @() }
    S 'federated-wrong-subject' @('federated-subject') { param($d) $d.Federated[0].subject = 'system:serviceaccount:poc:other' }
    S 'federated-missing' @('federated-subject') { param($d) $d.Federated = @() }
    S 'federated-extra-credential' @('federated-subject') {
        param($d)
        $extra = [pscustomobject]@{ name = 'extra'; issuer = 'https://oidc.example.invalid/x/'; subject = 'system:serviceaccount:default:default'; audiences = @('api://AzureADTokenExchange') }
        $d.Federated = @($d.Federated + $extra)
    }

    # ---- kubectl secrets (also proves secret values are never printed)
    S 'secrets-opaque-present' @('no-user-secrets') {
        param($d)
        $d.Secrets.items = @([pscustomobject]@{
                type = 'Opaque'; metadata = [pscustomobject]@{ name = 'leaked-credentials' }
                data = [pscustomobject]@{ password = 'U0VOVElORUwtU0VDUkVULVZBTFVF' }
            })
    } {
        param($r)
        if ($r.Text -like '*U0VOVElORUwtU0VDUkVULVZBTFVF*' -or $r.Text -like '*SENTINEL-SECRET-VALUE*') { 'secret data was printed' }
        if ($r.Text -notlike '*leaked-credentials*') { 'offending secret name not reported' }
    }
)

# ------------------------------------------------------------------ run
$failures = 0
$ran = 0
try {
    foreach ($s in $scenarios | Where-Object { $_.Name -like $Only }) {
        $ran++
        $r = Invoke-Scenario $s
        $expect = if (@($s.ExpectFail).Count) { "fails [$(@($s.ExpectFail) -join ', ')]" } else { 'passes all' }
        if ($r.Problems.Count -eq 0) {
            Write-Host ("ok    {0,-36} {1}, exit {2}" -f $r.Name, $expect, $r.Exit) -ForegroundColor Green
            if ($ShowOutput) { $r.Output | Write-Host }
        }
        else {
            $failures++
            Write-Host ("NOT OK {0,-35} expected: {1}" -f $r.Name, $expect) -ForegroundColor Red
            $r.Problems | ForEach-Object { Write-Host "      - $_" -ForegroundColor Red }
            $r.Output -split "`n" | ForEach-Object { Write-Host "      | $_" -ForegroundColor DarkGray }
        }
    }
}
finally {
    $server.Dispose()
    Remove-Item -Recurse -Force -Path $work -ErrorAction SilentlyContinue
}

if ($ran -eq 0) { Write-Host "No scenario matches '$Only'." -ForegroundColor Red; exit 2 }
Write-Host ("`n{0} scenario(s), {1} not as expected." -f $ran, $failures) -ForegroundColor $(if ($failures) { 'Red' } else { 'Green' })
exit ([int][bool]$failures)
