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

# Negative mode (-NegativeTest, ticket 06): every assertion id it reports on a fully successful run. The baseline and
# restored round trips reuse the standard HTTP building blocks, so their ids carry a phase prefix.
$roundTripIds = 'app-ready', 'consumer-ready', 'send-accepted', 'list-roundtrip', 'sse-delivery'
$negIds = @('negative-baseline') + @($roundTripIds | ForEach-Object { "negative-baseline-$_" }) + @(
    'negative-credential-broken', 'negative-auth-fails', 'negative-attributed-to-auth', 'negative-credential-restored', 'negative-restored'
) + @($roundTripIds | ForEach-Object { "negative-restored-$_" })
$originalSubject = 'system:serviceaccount:poc:ehpoc'

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

# Driver for the "interrupted run" scenario: runs the smoke test in a runspace, stops it (like Ctrl-C) as soon as the
# fake kubectl logs the first `rollout restart`, which in -NegativeTest mode is the restart after the break
# (the baseline does not restart), then prints what the script wrote and the stop's outcome.
$interruptDriver = Join-Path $work 'interrupt-driver.ps1'
Set-Content -Path $interruptDriver -Value @'
param([string] $Script, [string] $ArgsFile, [string] $CallsLog)
$h = @{}
(Get-Content -Raw $ArgsFile | ConvertFrom-Json).PSObject.Properties | ForEach-Object { $h[$_.Name] = $_.Value }
$ps = [powershell]::Create()
$null = $ps.AddScript('param($s, $a) & $s @a').AddArgument($Script).AddArgument($h)
$async = $ps.BeginInvoke()
$deadline = [DateTime]::UtcNow.AddSeconds(60)
while (-not $async.IsCompleted -and [DateTime]::UtcNow -lt $deadline) {
    if ((Test-Path $CallsLog) -and (Get-Content -Raw $CallsLog) -like '*rollout restart*') { Start-Sleep -Milliseconds 200; $ps.Stop(); break }
    Start-Sleep -Milliseconds 100
}
try { $null = $ps.EndInvoke($async); Write-Output 'driver: script ended normally' }
catch { Write-Output "driver: script stopped ($($_.Exception.GetType().Name))" }
foreach ($i in $ps.Streams.Information) { Write-Output "$($i.MessageData)" }
'@

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
        ExtraFiles = @{}                 # extra files to create in the scenario directory (name -> content)
        AzUpdateFailCalls = ''           # fake az: comma list of federated-credential update calls that fail
        KubectlFailRestart = $false      # fake kubectl: rollout restart fails
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
    foreach ($f in $d.ExtraFiles.Keys) { Set-Content -Path (Join-Path $dir $f) -Value $d.ExtraFiles[$f] }
    if ($d.AzUpdateFailCalls) { Set-Content -Path (Join-Path $dir 'update-fail-calls.txt') -Value $d.AzUpdateFailCalls }
    if ($d.KubectlFailRestart) { Set-Content -Path (Join-Path $dir 'fail-rollout-restart') -Value 'x' }

    $server.Reset()
    $server.DiagnosticsJson = ConvertTo-FixtureJson $d.Diag
    $server.FederationFile = Join-Path $dir 'federated-credentials.json'
    foreach ($k in $d.Server.Keys) { $server.$k = $d.Server[$k] }

    $argList = [ordered]@{
        StateFile = (Join-Path $dir 'deploy-outputs.json')
        ReadyTimeoutSeconds = 5
        PollTimeoutSeconds = 3
        PollIntervalSeconds = 1
    }
    if (-not $Scenario.UseServiceEndpoint) { $argList.Endpoint = $server.BaseUrl }
    foreach ($k in $d.ScriptArgs.Keys) { $argList[$k] = $d.ScriptArgs[$k] }
    # -NonInteractive: a prompt in the script under test fails instead of waiting on this console.
    $childArgs = @('-NoProfile', '-NonInteractive', '-File', $smokeScript)
    foreach ($k in $argList.Keys) {
        if ($argList[$k] -is [bool]) { if ($argList[$k]) { $childArgs += "-$k" } }   # a switch
        else { $childArgs += "-$k"; $childArgs += [string]$argList[$k] }
    }

    if ($Scenario.InterruptDuringBreak) {
        # Run the script inside a runspace and Stop() it once the pod restart after the break has been requested:
        # the same stop Ctrl-C causes (finally blocks run, the script never reaches its normal end).
        $hash = [ordered]@{}
        foreach ($k in $argList.Keys) { $hash[$k] = $argList[$k] }
        Set-Content -Path (Join-Path $dir 'script-args.json') -Value ($hash | ConvertTo-Json)
        $childArgs = @('-NoProfile', '-NonInteractive', '-File', $interruptDriver, '-Script', $smokeScript,
            '-ArgsFile', (Join-Path $dir 'script-args.json'), '-CallsLog', (Join-Path $dir 'calls.log'))
    }

    # The shims are on PATH for this child process only (restored right after).
    $oldPath = $env:PATH
    $env:PATH = $shimDir + [IO.Path]::PathSeparator + $env:PATH
    $env:SMOKE_FAKE_DIR = $dir
    $env:SMOKE_FAKE_APP_URL = $server.BaseUrl
    try { $output = & $pwsh @childArgs 2>&1 | ForEach-Object { "$_" }; $exit = $LASTEXITCODE }
    finally { $env:PATH = $oldPath; Remove-Item Env:\SMOKE_FAKE_DIR, Env:\SMOKE_FAKE_APP_URL -ErrorAction SilentlyContinue }

    $text = $output -join "`n"
    $passed = @([regex]::Matches($text, '(?m)^PASS \[([a-z0-9-]+)\]') | ForEach-Object { $_.Groups[1].Value })
    $failed = @([regex]::Matches($text, '(?m)^FAIL \[([a-z0-9-]+)\]') | ForEach-Object { $_.Groups[1].Value })
    $calls = if (Test-Path (Join-Path $dir 'calls.log')) { Get-Content -Raw (Join-Path $dir 'calls.log') } else { '' }

    $problems = @()
    $expected = @($Scenario.ExpectFail)
    $ids = if ($Scenario.Ids) { @($Scenario.Ids) } else { $allIds }
    if ($Scenario.InterruptDuringBreak) {
        # Stopped midway: there is no summary or exit code to judge, only that it was stopped and what the Check finds.
        if ($text -notlike '*driver: script stopped*') { $problems += 'the script was not stopped midway (interrupt did not happen)' }
        if ($text -like '*Summary:*') { $problems += 'the script ran to its end, so nothing was interrupted' }
    }
    elseif ($null -ne $Scenario.ExpectExit) {
        # A setup problem (exit 2): nothing is asserted, so no PASS/FAIL line may appear.
        if ($exit -ne $Scenario.ExpectExit) { $problems += "expected exit $($Scenario.ExpectExit), got $exit" }
        if ($passed -or $failed) { $problems += "no assertion should have run, saw: $((@($passed) + @($failed)) -join ', ')" }
    }
    elseif ($expected.Count -eq 0) {
        if ($exit -ne 0) { $problems += "expected exit 0, got $exit" }
        $missing = @($ids | Where-Object { $_ -notin $passed })
        if ($missing) { $problems += "assertions not reported as PASS: $($missing -join ', ')" }
        if ($text -notmatch "Summary: $($ids.Count) passed, 0 failed") { $problems += 'summary line missing or wrong' }
    }
    else {
        if ($exit -eq 0) { $problems += 'expected non-zero exit, got 0' }
        $unexpected = @($failed | Where-Object { $_ -notin $expected })
        $notFailed = @($expected | Where-Object { $_ -notin $failed })
        if ($unexpected) { $problems += "unexpected FAIL: $($unexpected -join ', ')" }
        if ($notFailed) { $problems += "expected FAIL but not reported: $($notFailed -join ', ')" }
        if ($text -notmatch "Summary: \d+ passed, $($expected.Count) failed") { $problems += "summary does not say $($expected.Count) failed" }
        # Every assertion must still be accounted for (PASS or FAIL), whatever failed.
        $unaccounted = @($ids | Where-Object { $_ -notin $passed -and $_ -notin $failed })
        if ($unaccounted) { $problems += "assertions neither PASS nor FAIL: $($unaccounted -join ', ')" }
    }
    if ($Scenario.Check) {
        $extra = & $Scenario.Check @{ Text = $text; Calls = $calls; Server = $server; Dir = $dir; Exit = $exit }
        if ($extra) { $problems += $extra }
    }

    [pscustomobject]@{ Name = $name; Problems = $problems; Output = $text; Exit = $exit; Failed = $failed }
}

# ------------------------------------------------------------------ scenarios
function S($Name, $ExpectFail, $Mutate, $Check = $null, [switch] $UseServiceEndpoint, $Ids = $null, $ExpectExit = $null, [switch] $InterruptDuringBreak) {
    [pscustomobject]@{ Name = $Name; ExpectFail = $ExpectFail; Mutate = $Mutate; Check = $Check; UseServiceEndpoint = [bool]$UseServiceEndpoint; Ids = $Ids; ExpectExit = $ExpectExit; InterruptDuringBreak = [bool]$InterruptDuringBreak }
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
        # A normal run never changes infrastructure: no credential update, no pod restart.
        if ($r.Calls -like '*federated-credential update*') { $e += 'standard run changed the federated credential' }
        if ($r.Calls -like '*rollout*') { $e += 'standard run restarted the deployment' }
        if ($r.Server.RequestLog -contains 'POST /__fake/restart') { $e += 'standard run restarted the pod' }
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

# ------------------------------------------------------------------ negative-mode scenarios (ticket 06)
# The fake app turns "auth broken" only when its pod restarts (fake kubectl `rollout restart`) while the federated
# credential (fake az `update`, same file) has no valid subject, like a real token exchange at pod start.
function Get-FedCreds($R) { @(Get-Content -Raw (Join-Path $R.Dir 'federated-credentials.json') | ConvertFrom-Json) }
function Get-AzUpdates($R) { @($R.Calls -split "`n" | Where-Object { $_ -like 'az identity federated-credential update *' }) }
function Get-Restarts($R) { @($R.Calls -split "`n" | Where-Object { $_ -like '*rollout restart*' }) }
$negArgs = @{ NegativeTest = $true; Force = $true; PropagationTimeoutSeconds = 20 }
# $negArgs with overrides/additions (a hashtable + cannot repeat a key).
function Get-NegArgs([hashtable] $Extra) { $a = $negArgs.Clone(); foreach ($k in $Extra.Keys) { $a[$k] = $Extra[$k] }; $a }
# Common post-conditions of every negative run that got as far as changing the credential.
function Test-CredentialRestored($R) {
    $e = @()
    $c = Get-FedCreds $R
    if ($c.Count -ne 1 -or $c[0].subject -ne $originalSubject -or $c[0].name -ne 'aks-poc-ehpoc' `
            -or $c[0].issuer -ne 'https://oidc.example.invalid/00000000-0000-0000-0000-000000000002/' -or ($c[0].audiences -join ',') -ne 'api://AzureADTokenExchange') {
        $e += "credential NOT restored to the original: $($c | ConvertTo-Json -Compress)"
    }
    if (Test-Path (Join-Path $R.Dir 'federated-credential-original.json')) { $e += 'backup file left behind after a verified restore' }
    $e
}

$negativeScenarios = @(
    S 'negative-full-cycle' @() { param($d) $d.ScriptArgs = $negArgs } -Ids $negIds -Check {
        param($r)
        $e = @()
        $e += Test-CredentialRestored $r
        $u = @(Get-AzUpdates $r)
        if ($u.Count -ne 2) { $e += "expected 2 credential updates (break, restore), got $($u.Count)" }
        else {
            $subjects = @($u | ForEach-Object { if ($_ -match ' --subject (\S+)') { $Matches[1] } })
            if ($subjects.Count -ne 2 -or $subjects[0] -eq $originalSubject) { $e += "break must set a subject that cannot match; subjects passed: $($subjects -join ' then ')" }
            elseif ($subjects[1] -ne $originalSubject) { $e += 'restore must set the original subject' }
        }
        if (@(Get-Restarts $r).Count -lt 2) { $e += 'pod was not restarted after the break and after the restore' }
        $e
    }
)
$baselineIds = @('negative-baseline') + @($roundTripIds | ForEach-Object { "negative-baseline-$_" })
$restoredIds = @('negative-restored') + @($roundTripIds | ForEach-Object { "negative-restored-$_" })
$phaseIds = 'negative-credential-broken', 'negative-auth-fails', 'negative-attributed-to-auth', 'negative-credential-restored'
$unchanged = { param($r) if (@(Get-AzUpdates $r).Count -or @(Get-Restarts $r).Count) { 'infrastructure was changed (credential update or restart)' } }
# What a scenario with a saved original in the scenario directory looks like (the same values as the fixture).
$backupJson = ConvertTo-Json -Compress -InputObject ([pscustomobject]@{
        identityName = 'ehpoc-id-test'; resourceGroup = 'rg-test'; name = 'aks-poc-ehpoc'; subject = $originalSubject
        issuer = 'https://oidc.example.invalid/00000000-0000-0000-0000-000000000002/'; audiences = @('api://AzureADTokenExchange'); savedAt = '2026-01-01T00:00:00Z' })

$negativeScenarios += @(
    # ---- the safety rails: nothing may change
    S 'negative-needs-confirmation' @() { param($d) $d.ScriptArgs = @{ NegativeTest = $true; PropagationTimeoutSeconds = 20 } } -ExpectExit 2 -Check {
        param($r)
        $e = @(& $unchanged $r)
        if ($r.Text -notlike '*TEMPORARILY BREAKS the federated credential*') { $e += 'warning not shown' }
        if ($r.Text -notlike '*-Force*') { $e += 'does not tell how to confirm non-interactively' }
        $e
    }
    S 'negative-switches-exclusive' @() { param($d) $d.ScriptArgs = (Get-NegArgs @{ RestoreFederatedCredential = $true }) } -ExpectExit 2 -Check $unchanged
    S 'negative-refuses-when-backup-exists' @() {
        param($d) $d.ScriptArgs = $negArgs; $d.ExtraFiles = @{ 'federated-credential-original.json' = $backupJson }
    } -ExpectExit 2 -Check {
        param($r)
        $e = @(& $unchanged $r)
        if ($r.Text -notlike '*-RestoreFederatedCredential*') { $e += 'does not point to the recovery switch' }
        $e
    }
    S 'negative-refuses-when-already-broken' @() { param($d) $d.ScriptArgs = $negArgs; $d.Federated[0].subject = 'system:serviceaccount:poc:somethingelse' } -ExpectExit 2 -Check $unchanged
    S 'negative-refuses-with-two-credentials' @() {
        param($d)
        $d.ScriptArgs = $negArgs
        $d.Federated = @($d.Federated + [pscustomobject]@{ name = 'extra'; issuer = 'https://oidc.example.invalid/x/'; subject = 'system:serviceaccount:default:default'; audiences = @('api://AzureADTokenExchange') })
    } -ExpectExit 2 -Check $unchanged
    S 'negative-baseline-fails-changes-nothing' (@('negative-baseline', 'negative-baseline-consumer-ready', 'negative-baseline-send-accepted', 'negative-baseline-list-roundtrip', 'negative-baseline-sse-delivery', 'negative-restored') + $phaseIds) {
        param($d) $d.ScriptArgs = $negArgs; $d.Diag.consumer.assignedPartitions = 0
    } -Ids ($baselineIds + $phaseIds + 'negative-restored') -Check {
        param($r)
        $e = @(& $unchanged $r)
        if (Test-Path (Join-Path $r.Dir 'federated-credential-original.json')) { $e += 'wrote a backup although nothing was changed' }
        $e
    }

    # ---- expected failure leg: attribution
    S 'negative-auth-reported-by-consumer-only' @() {
        # The send times out (no reason) but the consumer's last error is a login failure: still attributed to auth.
        param($d) $d.ScriptArgs = $negArgs; $d.Server = @{ BrokenReason = 'timeout' }
    } -Ids $negIds
    S 'negative-failure-not-authentication' @('negative-attributed-to-auth') {
        param($d) $d.ScriptArgs = $negArgs; $d.Server = @{ BrokenReason = 'unavailable'; BrokenLastErrorClass = 'org.apache.kafka.common.errors.TimeoutException' }
    } -Ids $negIds -Check { param($r) Test-CredentialRestored $r }
    S 'negative-failure-is-authorization-not-authentication' @('negative-attributed-to-auth') {
        param($d) $d.ScriptArgs = $negArgs; $d.Server = @{ BrokenReason = 'authorization'; BrokenLastErrorClass = 'org.apache.kafka.common.errors.AuthorizationException' }
    } -Ids $negIds

    # ---- propagation delay (Entra honours the change only after a few restarts, both ways)
    S 'negative-propagation-lag' @() {
        param($d) $d.ScriptArgs = (Get-NegArgs @{ PropagationTimeoutSeconds = 40 }); $d.Server = @{ FederationLagRestarts = 2 }
    } -Ids $negIds -Check {
        param($r)
        $e = Test-CredentialRestored $r
        # break: lag restart + broken restart; restore: finally restart (lag) + recovery restart.
        if (@(Get-Restarts $r).Count -lt 4) { $e += "expected at least 4 restarts with propagation lag, got $(@(Get-Restarts $r).Count)" }
        if ($r.Text -notlike '*may not have propagated*') { $e += 'lag not reported' }
        $e
    }
    S 'negative-credential-change-has-no-effect' @('negative-auth-fails', 'negative-attributed-to-auth') {
        # Control: the app keeps working, so the test must NOT claim the proof, and must still restore.
        param($d) $d.ScriptArgs = (Get-NegArgs @{ PropagationTimeoutSeconds = 8 }); $d.Server = @{ IgnoreFederation = $true }
    } -Ids $negIds -Check { param($r) Test-CredentialRestored $r }

    # ---- the credential is always restored
    S 'negative-error-mid-run-still-restores' @('negative-run-error', 'negative-auth-fails', 'negative-attributed-to-auth', 'negative-restored') {
        # kubectl rollout restart fails after the credential was broken: the finally block must restore it anyway.
        param($d) $d.ScriptArgs = $negArgs; $d.KubectlFailRestart = $true
    } -Ids ($baselineIds + $phaseIds + 'negative-restored' + 'negative-run-error') -Check { param($r) Test-CredentialRestored $r }
    S 'negative-restore-retried' @() {
        # The first restore attempt (update call 2) fails; the retry must succeed.
        param($d) $d.ScriptArgs = $negArgs; $d.AzUpdateFailCalls = '2'
    } -Ids $negIds -Check {
        param($r)
        $e = Test-CredentialRestored $r
        if (@(Get-AzUpdates $r).Count -ne 3) { $e += "expected 3 update calls (break, failed restore, restore), got $(@(Get-AzUpdates $r).Count)" }
        $e
    }
    S 'negative-restore-impossible-is-loud' @('negative-credential-restored', 'negative-restored') {
        param($d) $d.ScriptArgs = $negArgs; $d.AzUpdateFailCalls = '2,3,4'
    } -Ids ($baselineIds + $phaseIds + 'negative-restored') -Check {
        param($r)
        $e = @()
        if (-not (Test-Path (Join-Path $r.Dir 'federated-credential-original.json'))) { $e += 'backup removed although the restore failed' }
        if ($r.Text -notlike '*STILL BROKEN*-RestoreFederatedCredential*') { $e += 'no loud instruction to run the recovery switch' }
        $e
    }

    S 'negative-interrupted-still-restores' @() {
        # Ctrl-C equivalent while the credential is broken: the finally block must still restore it.
        param($d) $d.ScriptArgs = $negArgs
    } -InterruptDuringBreak -Check {
        param($r)
        $e = Test-CredentialRestored $r
        if ($r.Text -notlike '*PASS [[]negative-credential-broken]*') { $e += 'never got as far as breaking the credential' }
        if ($r.Text -notlike '*PASS [[]negative-credential-restored]*') { $e += 'no restore confirmation after the interrupt' }
        $e
    }

    # ---- recovery switch
    S 'restore-switch-recovers-a-broken-credential' @() {
        # A previous run died: the credential is broken, the pod runs broken, the saved original is on disk.
        param($d)
        $d.ScriptArgs = @{ RestoreFederatedCredential = $true; PropagationTimeoutSeconds = 20 }
        $d.Federated[0].subject = "$originalSubject-negative-test"
        $d.ExtraFiles = @{ 'federated-credential-original.json' = $backupJson }
        $d.Server = @{ AuthBroken = $true }
    } -Ids (@('negative-credential-restored') + $restoredIds) -Check {
        param($r)
        $e = Test-CredentialRestored $r
        if (@(Get-Restarts $r).Count -lt 1) { $e += 'pod was not restarted after the restore' }
        if ($r.Server.AuthBroken) { $e += 'the pod is still broken' }
        $e
    }
    S 'restore-switch-without-saved-state-does-nothing' @() { param($d) $d.ScriptArgs = @{ RestoreFederatedCredential = $true } } -ExpectExit 0 -Check {
        param($r)
        $e = @(& $unchanged $r)
        if ($r.Text -notlike '*nothing to restore*') { $e += 'does not say there is nothing to restore' }
        $e
    }
    S 'restore-switch-refuses-foreign-backup' @() {
        param($d)
        $d.ScriptArgs = @{ RestoreFederatedCredential = $true }
        $d.ExtraFiles = @{ 'federated-credential-original.json' = ($backupJson -replace 'ehpoc-id-test', 'some-other-identity') }
    } -ExpectExit 2 -Check $unchanged
)
$scenarios += $negativeScenarios

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
