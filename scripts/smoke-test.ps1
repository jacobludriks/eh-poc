#Requires -Version 7.3
<#
.SYNOPSIS
  Smoke test for the passwordless Event Hubs PoC: proves the claims against the DEPLOYED system and prints an
  evidence record. Exit code 0 only if every assertion passes; a non-zero exit names the failed assertion(s).

.DESCRIPTION
  Two seams only (see .scratch/eventhubs-passwordless-poc/spec.md, Testing Decisions):
    1. The deployed app's HTTP surface (readiness, /api/diagnostics, /api/events, the SSE stream, clear).
    2. Azure resource properties through the Azure CLI, and the `poc` namespace through kubectl.

  Needs only the Azure CLI (logged in), kubectl and PowerShell 7.3+. No Java, no Docker. Resource names come from
  the state written by deploy.ps1 (.deploy-state/deploy-outputs.json); the kubeconfig there is used for kubectl, so
  your ~/.kube is not touched.

  Role assignments can take up to about five minutes to take effect after a deploy, so the wait for a working
  consumer and the first send are retried within -ReadyTimeoutSeconds (default 360). Everything else is bounded
  by -PollTimeoutSeconds.

  Every assertion prints one `PASS [id] ...` or `FAIL [id] ...` line and the script ends with a summary line.
  Tokens and secret values are never read into the output: only booleans, class names, ids of events the script
  itself sent, and resource names are printed.

  Structure (for extending, e.g. the negative federated-credential test, ticket 06):
    - Assert-That             records one assertion (id, label, outcome, detail). Everything goes through it.
    - Invoke-Api / Open-Sse   the only HTTP code. Invoke-Az / Invoke-Kubectl are the only az / kubectl calls.
    - Wait-ForApp / Wait-ForConsumer / Send-EventWithRetry / Test-RoundTrip   reusable HTTP building blocks.
    - Test-Diagnostics / Test-Clear / Test-Infrastructure   assertion groups.
    - Invoke-SmokeTest        the standard run, called at the bottom. An opt-in mode is a new [switch] parameter
                              plus a branch in Invoke-SmokeTest that calls the same building blocks.
  scripts/tests/smoke-test.tests.ps1 runs this script against a fake app and fake az/kubectl.
#>
[CmdletBinding()]
param(
    # State written by scripts/deploy.ps1 (resource names, endpoint, kubeconfig path).
    [string] $StateFile = (Join-Path (Join-Path (Split-Path -Parent $PSScriptRoot) '.deploy-state') 'deploy-outputs.json'),
    # Override the app URL (default: `endpoint` in the state file, else the Service's external IP).
    [string] $Endpoint,
    # Budget for the app to become ready, the consumer to get partitions, and the first send to be accepted.
    # Role assignments can take about five minutes to propagate, so the default leaves a margin above that.
    [ValidateRange(1, 3600)]
    [int] $ReadyTimeoutSeconds = 360,
    # Budget for each "event appears" / "view is empty" poll.
    [ValidateRange(1, 600)]
    [int] $PollTimeoutSeconds = 30,
    [ValidateRange(1, 60)]
    [int] $PollIntervalSeconds = 3
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# The values the PoC must exhibit. Written here, not derived from deploy state, so a wrong deployment cannot
# define its own pass criteria.
$ExpectedHandlerClass = 'com.azure.spring.cloud.service.implementation.kafka.KafkaOAuth2AuthenticateCallbackHandler'
$ExpectedFederatedSubject = 'system:serviceaccount:poc:ehpoc'
$ExpectedRoles = @('Azure Event Hubs Data Receiver', 'Azure Event Hubs Data Sender')

$script:Results = [System.Collections.Generic.List[object]]::new()
$script:BaseUrl = $null
$script:State = $null

# ================================================================== assertions
function Assert-That {
    # Records and prints one assertion. -Ok false = FAIL. Detail is a short factual note, never a secret.
    param([string] $Id, [string] $Label, [bool] $Ok, [string] $Detail = '')
    $script:Results.Add([pscustomobject]@{ Id = $Id; Label = $Label; Ok = $Ok; Detail = $Detail })
    $text = if ($Detail) { "$Label ($Detail)" } else { $Label }
    if ($Ok) { Write-Host "PASS [$Id] $text" -ForegroundColor Green }
    else { Write-Host "FAIL [$Id] $text" -ForegroundColor Red }
}

function Assert-NotEvaluated {
    # Counts as a failure: a claim that could not be checked is not proven.
    param([string[]] $Id, [string] $Because)
    foreach ($i in $Id) { Assert-That -Id $i -Label 'not evaluated' -Ok $false -Detail $Because }
}

function Get-Prop {
    # Safe dotted-path read from parsed JSON; $null if any segment is missing (StrictMode-proof).
    param($Object, [string] $Path)
    $cur = $Object
    foreach ($segment in $Path.Split('.')) {
        if ($null -eq $cur) { return $null }
        $p = $cur.PSObject.Properties[$segment]
        if ($null -eq $p) { return $null }
        $cur = $p.Value
    }
    return $cur
}

function Wait-Until {
    # Polls the condition until it returns something truthy (that value is returned) or the budget is spent ($null).
    param([scriptblock] $Condition, [int] $TimeoutSeconds, [int] $IntervalMilliseconds)
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while ($true) {
        $value = & $Condition
        if ($value) { return $value }
        if ([DateTime]::UtcNow -ge $deadline) { return $null }
        Start-Sleep -Milliseconds $IntervalMilliseconds
    }
}

# ================================================================== external seams
function Invoke-Api {
    # The only plain-HTTP call. Never throws: a connection failure is Status 0.
    param([string] $Method, [string] $Path, $Body = $null, [int] $TimeoutSec = 30)
    $params = @{ Method = $Method; Uri = "$script:BaseUrl$Path"; TimeoutSec = $TimeoutSec; SkipHttpErrorCheck = $true }
    if ($null -ne $Body) { $params.ContentType = 'application/json'; $params.Body = ($Body | ConvertTo-Json -Compress) }
    try { $r = Invoke-WebRequest @params }
    catch { return [pscustomobject]@{ Status = 0; Json = $null; Error = $_.Exception.Message } }
    $json = $null
    try { if ($r.Content) { $json = $r.Content | ConvertFrom-Json } } catch { Write-Verbose "non-JSON body from $Path" }
    [pscustomobject]@{ Status = [int]$r.StatusCode; Json = $json; Error = $null }
}

function Open-Sse {
    # Opens GET <path> as a Server-Sent Events stream and returns a connection object that is read without threads:
    # Read-Sse pulls whatever lines have arrived. Throws if the stream does not answer within 15 s.
    param([string] $Path)
    $client = [System.Net.Http.HttpClient]::new()
    $client.Timeout = [System.Threading.Timeout]::InfiniteTimeSpan
    $request = [System.Net.Http.HttpRequestMessage]::new([System.Net.Http.HttpMethod]::Get, "$script:BaseUrl$Path")
    $request.Headers.Accept.ParseAdd('text/event-stream')
    $cts = [System.Threading.CancellationTokenSource]::new([TimeSpan]::FromSeconds(15))
    try {
        $response = $client.SendAsync($request, [System.Net.Http.HttpCompletionOption]::ResponseHeadersRead, $cts.Token).GetAwaiter().GetResult()
        if (-not $response.IsSuccessStatusCode) { throw "HTTP $([int]$response.StatusCode)" }
        $stream = $response.Content.ReadAsStreamAsync().GetAwaiter().GetResult()
    }
    catch { $client.Dispose(); throw "could not open SSE stream ${Path}: $($_.Exception.Message)" }
    [pscustomobject]@{
        Client = $client; Response = $response; Reader = [System.IO.StreamReader]::new($stream)
        Pending = $null; Closed = $false; Comments = [System.Collections.Generic.List[string]]::new()
        Events = [System.Collections.Generic.List[object]]::new()
        Name = 'message'; Data = [System.Collections.Generic.List[string]]::new()
    }
}

function Read-Sse {
    # Consumes every SSE line that has already arrived (never blocks) and files complete events into .Events as
    # @{Name; Json}. The wire format has no space after the colon, but one is tolerated.
    param($Connection)
    while (-not $Connection.Closed) {
        if ($null -eq $Connection.Pending) { $Connection.Pending = $Connection.Reader.ReadLineAsync() }
        if (-not $Connection.Pending.IsCompleted) { return }
        try { $line = $Connection.Pending.Result } catch { $Connection.Closed = $true; return }
        $Connection.Pending = $null
        if ($null -eq $line) { $Connection.Closed = $true; return }
        if ($line -eq '') {
            if ($Connection.Data.Count -gt 0) {
                $json = $null
                try { $json = ($Connection.Data -join "`n") | ConvertFrom-Json } catch { Write-Verbose 'non-JSON SSE data' }
                $Connection.Events.Add([pscustomobject]@{ Name = $Connection.Name; Json = $json })
            }
            $Connection.Name = 'message'; $Connection.Data.Clear()
        }
        elseif ($line.StartsWith(':')) { $Connection.Comments.Add($line) }
        elseif ($line -match '^event:\s?(.*)$') { $Connection.Name = $Matches[1] }
        elseif ($line -match '^data:\s?(.*)$') { $Connection.Data.Add($Matches[1]) }
    }
}

function Close-Sse {
    param($Connection)
    if ($null -eq $Connection) { return }
    try { $Connection.Client.Dispose() } catch { Write-Verbose 'SSE dispose failed' }
}

function Invoke-ExternalJson {
    # Runs a native command, returns its stdout parsed as JSON. Stderr is kept out of stdout and only surfaced
    # (first line) when the command fails. Never echoes stdout, which for kubectl could hold secret data.
    param([string] $Tool, [string[]] $Arguments)
    $output = & $Tool @Arguments 2>&1
    $code = $LASTEXITCODE
    $err = @($output | Where-Object { $_ -is [System.Management.Automation.ErrorRecord] } | ForEach-Object { "$_" })
    $out = @($output | Where-Object { $_ -isnot [System.Management.Automation.ErrorRecord] } | ForEach-Object { "$_" })
    if ($code -ne 0) {
        $why = if ($err.Count) { $err[0] } else { 'no error output' }
        throw "$Tool exited with code ${code}: $why"
    }
    $text = ($out -join "`n").Trim()
    if (-not $text) { return $null }
    return ($text | ConvertFrom-Json)
}

function Invoke-Az { param([string[]] $Arguments) Invoke-ExternalJson -Tool 'az' -Arguments ($Arguments + @('-o', 'json')) }

function Invoke-Kubectl {
    # kubectl against the kubeconfig deploy.ps1 saved, never against ~/.kube.
    param([string[]] $Arguments)
    Invoke-ExternalJson -Tool 'kubectl' -Arguments (@('--kubeconfig', $script:State.kubeconfig) + $Arguments)
}

# ================================================================== state and endpoint
function Read-DeployState {
    if (-not (Test-Path -LiteralPath $StateFile)) {
        throw "Deploy state not found at $StateFile. Run scripts/deploy.ps1 first (or pass -StateFile)."
    }
    $state = Get-Content -Raw -LiteralPath $StateFile | ConvertFrom-Json
    foreach ($key in 'resourceGroup', 'eventHubsNamespace', 'eventHubName', 'identityName', 'identityPrincipalId', 'kubeconfig') {
        if (-not (Get-Prop $state $key)) { throw "Deploy state $StateFile has no '$key'. Re-run scripts/deploy.ps1." }
    }
    if (-not (Test-Path -LiteralPath $state.kubeconfig)) { throw "kubeconfig $($state.kubeconfig) from the deploy state does not exist. Re-run scripts/deploy.ps1." }
    return $state
}

function Resolve-AppUrl {
    if ($Endpoint) { return $Endpoint.TrimEnd('/') }
    $fromState = Get-Prop $script:State 'endpoint'
    if ($fromState) { return ([string]$fromState).TrimEnd('/') }
    $namespace = Get-Prop $script:State 'namespace'
    if (-not $namespace) { $namespace = 'poc' }
    $service = Invoke-Kubectl -Arguments @('get', 'service', 'ehpoc', '-n', $namespace, '-o', 'json')
    $address = Get-Prop $service 'status.loadBalancer.ingress'
    $first = @($address)[0]
    if ($first -and (Get-Prop $first 'ip')) { return "http://$($first.ip)" }
    throw 'No app URL: the deploy state has no endpoint and the Service has no external IP yet.'
}

# ================================================================== HTTP building blocks
function Wait-ForApp {
    $r = Wait-Until -TimeoutSeconds $ReadyTimeoutSeconds -IntervalMilliseconds ($PollIntervalSeconds * 1000) -Condition {
        $x = Invoke-Api -Method GET -Path '/actuator/health/readiness' -TimeoutSec 10
        if ($x.Status -eq 200 -and (Get-Prop $x.Json 'status') -eq 'UP') { $x }
    }
    Assert-That -Id 'app-ready' -Label "GET $script:BaseUrl/actuator/health/readiness is UP" -Ok ($null -ne $r) `
        -Detail $(if ($r) { 'status UP' } else { "not UP within ${ReadyTimeoutSeconds}s" })
    return ($null -ne $r)
}

function Get-Diagnostics {
    $x = Invoke-Api -Method GET -Path '/api/diagnostics'
    if ($x.Status -eq 200 -and $x.Json) { $script:LastDiag = $x.Json; return $x.Json }
    return $null
}

function Wait-ForConsumer {
    # The consumer group starts at `latest`: an event sent before partitions are assigned is never delivered.
    # Role propagation can delay the first successful login, hence the long budget.
    Write-Host "  waiting up to ${ReadyTimeoutSeconds}s for the consumer to get partitions (role propagation can take ~5 min)..."
    $diag = Wait-Until -TimeoutSeconds $ReadyTimeoutSeconds -IntervalMilliseconds ($PollIntervalSeconds * 1000) -Condition {
        $d = Get-Diagnostics
        if ($d -and (Get-Prop $d 'consumer.listenerRunning') -eq $true -and [int](Get-Prop $d 'consumer.assignedPartitions') -gt 0) { $d }
    }
    if ($diag) {
        Assert-That -Id 'consumer-ready' -Label 'consumer listener running with partitions assigned' -Ok $true `
            -Detail ("assignedPartitions={0}" -f (Get-Prop $diag 'consumer.assignedPartitions'))
        return $true
    }
    $last = $script:LastDiag
    $detail = if ($last) {
        "not ready within ${ReadyTimeoutSeconds}s: listenerRunning={0} assignedPartitions={1} lastErrorClass={2}" -f `
            (Get-Prop $last 'consumer.listenerRunning'), (Get-Prop $last 'consumer.assignedPartitions'), (Get-Prop $last 'consumer.lastErrorClass')
    }
    else { "/api/diagnostics never answered within ${ReadyTimeoutSeconds}s" }
    Assert-That -Id 'consumer-ready' -Label 'consumer listener running with partitions assigned' -Ok $false -Detail $detail
    return $false
}

function Send-EventWithRetry {
    # POST /api/events, retrying 5xx / connection errors (role propagation) until the budget is spent. A 4xx is the
    # app rejecting the request: not retried. Returns the final response.
    param([string] $Message)
    $deadline = [DateTime]::UtcNow.AddSeconds($ReadyTimeoutSeconds)
    while ($true) {
        $r = Invoke-Api -Method POST -Path '/api/events' -Body @{ message = $Message; source = 'api' } -TimeoutSec 60
        if ($r.Status -eq 201) { return $r }
        if ($r.Status -ge 400 -and $r.Status -lt 500) { return $r }
        if ([DateTime]::UtcNow -ge $deadline) { return $r }
        $why = if ($r.Status -eq 0) { 'no response' } else { "HTTP $($r.Status) reason=$(Get-Prop $r.Json 'reason')" }
        Write-Host "  send not accepted yet ($why); retrying..." -ForegroundColor Yellow
        Start-Sleep -Seconds $PollIntervalSeconds
    }
}

function Test-RoundTrip {
    # Sends one event and asserts it comes back through the consumer, via the list and via an SSE stream opened
    # BEFORE the send. Returns nothing; results are recorded.
    $sse = $null
    try {
        $sseOpened = $false; $sseError = $null
        try {
            $sse = Open-Sse -Path '/api/events/stream'
            $null = Wait-Until -TimeoutSeconds 10 -IntervalMilliseconds 200 -Condition { Read-Sse $sse; $sse.Comments -contains ':connected' }
            $sseOpened = $sse.Comments -contains ':connected'
            if (-not $sseOpened) { $sseError = 'stream opened but no :connected comment' }
        }
        catch { $sseError = $_.Exception.Message }

        $message = "smoke-test-$([guid]::NewGuid().ToString('N').Substring(0, 8))"
        $send = Send-EventWithRetry -Message $message
        $sent = Get-Prop $send.Json 'event'
        $ackOk = $send.Status -eq 201 -and (Get-Prop $sent 'id') -and ((Get-Prop $send.Json 'partition') -match '^\d+$') -and ((Get-Prop $send.Json 'offset') -match '^\d+$')
        $detail = if ($send.Status -eq 201) {
            "HTTP 201 id=$(Get-Prop $sent 'id') partition=$(Get-Prop $send.Json 'partition') offset=$(Get-Prop $send.Json 'offset')"
        }
        else {
            $r = Get-Prop $send.Json 'reason'
            "HTTP $($send.Status)$(if ($r) { " reason=$r" })$(if ($send.Error) { " $($send.Error)" })"
        }
        Assert-That -Id 'send-accepted' -Label 'POST /api/events returns 201 with partition and offset' -Ok ([bool]$ackOk) -Detail $detail
        if (-not $sent -or $send.Status -ne 201) {
            Assert-NotEvaluated -Id 'list-roundtrip', 'sse-delivery' -Because 'no event was accepted'
            return
        }
        $id = [string]$sent.id

        # Through the consumer, via the list.
        $found = Wait-Until -TimeoutSeconds $PollTimeoutSeconds -IntervalMilliseconds 1000 -Condition {
            $l = Invoke-Api -Method GET -Path '/api/events'
            if ($l.Status -eq 200) { @(Get-Prop $l.Json 'events') | Where-Object { $_ -and (Get-Prop $_ 'id') -eq $id } | Select-Object -First 1 }
        }
        if ($found) {
            $wrong = @()
            if ((Get-Prop $found 'message') -ne $message) { $wrong += 'message' }
            if ((Get-Prop $found 'key') -ne $id) { $wrong += 'key' }
            $ackP = Get-Prop $send.Json 'partition'; $ackO = Get-Prop $send.Json 'offset'
            if ($null -ne $ackP -and "$(Get-Prop $found 'partition')" -ne "$ackP") { $wrong += 'partition' }
            if ($null -ne $ackO -and "$(Get-Prop $found 'offset')" -ne "$ackO") { $wrong += 'offset' }
            Assert-That -Id 'list-roundtrip' -Label 'event returned by GET /api/events with the same id, message, key, partition and offset' -Ok ($wrong.Count -eq 0) `
                -Detail $(if ($wrong) { "differs in: $($wrong -join ', ')" } else { "id=$id" })
        }
        else {
            Assert-That -Id 'list-roundtrip' -Label 'event returned by GET /api/events with the same id, message, key, partition and offset' -Ok $false `
                -Detail "id=$id not in the list within ${PollTimeoutSeconds}s"
        }

        # Through the consumer, via the stream that was open before the send.
        if (-not $sseOpened) {
            Assert-That -Id 'sse-delivery' -Label 'event pushed over GET /api/events/stream' -Ok $false -Detail "stream not usable: $sseError"
        }
        else {
            $hit = Wait-Until -TimeoutSeconds $PollTimeoutSeconds -IntervalMilliseconds 300 -Condition {
                Read-Sse $sse
                $sse.Events | Where-Object { $_.Name -eq 'event' -and (Get-Prop $_.Json 'id') -eq $id } | Select-Object -First 1
            }
            $ok = $null -ne $hit -and (Get-Prop $hit.Json 'message') -eq $message
            Assert-That -Id 'sse-delivery' -Label 'event pushed over GET /api/events/stream (opened before the send)' -Ok ([bool]$ok) `
                -Detail $(if ($ok) { "id=$id" } elseif ($hit) { 'event arrived with a different message' } else { "id=$id not pushed within ${PollTimeoutSeconds}s" })
        }
    }
    finally { Close-Sse $sse }
}

function Test-Diagnostics {
    param($Diag)
    if (-not $Diag) {
        Assert-NotEvaluated -Id 'sasl-mechanism', 'login-callback-handler', 'passwordless-autoconfig', 'no-credential-properties', 'private-endpoint-ip', 'workload-identity-env' -Because '/api/diagnostics unavailable'
        return
    }
    $sasl = Get-Prop $Diag 'kafka.saslMechanism'
    Assert-That -Id 'sasl-mechanism' -Label 'kafka.saslMechanism is OAUTHBEARER' -Ok ($sasl -eq 'OAUTHBEARER') -Detail "saslMechanism=$sasl"

    $handler = Get-Prop $Diag 'kafka.loginCallbackHandlerClass'
    Assert-That -Id 'login-callback-handler' -Label "kafka.loginCallbackHandlerClass is Azure's KafkaOAuth2AuthenticateCallbackHandler" `
        -Ok ($handler -eq $ExpectedHandlerClass) -Detail "class=$(if ($handler) { ($handler -split '\.')[-1] } else { 'null' })"

    $applied = Get-Prop $Diag 'kafka.passwordlessAutoConfigApplied'
    Assert-That -Id 'passwordless-autoconfig' -Label "Spring Cloud Azure's passwordless auto-configuration is applied" -Ok ($applied -eq $true) `
        -Detail "passwordlessAutoConfigApplied=$applied$(if ($applied -ne $true) { ' problems=' + ((@(Get-Prop $Diag 'kafka.problems')) -join '; ') })"

    $cred = Get-Prop $Diag 'kafka.credentialPropertiesConfigured'
    Assert-That -Id 'no-credential-properties' -Label 'no credential properties are configured in the app' -Ok ($cred -eq $false) `
        -Detail "credentialPropertiesConfigured=$cred$(if ($cred -ne $false) { ' names=' + ((@(Get-Prop $Diag 'kafka.credentialPropertyNames')) -join ', ') })"

    # The flag is the app's own verdict; the address is checked independently so the two must agree.
    $ip = Get-Prop $Diag 'eventHubs.resolvedIp'
    $flag = Get-Prop $Diag 'eventHubs.resolvedIpIsPrivate'
    $rfc1918 = [bool]($ip -match '^(10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.)')
    Assert-That -Id 'private-endpoint-ip' -Label 'Event Hubs host resolves to a private IP (private endpoint)' -Ok ($flag -eq $true -and $rfc1918) `
        -Detail $(if ($ip) { "$(Get-Prop $Diag 'eventHubs.bootstrapHost') -> $ip, resolvedIpIsPrivate=$flag" } else { "not resolved: $(Get-Prop $Diag 'eventHubs.resolutionError')" })

    $missing = @('AZURE_CLIENT_ID', 'AZURE_TENANT_ID', 'AZURE_FEDERATED_TOKEN_FILE' | Where-Object { (Get-Prop $Diag "workloadIdentity.$_") -ne $true })
    Assert-That -Id 'workload-identity-env' -Label 'workload identity environment injected (client id, tenant id, token file)' -Ok ($missing.Count -eq 0) `
        -Detail $(if ($missing) { "missing: $($missing -join ', ')" } else { 'all three set' })
}

function Test-Clear {
    $clear = Invoke-Api -Method POST -Path '/api/events/clear'
    $purged = Get-Prop $clear.Json 'eventHubPurged'
    Assert-That -Id 'clear-view-only' -Label 'POST /api/events/clear clears the in-memory view only (eventHubPurged false)' `
        -Ok ($clear.Status -eq 200 -and $purged -eq $false) -Detail "HTTP $($clear.Status) eventHubPurged=$purged cleared=$(Get-Prop $clear.Json 'cleared')"

    $seen = @{ Count = $null }   # a hashtable, because the condition runs in a child scope
    $empty = Wait-Until -TimeoutSeconds $PollTimeoutSeconds -IntervalMilliseconds 1000 -Condition {
        $l = Invoke-Api -Method GET -Path '/api/events'
        $seen.Count = Get-Prop $l.Json 'count'
        if ($l.Status -eq 200 -and "$($seen.Count)" -eq '0') { 'empty' }
    }
    Assert-That -Id 'clear-empties-view' -Label 'GET /api/events returns count 0 after clear' -Ok ($null -ne $empty) -Detail "count=$($seen.Count)"
}

# ================================================================== Azure CLI / kubectl assertions
function Test-Infrastructure {
    $rg = $script:State.resourceGroup
    $ns = $script:State.eventHubsNamespace
    $hub = $script:State.eventHubName
    $k8sNamespace = if (Get-Prop $script:State 'namespace') { $script:State.namespace } else { 'poc' }

    try {
        $nsInfo = Invoke-Az -Arguments @('eventhubs', 'namespace', 'show', '--resource-group', $rg, '--name', $ns)
        $local = Get-Prop $nsInfo 'disableLocalAuth'
        $public = Get-Prop $nsInfo 'publicNetworkAccess'
        Assert-That -Id 'local-auth-disabled' -Label "namespace $ns has local (key/SAS) authentication disabled" -Ok ($local -eq $true) -Detail "disableLocalAuth=$local"
        Assert-That -Id 'public-network-disabled' -Label "namespace $ns has public network access disabled" -Ok ($public -eq 'Disabled') -Detail "publicNetworkAccess=$public"
    }
    catch {
        $why = "az failed: $($_.Exception.Message)"
        Assert-That -Id 'local-auth-disabled' -Label "namespace $ns has local (key/SAS) authentication disabled" -Ok $false -Detail $why
        Assert-That -Id 'public-network-disabled' -Label "namespace $ns has public network access disabled" -Ok $false -Detail $why
    }

    try {
        $assignments = @(Invoke-Az -Arguments @('role', 'assignment', 'list', '--assignee', $script:State.identityPrincipalId, '--all'))
        $roles = @($assignments | ForEach-Object { Get-Prop $_ 'roleDefinitionName' } | Sort-Object)
        Assert-That -Id 'roles-sender-receiver-only' -Label 'identity holds exactly Event Hubs Data Sender and Data Receiver, nothing else' `
            -Ok ((($roles -join '|') -eq ($ExpectedRoles -join '|'))) -Detail "roles: $(if ($roles) { $roles -join ', ' } else { 'none' })"
        $scopePattern = "(?i)/namespaces/$([regex]::Escape($ns))/eventhubs/$([regex]::Escape($hub))$"
        $outside = @($assignments | Where-Object { (Get-Prop $_ 'scope') -notmatch $scopePattern } | ForEach-Object { "$(Get-Prop $_ 'roleDefinitionName') at $(([string](Get-Prop $_ 'scope')) -replace '^.*/providers/', '')" })
        Assert-That -Id 'roles-scoped-to-event-hub' -Label "every role assignment is scoped to event hub $hub (not namespace or resource group)" `
            -Ok ($assignments.Count -gt 0 -and $outside.Count -eq 0) `
            -Detail $(if ($assignments.Count -eq 0) { 'no assignments found' } elseif ($outside) { "outside the event hub: $($outside -join '; ')" } else { "$($assignments.Count) assignment(s) at $ns/$hub" })
    }
    catch {
        $why = "az failed: $($_.Exception.Message)"
        Assert-That -Id 'roles-sender-receiver-only' -Label 'identity holds exactly Event Hubs Data Sender and Data Receiver, nothing else' -Ok $false -Detail $why
        Assert-That -Id 'roles-scoped-to-event-hub' -Label "every role assignment is scoped to event hub $hub (not namespace or resource group)" -Ok $false -Detail $why
    }

    try {
        $creds = @(Invoke-Az -Arguments @('identity', 'federated-credential', 'list', '--identity-name', $script:State.identityName, '--resource-group', $rg))
        $subjects = @($creds | ForEach-Object { Get-Prop $_ 'subject' })
        Assert-That -Id 'federated-subject' -Label "federated credential subject is $ExpectedFederatedSubject (and no other credential)" `
            -Ok ($subjects.Count -eq 1 -and $subjects[0] -eq $ExpectedFederatedSubject) `
            -Detail $(if ($subjects) { "subject(s): $($subjects -join ', ')" } else { 'no federated credential' })
    }
    catch {
        Assert-That -Id 'federated-subject' -Label "federated credential subject is $ExpectedFederatedSubject (and no other credential)" -Ok $false -Detail "az failed: $($_.Exception.Message)"
    }

    try {
        $secrets = Invoke-Kubectl -Arguments @('get', 'secrets', '-n', $k8sNamespace, '-o', 'json')
        # Only names and types are read; secret data is never touched or printed. Service-account tokens are
        # cluster-managed, not user secrets.
        $user = @(@(Get-Prop $secrets 'items') | Where-Object { $_ -and (Get-Prop $_ 'type') -ne 'kubernetes.io/service-account-token' } |
                ForEach-Object { "$(Get-Prop $_ 'metadata.name') ($(Get-Prop $_ 'type'))" })
        Assert-That -Id 'no-user-secrets' -Label "namespace $k8sNamespace contains no secrets (service-account tokens ignored)" -Ok ($user.Count -eq 0) `
            -Detail $(if ($user) { "found: $($user -join ', ')" } else { 'none' })
    }
    catch {
        Assert-That -Id 'no-user-secrets' -Label "namespace $k8sNamespace contains no secrets (service-account tokens ignored)" -Ok $false -Detail "kubectl failed: $($_.Exception.Message)"
    }
}

# ================================================================== run
function Show-Summary {
    $failed = @($script:Results | Where-Object { -not $_.Ok })
    $passed = $script:Results.Count - $failed.Count
    Write-Host ''
    if ($failed.Count) {
        Write-Host "Summary: $passed passed, $($failed.Count) failed ($($script:Results.Count) assertions). Failed: $(($failed | ForEach-Object Id) -join ', ')" -ForegroundColor Red
    }
    else { Write-Host "Summary: $passed passed, 0 failed ($($script:Results.Count) assertions)." -ForegroundColor Green }
}

function Invoke-SmokeTest {
    foreach ($tool in 'az', 'kubectl') {
        if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) { throw "$tool not found on PATH (needed for the infrastructure assertions)." }
    }
    $script:State = Read-DeployState
    $script:BaseUrl = Resolve-AppUrl
    Write-Host "Smoke test: app $script:BaseUrl, resource group $($script:State.resourceGroup), namespace $($script:State.eventHubsNamespace)" -ForegroundColor Cyan

    Write-Host "`n-- App (HTTP)" -ForegroundColor Cyan
    $script:LastDiag = $null
    if (Wait-ForApp) {
        $consumerReady = Wait-ForConsumer
        if ($consumerReady) { Test-RoundTrip }
        else { Assert-NotEvaluated -Id 'send-accepted', 'list-roundtrip', 'sse-delivery' -Because 'the consumer is not ready (events sent now would not be delivered)' }
        Test-Diagnostics -Diag (Get-Diagnostics)
        Test-Clear
    }
    else {
        Assert-NotEvaluated -Id 'consumer-ready', 'send-accepted', 'list-roundtrip', 'sse-delivery', 'sasl-mechanism', 'login-callback-handler', `
            'passwordless-autoconfig', 'no-credential-properties', 'private-endpoint-ip', 'workload-identity-env', 'clear-view-only', 'clear-empties-view' `
            -Because 'the app is not ready'
    }

    Write-Host "`n-- Infrastructure (Azure CLI, kubectl)" -ForegroundColor Cyan
    Test-Infrastructure
    Show-Summary
}

try { Invoke-SmokeTest }
catch {
    # Setup problem (no state, tools missing): nothing was proven.
    Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
    exit 2
}
exit ([int](@($script:Results | Where-Object { -not $_.Ok }).Count -gt 0))
