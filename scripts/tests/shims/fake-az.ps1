# Fake `az` for scripts/tests/smoke-test.tests.ps1. Serves canned JSON from $env:SMOKE_FAKE_DIR and logs each call
# to calls.log there. Anything it does not know is an error, so the script under test cannot reach for something
# unplanned. Never talks to Azure.
#
# `identity federated-credential update` rewrites federated-credentials.json in that directory (so the next `list`
# shows the change, and the fake app reads the same file at a pod restart). update-fail-calls.txt, if present, is a
# comma list of update call numbers (1 = first update) that fail with exit 1, to simulate a flaky or failing restore.
$dir = $env:SMOKE_FAKE_DIR
$line = ($args -join ' ')
Add-Content -Path (Join-Path $dir 'calls.log') -Value "az $line"

function Send([string] $File) { Get-Content -Raw -Path (Join-Path $dir $File); exit 0 }

if ($line -match '^eventhubs namespace show ')              { Send 'namespace.json' }
if ($line -match '^role assignment list ')                  { Send 'role-assignments.json' }
if ($line -match '^identity federated-credential list ')    { Send 'federated-credentials.json' }

if ($line -match '^identity federated-credential update ') {
    $countFile = Join-Path $dir 'update-count.txt'
    $n = 1 + $(if (Test-Path $countFile) { [int](Get-Content -Raw $countFile) } else { 0 })
    Set-Content -Path $countFile -Value $n
    $failFile = Join-Path $dir 'update-fail-calls.txt'
    if ((Test-Path $failFile) -and ((Get-Content -Raw $failFile).Trim() -split ',') -contains "$n") {
        [Console]::Error.WriteLine("fake az: simulated failure of update call $n")
        exit 1
    }
    # Parse --name/--subject/--issuer/--audiences (the latter takes one or more values).
    $opts = @{}; $current = $null
    foreach ($a in $args) {
        if ($a -like '--*') { $current = $a.Substring(2); $opts[$current] = @() }
        elseif ($a -like '-?') { $current = $null }   # a short option such as -o ends the value list
        elseif ($current) { $opts[$current] += $a }
    }
    $file = Join-Path $dir 'federated-credentials.json'
    $creds = @(Get-Content -Raw $file | ConvertFrom-Json)
    $hit = $creds | Where-Object { $_.name -eq $opts['name'][0] }
    if (-not $hit) { [Console]::Error.WriteLine("fake az: no federated credential named $($opts['name'])"); exit 3 }
    if ($opts.ContainsKey('subject')) { $hit.subject = $opts['subject'][0] }
    if ($opts.ContainsKey('issuer')) { $hit.issuer = $opts['issuer'][0] }
    if ($opts.ContainsKey('audiences')) { $hit.audiences = @($opts['audiences']) }
    ConvertTo-Json -InputObject @($creds) -Depth 10 | Set-Content -Path $file
    ConvertTo-Json -InputObject $hit -Depth 10
    exit 0
}

[Console]::Error.WriteLine("fake az: unexpected command: az $line")
exit 2
