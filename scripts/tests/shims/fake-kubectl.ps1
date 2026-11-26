# Fake `kubectl` for scripts/tests/smoke-test.tests.ps1 (see fake-az.ps1). Never talks to a cluster.
# `rollout restart` tells the fake app (the in-process fake of the deployed app, $env:SMOKE_FAKE_APP_URL) that its
# pod restarted, unless the file fail-rollout-restart exists in the scenario directory (simulated kubectl failure).
$dir = $env:SMOKE_FAKE_DIR
$line = ($args -join ' ')
Add-Content -Path (Join-Path $dir 'calls.log') -Value "kubectl $line"

if ($line -match ' get secrets ') { Get-Content -Raw -Path (Join-Path $dir 'secrets.json'); exit 0 }
if ($line -match ' get service ') { Get-Content -Raw -Path (Join-Path $dir 'service.json'); exit 0 }

if ($line -match ' rollout restart deployment/ehpoc ') {
    if (Test-Path (Join-Path $dir 'fail-rollout-restart')) {
        [Console]::Error.WriteLine('fake kubectl: simulated rollout restart failure')
        exit 1
    }
    $null = Invoke-WebRequest -Method POST -Uri "$($env:SMOKE_FAKE_APP_URL)/__fake/restart" -SkipHttpErrorCheck
    Write-Output 'deployment.apps/ehpoc restarted'
    exit 0
}
if ($line -match ' rollout status deployment/ehpoc ') {
    Write-Output 'deployment "ehpoc" successfully rolled out'
    exit 0
}

[Console]::Error.WriteLine("fake kubectl: unexpected command: kubectl $line")
exit 2
