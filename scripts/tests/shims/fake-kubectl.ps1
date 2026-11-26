# Fake `kubectl` for scripts/tests/smoke-test.tests.ps1 (see fake-az.ps1). Never talks to a cluster.
$dir = $env:SMOKE_FAKE_DIR
$line = ($args -join ' ')
Add-Content -Path (Join-Path $dir 'calls.log') -Value "kubectl $line"

if ($line -match ' get secrets ') { Get-Content -Raw -Path (Join-Path $dir 'secrets.json'); exit 0 }
if ($line -match ' get service ') { Get-Content -Raw -Path (Join-Path $dir 'service.json'); exit 0 }

[Console]::Error.WriteLine("fake kubectl: unexpected command: kubectl $line")
exit 2
