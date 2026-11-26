# Fake `az` for scripts/tests/smoke-test.tests.ps1. Serves canned JSON from $env:SMOKE_FAKE_DIR and logs each call
# to calls.log there. Anything it does not know is an error, so the script under test cannot reach for something
# unplanned. Never talks to Azure.
$dir = $env:SMOKE_FAKE_DIR
$line = ($args -join ' ')
Add-Content -Path (Join-Path $dir 'calls.log') -Value "az $line"

function Send([string] $File) { Get-Content -Raw -Path (Join-Path $dir $File); exit 0 }

if ($line -match '^eventhubs namespace show ')              { Send 'namespace.json' }
if ($line -match '^role assignment list ')                  { Send 'role-assignments.json' }
if ($line -match '^identity federated-credential list ')    { Send 'federated-credentials.json' }

[Console]::Error.WriteLine("fake az: unexpected command: az $line")
exit 2
