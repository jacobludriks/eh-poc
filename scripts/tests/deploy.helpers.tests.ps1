#Requires -Version 7.3
<#
.SYNOPSIS
  Local checks for scripts/deploy.ps1 that need no Azure: every script parses, and the pure helper functions
  (IP validation, role check, ACR Tasks hint) behave as specified. deploy.ps1 itself is never run.

  Usage:   pwsh -File scripts/tests/deploy.helpers.tests.ps1      (exit code 0 = all checks passed)
#>
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$scripts = Join-Path (Split-Path -Parent $PSScriptRoot) '*.ps1'
$failures = [System.Collections.Generic.List[string]]::new()
$checks = 0
function Check([string] $Name, [bool] $Condition) {
    $script:checks++
    if (-not $Condition) { $script:failures.Add($Name); Write-Host "FAIL  $Name" -ForegroundColor Red }
    else { Write-Host "ok    $Name" }
}

# ---- every script parses
foreach ($file in @(Get-ChildItem $scripts) + @(Get-ChildItem (Join-Path $PSScriptRoot '*.ps1')) + @(Get-ChildItem (Join-Path $PSScriptRoot 'shims/*.ps1'))) {
    $errors = $null
    [void][System.Management.Automation.Language.Parser]::ParseFile($file.FullName, [ref]$null, [ref]$errors)
    Check "parses: $($file.Name)" ($errors.Count -eq 0)
}

# ---- load only the pure helpers from deploy.ps1 (the script body is not executed)
$ast = [System.Management.Automation.Language.Parser]::ParseFile((Join-Path (Split-Path -Parent $PSScriptRoot) 'deploy.ps1'), [ref]$null, [ref]$null)
foreach ($name in 'Test-IPv4Address', 'Test-RoleCanAssignRoles', 'Get-AcrBuildBlockedHint') {
    $definition = $ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name }, $true)
    if (-not $definition) { throw "deploy.ps1 no longer defines $name" }
    . ([scriptblock]::Create($definition.Extent.Text))
}

# ---- IPv4 validation: octets are bounded to 0-255
foreach ($ok in '0.0.0.0', '1.2.3.4', '203.0.113.9', '255.255.255.255', '10.20.30.40', '192.168.1.100', '249.250.199.99') {
    Check "IPv4 accepts $ok" (Test-IPv4Address $ok)
}
foreach ($bad in '256.1.1.1', '1.1.1.256', '999.999.999.999', '300.1.1.1', '1.2.3', '1.2.3.4.5', '1.2.3.', '', 'a.b.c.d', '1.2.3.4 ', ' 1.2.3.4',
    '01.2.3.4', '1.2.3.04', '1.2.3.-4', '1..3.4', '::1', '1.2.3.4/32', "1.2.3.4`n") {
    Check "IPv4 rejects '$bad'" (-not (Test-IPv4Address $bad))
}

# ---- role check
Check 'Owner can assign roles' (Test-RoleCanAssignRoles @('Reader', 'Owner'))
Check 'User Access Administrator can assign roles' (Test-RoleCanAssignRoles @('User Access Administrator'))
Check 'RBAC Administrator can assign roles' (Test-RoleCanAssignRoles @('Role Based Access Control Administrator'))
Check 'Contributor alone cannot' (-not (Test-RoleCanAssignRoles @('Contributor', 'Reader')))
Check 'no roles cannot' (-not (Test-RoleCanAssignRoles @()))

# ---- ACR Tasks blocked hint
$blocked = "ERROR: (TasksOperationsNotAllowed) ACR Tasks requests for the registry ehpocacr123 and 0000 are not permitted. Please file an Azure support request."
$hint = Get-AcrBuildBlockedHint -BuildOutput $blocked -Image 'ehpocacr123.azurecr.io/ehpoc:abc123'
Check 'blocked tasks error gives a hint' ([bool]$hint)
Check 'hint names the image and the README section' ($hint -match 'ehpocacr123\.azurecr\.io/ehpoc:abc123' -and $hint -match 'README\.md' -and $hint -match 'ACR Tasks are blocked')
Check 'other build failures give no hint' ($null -eq (Get-AcrBuildBlockedHint -BuildOutput 'ERROR: dockerfile build failed: mvn compile error' -Image 'x'))

# ---- the failure-capture pattern used around `az acr build`: stderr lines are kept, the exit code still throws
$roleCheck = $ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'Test-CanCreateRoleAssignments' }, $true)
. ([scriptblock]::Create($roleCheck.Extent.Text))
$ResourceGroup = 'rg-test'
$script:azCalls = [System.Collections.Generic.List[string]]::new()
$script:azRoles = 'Contributor'
$script:azFail = $false
function az {
    # Fake az: records every call and serves canned read-only answers.
    $script:azCalls.Add(($args -join ' '))
    if ($script:azFail) { throw 'simulated az failure' }
    switch -Wildcard (($args | Select-Object -First 2) -join ' ') {
        'account show' { return '{"id":"00000000-0000-0000-0000-000000000000","user":{"name":"someone@example.com","type":"user"}}' }
        'ad signed-in-user' { return '11111111-1111-1111-1111-111111111111' }
        'role assignment' { return $script:azRoles }
        'group exists' { return 'true' }
        default { throw "unexpected az call: $($args -join ' ')" }
    }
}
$script:azRoles = @('Reader', 'Owner')
$w = @(Test-CanCreateRoleAssignments 3>&1 | Where-Object { $_ -is [System.Management.Automation.WarningRecord] })
Check 'role preflight: Owner gives no warning' ($w.Count -eq 0)
$script:azRoles = @('Contributor')
$w = @(Test-CanCreateRoleAssignments 3>&1 | Where-Object { $_ -is [System.Management.Automation.WarningRecord] })
Check 'role preflight: Contributor only warns (does not throw)' ($w.Count -eq 1 -and "$($w[0])" -match 'Could not confirm')
$script:azFail = $true
$w = @(Test-CanCreateRoleAssignments 3>&1 | Where-Object { $_ -is [System.Management.Automation.WarningRecord] })
Check 'role preflight: an az failure only warns (does not throw)' ($w.Count -eq 1 -and "$($w[0])" -match 'Could not determine')
$script:azFail = $false
Check 'role preflight: only read-only az calls' (-not ($script:azCalls | Where-Object { $_ -notmatch '^(account show|ad signed-in-user show|role assignment list|group exists)' }))

$PSNativeCommandUseErrorActionPreference = $true
Remove-Item Function:\az
$log = [System.Collections.Generic.List[string]]::new()
$threw = $false
try {
    & (Get-Process -Id $PID).Path -NoProfile -Command '[Console]::Error.WriteLine("(TasksOperationsNotAllowed) ACR Tasks requests for the registry r are not permitted."); exit 1' 2>&1 |
        ForEach-Object { $log.Add("$_") }
}
catch { $threw = $true }
Check 'failing native command still throws through the capture pipeline' $threw
Check 'its stderr was captured' ((($log -join "`n")) -match 'TasksOperationsNotAllowed')

Write-Host "`n$checks checks, $($failures.Count) failed."
if ($failures.Count -gt 0) { exit 1 }
