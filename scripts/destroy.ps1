#Requires -Version 7.3
<#
.SYNOPSIS
  Deletes the whole PoC by deleting its resource group (including the AKS node resource group).

.DESCRIPTION
  Asks for confirmation (type the resource group name) unless -Force is given. Also removes the local
  .deploy-state/ folder (kubeconfig and deploy outputs).
#>
[CmdletBinding()]
param(
    [string] $ResourceGroup = 'rg-eh-passwordless-poc',
    # Skip the confirmation prompt.
    [switch] $Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

$repoRoot = Split-Path -Parent $PSScriptRoot
$stateDir = Join-Path $repoRoot '.deploy-state'

if (-not (Get-Command az -ErrorAction SilentlyContinue)) { throw 'Azure CLI (az) not found on PATH.' }
try { $null = az account show -o none } catch { throw 'Not logged in to Azure. Run `az login` first.' }

if ((az group exists --name $ResourceGroup) -ne 'true') {
    Write-Host "Resource group '$ResourceGroup' does not exist; nothing to delete."
}
else {
    if (-not $Force) {
        Write-Host "This permanently deletes resource group '$ResourceGroup' in subscription '$(az account show --query name -o tsv)' and everything in it." -ForegroundColor Yellow
        $answer = Read-Host "Type the resource group name to confirm"
        if ($answer -ne $ResourceGroup) { Write-Host 'Confirmation did not match; aborting.'; exit 1 }
    }
    Write-Host "Deleting resource group $ResourceGroup (waits until finished)..."
    az group delete --name $ResourceGroup --yes
    Write-Host "Deleted $ResourceGroup." -ForegroundColor Green
}

if (Test-Path $stateDir) {
    Remove-Item -Recurse -Force $stateDir
    Write-Host 'Removed local .deploy-state/.'
}
