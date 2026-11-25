// Passwordless Event Hubs PoC: base environment (ticket 01).
// Deployed at resource-group scope by scripts/deploy.ps1 into rg-eh-passwordless-poc.
// Ticket 02 adds modules for Event Hubs, the private endpoint/DNS and the workload identity.
targetScope = 'resourceGroup'

@description('Azure region for all resources.')
param location string = 'australiaeast'

@description('Short prefix used in every resource name.')
@minLength(2)
@maxLength(8)
param namePrefix string = 'ehpoc'

@description('Caller public IPv4 address (no mask), detected once by deploy.ps1. Used for the AKS API server authorized ranges.')
param callerIp string

var suffix = uniqueString(resourceGroup().id)
var vnetName = '${namePrefix}-vnet-${suffix}'
var aksName = '${namePrefix}-aks-${suffix}'
var acrName = '${namePrefix}acr${suffix}'

module network 'modules/network.bicep' = {
  name: 'network'
  params: {
    location: location
    vnetName: vnetName
  }
}

module acr 'modules/acr.bicep' = {
  name: 'acr'
  params: {
    location: location
    acrName: acrName
  }
}

module aks 'modules/aks.bicep' = {
  name: 'aks'
  params: {
    location: location
    aksName: aksName
    acrName: acr.outputs.acrName
    nodesSubnetId: network.outputs.aksNodesSubnetId
    apiServerAuthorizedRanges: [
      '${callerIp}/32'
    ]
  }
}

output resourceSuffix string = suffix
output vnetName string = network.outputs.vnetName
output vnetId string = network.outputs.vnetId
output privateEndpointsSubnetId string = network.outputs.privateEndpointsSubnetId
output aksName string = aks.outputs.aksName
output aksOidcIssuerUrl string = aks.outputs.oidcIssuerUrl
output acrName string = acr.outputs.acrName
output acrLoginServer string = acr.outputs.acrLoginServer
