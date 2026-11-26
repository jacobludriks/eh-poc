// Passwordless Event Hubs PoC: base environment (ticket 01).
// Deployed at resource-group scope by scripts/deploy.ps1 into rg-eh-passwordless-poc.
// Ticket 02 adds the private Event Hubs (modules/eventhubs.bicep) and the workload identity (modules/identity.bicep).
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
var eventHubsNamespaceName = '${namePrefix}-ehns-${suffix}'
var identityName = '${namePrefix}-id-${suffix}'

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

module eventHubs 'modules/eventhubs.bicep' = {
  name: 'eventhubs'
  params: {
    location: location
    namespaceName: eventHubsNamespaceName
    vnetId: network.outputs.vnetId
    privateEndpointsSubnetId: network.outputs.privateEndpointsSubnetId
  }
}

// The service account name (ehpoc) and namespace (poc) must match k8s/10-serviceaccount.yaml.
module identity 'modules/identity.bicep' = {
  name: 'identity'
  params: {
    location: location
    identityName: identityName
    oidcIssuerUrl: aks.outputs.oidcIssuerUrl
    eventHubsNamespaceName: eventHubs.outputs.namespaceName
    eventHubName: eventHubs.outputs.eventHubName
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
output eventHubsNamespaceName string = eventHubs.outputs.namespaceName
output eventHubsFqdn string = eventHubs.outputs.namespaceFqdn
output eventHubName string = eventHubs.outputs.eventHubName
output eventHubsPrivateDnsZoneName string = eventHubs.outputs.privateDnsZoneName
output eventHubsPrivateEndpointName string = eventHubs.outputs.privateEndpointName
output identityName string = identity.outputs.identityName
output identityClientId string = identity.outputs.clientId
output identityPrincipalId string = identity.outputs.principalId
output tenantId string = identity.outputs.tenantId
