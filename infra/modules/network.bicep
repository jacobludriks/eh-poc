// VNet with the AKS nodes subnet and the private-endpoints subnet (unused until ticket 02).
param location string
param vnetName string

var vnetAddressPrefix = '10.0.0.0/16'
var aksNodesSubnetPrefix = '10.0.0.0/22'
var privateEndpointsSubnetPrefix = '10.0.4.0/24'

resource vnet 'Microsoft.Network/virtualNetworks@2024-05-01' = {
  name: vnetName
  location: location
  properties: {
    addressSpace: {
      addressPrefixes: [
        vnetAddressPrefix
      ]
    }
    subnets: [
      {
        name: 'aks-nodes'
        properties: {
          addressPrefix: aksNodesSubnetPrefix
        }
      }
      {
        name: 'private-endpoints'
        properties: {
          addressPrefix: privateEndpointsSubnetPrefix
          privateEndpointNetworkPolicies: 'Disabled'
        }
      }
    ]
  }
}

output vnetId string = vnet.id
output vnetName string = vnet.name
output aksNodesSubnetId string = '${vnet.id}/subnets/aks-nodes'
output privateEndpointsSubnetId string = '${vnet.id}/subnets/private-endpoints'
