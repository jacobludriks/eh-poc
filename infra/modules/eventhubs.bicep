// Private Event Hubs: Standard namespace (1 TU, no auto-inflate), local (key/SAS) auth disabled, public network
// access disabled, one event hub (the Kafka topic) and a private endpoint with its private DNS zone.
// No Event Hubs consumer group resource: Kafka consumer groups on Event Hubs are auto-created.
param location string
param namespaceName string

@description('Event hub name = the Kafka topic the app produces to and consumes from.')
param eventHubName string = 'events'

param vnetId string
param privateEndpointsSubnetId string

// Event Hubs private link zone; linked to the VNet so the public namespace name resolves to the private IP.
var privateDnsZoneName = 'privatelink.servicebus.windows.net'

resource namespace 'Microsoft.EventHub/namespaces@2024-01-01' = {
  name: namespaceName
  location: location
  sku: {
    name: 'Standard'
    tier: 'Standard'
    capacity: 1
  }
  properties: {
    isAutoInflateEnabled: false
    disableLocalAuth: true
    publicNetworkAccess: 'Disabled'
    minimumTlsVersion: '1.2'
    zoneRedundant: false
  }
}

resource eventHub 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
  parent: namespace
  name: eventHubName
  properties: {
    partitionCount: 2
    messageRetentionInDays: 1
  }
}

resource privateEndpoint 'Microsoft.Network/privateEndpoints@2024-05-01' = {
  name: '${namespaceName}-pe'
  location: location
  properties: {
    subnet: {
      id: privateEndpointsSubnetId
    }
    privateLinkServiceConnections: [
      {
        name: 'eventhubs'
        properties: {
          privateLinkServiceId: namespace.id
          groupIds: [
            'namespace'
          ]
        }
      }
    ]
  }
}

resource privateDnsZone 'Microsoft.Network/privateDnsZones@2024-06-01' = {
  name: privateDnsZoneName
  location: 'global'
}

resource dnsZoneVnetLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: privateDnsZone
  name: 'link-${last(split(vnetId, '/'))}'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnetId
    }
  }
}

// Registers the namespace's A record in the zone automatically (and keeps it in step with the endpoint IP).
resource dnsZoneGroup 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-05-01' = {
  parent: privateEndpoint
  name: 'default'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'servicebus'
        properties: {
          privateDnsZoneId: privateDnsZone.id
        }
      }
    ]
  }
  dependsOn: [
    dnsZoneVnetLink
  ]
}

output namespaceName string = namespace.name
output namespaceFqdn string = '${namespace.name}.servicebus.windows.net'
output eventHubName string = eventHub.name
output privateDnsZoneName string = privateDnsZone.name
output privateEndpointName string = privateEndpoint.name
