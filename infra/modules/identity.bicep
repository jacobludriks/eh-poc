// Workload identity for the app: a user-assigned managed identity trusted (via federation) by exactly one
// Kubernetes service account, and allowed to send/receive on the single event hub only.
param location string

param identityName string

@description('AKS OIDC issuer URL (token issuer the federated credential trusts).')
param oidcIssuerUrl string

@description('Kubernetes namespace of the service account.')
param serviceAccountNamespace string = 'poc'

@description('Kubernetes service account name. The federated subject is system:serviceaccount:<namespace>:<name>.')
param serviceAccountName string = 'ehpoc'

param eventHubsNamespaceName string
param eventHubName string

// Built-in roles: Azure Event Hubs Data Sender / Data Receiver
var dataSenderRoleId = '2b629674-e913-4c01-ae53-ef4638d8f975'
var dataReceiverRoleId = 'a638d3c7-ab3a-418d-83e6-5f17a39d4fde'

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: identityName
  location: location
}

resource federatedCredential 'Microsoft.ManagedIdentity/userAssignedIdentities/federatedIdentityCredentials@2023-01-31' = {
  parent: identity
  name: 'aks-${serviceAccountNamespace}-${serviceAccountName}'
  properties: {
    issuer: oidcIssuerUrl
    subject: 'system:serviceaccount:${serviceAccountNamespace}:${serviceAccountName}'
    audiences: [
      'api://AzureADTokenExchange'
    ]
  }
}

resource namespace 'Microsoft.EventHub/namespaces@2024-01-01' existing = {
  name: eventHubsNamespaceName
}

resource eventHub 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' existing = {
  parent: namespace
  name: eventHubName
}

// Scoped to the event hub only (not the namespace or resource group).
resource dataSender 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(eventHub.id, identity.id, dataSenderRoleId)
  scope: eventHub
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', dataSenderRoleId)
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource dataReceiver 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(eventHub.id, identity.id, dataReceiverRoleId)
  scope: eventHub
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', dataReceiverRoleId)
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

output identityName string = identity.name
output clientId string = identity.properties.clientId
output principalId string = identity.properties.principalId
output tenantId string = identity.properties.tenantId
output federatedSubject string = federatedCredential.properties.subject
