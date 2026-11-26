// Basic-tier container registry with public endpoints (documented exception to "all resources private").
// Admin user is disabled: the cluster pulls with its kubelet identity (AcrPull, see aks.bicep), no secrets.
param location string
param acrName string

resource acr 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: acrName
  location: location
  sku: {
    name: 'Basic'
  }
  properties: {
    adminUserEnabled: false
    publicNetworkAccess: 'Enabled'
  }
}

output acrName string = acr.name
output acrId string = acr.id
output acrLoginServer string = acr.properties.loginServer
