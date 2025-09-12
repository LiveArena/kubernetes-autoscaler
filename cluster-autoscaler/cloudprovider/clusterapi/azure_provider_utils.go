package clusterapi

import "strings"

// AzureProviderIDNormalizer handles Azure-specific provider ID normalization
type AzureProviderIDNormalizer struct{}

// NormalizeProviderID normalizes Azure provider IDs
func (a *AzureProviderIDNormalizer) NormalizeProviderID(s string) normalizedProviderID {
	if strings.HasPrefix(s, "azure://") {
		return normalizedProviderID(s)
	}
	return normalizedProviderID("")
}

// IsAzureProviderID checks if provider ID is Azure-specific
func (a *AzureProviderIDNormalizer) IsAzureProviderID(s string) bool {
	return strings.HasPrefix(s, "azure://")
}
