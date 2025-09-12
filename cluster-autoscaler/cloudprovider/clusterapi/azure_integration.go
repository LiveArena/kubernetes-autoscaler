package clusterapi

import (
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic/dynamicinformer"
)

// AzureIntegration coordinates all Azure-specific functionality
type AzureIntegration struct {
	controller         *machineController
	extension          *AzureControllerExtension
	lookup             *AzureMachineLookup
	deletionHandler    *AzureNodeDeletionHandler
	resourceMarker     *AzureResourceMarker
	providerNormalizer *AzureProviderIDNormalizer
}

// NewAzureIntegration creates a new Azure integration instance
func NewAzureIntegration(
	controller *machineController,
	managementInformerFactory dynamicinformer.DynamicSharedInformerFactory,
	managementDiscoveryClient discovery.DiscoveryInterface,
) (*AzureIntegration, error) {
	extension, err := NewAzureControllerExtension(
		managementInformerFactory,
		managementDiscoveryClient,
	)
	if err != nil {
		return nil, err
	}

	integration := &AzureIntegration{
		controller: controller,
		extension:  extension,
	}

	integration.lookup = &AzureMachineLookup{
		controller: controller,
		extension:  extension,
	}

	integration.deletionHandler = &AzureNodeDeletionHandler{
		controller: controller,
		extension:  extension,
	}

	integration.resourceMarker = &AzureResourceMarker{
		controller: controller,
		extension:  extension,
	}

	integration.providerNormalizer = &AzureProviderIDNormalizer{}

	return integration, nil
}
