/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
