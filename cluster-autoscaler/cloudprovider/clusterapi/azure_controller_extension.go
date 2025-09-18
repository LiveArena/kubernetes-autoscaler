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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	klog "k8s.io/klog/v2"
)

// AzureControllerExtension extends machineController with Azure-specific capabilities
type AzureControllerExtension struct {
	azureMachinePoolMachineInformer  informers.GenericInformer
	azureMachinePoolMachineResource  schema.GroupVersionResource
	azureMachinePoolMachineAvailable bool
}

// NewAzureControllerExtension creates a new Azure controller extension
func NewAzureControllerExtension(
	managementInformerFactory dynamicinformer.DynamicSharedInformerFactory,
	managementDiscoveryClient discovery.DiscoveryInterface,
) (*AzureControllerExtension, error) {
	extension := &AzureControllerExtension{
		azureMachinePoolMachineAvailable: false,
	}

	// Check if AzureMachinePoolMachine resources are available
	capiVersion := getCAPIVersion()
	azureMachinePoolMachineAvailable, err := groupVersionHasResource(
		managementDiscoveryClient,
		azureMachinePoolMachineApiGroup+"/"+azureMachinePoolMachineApiVersion,
		resourceNameAzureMachinePoolMachine,
	)
	if err != nil {
		klog.V(4).Infof("Failed to check for AzureMachinePoolMachine availability: %v", err)
		return extension, nil // Return extension but mark as unavailable
	}

	if !azureMachinePoolMachineAvailable {
		klog.V(4).Infof("AzureMachinePoolMachine resources not available")
		return extension, nil
	}

	// Set up Azure MachinePool Machine resource
	extension.azureMachinePoolMachineResource = schema.GroupVersionResource{
		Group:    azureMachinePoolMachineApiGroup,
		Version:  azureMachinePoolMachineApiVersion,
		Resource: resourceNameAzureMachinePoolMachine,
	}

	// Create informer for Azure MachinePool Machines
	extension.azureMachinePoolMachineInformer = managementInformerFactory.ForResource(extension.azureMachinePoolMachineResource)

	// Add indexing for provider ID lookup
	if err := extension.azureMachinePoolMachineInformer.Informer().GetIndexer().AddIndexers(map[string]cache.IndexFunc{
		machineProviderIDIndex: indexMachineByProviderID,
	}); err != nil {
		klog.Errorf("Failed to add provider ID indexer for AzureMachinePoolMachine: %v", err)
		return nil, err
	}

	extension.azureMachinePoolMachineAvailable = true
	klog.V(4).Infof("AzureMachinePoolMachine support enabled with version %s", capiVersion)

	return extension, nil
}
