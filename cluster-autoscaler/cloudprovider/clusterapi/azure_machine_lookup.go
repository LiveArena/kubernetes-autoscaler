package clusterapi

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// AzureMachineLookup handles Azure-specific machine lookups with fallback
type AzureMachineLookup struct {
	controller *machineController
	extension  *AzureControllerExtension
}

// FindMachineByProviderID finds machine with Azure fallback logic
func (a *AzureMachineLookup) FindMachineByProviderID(providerID normalizedProviderID) (*unstructured.Unstructured, error) {
	var objs []interface{}
	var err error

	// First check for AzureMachinePoolMachine if available
	if a.extension.azureMachinePoolMachineAvailable {
		objs, err = a.extension.azureMachinePoolMachineInformer.Informer().GetIndexer().ByIndex(machineProviderIDIndex, string(providerID))
		if err != nil {
			return nil, err
		}
	}

	// Fallback to standard Machine lookup if no Azure machine found
	if len(objs) == 0 {
		objs, err = a.controller.machineInformer.Informer().GetIndexer().ByIndex(machineProviderIDIndex, string(providerID))
		if err != nil {
			return nil, err
		}
	}

	// Return first match with deep copy
	if len(objs) > 0 {
		return objs[0].(*unstructured.Unstructured).DeepCopy(), nil
	}
	return nil, nil
}
