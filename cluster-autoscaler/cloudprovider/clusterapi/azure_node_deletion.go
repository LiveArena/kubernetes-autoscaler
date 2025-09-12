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
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// AzureNodeDeletionHandler handles Azure-specific node deletion logic
type AzureNodeDeletionHandler struct {
	controller *machineController
	extension  *AzureControllerExtension
}

// HandleAzureMachinePoolDeletion handles Azure machine pool machine deletion
func (a *AzureNodeDeletionHandler) HandleAzureMachinePoolDeletion(
	machine *unstructured.Unstructured,
	nodeGroup *nodegroup,
) error {
	// Check if machine is AzureMachinePoolMachine
	if machine.GetKind() != azureMachinePoolMachineKind {
		return nil // Not an Azure machine pool machine, skip
	}

	// Check for nil controller or managementClient (e.g., in tests)
	if a.controller == nil || a.controller.managementClient == nil || a.extension == nil {
		return nil // Cannot perform deletion without proper initialization
	}

	// Delete AzureMachinePoolMachine before scaling down pool
	err := a.controller.managementClient.Resource(a.extension.azureMachinePoolMachineResource).
		Namespace(machine.GetNamespace()).
		Delete(context.TODO(), machine.GetName(), metav1.DeleteOptions{})

	if err != nil {
		// Unmark machine for deletion on error
		if nodeGroup != nil && nodeGroup.scalableResource != nil {
			_ = nodeGroup.scalableResource.UnmarkMachineForDeletion(machine)
		}
		return err
	}

	return nil
}
