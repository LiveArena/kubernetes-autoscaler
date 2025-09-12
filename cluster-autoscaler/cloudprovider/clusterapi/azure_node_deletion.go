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

	// Delete AzureMachinePoolMachine before scaling down pool
	err := a.controller.managementClient.Resource(a.extension.azureMachinePoolMachineResource).
		Namespace(machine.GetNamespace()).
		Delete(context.TODO(), machine.GetName(), metav1.DeleteOptions{})

	if err != nil {
		// Unmark machine for deletion on error
		_ = nodeGroup.scalableResource.UnmarkMachineForDeletion(machine)
		return err
	}

	return nil
}
