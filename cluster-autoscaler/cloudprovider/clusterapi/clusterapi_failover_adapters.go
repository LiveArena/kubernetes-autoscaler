package clusterapi

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

func (group *nodegroup) Object() *unstructured.Unstructured {
	return group.scalableResource.unstructured
}

func (group *nodegroup) Resource() (schema.GroupVersionResource, error) {
	return group.scalableResource.GroupVersionResource()
}

func (group *nodegroup) Replicas() (int, error)         { return group.scalableResource.Replicas() }
func (group *nodegroup) ProviderIDs() ([]string, error) { return group.scalableResource.ProviderIDs() }

func (controller *machineController) NodeGroups() ([]cloudprovider.NodeGroup, error) {
	return controller.nodeGroups()
}

func (controller *machineController) MachinePoolResource() schema.GroupVersionResource {
	return controller.machinePoolResource
}

func (controller *machineController) FindNodeByProviderID(id string) (*corev1.Node, error) {
	return controller.findNodeByProviderID(normalizedProviderString(id))
}

func (controller *machineController) FindMachineByProviderID(id string) (*unstructured.Unstructured, error) {
	return controller.azureIntegration.lookup.FindMachineByProviderID(normalizedProviderString(id))
}
