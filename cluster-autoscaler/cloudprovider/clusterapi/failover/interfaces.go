package failover

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

type Group interface {
	cloudprovider.NodeGroup
	Object() *unstructured.Unstructured
	Resource() (schema.GroupVersionResource, error)
	Replicas() (int, error)
	ProviderIDs() ([]string, error)
}

type Environment interface {
	NodeGroups() ([]cloudprovider.NodeGroup, error)
	MachinePoolResource() schema.GroupVersionResource
	FindNodeByProviderID(string) (*corev1.Node, error)
	FindMachineByProviderID(string) (*unstructured.Unstructured, error)
}
