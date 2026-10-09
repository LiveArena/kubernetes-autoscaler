/*
Copyright 2026 The Kubernetes Authors.

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

package failover

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

// Group extends node-group operations with metadata and live resource access.
type Group interface {
	cloudprovider.NodeGroup
	Object() *unstructured.Unstructured
	Resource() (schema.GroupVersionResource, error)
	Replicas() (int, error)
	ProviderIDs() ([]string, error)
}

// Environment adapts parent discovery and workload lookups for the failover policy.
type Environment interface {
	NodeGroups() ([]cloudprovider.NodeGroup, error)
	MachinePoolResource() schema.GroupVersionResource
	FindNodeByProviderID(string) (*corev1.Node, error)
	FindMachineByProviderID(string) (*unstructured.Unstructured, error)
	// VisitFailures visits terminal failures from a synchronized management informer snapshot.
	VisitFailures(func(FailureObservation)) error
}
