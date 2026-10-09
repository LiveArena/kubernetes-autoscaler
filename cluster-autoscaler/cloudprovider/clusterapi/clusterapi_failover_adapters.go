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

package clusterapi

import (
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/clusterapi/failover"
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

func (controller *machineController) VisitFailures(visit func(failover.FailureObservation)) error {
	if controller.azureIntegration == nil || controller.azureIntegration.extension.azureMachinePoolMachineInformer == nil {
		return fmt.Errorf("AMPM informer is unavailable for failure resynchronization")
	}
	informer := controller.azureIntegration.extension.azureMachinePoolMachineInformer.Informer()
	if !informer.HasSynced() {
		return fmt.Errorf("AMPM informer has not synchronized for failure resynchronization")
	}
	store := informer.GetStore()
	for _, key := range store.ListKeys() {
		object, exists, err := store.GetByKey(key)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		machine, ok := object.(*unstructured.Unstructured)
		if !ok {
			return fmt.Errorf("invalid AMPM informer object during failure resynchronization")
		}
		if observation, terminal := failover.TerminalAMPMFailure(machine); terminal {
			visit(observation)
		}
	}
	return nil
}
