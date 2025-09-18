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
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// AzureResourceMarker handles resource marking for Azure machines
type AzureResourceMarker struct {
	controller *machineController
	extension  *AzureControllerExtension
}

// GetResourceGVR returns appropriate GroupVersionResource based on machine kind
func (a *AzureResourceMarker) GetResourceGVR(machine *unstructured.Unstructured) (schema.GroupVersionResource, error) {
	switch machine.GetKind() {
	case azureMachinePoolMachineKind:
		return a.extension.azureMachinePoolMachineResource, nil
	case machineKind:
		return a.controller.machineResource, nil
	default:
		return schema.GroupVersionResource{}, fmt.Errorf("unknown machine kind %s", machine.GetKind())
	}
}

// MarkMachineForDeletion marks machine for deletion with proper resource type
func (a *AzureResourceMarker) MarkMachineForDeletion(machine *unstructured.Unstructured) error {
	gvr, err := a.GetResourceGVR(machine)
	if err != nil {
		return err
	}

	u, err := a.controller.managementClient.Resource(gvr).
		Namespace(machine.GetNamespace()).
		Get(context.TODO(), machine.GetName(), metav1.GetOptions{})
	if err != nil {
		return err
	}

	annotations := u.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[machineDeleteAnnotationKey] = time.Now().String()
	u.SetAnnotations(annotations)

	_, updateErr := a.controller.managementClient.Resource(gvr).
		Namespace(u.GetNamespace()).
		Update(context.TODO(), u, metav1.UpdateOptions{})

	return updateErr
}

// UnmarkMachineForDeletion removes deletion mark with proper resource type
func (a *AzureResourceMarker) UnmarkMachineForDeletion(machine *unstructured.Unstructured) error {
	gvr, err := a.GetResourceGVR(machine)
	if err != nil {
		return err
	}

	u, err := a.controller.managementClient.Resource(gvr).
		Namespace(machine.GetNamespace()).
		Get(context.TODO(), machine.GetName(), metav1.GetOptions{})
	if err != nil {
		return err
	}

	annotations := u.GetAnnotations()
	if annotations != nil {
		delete(annotations, machineDeleteAnnotationKey)
		u.SetAnnotations(annotations)
	}

	_, updateErr := a.controller.managementClient.Resource(gvr).
		Namespace(u.GetNamespace()).
		Update(context.TODO(), u, metav1.UpdateOptions{})

	return updateErr
}
