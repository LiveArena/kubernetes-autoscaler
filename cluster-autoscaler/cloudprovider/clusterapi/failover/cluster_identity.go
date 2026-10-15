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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func matchesClusterOwner(owner metav1.OwnerReference, cluster *unstructured.Unstructured) bool {
	return sameResourceAPIGroup(owner.APIVersion, cluster.GetAPIVersion()) && owner.Kind == "Cluster" &&
		owner.Name == cluster.GetName() && owner.UID != "" && owner.UID == cluster.GetUID()
}

func sameResourceAPIGroup(previous, current string) bool {
	actual, actualErr := schema.ParseGroupVersion(previous)
	expected, expectedErr := schema.ParseGroupVersion(current)
	return actualErr == nil && expectedErr == nil && actual.Group != "" && actual.Group == expected.Group && actual.Version != "" && expected.Version != ""
}
