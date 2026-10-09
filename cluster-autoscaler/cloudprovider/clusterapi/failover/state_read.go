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
	"context"
	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	strictjson "sigs.k8s.io/json"
)

// Load validates stored Cluster state or returns unsaved empty state when the object is absent.
func (store *Store) Load(ctx context.Context, cluster *unstructured.Unstructured) (*State, *unstructured.Unstructured, error) {
	if cluster.GetUID() == "" || cluster.GetNamespace() == "" {
		return nil, nil, fmt.Errorf("failover requires a namespaced Cluster with a UID")
	}
	object, err := store.Client.Resource(ConfigMaps).Namespace(cluster.GetNamespace()).Get(ctx, cluster.GetName()+"-autoscaler-failover-state", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return NewState(cluster), nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	owned := false
	for _, owner := range object.GetOwnerReferences() {
		if owner.UID == cluster.GetUID() && owner.Name == cluster.GetName() && owner.Kind == "Cluster" && owner.APIVersion == cluster.GetAPIVersion() {
			owned = true
		}
	}
	if !owned {
		return nil, nil, fmt.Errorf("failover state has a different Cluster owner")
	}
	encoded, found, err := unstructured.NestedString(object.Object, "data", "state.json")
	if err != nil || !found || len(encoded) > StateLimit {
		return nil, nil, fmt.Errorf("missing, invalid, or oversized failover state")
	}
	state := &State{}
	strictErrors, err := strictjson.UnmarshalStrict([]byte(encoded), state, strictjson.DisallowDuplicateFields, strictjson.DisallowUnknownFields)
	if err != nil {
		return nil, nil, fmt.Errorf("decode failover state: %w", err)
	}
	if len(strictErrors) > 0 {
		return nil, nil, fmt.Errorf("invalid failover state fields: %v", strictErrors)
	}
	if err := state.Validate(cluster.GetUID()); err != nil {
		return nil, nil, err
	}
	return state, object, nil
}
