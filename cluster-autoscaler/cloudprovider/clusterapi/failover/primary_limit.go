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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"strconv"
)

// PrimaryAtMaximum verifies the selected primary identity and compares its live target with its maximum.
func (policy *Policy) PrimaryAtMaximum(ctx context.Context, cluster *unstructured.Unstructured, pair string, uid types.UID) (bool, error) {
	groups, err := policy.Environment.NodeGroups()
	if err != nil {
		return false, err
	}
	for _, candidate := range groups {
		group := candidate.(Group)
		object := group.Object()
		if object.GetNamespace() != cluster.GetNamespace() || object.GetUID() != uid {
			continue
		}
		gvr, err := group.Resource()
		if err != nil {
			return false, err
		}
		object, err = policy.Store.Client.Resource(gvr).Namespace(object.GetNamespace()).Get(ctx, object.GetName(), metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		annotations := object.GetAnnotations()
		if object.GetUID() != uid || annotations[PairKey] != pair || annotations[RoleKey] != "primary" || !object.GetDeletionTimestamp().IsZero() {
			return false, fmt.Errorf("primary identity changed during admission")
		}
		maximum, err := strconv.Atoi(annotations[MaxSizeAnnotationKey])
		if err != nil || maximum < 0 {
			return false, fmt.Errorf("invalid primary maximum")
		}
		target, found, err := unstructured.NestedInt64(object.Object, "spec", "replicas")
		if err != nil || !found {
			return false, fmt.Errorf("invalid primary target")
		}
		return target >= int64(maximum), nil
	}
	return false, fmt.Errorf("current primary pool cannot be resolved")
}
