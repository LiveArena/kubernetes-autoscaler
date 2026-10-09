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
	"fmt"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/scale"
)

// IncreaseSize handles policy-managed increments after live identity, limit and durable-intent checks.
// The returned boolean indicates whether the policy handled the request.
func (writer *Policy) IncreaseSize(group Group, delta int, scales scale.ScalesGetter) (bool, error) {
	policy := writer.Capacity(group)
	if policy.ScaleUpBlocked {
		return true, fmt.Errorf("scale-up admission blocked: %s", policy.Reason)
	}
	if policy.ExpectedTarget == nil {
		return false, nil
	}
	if !writer.BeginWriterOperation() {
		return true, fmt.Errorf("failover writer has stopped")
	}
	defer writer.WriterOperations.Done()
	ctx := writer.RequestContext()
	if err := writer.AssertWriter(ctx, group); err != nil {
		return true, err
	}
	gvr, err := group.Resource()
	if err != nil {
		return true, err
	}
	object := group.Object()
	current, err := scales.Scales(object.GetNamespace()).Get(ctx, gvr.GroupResource(), object.GetName(), metav1.GetOptions{})
	if err != nil {
		return true, err
	}
	if int(current.Spec.Replicas) != *policy.ExpectedTarget {
		return true, fmt.Errorf("failover target changed since admission; retry after reconciliation")
	}
	pool, err := writer.Store.Client.Resource(gvr).Namespace(object.GetNamespace()).Get(ctx, object.GetName(), metav1.GetOptions{})
	if err != nil {
		return true, err
	}
	if pool.GetUID() != object.GetUID() {
		return true, fmt.Errorf("failover pool identity changed since admission")
	}
	for _, annotation := range []string{PairKey, RoleKey} {
		if pool.GetAnnotations()[annotation] != object.GetAnnotations()[annotation] {
			return true, fmt.Errorf("failover generation metadata changed since admission")
		}
	}
	maximum, err := strconv.Atoi(pool.GetAnnotations()[MaxSizeAnnotationKey])
	if err != nil || maximum < 0 || maximum != group.MaxSize() {
		return true, fmt.Errorf("failover maximum changed or invalid; retry after reconciliation")
	}
	if int(current.Spec.Replicas)+delta > maximum {
		return true, fmt.Errorf("failover scale-up exceeds group maximum")
	}
	if err := writer.AssertCurrentPool(ctx, group); err != nil {
		return true, err
	}
	if policy.ScaleUpLimit > 0 && delta > policy.ScaleUpLimit {
		return true, fmt.Errorf("failover request exceeds its admitted capacity window")
	}
	if err := writer.PrepareScaleRequest(group, delta); err != nil {
		return true, err
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	current.Spec.Replicas += int32(delta)
	_, err = scales.Scales(object.GetNamespace()).Update(ctx, gvr.GroupResource(), current, metav1.UpdateOptions{})
	return true, err
}
