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
	"path"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func (policy *Policy) beginPairOperation(id string) func() {
	policy.WriterMu.Lock()
	if policy.PairOperations == nil {
		policy.PairOperations = map[string]int{}
	}
	policy.PairOperations[id]++
	policy.WriterMu.Unlock()
	return func() {
		policy.WriterMu.Lock()
		policy.PairOperations[id]--
		if policy.PairOperations[id] == 0 {
			delete(policy.PairOperations, id)
		}
		policy.WriterMu.Unlock()
	}
}

func (policy *Policy) pairOperationsSettled(cluster *unstructured.Unstructured, pair *Pair) bool {
	policy.WriterMu.Lock()
	defer policy.WriterMu.Unlock()
	return policy.PairOperations[path.Join(MachinePoolKind, cluster.GetNamespace(), pair.Primary.PoolName)] == 0 && policy.PairOperations[path.Join(MachinePoolKind, cluster.GetNamespace(), pair.Secondary.PoolName)] == 0
}

func (policy *Policy) reconcileRetiringRequests(ctx context.Context, cluster *unstructured.Unstructured, pair *Pair) error {
	for _, operation := range []struct {
		role    *Role
		request **Request
	}{{&pair.Primary, &pair.PrimaryRequest}, {&pair.Secondary, &pair.SecondaryRequest}} {
		if *operation.request == nil {
			continue
		}
		version, err := schema.ParseGroupVersion(operation.role.PoolVersion)
		if err != nil {
			return err
		}
		pool, err := policy.Store.readResource(ctx, schema.GroupKind{Group: version.Group, Kind: MachinePoolKind}, operation.role.PoolVersion, cluster.GetNamespace(), operation.role.PoolName)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}
		if pool.GetUID() != operation.role.PoolUID {
			return fmt.Errorf("retiring request resource identity changed")
		}
		target, found, err := unstructured.NestedInt64(pool.Object, "spec", "replicas")
		if err != nil || !found {
			return fmt.Errorf("retiring request has no authoritative target")
		}
		operation.role.ObservedTarget = int(target)
		if int(target) == (*operation.request).ToTarget {
			*operation.request = nil
		}
	}
	return nil
}

func (policy *Policy) observeRetiringPair(pair *Pair, primary, secondary *Member, observations map[string]FailureObservation, now time.Time) error {
	for role, member := range map[string]*Member{"primary": primary, "secondary": secondary} {
		record := &pair.Primary
		if role == "secondary" {
			record = &pair.Secondary
		}
		if record.PoolUID != member.Group.Object().GetUID() || record.InfrastructureUID != member.Infrastructure.GetUID() {
			return fmt.Errorf("retiring member identities changed")
		}
		key := member.Cluster.GetNamespace() + "/" + string(member.Infrastructure.GetUID())
		if observation, found := observations[key]; found && observation.OwnerName == member.Infrastructure.GetName() {
			if record.ObserveFailure(observation, member.Ready, now, policy.ScanInterval) && role == "primary" {
				pair.Phase = Degraded
			}
		}
	}
	return nil
}
