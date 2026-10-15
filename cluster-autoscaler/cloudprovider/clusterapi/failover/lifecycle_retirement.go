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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
)

func (policy *Policy) reconcileRetiringPairs(ctx context.Context) {
	policy.RLock()
	clusters := make([]*unstructured.Unstructured, 0, len(policy.LifecycleClusters))
	for _, cluster := range policy.LifecycleClusters {
		clusters = append(clusters, cluster.DeepCopy())
	}
	policy.RUnlock()
	for _, cluster := range clusters {
		configuration, err := readScanConfiguration(ctx, policy.Store.Client, cluster)
		if err != nil || configuration.APIVersion != LifecycleConfigVersion {
			continue
		}
		for name, configured := range configuration.Pairs {
			if configured.Intent == nil || configured.Intent.State != "Retiring" {
				continue
			}
			_, err := policy.Store.Reconcile(ctx, cluster, func(state *State) error {
				pair := state.Pairs[name]
				if state.Version != StateVersion || pair == nil || pair.Lifecycle == nil || pair.Lifecycle.Version != LifecycleStateVersion || pair.Lifecycle.RegistrationID != configured.Intent.RegistrationID {
					return fmt.Errorf("retirement requires an existing matching registration")
				}
				current, err := ReadConfiguration(ctx, policy.Store.Client, cluster)
				if err != nil {
					return err
				}
				intent := current.Pairs[name].Intent
				if intent == nil || intent.State != "Retiring" || intent.RegistrationID != pair.Lifecycle.RegistrationID {
					return fmt.Errorf("retirement intent changed during reconciliation")
				}
				if pair.Lifecycle.State == "Retired" {
					return nil
				}
				pair.Lifecycle.State = "Retiring"
				if err := policy.reconcileRetiringRequests(ctx, cluster, pair); err != nil {
					return err
				}
				pair.Lifecycle.OperationsSettled = pair.PrimaryRequest == nil && pair.SecondaryRequest == nil && policy.pairOperationsSettled(cluster, pair)
				pair.Lifecycle.Reason = "waiting for ordinary resource retirement"
				if !intent.ResourceRetirementComplete {
					return nil
				}
				gone, err := policy.Store.recordedPairResourcesGone(ctx, cluster, pair)
				if err != nil || !gone {
					return err
				}
				if !policy.pairOperationsSettled(cluster, pair) {
					pair.Lifecycle.OperationsSettled = false
					pair.Lifecycle.Reason = "waiting for admitted pair operations"
					return nil
				}
				pair.PrimaryRequest, pair.SecondaryRequest = nil, nil
				pair.FallbackAllowance = 0
				pair.Lifecycle.State = "Retired"
				pair.Lifecycle.OperationsSettled = true
				pair.Lifecycle.Reason = "ordinary resource retirement confirmed"
				return nil
			})
			if err != nil {
				klog.Warningf("Pair %s retirement status unavailable: %v", name, err)
			}
		}
	}
}

func (store *Store) recordedPairResourcesGone(ctx context.Context, cluster *unstructured.Unstructured, pair *Pair) (bool, error) {
	for _, role := range []Role{pair.Primary, pair.Secondary} {
		if role.PoolName == "" || role.InfrastructureName == "" {
			return false, fmt.Errorf("registered resource names are missing")
		}
		version, err := schema.ParseGroupVersion(role.PoolVersion)
		if err != nil || version.Group == "" {
			return false, fmt.Errorf("invalid registered pool version")
		}
		_, err = store.readResource(ctx, schema.GroupKind{Group: version.Group, Kind: MachinePoolKind}, role.PoolVersion, cluster.GetNamespace(), role.PoolName)
		if err == nil {
			return false, nil
		}
		if !apierrors.IsNotFound(err) {
			return false, err
		}
		version, err = schema.ParseGroupVersion(role.InfrastructureVersion)
		if err != nil || version.Group != AzureAPIGroup {
			return false, fmt.Errorf("invalid registered infrastructure version")
		}
		_, err = store.readResource(ctx, schema.GroupKind{Group: version.Group, Kind: "AzureMachinePool"}, role.InfrastructureVersion, cluster.GetNamespace(), role.InfrastructureName)
		if err == nil {
			return false, nil
		}
		if !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	return true, nil
}
