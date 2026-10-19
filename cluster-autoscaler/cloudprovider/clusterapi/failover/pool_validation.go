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

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// AssertCurrentPool verifies that the group's live identity still matches explicit configuration.
func (policy *Policy) AssertCurrentPool(ctx context.Context, group Group) error {
	member, err := policy.Member(ctx, group)
	if err != nil {
		return err
	}
	configuration, err := ReadConfiguration(ctx, policy.Store.Client, member.Cluster)
	if err != nil {
		return err
	}
	if configuration.APIVersion == LifecycleConfigVersion {
		pair, found := configuration.Pairs[group.Object().GetAnnotations()[PairKey]]
		if !found {
			return fmt.Errorf("pool is absent from lifecycle configuration")
		}
		configuration.Pairs = map[string]ConfiguredPair{group.Object().GetAnnotations()[PairKey]: pair}
	}
	resolved, err := policy.ResolveConfiguredPools(ctx, member.Cluster, configuration)
	if err != nil {
		return err
	}
	annotations := group.Object().GetAnnotations()
	pair, found := configuration.Pairs[annotations[PairKey]]
	if !found {
		return fmt.Errorf("pool is not in current failover configuration")
	}
	reference := pair.Primary
	if annotations[RoleKey] == "secondary" {
		reference = pair.Secondary
	} else if annotations[RoleKey] != "primary" {
		return fmt.Errorf("invalid configured role")
	}
	if reference.Name != group.Object().GetName() || resolved[reference.Name] != group.Object().GetUID() {
		return fmt.Errorf("pool generation changed in failover configuration")
	}
	resource := policy.Environment.MachinePoolResource()
	pool, err := policy.Store.readResource(ctx, schema.GroupKind{Group: resource.Group, Kind: MachinePoolKind}, resource.GroupVersion().String(), member.Cluster.GetNamespace(), reference.Name)
	if err != nil {
		return err
	}
	infrastructure, found, err := unstructured.NestedStringMap(pool.Object, "spec", "template", "spec", "infrastructureRef")
	if err != nil || !found || infrastructure["kind"] != "AzureMachinePool" || infrastructure["name"] != member.Infrastructure.GetName() || infrastructure["namespace"] != "" && infrastructure["namespace"] != member.Cluster.GetNamespace() {
		return fmt.Errorf("live pool infrastructure reference changed since admission")
	}
	version, err := policy.infrastructureVersion(infrastructure)
	if err != nil || !sameResourceAPIGroup(version.String(), member.Infrastructure.GetAPIVersion()) {
		return fmt.Errorf("live pool infrastructure API identity changed since admission: %v", err)
	}
	return nil
}

// ResolveConfiguredPools validates all selected roles and returns their live MachinePool UIDs.
func (policy *Policy) ResolveConfiguredPools(ctx context.Context, cluster *unstructured.Unstructured, configuration *Configuration) (map[string]types.UID, error) {
	resolved := map[string]types.UID{}
	resource := policy.Environment.MachinePoolResource()
	for name, pair := range configuration.Pairs {
		for role, reference := range map[string]ConfiguredPool{"primary": pair.Primary, "secondary": pair.Secondary} {
			pool, err := policy.Store.readResource(ctx, schema.GroupKind{Group: resource.Group, Kind: MachinePoolKind}, resource.GroupVersion().String(), cluster.GetNamespace(), reference.Name)
			if err != nil {
				return nil, fmt.Errorf("pair %q role %s reference %q cannot be resolved: %w", name, role, reference.Name, err)
			}
			clusterName, found, err := unstructured.NestedString(pool.Object, "spec", "clusterName")
			annotations := pool.GetAnnotations()
			if err != nil || !found || clusterName != cluster.GetName() || pool.GetKind() != MachinePoolKind || pool.GetUID() == "" || !pool.GetDeletionTimestamp().IsZero() || annotations[PairKey] != name || annotations[RoleKey] != role {
				return nil, fmt.Errorf("configured pool %s has invalid identity or role", reference.Name)
			}
			owned := false
			for _, owner := range pool.GetOwnerReferences() {
				if matchesClusterOwner(owner, cluster) {
					owned = true
				}
			}
			if !owned {
				return nil, fmt.Errorf("configured pool %s has a different Cluster owner", reference.Name)
			}
			resolved[reference.Name] = pool.GetUID()
		}
	}
	return resolved, nil
}
