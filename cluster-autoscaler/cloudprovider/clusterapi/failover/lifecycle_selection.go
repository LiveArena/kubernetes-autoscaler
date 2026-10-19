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
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

func (policy *Policy) configuredLifecycleMembers(ctx context.Context, cluster *unstructured.Unstructured, configuration *Configuration, candidates map[string]*Member, policies map[string]cloudprovider.NodeGroupCapacityPolicy) map[string]map[string]*Member {
	selected := map[string]map[string]*Member{}
	for name, pair := range configuration.Pairs {
		roles := map[string]*Member{}
		pairCandidates := map[string]*Member{}
		for _, reference := range []ConfiguredPool{pair.Primary, pair.Secondary} {
			if member := candidates[reference.Name]; member != nil {
				pairCandidates[reference.Name] = member
			}
		}
		single := &Configuration{APIVersion: configuration.APIVersion, Cluster: configuration.Cluster, Pairs: map[string]ConfiguredPair{name: pair}}
		resolved, err := policy.ResolveConfiguredPools(ctx, cluster, single)
		if err != nil {
			BlockMembers(pairCandidates, policies, err)
			continue
		}
		for role, reference := range map[string]ConfiguredPool{"primary": pair.Primary, "secondary": pair.Secondary} {
			member := candidates[reference.Name]
			if member != nil && member.Group.Object().GetUID() == resolved[reference.Name] {
				roles[role] = member
			}
		}
		if len(roles) != 2 {
			BlockMembers(pairCandidates, policies, fmt.Errorf("lifecycle pair is not complete"))
			continue
		}
		selected[name] = roles
	}
	return selected
}

func (policy *Policy) rememberUnpairedLifecycleCluster(ctx context.Context, group Group) {
	cluster, err := policy.owningCluster(group)
	if err != nil {
		return
	}
	configuration, err := readScanConfiguration(ctx, policy.Store.Client, cluster)
	if err != nil || configuration.APIVersion != LifecycleConfigVersion {
		return
	}
	key := cluster.GetNamespace() + "/" + string(cluster.GetUID())
	policy.Lock()
	if policy.LifecycleClusters == nil {
		policy.LifecycleClusters = map[string]*unstructured.Unstructured{}
	}
	policy.LifecycleClusters[key] = cluster.DeepCopy()
	policy.Unlock()
}
