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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// Validate checks the Cluster UID, schema and per-pair state invariants.
func (state *State) Validate(clusterUID types.UID) error {
	if state.Version != StateVersion || state.ClusterUID != clusterUID || state.Pairs == nil {
		return fmt.Errorf("unsupported, stale, or unbounded failover state")
	}
	members := map[types.UID]string{}
	for name, pair := range state.Pairs {
		if !ValidPairIdentifier(name) || pair == nil || pair.RecoveryScans < 0 || pair.RecoveryScans > 1 {
			return fmt.Errorf("invalid failover pair state")
		}
		if pair.Lifecycle != nil && (pair.Lifecycle.Version != LifecycleStateVersion || !ValidPairIdentifier(pair.Lifecycle.RegistrationID) || !validLifecycleState(pair.Lifecycle.State)) {
			return fmt.Errorf("invalid pair lifecycle status")
		}
		if pair.Phase != Healthy && pair.Phase != Degraded && pair.Phase != Recovering {
			return fmt.Errorf("unknown failover phase")
		}
		if (pair.Phase == Healthy) == pair.Primary.Failed {
			return fmt.Errorf("inconsistent failover phase and primary failure state")
		}
		if pair.FallbackAllowance < 0 {
			return fmt.Errorf("invalid fallback allowance")
		}
		for _, request := range []*Request{pair.PrimaryRequest, pair.SecondaryRequest} {
			if request != nil && (request.FromTarget < 0 || request.ToTarget <= request.FromTarget || request.Started.IsZero() || request.ReadyAtStart < 0) {
				return fmt.Errorf("invalid failover scale request")
			}
			if request != nil && request.Deadline != nil && !request.Deadline.Time.After(request.Started.Time) {
				return fmt.Errorf("invalid primary provisioning deadline")
			}
		}
		for _, role := range []Role{pair.Primary, pair.Secondary} {
			if pair.Lifecycle != nil {
				poolVersion, poolError := schema.ParseGroupVersion(role.PoolVersion)
				infrastructureVersion, infrastructureError := schema.ParseGroupVersion(role.InfrastructureVersion)
				if role.PoolUID == "" || role.InfrastructureUID == "" || role.PoolName == "" || role.InfrastructureName == "" || poolError != nil || poolVersion.Group == "" || infrastructureError != nil || infrastructureVersion.Group != AzureAPIGroup {
					return fmt.Errorf("lifecycle state requires complete registered member identities")
				}
			} else if role.PoolName != "" || role.PoolVersion != "" || role.InfrastructureName != "" || role.InfrastructureVersion != "" {
				return fmt.Errorf("lifecycle binding fields require a versioned lifecycle record")
			}
			for _, uid := range []types.UID{role.PoolUID, role.InfrastructureUID} {
				if uid == "" {
					continue
				}
				if previous, found := members[uid]; found {
					return fmt.Errorf("member UID %q is already registered to pair %q", uid, previous)
				}
				members[uid] = name
			}
			if role.CheckInterval < 0 || role.CheckInterval > 15*60 || role.ObservedTarget < 0 || role.ReadyCountAtFailure < 0 || role.FailureEpoch < 0 {
				return fmt.Errorf("invalid failover role state")
			}
		}
	}
	return nil
}
