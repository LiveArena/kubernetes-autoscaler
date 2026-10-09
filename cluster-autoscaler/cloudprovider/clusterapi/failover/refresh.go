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
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/klog/v2"
)

// Refresh reconciles live configuration and durable state before publishing capacity snapshots.
func (policy *Policy) Refresh() {
	if !policy.BeginWriterOperation() {
		return
	}
	defer policy.WriterOperations.Done()
	ctx := policy.RequestContext()
	now := policy.Clock.Now()
	groups, err := policy.Environment.NodeGroups()
	if err != nil {
		policy.Lock()
		closed := make(map[string]cloudprovider.NodeGroupCapacityPolicy, len(policy.Policies))
		for id, previous := range policy.Policies {
			previous.ScaleUpBlocked = true
			previous.ConsiderPrimaryUnfit = false
			previous.RetainTarget = true
			previous.Reason = "failover discovery failed"
			closed[id] = previous
		}
		policy.Policies = closed
		policy.FitExceptions = map[string]string{}
		policy.Unlock()
		klog.Errorf("Failover discovery failed: %v", err)
		return
	}
	policy.RLock()
	observations := make(map[string]FailureObservation, len(policy.Observations))
	for key, observation := range policy.Observations {
		observations[key] = observation
	}
	previousPolicies := policy.Policies
	overflow := policy.Overflow
	overflowVersion := policy.OverflowVersion
	policy.RUnlock()
	policies := map[string]cloudprovider.NodeGroupCapacityPolicy{}
	available := map[string]map[string]*Member{}
	for _, candidate := range groups {
		group := candidate.(Group)
		annotations := group.Object().GetAnnotations()
		pair, role := annotations[PairKey], annotations[RoleKey]
		if pair == "" && role == "" {
			continue
		}
		fallback := previousPolicies[group.Id()]
		fallback.ScaleUpBlocked = true
		fallback.ConsiderPrimaryUnfit = false
		fallback.RetainTarget = true
		fallback.Reason = SelectionReason(group, fmt.Errorf("invalid or unselected failover metadata"))
		policies[group.Id()] = fallback
		if !ValidPairIdentifier(pair) || (role != "primary" && role != "secondary") {
			continue
		}
		member, err := policy.Member(ctx, group)
		if err != nil {
			klog.Warningf("Failover group %s unavailable: %v", group.Id(), err)
			continue
		}
		fallback.ExpectedTarget = &member.Target
		policies[group.Id()] = fallback
		key := member.Cluster.GetNamespace() + "/" + string(member.Cluster.GetUID())
		if available[key] == nil {
			available[key] = map[string]*Member{}
		}
		available[key][group.Object().GetName()] = member
	}
	members := policy.ConfiguredMembers(ctx, available, policies)
	resynchronized := len(members) > 0 && len(members) == len(available)
	for _, pairs := range members {
		for name, roles := range pairs {
			primary, secondary := roles["primary"], roles["secondary"]
			if primary == nil || secondary == nil {
				resynchronized = false
				continue
			}
			pairObservations := observations
			if overflow {
				pairObservations, err = policy.resyncPairObservations(primary, secondary, observations)
				if err != nil {
					resynchronized = false
					BlockMembers(roles, policies, err)
					continue
				}
			}
			var state *State
			state, err := policy.Store.Reconcile(ctx, primary.Cluster, func(state *State) error {
				return policy.reconcilePair(state, name, primary, secondary, pairObservations, now)
			})
			if err != nil {
				resynchronized = false
				BlockMembers(roles, policies, err)
				klog.Warningf("Failover pair %s/%s admission suppressed: %v", primary.Cluster.GetNamespace(), name, err)
				continue
			}
			policy.publishPair(name, state.Pairs[name], primary, secondary, pairObservations, policies)
		}
	}
	policy.Lock()
	if policy.Overflow {
		if overflow && resynchronized && policy.OverflowVersion == overflowVersion {
			for key, processed := range observations {
				if current := policy.Observations[key]; current.UID == processed.UID && current.Created.Equal(&processed.Created) {
					delete(policy.Observations, key)
				}
			}
			policy.Overflow = false
		} else {
			for id, snapshot := range policies {
				snapshot.ScaleUpBlocked = true
				snapshot.ConsiderPrimaryUnfit = false
				snapshot.RetainTarget = true
				snapshot.Reason = "failure observation overflow requires successful resynchronization"
				policies[id] = snapshot
			}
		}
	}
	policy.Policies = policies
	policy.FitExceptions = map[string]string{}
	policy.Unlock()
}
