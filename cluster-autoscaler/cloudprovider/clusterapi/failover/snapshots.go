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

import "k8s.io/autoscaler/cluster-autoscaler/cloudprovider"

func (policy *Policy) publishPair(name string, pair *Pair, primary, secondary *Member, observations map[string]FailureObservation, policies map[string]cloudprovider.NodeGroupCapacityPolicy) {
	for role, member := range map[string]*Member{"primary": primary, "secondary": secondary} {
		key := member.Cluster.GetNamespace() + "/" + string(member.Infrastructure.GetUID())
		if processed, found := observations[key]; found {
			policy.Lock()
			if current := policy.Observations[key]; current.UID == processed.UID && current.Created.Equal(&processed.Created) {
				delete(policy.Observations, key)
			}
			policy.Unlock()
		}
		record := pair.Primary
		if role == "secondary" {
			record = pair.Secondary
		}
		blocked, reliable, limit := pair.RequestAdmission(role, primary.Target, primary.Group.MaxSize(), policy.Frozen)
		if role == "primary" && reliable > 0 {
			reliable = pair.ReliablePrimaryGap(primary)
		}
		policies[member.Group.Id()] = cloudprovider.NodeGroupCapacityPolicy{
			BlockUnregistered: record.Failed, FailedRegisteredNodes: member.FailedNodes,
			RetainTarget: record.Failed, ScaleUpBlocked: blocked, Reason: "failover " + pair.Phase,
			ExpectedTarget: &member.Target, RequireFullScaleUp: role == "secondary",
			ReliableUnregistered: reliable, ScaleUpLimit: limit,
			ScaleDownPair:      member.Cluster.GetNamespace() + "/" + string(member.Cluster.GetUID()) + "/" + name,
			ScaleDownSecondary: role == "secondary", PrimaryNodeGroupID: primary.Group.Id(),
			ConsiderPrimaryUnfit: role == "secondary" && !policy.Frozen && !pair.Secondary.Failed && pair.SecondaryRequest == nil && pair.FallbackAllowance == 0 &&
				(pair.PrimaryRequest == nil && primary.Target < primary.Group.MaxSize() || pair.PrimaryRequest != nil && primary.Target == pair.PrimaryRequest.ToTarget),
		}
	}
}
