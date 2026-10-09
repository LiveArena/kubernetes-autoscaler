package failover

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/klog/v2"
)

func SelectionReason(group Group, reason error) string {
	annotations := group.Object().GetAnnotations()
	message := fmt.Sprintf("failover pair %q role %q pool %s uid %s: %v", annotations[PairKey], annotations[RoleKey], group.Object().GetName(), group.Object().GetUID(), reason)
	return message[:min(len(message), 1024)]
}
func BlockMembers(candidates map[string]*Member, policies map[string]cloudprovider.NodeGroupCapacityPolicy, reason error) {
	for _, member := range candidates {
		snapshot := policies[member.Group.Id()]
		snapshot.ScaleUpBlocked = true
		snapshot.ConsiderPrimaryUnfit = false
		snapshot.Reason = SelectionReason(member.Group, reason)
		policies[member.Group.Id()] = snapshot
	}
}
func (policy *Policy) ConfiguredMembers(ctx context.Context, available map[string]map[string]*Member, policies map[string]cloudprovider.NodeGroupCapacityPolicy) map[string]map[string]map[string]*Member {
	result := map[string]map[string]map[string]*Member{}
	for key, candidates := range available {
		var cluster *unstructured.Unstructured
		for _, member := range candidates {
			cluster = member.Cluster
			break
		}
		configuration, err := ReadConfiguration(ctx, policy.Store.Client, cluster)
		if err != nil {
			BlockMembers(candidates, policies, err)
			klog.Warningf("Failover configuration %s/%s unavailable: %v", cluster.GetNamespace(), cluster.GetName(), err)
			continue
		}
		resolved, err := policy.ResolveConfiguredPools(ctx, cluster, configuration)
		if err != nil {
			BlockMembers(candidates, policies, err)
			klog.Warningf("Failover configuration %s/%s is incomplete: %v", cluster.GetNamespace(), cluster.GetName(), err)
			continue
		}
		selected := map[string]map[string]*Member{}
		valid := true
		for name, pair := range configuration.Pairs {
			selected[name] = map[string]*Member{}
			for role, reference := range map[string]ConfiguredPool{"primary": pair.Primary, "secondary": pair.Secondary} {
				member := candidates[reference.Name]
				if member == nil || member.Group.Object().GetUID() != resolved[reference.Name] {
					valid = false
					break
				}
				annotations := member.Group.Object().GetAnnotations()
				if annotations[PairKey] != name || annotations[RoleKey] != role {
					valid = false
					break
				}
				selected[name][role] = member
			}
		}
		if valid {
			result[key] = selected
		} else {
			BlockMembers(candidates, policies, fmt.Errorf("configuration references missing or mismatched current pools"))
			klog.Warningf("Failover configuration %s/%s references missing or mismatched current pools", cluster.GetNamespace(), cluster.GetName())
		}
	}
	return result
}
