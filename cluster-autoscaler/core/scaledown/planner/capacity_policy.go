package planner

import (
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
)

func orderPairPositions[Item any](items []Item, describe func(Item) (string, bool)) []Item {
	positions := map[string][]int{}
	for index, item := range items {
		key, _ := describe(item)
		if key != "" {
			positions[key] = append(positions[key], index)
		}
	}
	ordered := slices.Clone(items)
	for _, indices := range positions {
		preferred, remaining := []Item{}, []Item{}
		for _, index := range indices {
			_, secondary := describe(items[index])
			if secondary {
				preferred = append(preferred, items[index])
			} else {
				remaining = append(remaining, items[index])
			}
		}
		for offset, item := range append(preferred, remaining...) {
			ordered[indices[offset]] = item
		}
	}
	return ordered
}

func (planner *Planner) nodeCapacityPolicy(node *corev1.Node) cloudprovider.NodeGroupCapacityPolicy {
	group, err := planner.context.CloudProvider.NodeGroupForNode(node)
	if err != nil || group == nil {
		return cloudprovider.NodeGroupCapacityPolicy{}
	}
	return cloudprovider.GetNodeGroupCapacityPolicy(group)
}

func (planner *Planner) orderPairedCandidates(nodes []*corev1.Node) []*corev1.Node {
	return orderPairPositions(nodes, func(node *corev1.Node) (string, bool) {
		policy := planner.nodeCapacityPolicy(node)
		if policy.ScaleDownPair == "" {
			return "", false
		}
		info, err := planner.context.ClusterSnapshot.GetNodeInfo(node.Name)
		if err != nil {
			return "", false
		}
		return fmt.Sprintf("%s/occupied=%t", policy.ScaleDownPair, len(info.Pods()) > 0), policy.ScaleDownSecondary
	})
}

func (planner *Planner) orderPairedRemovals(nodes []simulator.NodeToBeRemoved) []simulator.NodeToBeRemoved {
	return orderPairPositions(nodes, func(node simulator.NodeToBeRemoved) (string, bool) {
		policy := planner.nodeCapacityPolicy(node.Node)
		if policy.ScaleDownPair == "" {
			return "", false
		}
		return fmt.Sprintf("%s/drain=%t/risky=%t", policy.ScaleDownPair, len(node.PodsToReschedule) > 0, node.IsRisky), policy.ScaleDownSecondary
	})
}
