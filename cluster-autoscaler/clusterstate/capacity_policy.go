package clusterstate

import "k8s.io/autoscaler/cluster-autoscaler/cloudprovider"

func applyUpcomingCapacityPolicy(nodeGroup cloudprovider.NodeGroup, count int, registered []string) (int, []string) {
	policy := cloudprovider.GetNodeGroupCapacityPolicy(nodeGroup)
	if !policy.BlockUnregistered && len(policy.FailedRegisteredNodes) == 0 {
		return count, registered
	}
	viable := make([]string, 0, len(registered))
	for _, nodeName := range registered {
		if !policy.FailedRegisteredNodes[nodeName] {
			viable = append(viable, nodeName)
		}
	}
	count -= len(registered) - len(viable)
	if policy.BlockUnregistered {
		count = min(count, len(viable)+policy.ReliableUnregistered)
		if count > 0 {
			viable = viable[:min(count, len(viable))]
		}
	}
	return count, viable
}
