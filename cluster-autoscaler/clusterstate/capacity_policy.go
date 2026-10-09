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
