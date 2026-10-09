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

package cloudprovider

// NodeGroupCapacityPolicy describes optional capacity accounting and admission constraints.
type NodeGroupCapacityPolicy struct {
	BlockUnregistered     bool
	FailedRegisteredNodes map[string]bool
	RetainTarget          bool
	ScaleUpBlocked        bool
	Reason                string
	ExpectedTarget        *int
	RequireFullScaleUp    bool
	ReliableUnregistered  int
	ScaleUpLimit          int
	ScaleDownPair         string
	ScaleDownSecondary    bool
	PrimaryNodeGroupID    string
	ConsiderPrimaryUnfit  bool
}

// NodeGroupPrimaryFitFallback exposes scan-local admission for verified primary fit failures.
type NodeGroupPrimaryFitFallback interface {
	ResetPrimaryFitFallback()
	AllowPrimaryFitFallback(reason string) error
}

// NodeGroupCapacityPolicyProvider supplies optional node-group policy without changing NodeGroup.
type NodeGroupCapacityPolicyProvider interface {
	GetCapacityPolicy() NodeGroupCapacityPolicy
}

// GetNodeGroupCapacityPolicy returns the group's policy or unrestricted defaults.
func GetNodeGroupCapacityPolicy(nodeGroup NodeGroup) NodeGroupCapacityPolicy {
	if provider, ok := nodeGroup.(NodeGroupCapacityPolicyProvider); ok {
		return provider.GetCapacityPolicy()
	}
	return NodeGroupCapacityPolicy{}
}
