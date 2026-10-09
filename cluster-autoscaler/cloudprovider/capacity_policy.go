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
