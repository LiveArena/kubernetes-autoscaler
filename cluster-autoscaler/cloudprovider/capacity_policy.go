package cloudprovider

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

type NodeGroupPrimaryFitFallback interface {
	ResetPrimaryFitFallback()
	AllowPrimaryFitFallback(reason string) error
}

type NodeGroupCapacityPolicyProvider interface {
	GetCapacityPolicy() NodeGroupCapacityPolicy
}

func GetNodeGroupCapacityPolicy(nodeGroup NodeGroup) NodeGroupCapacityPolicy {
	if provider, ok := nodeGroup.(NodeGroupCapacityPolicyProvider); ok {
		return provider.GetCapacityPolicy()
	}
	return NodeGroupCapacityPolicy{}
}
