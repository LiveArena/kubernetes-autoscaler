package failover

import (
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

const (
	ConfigVersion = "aiproducer.com/worker-failover/v1"
	ModeDisabled  = "disabled"
	ModeActive    = "active"
	ModeFreeze    = "freeze"
	PairLimit     = 2
)

type Configuration struct {
	APIVersion string                    `json:"apiVersion"`
	Cluster    ClusterIdentity           `json:"cluster"`
	Pairs      map[string]ConfiguredPair `json:"pairs"`
}
type ClusterIdentity struct {
	Name      string    `json:"name"`
	Namespace string    `json:"namespace"`
	UID       types.UID `json:"uid"`
}
type ConfiguredPair struct {
	Primary   ConfiguredPool `json:"primary"`
	Secondary ConfiguredPool `json:"secondary"`
}
type ConfiguredPool struct {
	Name string `json:"name"`
}

func ValidateMode(mode string) error {
	if mode != ModeDisabled && mode != ModeActive && mode != ModeFreeze {
		return fmt.Errorf("invalid azure-machinepool-failover-mode %q", mode)
	}
	return nil
}
func DisabledCapacity(pool *unstructured.Unstructured) cloudprovider.NodeGroupCapacityPolicy {
	annotations := pool.GetAnnotations()
	pair := annotations[PairKey]
	if pair != "" && annotations[RoleKey] == "secondary" {
		return cloudprovider.NodeGroupCapacityPolicy{ScaleUpBlocked: true, RequireFullScaleUp: true, Reason: "Azure MachinePool failover mode is disabled"}
	}
	return cloudprovider.NodeGroupCapacityPolicy{}
}
func ValidPairIdentifier(identifier string) bool {
	return identifier != "" && len(validation.IsValidLabelValue(identifier)) == 0
}
