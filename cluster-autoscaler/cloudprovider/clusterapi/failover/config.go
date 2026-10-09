package failover

import (
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

// ConfigVersion and the mode constants define the accepted configuration and activation contract.
const (
	ConfigVersion = "aiproducer.com/worker-failover/v1"
	ModeDisabled  = "disabled"
	ModeActive    = "active"
	ModeFreeze    = "freeze"
)

// Configuration selects failover pairs for one actual Cluster identity.
type Configuration struct {
	APIVersion string                    `json:"apiVersion"`
	Cluster    ClusterIdentity           `json:"cluster"`
	Pairs      map[string]ConfiguredPair `json:"pairs"`
}
// ClusterIdentity binds configuration to a Cluster's namespace, name and UID.
type ClusterIdentity struct {
	Name      string    `json:"name"`
	Namespace string    `json:"namespace"`
	UID       types.UID `json:"uid"`
}
// ConfiguredPair names the primary and secondary MachinePools selected for one identifier.
type ConfiguredPair struct {
	Primary   ConfiguredPool `json:"primary"`
	Secondary ConfiguredPool `json:"secondary"`
}
// ConfiguredPool selects a MachinePool by name in the owning Cluster's namespace.
type ConfiguredPool struct {
	Name string `json:"name"`
}

// ValidateMode rejects unsupported failover activation modes.
func ValidateMode(mode string) error {
	if mode != ModeDisabled && mode != ModeActive && mode != ModeFreeze {
		return fmt.Errorf("invalid azure-machinepool-failover-mode %q", mode)
	}
	return nil
}
// DisabledCapacity blocks recognized secondary growth without hiding existing capacity.
func DisabledCapacity(pool *unstructured.Unstructured) cloudprovider.NodeGroupCapacityPolicy {
	annotations := pool.GetAnnotations()
	pair := annotations[PairKey]
	if pair != "" && annotations[RoleKey] == "secondary" {
		return cloudprovider.NodeGroupCapacityPolicy{ScaleUpBlocked: true, RequireFullScaleUp: true, Reason: "Azure MachinePool failover mode is disabled"}
	}
	return cloudprovider.NodeGroupCapacityPolicy{}
}
// ValidPairIdentifier accepts nonempty Kubernetes label values as opaque pair keys.
func ValidPairIdentifier(identifier string) bool {
	return identifier != "" && len(validation.IsValidLabelValue(identifier)) == 0
}
