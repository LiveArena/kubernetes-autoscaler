package failover

import (
	"fmt"
	"k8s.io/apimachinery/pkg/types"
)

func (state *State) Validate(clusterUID types.UID) error {
	if state.Version != StateVersion || state.ClusterUID != clusterUID || len(state.Pairs) > PairLimit || state.Pairs == nil {
		return fmt.Errorf("unsupported, stale, or unbounded failover state")
	}
	for name, pair := range state.Pairs {
		if !ValidPairIdentifier(name) || pair == nil || pair.RecoveryScans < 0 || pair.RecoveryScans > 1 {
			return fmt.Errorf("invalid failover pair state")
		}
		if pair.Phase != Healthy && pair.Phase != Degraded && pair.Phase != Recovering {
			return fmt.Errorf("unknown failover phase")
		}
		if (pair.Phase == Healthy) == pair.Primary.Failed {
			return fmt.Errorf("inconsistent failover phase and primary failure state")
		}
		if pair.FallbackAllowance < 0 {
			return fmt.Errorf("invalid fallback allowance")
		}
		for _, request := range []*Request{pair.PrimaryRequest, pair.SecondaryRequest} {
			if request != nil && (request.FromTarget < 0 || request.ToTarget <= request.FromTarget || request.Started.IsZero() || request.ReadyAtStart < 0) {
				return fmt.Errorf("invalid failover scale request")
			}
			if request != nil && request.Deadline != nil && !request.Deadline.Time.After(request.Started.Time) {
				return fmt.Errorf("invalid primary provisioning deadline")
			}
		}
		for _, role := range []Role{pair.Primary, pair.Secondary} {
			if role.CheckInterval < 0 || role.CheckInterval > 15*60 || role.ObservedTarget < 0 || role.ReadyCountAtFailure < 0 || role.FailureEpoch < 0 {
				return fmt.Errorf("invalid failover role state")
			}
		}
	}
	return nil
}
