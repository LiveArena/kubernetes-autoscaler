package failover

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/clock"
	"sync"
	"sync/atomic"
	"time"
)

// Policy reconciles durable pair state and publishes capacity and admission snapshots.
type Policy struct {
	sync.RWMutex
	Environment       Environment
	Store             *Store
	Clock             clock.Clock
	Stopped           atomic.Bool
	WriterContext     context.Context
	CancelWriter      context.CancelFunc
	WriterMu          sync.Mutex
	WriterOperations  sync.WaitGroup
	FitExceptions     map[string]string
	ScanInterval      time.Duration
	NodeGroupDefaults config.NodeGroupAutoscalingOptions
	Frozen            bool
	Policies          map[string]cloudprovider.NodeGroupCapacityPolicy
	Observations      map[string]FailureObservation
	Overflow          bool
	Handler           cache.ResourceEventHandlerRegistration
}

// Observe retains the newest terminal failure per AMP owner in a bounded buffer.
func (policy *Policy) Observe(object interface{}) {
	machine, ok := object.(*unstructured.Unstructured)
	if !ok {
		return
	}
	observation, terminal := TerminalAMPMFailure(machine)
	if !terminal {
		return
	}
	key := observation.Namespace + "/" + string(observation.OwnerUID)
	policy.Lock()
	defer policy.Unlock()
	previous, exists := policy.Observations[key]
	if !exists && len(policy.Observations) >= 64 {
		policy.Overflow = true
		return
	}
	if !exists || observation.Created.After(previous.Created.Time) || (observation.Created.Equal(&previous.Created) && observation.UID > previous.UID) {
		policy.Observations[key] = observation
	}
}
// Capacity returns the group's current snapshot, including eligible scan-local fit exceptions.
func (policy *Policy) Capacity(group Group) cloudprovider.NodeGroupCapacityPolicy {
	policy.RLock()
	defer policy.RUnlock()
	if result, found := policy.Policies[group.Id()]; found {
		if reason := policy.FitExceptions[group.Id()]; reason != "" && result.ConsiderPrimaryUnfit {
			result.ScaleUpBlocked = false
			result.Reason = reason
		}
		return result
	}
	annotations := group.Object().GetAnnotations()
	if annotations[PairKey] != "" || annotations[RoleKey] != "" {
		return cloudprovider.NodeGroupCapacityPolicy{ScaleUpBlocked: true, RetainTarget: true, Reason: "failover state not reconciled"}
	}
	return cloudprovider.NodeGroupCapacityPolicy{}
}
// AssertWriter verifies local writer activity and the group's Cluster owner reference.
func (policy *Policy) AssertWriter(ctx context.Context, group Group) error {
	if policy.Stopped.Load() {
		return fmt.Errorf("autoscaler failover writer is stopped")
	}
	if err := policy.RequestContext().Err(); err != nil {
		return err
	}
	pool := group.Object()
	for _, owner := range pool.GetOwnerReferences() {
		if owner.Kind != "Cluster" || owner.UID == "" {
			continue
		}
		return nil
	}
	return fmt.Errorf("failover writer has no owning Cluster")
}
