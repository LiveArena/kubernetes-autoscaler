package failover

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (policy *Policy) reconcilePair(state *State, name string, primary, secondary *Member, observations map[string]FailureObservation, now time.Time) error {
	pair := state.Pairs[name]
	if pair == nil {
		pair = &Pair{Phase: Healthy}
		state.Pairs[name] = pair
	}
	for role, member := range map[string]*Member{"primary": primary, "secondary": secondary} {
		record := pair.Primary
		if role == "secondary" {
			record = pair.Secondary
		}
		poolUID, infrastructureUID := member.Group.Object().GetUID(), member.Infrastructure.GetUID()
		if (record.PoolUID != "" && record.PoolUID != poolUID) || (record.InfrastructureUID != "" && record.InfrastructureUID != infrastructureUID) {
			return fmt.Errorf("failover identifier %q is already registered to different %s resources; prior records must be fully purged through authorized retirement before reuse", name, role)
		}
	}
	if err := pair.ReconcileRequestTargets(primary.Target, secondary.Target); err != nil {
		return err
	}
	newPrimaryFailure := false
	for role, member := range map[string]*Member{"primary": primary, "secondary": secondary} {
		record := &pair.Primary
		if role == "secondary" {
			record = &pair.Secondary
		}
		poolUID, infrastructureUID := member.Group.Object().GetUID(), member.Infrastructure.GetUID()
		record.PoolUID, record.InfrastructureUID = poolUID, infrastructureUID
		key := member.Cluster.GetNamespace() + "/" + string(infrastructureUID)
		if observation, found := observations[key]; found && observation.OwnerName == member.Infrastructure.GetName() {
			wasFailed := record.Failed
			fresh := record.ObserveFailure(observation, member.Ready, now, policy.ScanInterval)
			if role == "primary" && fresh {
				pair.AcceptPrimaryFailure(observation, wasFailed, member.Target, len(member.Ready))
			}
			newPrimaryFailure = newPrimaryFailure || role == "primary" && fresh
		}
	}
	pair.CompletePrimaryArrival(primary.Ready, primary.Target)
	if request := pair.PrimaryRequest; request != nil && request.Deadline == nil {
		duration, err := policy.PrimaryTrialDuration(primary.Group)
		if err != nil {
			return err
		}
		deadline := metav1.NewTime(request.Started.Add(duration))
		request.Deadline = &deadline
	}
	newPrimaryFailure = pair.ExpirePrimaryTrial(primary, now) || newPrimaryFailure
	pair.ReconcileReadiness(primary.Ready, secondary.Ready, primary.Target, secondary.Target, newPrimaryFailure, now)
	return nil
}
