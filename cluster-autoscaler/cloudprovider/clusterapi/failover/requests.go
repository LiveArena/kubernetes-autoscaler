package failover

import (
	"fmt"
)

// ReconcileRequestTargets validates durable intents and retires committed secondary requests.
func (pair *Pair) ReconcileRequestTargets(primaryTarget, secondaryTarget int) error {
	for _, observation := range []struct {
		request *Request
		Target  int
	}{{pair.PrimaryRequest, primaryTarget}, {pair.SecondaryRequest, secondaryTarget}} {
		if observation.request != nil && observation.Target != observation.request.FromTarget && observation.Target != observation.request.ToTarget {
			return fmt.Errorf("failover target differs from its durable request")
		}
	}
	if pair.SecondaryRequest != nil && secondaryTarget == pair.SecondaryRequest.ToTarget {
		pair.FallbackAllowance = 0
		pair.SecondaryRequest = nil
	}
	return nil
}
// AcceptPrimaryFailure grants fallback allowance for newly failed requested capacity.
func (pair *Pair) AcceptPrimaryFailure(observation FailureObservation, alreadyFailed bool, target, ready int) {
	if request := pair.PrimaryRequest; request != nil {
		if target == request.ToTarget && observation.UID != request.FailureUIDAtStart && observation.Created.Time.After(request.Started.Time) {
			pair.FallbackAllowance = request.ToTarget - request.FromTarget
			pair.PrimaryRequest = nil
		}
		return
	}
	if !alreadyFailed {
		pair.FallbackAllowance = max(0, target-ready)
	}
}
// CompletePrimaryArrival clears a fulfilled trial and unused failure-derived secondary intent.
func (pair *Pair) CompletePrimaryArrival(ready []string, target int) {
	request := pair.PrimaryRequest
	if request != nil && target == request.ToTarget && len(ready) >= request.ReadyAtStart+request.ToTarget-request.FromTarget && ReadyFingerprint(ready) != request.ReadyFingerprint {
		pair.PrimaryRequest = nil
		pair.FallbackAllowance = 0
		if pair.SecondaryRequest != nil && pair.SecondaryRequest.PrimaryUnavailableReason == "" && pair.Secondary.ObservedTarget == pair.SecondaryRequest.FromTarget {
			pair.SecondaryRequest = nil
		}
	}
}
// RequestAdmission computes role-specific growth blocking, incoming credit and request limits.
func (pair *Pair) RequestAdmission(role string, primaryTarget, primaryMaximum int, frozen bool) (blocked bool, reliable, limit int) {
	if role == "secondary" {
		if frozen || pair.Secondary.Failed || pair.PrimaryRequest != nil && (pair.SecondaryRequest == nil || pair.SecondaryRequest.PrimaryUnavailableReason == "") {
			return true, 0, 0
		}
		if pair.SecondaryRequest != nil {
			return false, 0, pair.SecondaryRequest.ToTarget - pair.SecondaryRequest.FromTarget
		}
		if pair.FallbackAllowance > 0 {
			return false, 0, pair.FallbackAllowance
		}
		return primaryTarget < primaryMaximum, 0, 0
	}
	if request := pair.PrimaryRequest; request != nil {
		if primaryTarget == request.FromTarget {
			return false, 0, request.ToTarget - request.FromTarget
		}
		return true, request.ToTarget - request.FromTarget, 0
	}
	return pair.FallbackAllowance > 0 || pair.SecondaryRequest != nil || primaryTarget >= primaryMaximum, 0, 0
}
// ReliablePrimaryGap subtracts new Ready and viable registered arrivals from the trial increment.
func (pair *Pair) ReliablePrimaryGap(primary *Member) int {
	request := pair.PrimaryRequest
	if request == nil || primary.Target != request.ToTarget {
		return 0
	}
	remaining := request.ToTarget - request.FromTarget - max(0, len(primary.Ready)-request.ReadyAtStart)
	for name, created := range primary.NotReadyCreated {
		if created.Time.After(request.Started.Time) && !primary.FailedNodes[name] {
			remaining--
		}
	}
	return max(0, remaining)
}
