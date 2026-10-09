package failover

import (
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"
)

func (policy *Policy) PrepareScaleRequest(group Group, delta int) error {
	if !policy.BeginWriterOperation() {
		return fmt.Errorf("failover writer has stopped")
	}
	defer policy.WriterOperations.Done()
	ctx := policy.RequestContext()
	member, err := policy.Member(ctx, group)
	if err != nil {
		return err
	}
	annotations := group.Object().GetAnnotations()
	name, role := annotations[PairKey], annotations[RoleKey]
	_, err = policy.Store.Reconcile(ctx, member.Cluster, func(state *State) error {
		pair := state.Pairs[name]
		if pair == nil {
			return fmt.Errorf("failover pair not durably reconciled")
		}
		request := &pair.PrimaryRequest
		record := &pair.Primary
		if role == "secondary" {
			request = &pair.SecondaryRequest
			record = &pair.Secondary
		} else if role != "primary" {
			return fmt.Errorf("invalid failover request role")
		}
		if role == "secondary" && (policy.Frozen || pair.Secondary.Failed) {
			return fmt.Errorf("secondary request window is not available")
		}
		if *request != nil {
			if member.Target == (*request).FromTarget && member.Target+delta == (*request).ToTarget {
				return nil
			}
			return fmt.Errorf("another durable failover request is outstanding")
		}
		if role == "primary" && (pair.FallbackAllowance > 0 || pair.SecondaryRequest != nil) {
			return fmt.Errorf("primary request window is not available")
		}
		if role == "secondary" {
			fitReason := policy.PrimaryFitFallbackReason(group.Id())
			if policy.Frozen || pair.PrimaryRequest != nil && (member.Target < 0 || fitReason == "") || pair.Secondary.Failed || pair.FallbackAllowance > 0 && delta > pair.FallbackAllowance {
				return fmt.Errorf("secondary request window is not available")
			}
			if pair.FallbackAllowance == 0 {
				capped, err := policy.PrimaryAtMaximum(ctx, member.Cluster, name, pair.Primary.PoolUID)
				if err != nil {
					return err
				}
				if !capped && fitReason == "" {
					return fmt.Errorf("new request must try primary first")
				}
			}
		}
		*request = &Request{FromTarget: member.Target, ToTarget: member.Target + delta, Started: metav1.NewTime(policy.Clock.Now().Truncate(time.Second)), FailureUIDAtStart: record.LastAttemptUID, ReadyAtStart: len(member.Ready), ReadyFingerprint: ReadyFingerprint(member.Ready)}
		if role == "secondary" {
			(*request).PrimaryUnavailableReason = policy.PrimaryFitFallbackReason(group.Id())
		}
		if role == "primary" {
			duration, err := policy.PrimaryTrialDuration(group)
			if err != nil {
				return err
			}
			deadline := metav1.NewTime((*request).Started.Add(duration))
			(*request).Deadline = &deadline
			pair.FallbackAllowance = 0
		}
		return nil
	})
	return err
}
