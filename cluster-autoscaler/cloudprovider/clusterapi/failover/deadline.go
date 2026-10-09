package failover

import (
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"time"
)

// Request records a durable replica increment and its reconciliation evidence.
type Request struct {
	FromTarget               int          `json:"fromTarget"`
	ToTarget                 int          `json:"toTarget"`
	Started                  metav1.Time  `json:"started"`
	FailureUIDAtStart        types.UID    `json:"failureUIDAtStart"`
	ReadyAtStart             int          `json:"readyAtStart"`
	ReadyFingerprint         string       `json:"readyFingerprint"`
	Deadline                 *metav1.Time `json:"deadline,omitempty"`
	PrimaryUnavailableReason string       `json:"primaryUnavailableReason,omitempty"`
}

// ExpirePrimaryTrial converts an expired primary trial at its requested target into fallback allowance.
func (pair *Pair) ExpirePrimaryTrial(primary *Member, now time.Time) bool {
	request := pair.PrimaryRequest
	if request == nil || request.Deadline == nil || primary.Target != request.ToTarget || now.Before(request.Deadline.Time) {
		return false
	}
	pair.FallbackAllowance = pair.ReliablePrimaryGap(primary)
	pair.PrimaryRequest = nil
	pair.Phase = Degraded
	pair.RecoveryScans = 0
	pair.RecoveryScanTime = metav1.Time{}
	pair.Primary.Failed = true
	pair.Primary.ReadyCountAtFailure = len(primary.Ready)
	pair.Primary.ReadyFingerprintAtFailure = ReadyFingerprint(primary.Ready)
	return true
}
// PrimaryTrialDuration returns the group's positive configured provisioning timeout.
func (policy *Policy) PrimaryTrialDuration(group Group) (time.Duration, error) {
	options, err := group.GetOptions(policy.NodeGroupDefaults)
	if err != nil {
		return 0, err
	}
	if options == nil || options.MaxNodeProvisionTime <= 0 {
		return 0, fmt.Errorf("primary trial requires a positive configured MaxNodeProvisionTime")
	}
	return options.MaxNodeProvisionTime, nil
}
