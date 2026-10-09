package failover

import "fmt"

func (policy *Policy) ResetPrimaryFitFallback(id string) {
	policy.Lock()
	delete(policy.FitExceptions, id)
	policy.Unlock()
}

func (policy *Policy) AllowPrimaryFitFallback(id, reason string) error {
	if reason == "" {
		return fmt.Errorf("primary-fit fallback is disabled")
	}
	policy.Lock()
	defer policy.Unlock()
	if policy.Stopped.Load() || !policy.Policies[id].ConsiderPrimaryUnfit {
		return fmt.Errorf("primary-fit fallback is not currently eligible")
	}
	if policy.FitExceptions == nil {
		policy.FitExceptions = map[string]string{}
	}
	policy.FitExceptions[id] = reason
	return nil
}

func (policy *Policy) PrimaryFitFallbackReason(id string) string {
	policy.RLock()
	defer policy.RUnlock()
	return policy.FitExceptions[id]
}
