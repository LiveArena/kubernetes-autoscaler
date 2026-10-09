/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package failover

import "fmt"

// ResetPrimaryFitFallback discards a group's scan-local primary-fit exception.
func (policy *Policy) ResetPrimaryFitFallback(id string) {
	policy.Lock()
	delete(policy.FitExceptions, id)
	policy.Unlock()
}

// AllowPrimaryFitFallback records a reason only for a currently eligible group.
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

// PrimaryFitFallbackReason returns the group's current scan-local exception reason.
func (policy *Policy) PrimaryFitFallbackReason(id string) string {
	policy.RLock()
	defer policy.RUnlock()
	return policy.FitExceptions[id]
}
