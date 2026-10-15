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

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"
)

// ReconcileReadiness applies positive-evidence recovery and bounded secondary rechecks.
func (pair *Pair) ReconcileReadiness(primaryReady, secondaryReady []string, primaryTarget, secondaryTarget int, newPrimaryFailure bool, now time.Time) {
	pair.Primary.ObservedTarget = primaryTarget
	pair.Secondary.ObservedTarget = secondaryTarget
	if newPrimaryFailure {
		pair.Phase = Degraded
		pair.RecoveryScans = 0
		pair.RecoveryScanTime = metav1.Time{}
	}
	if pair.Primary.Failed && !newPrimaryFailure {
		positive := len(primaryReady) > 0 && len(primaryReady) >= pair.Primary.ReadyCountAtFailure && ReadyFingerprint(primaryReady) != pair.Primary.ReadyFingerprintAtFailure
		if positive {
			pair.Phase = Recovering
			if len(primaryReady) >= primaryTarget {
				if now.After(pair.RecoveryScanTime.Time) {
					pair.RecoveryScans++
					pair.RecoveryScanTime = metav1.NewTime(now)
				}
			} else {
				pair.RecoveryScans = 0
				pair.RecoveryScanTime = metav1.Time{}
			}
			if pair.RecoveryScans >= 2 {
				pair.Primary.Failed = false
				pair.Primary.CheckInterval = 0
				pair.Primary.NextCheck = metav1.Time{}
				pair.Phase = Healthy
				pair.RecoveryScans = 0
				pair.RecoveryScanTime = metav1.Time{}
				pair.FallbackAllowance = 0
				if pair.SecondaryRequest != nil && pair.SecondaryRequest.PrimaryUnavailableReason == "" && secondaryTarget == pair.SecondaryRequest.FromTarget {
					pair.SecondaryRequest = nil
				}
			}
		} else {
			pair.RecoveryScans = 0
			pair.RecoveryScanTime = metav1.Time{}
		}
	}
	if pair.Secondary.Failed && !now.Before(pair.Secondary.NextCheck.Time) {
		if len(secondaryReady) > 0 && len(secondaryReady) >= secondaryTarget && ReadyFingerprint(secondaryReady) != pair.Secondary.ReadyFingerprintAtFailure {
			pair.Secondary.Failed = false
			pair.Secondary.CheckInterval = 0
			pair.Secondary.NextCheck = metav1.Time{}
		} else {
			pair.Secondary.CheckInterval = min(15*60, max(1, pair.Secondary.CheckInterval*2))
			pair.Secondary.NextCheck = metav1.NewTime(now.Add(time.Duration(pair.Secondary.CheckInterval) * time.Second))
		}
	}
}
