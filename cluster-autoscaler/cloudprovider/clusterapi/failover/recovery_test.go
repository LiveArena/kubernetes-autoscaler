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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"testing"
	"time"
)

func TestOnlyLiveTerminalAMPMFailuresProduceFailureObservations(t *testing.T) {
	for _, provisioning := range []string{"Pending", "Succeeded", "Failed", ""} {
		statement := map[string]string{
			"Pending":   "PendingProvisioningDoesNotProduceTerminalFailureEvidence",
			"Succeeded": "SuccessfulProvisioningDoesNotProduceTerminalFailureEvidence",
			"Failed":    "LiveFailedProvisioningProducesEvidenceButDeletingMachinesDoNot",
			"":          "MissingProvisioningStateDoesNotProduceTerminalFailureEvidence",
		}[provisioning]
		t.Run(statement, func(t *testing.T) {
			machine := &unstructured.Unstructured{Object: map[string]interface{}{"status": map[string]interface{}{"provisioningState": provisioning}}}
			machine.SetNamespace("tenant")
			machine.SetUID("attempt")
			machine.SetCreationTimestamp(metav1.NewTime(time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)))
			machine.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "infrastructure.cluster.x-k8s.io/v1beta1", Kind: "AzureMachinePool", Name: "primary", UID: "amp"}})
			observation, terminal := TerminalAMPMFailure(machine)
			assert.Equal(t, provisioning == "Failed", terminal)
			if terminal {
				assert.Equal(t, types.UID("amp"), observation.OwnerUID)
			}
			machine.SetDeletionTimestamp(&metav1.Time{Time: time.Now()})
			_, terminal = TerminalAMPMFailure(machine)
			assert.False(t, terminal)
		})
	}
}
func TestRepeatedFailuresAreDeduplicatedAndNewReadinessRestoresPrimaryPreference(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	pair := &Pair{Phase: Healthy}
	failure := FailureObservation{UID: "failure", Created: metav1.NewTime(now)}
	require.True(t, pair.Primary.ObserveFailure(failure, []string{"occupied"}, now, 10*time.Second))
	pair.ReconcileReadiness([]string{"occupied"}, nil, 3, 0, true, now)
	assert.Equal(t, Degraded, pair.Phase)
	for index := 0; index < 100; index++ {
		assert.False(t, pair.Primary.ObserveFailure(failure, []string{"occupied"}, now, 10*time.Second))
		pair.ReconcileReadiness([]string{"occupied"}, nil, 3, 2, false, now)
	}
	assert.Equal(t, int64(1), pair.Primary.FailureEpoch)
	assert.True(t, pair.Primary.Failed)
	pair.FallbackAllowance = 2
	pair.SecondaryRequest = &Request{FromTarget: 2, ToTarget: 3, Started: metav1.NewTime(now)}
	ready := []string{"occupied", "late-1", "late-2"}
	pair.ReconcileReadiness(ready, []string{"secondary-1", "secondary-2"}, 3, 2, false, now)
	assert.Equal(t, Recovering, pair.Phase)
	pair.ReconcileReadiness(ready, []string{"secondary-1", "secondary-2"}, 3, 2, false, now)
	assert.Equal(t, 1, pair.RecoveryScans)
	pair.ReconcileReadiness(ready, []string{"secondary-1", "secondary-2"}, 3, 2, false, now.Add(10*time.Second))
	assert.Equal(t, Healthy, pair.Phase)
	assert.False(t, pair.Primary.Failed)
	assert.Zero(t, pair.FallbackAllowance)
	assert.Nil(t, pair.SecondaryRequest)
	blocked, _, _ := pair.RequestAdmission("primary", 3, 4, false)
	assert.False(t, blocked)
	assert.False(t, pair.Primary.ObserveFailure(failure, ready, now, 10*time.Second))
	assert.False(t, pair.Primary.ObserveFailure(FailureObservation{UID: "older", Created: metav1.NewTime(now.Add(-time.Second))}, ready, now, 10*time.Second))
}
func TestFailedSecondaryRechecksWaitForTheirDeadlineAndBackOffWithinTheCeiling(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	pair := &Pair{Phase: Degraded}
	failure := FailureObservation{UID: "secondary-failure", Created: metav1.NewTime(now)}
	require.True(t, pair.Secondary.ObserveFailure(failure, nil, now, 10*time.Second))
	deadline := pair.Secondary.NextCheck
	pair.ReconcileReadiness(nil, nil, 3, 2, false, now.Add(9*time.Second))
	assert.Equal(t, deadline, pair.Secondary.NextCheck)
	for index := 0; index < 20; index++ {
		pair.ReconcileReadiness(nil, nil, 3, 2, false, pair.Secondary.NextCheck.Time)
		assert.LessOrEqual(t, pair.Secondary.CheckInterval, int64(900))
		assert.True(t, pair.Secondary.Failed)
	}
}
