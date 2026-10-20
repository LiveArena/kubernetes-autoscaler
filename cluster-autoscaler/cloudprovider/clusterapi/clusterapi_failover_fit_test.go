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

package clusterapi

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerifiedFitAndFreezeControlSecondaryRequestsAcrossRestart(t *testing.T) {
	for _, pair := range []string{"lin", "win2"} {
		for _, scenario := range []struct {
			statement string
			bothUnfit bool
			frozen    bool
		}{
			{"UnfitPrimaryPermitsFittingSecondaryDemand", false, false},
			{"DemandThatFitsNeitherPoolDoesNotRequestCapacity", true, false},
			{"UnfitPrimaryDoesNotBypassFrozenSecondaryAdmission", false, true},
		} {
			t.Run(scenario.statement+"/Pair="+pair, func(t *testing.T) {
				fixture := newFailoverOrchestratorFixture(t, pair, false)
				fixture.supplyFailedPrimaryDemand()
				assert.Zero(t, fixture.scan())
				fixture.pending = append(fixture.pending, fixture.pendingPod("new-demand"))
				fixture.templates["MachinePool/tenant/"+pair+"-primary"].Node().Labels["test/workload"] = "incompatible-primary"
				if scenario.bothUnfit {
					fixture.templates["MachinePool/tenant/"+pair+"-secondary"].Node().Labels["test/workload"] = "incompatible-secondary"
				} else if scenario.frozen {
					require.NoError(t, fixture.controller.enableAzureFailover(true, 10*time.Second, fixture.options.NodeGroupDefaults))
					fixture.controller.failover.Clock = fixture.clock
				}
				assert.Equal(t, 1, fixture.scan())
				wantedWrites := 2
				if scenario.bothUnfit || scenario.frozen {
					wantedWrites = 1
				}
				assert.Equal(t, wantedWrites, fixture.writes())
				if wantedWrites == 2 {
					state := fixture.pairState()
					require.NotNil(t, state.SecondaryRequest)
					assert.NotEmpty(t, state.SecondaryRequest.PrimaryUnavailableReason)
					assert.Zero(t, fixture.scan())
					assert.Equal(t, wantedWrites, fixture.writes())
					fixture.restart()
					assert.Zero(t, fixture.scan())
					assert.Equal(t, wantedWrites, fixture.writes())
				}
			})
		}
	}
}

func TestIndependentSecondaryOnlyDemandDoesNotCancelOrReplayThePrimaryTrial(t *testing.T) {
	for _, pair := range []string{"lin", "win2"} {
		t.Run("Pair="+pair, func(t *testing.T) {
			fixture := newFailoverOrchestratorFixture(t, pair, false)
			fixture.supplyFailedPrimaryDemand()
			for index, pod := range fixture.pending {
				bound := pod.DeepCopy()
				bound.Spec.NodeName = fmt.Sprintf("%s-secondary-ready-%d", pair, index)
				fixture.boundPods = append(fixture.boundPods, bound)
			}
			fixture.pending = nil
			assert.Zero(t, fixture.scan())
			fixture.setMaximum("primary", "6")
			fixture.templates["MachinePool/tenant/"+pair+"-primary"].Node().Labels["test/primary-only"] = "true"
			primaryOnly := fixture.pendingPod("new-demand")
			primaryOnly.Spec.NodeSelector["test/primary-only"] = "true"
			fixture.pending = append(fixture.pending, primaryOnly)
			assert.Equal(t, 1, fixture.scan())
			assert.Equal(t, 2, fixture.writes())
			assert.Equal(t, pair+"-primary", fixture.lastWrite().Name)
			assert.Equal(t, int32(4), fixture.lastWrite().Spec.Replicas)
			fixture.templates["MachinePool/tenant/"+pair+"-secondary"].Node().Labels["test/secondary-only"] = "true"
			secondaryOnly := primaryOnly.DeepCopy()
			secondaryOnly.Name, secondaryOnly.UID = "secondary-only-demand", "secondary-only-demand"
			delete(secondaryOnly.Spec.NodeSelector, "test/primary-only")
			secondaryOnly.Spec.NodeSelector["test/secondary-only"] = "true"
			fixture.pending = append(fixture.pending, secondaryOnly)
			require.Equal(t, 1, fixture.scan())
			require.Equal(t, 3, fixture.writes(), "secondary-only residual must not wait for an unrelated primary trial")
			assert.Equal(t, pair+"-secondary", fixture.lastWrite().Name)
			assert.Equal(t, int32(3), fixture.lastWrite().Spec.Replicas)
			state := fixture.pairState()
			require.NotNil(t, state.PrimaryRequest, "independent primary intent must survive")
			assert.Equal(t, 4, state.PrimaryRequest.ToTarget)
			require.NotNil(t, state.SecondaryRequest)
			assert.NotEmpty(t, state.SecondaryRequest.PrimaryUnavailableReason)
			assert.Zero(t, fixture.scan())
			assert.Equal(t, 3, fixture.writes())
			fixture.restart()
			assert.Zero(t, fixture.scan())
			assert.Equal(t, 3, fixture.writes())
		})
	}
}
