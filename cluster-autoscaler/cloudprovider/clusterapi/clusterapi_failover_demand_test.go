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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDemandBeyondFailureAllowanceGetsAPrimaryTrialWithoutLosingPriorFallback(t *testing.T) {
	for _, pair := range []string{"lin", "win2"} {
		t.Run("Pair="+pair, func(t *testing.T) {
			fixture := newFailoverOrchestratorFixture(t, pair, false)
			require.Zero(t, fixture.scan())
			fixture.observeAttempt("initial-primary-failure")
			fixture.pending = append(fixture.pending, fixture.pendingPod("fresh-demand"))
			require.Equal(t, 3, fixture.scan())
			require.Equal(t, 1, fixture.writes(), "fresh demand beyond the two failed slots must not stall both roles")
			state := fixture.pairState()
			require.NotNil(t, state.PrimaryRequest)
			assert.Equal(t, 3, state.PrimaryRequest.FromTarget)
			assert.Equal(t, 4, state.PrimaryRequest.ToTarget, "only demand beyond the retained failed gap is a fresh trial")
			assert.Equal(t, 2, state.FallbackAllowance)
			assert.Nil(t, state.SecondaryRequest)
			fixture.restart()
			require.Equal(t, 2, fixture.scan(), "the fresh trial gets incoming credit separately from prior failed demand")
			require.Equal(t, 2, fixture.writes(), "the full prior fallback can proceed while the fresh primary trial is pending")
			require.Zero(t, fixture.scan())
			assert.Equal(t, 2, fixture.writes(), "unchanged demand must not replay either request")
			state = fixture.pairState()
			require.NotNil(t, state.PrimaryRequest)
			assert.Equal(t, 4, state.Primary.ObservedTarget)
			assert.Equal(t, 2, state.Secondary.ObservedTarget)
		})
	}
}

func TestFreshDemandTriesPrimaryBeforeIndependentFallbackAndSurvivesRestart(t *testing.T) {
	for _, pair := range []string{"lin", "win2"} {
		for _, primarySucceeds := range []bool{true, false} {
			statement := "NewDemandTriesPrimaryAndPrimarySuccessPreventsAnotherFallback"
			if !primarySucceeds {
				statement = "FreshPrimaryFailurePermitsIndependentSecondaryCapacity"
			}
			t.Run(statement+"/Pair="+pair, func(t *testing.T) {
				fixture := newFailoverOrchestratorFixture(t, pair, false)
				fixture.supplyFailedPrimaryDemand()
				assert.Zero(t, fixture.scan())
				fixture.pending = append(fixture.pending, fixture.pendingPod("new-demand"))
				assert.Equal(t, 1, fixture.scan())
				assert.Equal(t, 2, fixture.writes())
				assert.Equal(t, pair+"-primary", fixture.lastWrite().Name)
				assert.Equal(t, int32(4), fixture.lastWrite().Spec.Replicas)
				fixture.restart()
				assert.Zero(t, fixture.scan())
				assert.Equal(t, 2, fixture.writes())
				if primarySucceeds {
					fixture.registerReady("primary", 1)
					assert.Zero(t, fixture.scan())
					assert.Equal(t, 2, fixture.writes())
					return
				}
				failed := fixture.observeAttempt("new-demand-failure")
				assert.Equal(t, 1, fixture.scan())
				assert.Equal(t, 3, fixture.writes())
				assert.Equal(t, pair+"-secondary", fixture.lastWrite().Name)
				assert.Equal(t, int32(3), fixture.lastWrite().Spec.Replicas)
				require.NoError(t, fixture.attempts.Delete(context.Background(), failed.GetName(), metav1.DeleteOptions{}))
				fixture.registerReady("secondary", 1)
				assert.Zero(t, fixture.scan())
				fixture.registerReady("primary", 1)
				assert.Zero(t, fixture.scan())
				assert.Equal(t, 3, fixture.writes())
			})
		}
	}
}
