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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

func TestSaturatedPrimaryAllowsFullResidualDemandBeyondItsFailureAllowance(t *testing.T) {
	for _, pair := range []string{"lin", "win2"} {
		t.Run("Pair="+pair, func(t *testing.T) {
			fixture := newFailoverOrchestratorFixture(t, pair, false)
			fixture.setMaximum("primary", "3")
			require.Zero(t, fixture.scan())
			fixture.observeAttempt("saturated-primary-failure")
			fixture.pending = append(fixture.pending, fixture.pendingPod("fresh-demand"))
			require.Equal(t, 3, fixture.scan())
			require.Equal(t, 1, fixture.writes(), "primary saturation must allow the full fitting secondary request")
			assert.Equal(t, pair+"-secondary", fixture.lastWrite().Name)
			assert.Equal(t, int32(3), fixture.lastWrite().Spec.Replicas)
			fixture.restart()
			require.Zero(t, fixture.scan())
			assert.Equal(t, 1, fixture.writes())
		})
	}
}

func TestClusterWideLimitsRejectPartialSecondaryRequestsWithoutConsumingAllowance(t *testing.T) {
	for _, pair := range []string{"lin", "win2"} {
		for _, limit := range []string{"nodes", "cores"} {
			statement := map[string]string{"nodes": "ClusterNodeCountLimitRejectsPartialSecondaryCapacity", "cores": "ClusterCoreLimitRejectsPartialSecondaryCapacity"}[limit]
			t.Run(statement+"/Pair="+pair, func(t *testing.T) {
				fixture := newFailoverOrchestratorFixture(t, pair, false)
				require.Zero(t, fixture.scan())
				fixture.observeAttempt("primary-failed")
				if limit == "nodes" {
					fixture.options.MaxNodesTotal = len(fixture.nodes) + 3
				} else {
					fixture.options.MaxCoresTotal = 14
					fixture.provider.resourceLimiter = cloudprovider.NewResourceLimiter(nil, map[string]int64{cloudprovider.ResourceNameCores: 14})
				}
				fixture.registry, fixture.engine, fixture.filterPods = fixture.newScan()
				require.Equal(t, 2, fixture.scan())
				require.Zero(t, fixture.writes(), "a two-node secondary request must not be truncated to one")
				state := fixture.pairState()
				assert.Equal(t, 2, state.FallbackAllowance)
				assert.Nil(t, state.SecondaryRequest)
				for _, candidate := range fixture.provider.NodeGroups() {
					group := candidate.(*nodegroup)
					if group.scalableResource.Name() == pair+"-secondary" {
						target, err := group.TargetSize()
						require.NoError(t, err)
						assert.Zero(t, target)
					}
				}
				fixture.options.MaxNodesTotal, fixture.options.MaxCoresTotal = 20, 100
				fixture.provider.resourceLimiter = cloudprovider.NewResourceLimiter(nil, nil)
				fixture.registry, fixture.engine, fixture.filterPods = fixture.newScan()
				require.Equal(t, 2, fixture.scan())
				require.Equal(t, 1, fixture.writes(), "restoring capacity must admit the full secondary request")
				assert.Equal(t, pair+"-secondary", fixture.lastWrite().Name)
				assert.Equal(t, int32(2), fixture.lastWrite().Spec.Replicas)
			})
		}
	}
}

func TestPoolMaximumsKeepNewDemandPrimaryFirstAndFallbackRequestsWhole(t *testing.T) {
	for _, pair := range []string{"lin", "win2"} {
		for _, primaryAtMaximum := range []bool{true, false} {
			statement := "PrimaryMaximumPermitsFittingSecondaryDemand"
			if !primaryAtMaximum {
				statement = "SecondaryMaximumRejectsTheFullRequestWithoutReducingPrimaryIntent"
			}
			t.Run(statement+"/Pair="+pair, func(t *testing.T) {
				fixture := newFailoverOrchestratorFixture(t, pair, false)
				fixture.supplyFailedPrimaryDemand()
				assert.Zero(t, fixture.scan())
				if primaryAtMaximum {
					fixture.setMaximum("primary", "3")
				} else {
					fixture.setMaximum("secondary", "2")
				}
				fixture.pending = append(fixture.pending, fixture.pendingPod("new-demand"))
				assert.Equal(t, 1, fixture.scan())
				assert.Equal(t, 2, fixture.writes())
				if primaryAtMaximum {
					assert.Equal(t, pair+"-secondary", fixture.lastWrite().Name)
					assert.Equal(t, int32(3), fixture.lastWrite().Spec.Replicas)
					assert.Zero(t, fixture.scan())
					return
				}
				assert.Equal(t, pair+"-primary", fixture.lastWrite().Name)
				assert.Equal(t, int32(4), fixture.lastWrite().Spec.Replicas)
				fixture.restart()
				assert.Zero(t, fixture.scan())
				assert.Equal(t, 2, fixture.writes())
				fixture.observeAttempt("new-demand-failure")
				assert.Equal(t, 1, fixture.scan())
				assert.Equal(t, 2, fixture.writes())
				assert.Equal(t, pair+"-primary", fixture.lastWrite().Name)
				assert.Equal(t, int32(4), fixture.lastWrite().Spec.Replicas)
			})
		}
	}
}
