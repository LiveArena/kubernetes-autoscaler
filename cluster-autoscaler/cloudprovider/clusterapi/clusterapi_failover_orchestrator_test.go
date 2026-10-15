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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

func TestResidualDemandScalingSurvivesRestartWithoutDuplicatePoolRequests(t *testing.T) {
	for _, pair := range []string{"lin", "win2"} {
		for _, disableAccounting := range []bool{false, true} {
			statement := "FailureRevealsResidualDemandAndRestartDoesNotReplayFallback"
			if disableAccounting {
				statement = "RemovingBlockedGapAccountingPreventsCanonicalFallback"
			}
			t.Run(statement+"/Pair="+pair, func(t *testing.T) {
				fixture := newFailoverOrchestratorFixture(t, pair, disableAccounting)
				if disableAccounting {
					fixture.expectHealthyBootstrap()
					fixture.observeAttempt("primary-failed")
					assert.Zero(t, fixture.scan())
					assert.Zero(t, fixture.writes())
					return
				}
				fixture.supplyFailedPrimaryDemand()
				assert.Zero(t, fixture.scan())
				fixture.registerReady("primary", 2)
				assert.Zero(t, fixture.scan())
				assert.Zero(t, fixture.scan())
				assert.Equal(t, 1, fixture.writes())
				for _, candidate := range fixture.provider.NodeGroups() {
					group := candidate.(*nodegroup)
					if !strings.HasPrefix(group.scalableResource.Name(), pair+"-") {
						assert.False(t, group.GetCapacityPolicy().BlockUnregistered)
						assert.Equal(t, strings.HasSuffix(group.scalableResource.Name(), "-secondary"), group.GetCapacityPolicy().ScaleUpBlocked)
					}
					if group.scalableResource.Name() == pair+"-primary" {
						target, err := group.TargetSize()
						require.NoError(t, err)
						assert.Equal(t, 3, target)
						assert.False(t, group.GetCapacityPolicy().BlockUnregistered)
					}
					if group.scalableResource.Name() == pair+"-secondary" {
						target, err := group.TargetSize()
						require.NoError(t, err)
						assert.Equal(t, 2, target)
						assert.True(t, group.GetCapacityPolicy().ScaleUpBlocked)
					}
				}
			})
		}
	}
}

func TestFailureResynchronizationRejectsAnUnsynchronizedInformer(t *testing.T) {
	controller, _ := newFailoverTestController(t)
	require.False(t, controller.azureIntegration.extension.azureMachinePoolMachineInformer.Informer().HasSynced())
	require.Error(t, controller.VisitFailures(nil), "an incomplete initial informer snapshot cannot establish resynchronization")
}

func TestObservationOverflowResynchronizesDroppedFailuresWithoutRestartOrScaleReplay(t *testing.T) {
	fixture := newFailoverOrchestratorFixture(t, "lin", false)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fixture.controller.managementInformerFactory.Start(fixture.controller.stopChannel)
	require.True(t, cache.WaitForCacheSync(ctx.Done(), fixture.controller.azureIntegration.extension.azureMachinePoolMachineInformer.Informer().HasSynced))
	require.Zero(t, fixture.scan())
	informerStore := fixture.controller.azureIntegration.extension.azureMachinePoolMachineInformer.Informer().GetStore()
	failures := make([]*unstructured.Unstructured, 0, 65)
	for index := 0; index < 65; index++ {
		name := fmt.Sprintf("overflow-attempt-%d", index)
		ownerName := fmt.Sprintf("unselected-pool-%d", index)
		ownerUID := types.UID(ownerName)
		if index == 64 {
			ownerName, ownerUID = "lin-primary", "lin-primary-amp"
		}
		attempt := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": azureMachinePoolMachineApiGroup + "/v1beta1", "kind": "AzureMachinePoolMachine", "status": map[string]interface{}{"provisioningState": "Failed"}}}
		attempt.SetName(name)
		attempt.SetNamespace("tenant")
		attempt.SetUID(types.UID(name))
		attempt.SetCreationTimestamp(metav1.NewTime(fixture.clock.Now()))
		attempt.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: azureMachinePoolMachineApiGroup + "/v1beta1", Kind: "AzureMachinePool", Name: ownerName, UID: ownerUID}})
		require.NoError(t, informerStore.Add(attempt))
		fixture.controller.failover.Observe(attempt)
		failures = append(failures, attempt)
	}
	require.True(t, fixture.controller.failover.Overflow)
	require.Len(t, fixture.controller.failover.Observations, 64)
	assert.Equal(t, 2, fixture.scan())
	assert.False(t, fixture.controller.failover.Overflow, "overflow must be recoverable through the informer snapshot")
	assert.Empty(t, fixture.controller.failover.Observations, "a successful resync must retire the retained batch")
	require.Equal(t, 1, fixture.writes(), "the dropped selected failure must expose exactly one fallback request")
	state := fixture.pairState()
	assert.Equal(t, failures[64].GetUID(), state.Primary.LastAttemptUID)
	assert.Equal(t, int64(1), state.Primary.FailureEpoch)
	for _, attempt := range failures {
		fixture.controller.failover.Observe(attempt.DeepCopy())
	}
	assert.Zero(t, fixture.scan())
	assert.False(t, fixture.controller.failover.Overflow)
	assert.Equal(t, 1, fixture.writes(), "replayed overflow observations must not repeat scaling")
	assert.Equal(t, int64(1), fixture.pairState().Primary.FailureEpoch)
}
