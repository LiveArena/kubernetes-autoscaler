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
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	testutils "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	fakescale "k8s.io/client-go/scale/fake"
	clienttesting "k8s.io/client-go/testing"
	"testing"
	"time"
)

func TestSecondaryPoolFailureDoesNotBlockPrimaryPoolAdmission(t *testing.T) {
	pair := &Pair{Phase: Healthy, Secondary: Role{Failed: true}}
	blocked, reliable, limit := pair.RequestAdmission("primary", 3, 4, false)
	assert.False(t, blocked, "secondary failure must not block a healthy fitting primary trial")
	assert.Zero(t, reliable)
	assert.Zero(t, limit)
	blocked, _, _ = pair.RequestAdmission("secondary", 3, 4, false)
	assert.True(t, blocked, "secondary failure must continue blocking new secondary requests")
}

func TestSecondaryPoolFailureDoesNotBlockPrimaryPoolScaling(t *testing.T) {
	controller, clock := newFailoverTestController(t)
	provider := &testProvider{controller: controller}
	failoverTestFailure(controller, "lin", "secondary", clock.Now())
	require.NoError(t, provider.Refresh())
	var primary *testGroup
	for _, candidate := range provider.NodeGroups() {
		if candidate.(*testGroup).Object().GetName() == "lin-primary" {
			primary = candidate.(*testGroup)
		}
	}
	require.NotNil(t, primary)
	require.False(t, primary.GetCapacityPolicy().ScaleUpBlocked)
	require.NoError(t, primary.IncreaseSize(1))
	target, err := primary.TargetSize()
	require.NoError(t, err)
	assert.Equal(t, 4, target)
	require.Error(t, primary.IncreaseSize(1), "committed primary intent must not replay")
}

func TestPrimaryPoolRecoveryPreservesIndependentSecondaryPoolRequests(t *testing.T) {
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	for _, reason := range []string{"", "primary cannot fit secondary-only demand"} {
		statement := "IndependentSecondaryPoolRequestSurvivesPrimaryPoolRecovery"
		if reason == "" {
			statement = "UnusedFailureTriggeredSecondaryPoolRequestIsReleased"
		}
		t.Run(statement, func(t *testing.T) {
			intent := &Request{FromTarget: 2, ToTarget: 3, Started: metav1.NewTime(now), PrimaryUnavailableReason: reason}
			pair := &Pair{Phase: Degraded, Primary: Role{Failed: true, ReadyCountAtFailure: 1, ReadyFingerprintAtFailure: ReadyFingerprint([]string{"occupied"})}, SecondaryRequest: intent}
			ready := []string{"occupied", "new-1", "new-2"}
			pair.ReconcileReadiness(ready, nil, 3, 2, false, now)
			pair.ReconcileReadiness(ready, nil, 3, 2, false, now.Add(time.Second))
			require.Equal(t, Healthy, pair.Phase)
			if reason == "" {
				assert.Nil(t, pair.SecondaryRequest, "unused failure-triggered intent should be released")
			} else {
				assert.Equal(t, intent, pair.SecondaryRequest, "primary recovery does not satisfy secondary-only demand")
			}
		})
	}
}

func TestRequestLifecycleKeepsPoolAdmissionAndCapacityCreditsBounded(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	for _, scenario := range []struct{ state, statement string }{
		{"pending", "PrimaryPoolRequestInFlightBlocksAdditionalScalingAndCreditsItsIncrement"},
		{"primary-success", "NewPrimaryPoolCapacityClearsItsRequestAndIncomingCredits"},
		{"fresh-failure", "NewPrimaryPoolFailurePermitsABoundedSecondaryPoolRequest"},
		{"old-failure", "OldPrimaryPoolFailuresDoNotReleaseTheCurrentPrimaryPoolTrial"},
		{"maximum", "PrimaryPoolAtMaximumPermitsSecondaryPoolRequestsUnlessFrozen"},
		{"before-scale", "UnwrittenPrimaryPoolIntentCanRetryOnlyItsOriginalIncrement"},
		{"after-secondary-scale", "CommittedSecondaryPoolRequestConsumesItsAllowanceAndIntent"},
	} {
		t.Run(scenario.statement, func(t *testing.T) {
			pair := &Pair{Phase: Degraded, Primary: Role{Failed: true}}
			pair.PrimaryRequest = &Request{FromTarget: 3, ToTarget: 4, Started: metav1.NewTime(now), FailureUIDAtStart: "old", ReadyAtStart: 1, ReadyFingerprint: ReadyFingerprint([]string{"occupied"})}
			target := 4
			if scenario.state == "before-scale" {
				target = 3
			}
			require.NoError(t, pair.ReconcileRequestTargets(target, 2))
			switch scenario.state {
			case "primary-success":
				pair.CompletePrimaryArrival([]string{"occupied", "new-primary"}, target)
				assert.Nil(t, pair.PrimaryRequest)
			case "fresh-failure":
				pair.AcceptPrimaryFailure(FailureObservation{UID: "fresh", Created: metav1.NewTime(now.Add(time.Second))}, true, target, 1)
				assert.Nil(t, pair.PrimaryRequest)
				assert.Equal(t, 1, pair.FallbackAllowance)
				blocked, _, limit := pair.RequestAdmission("secondary", target, 5, false)
				assert.False(t, blocked)
				assert.Equal(t, 1, limit)
				return
			case "old-failure":
				for _, timestamp := range []time.Time{now.Add(-time.Second), now} {
					pair.AcceptPrimaryFailure(FailureObservation{UID: "replaced-old", Created: metav1.NewTime(timestamp)}, true, target, 1)
					assert.NotNil(t, pair.PrimaryRequest)
					assert.Zero(t, pair.FallbackAllowance)
				}
			case "maximum":
				pair.PrimaryRequest = nil
				blocked, _, _ := pair.RequestAdmission("secondary", 4, 4, false)
				assert.False(t, blocked)
				blocked, _, _ = pair.RequestAdmission("secondary", 4, 4, true)
				assert.True(t, blocked)
				return
			case "after-secondary-scale":
				pair.PrimaryRequest = nil
				pair.FallbackAllowance = 1
				pair.SecondaryRequest = &Request{FromTarget: 2, ToTarget: 3, Started: metav1.NewTime(now)}
				require.NoError(t, pair.ReconcileRequestTargets(4, 3))
				assert.Zero(t, pair.FallbackAllowance)
				assert.Nil(t, pair.SecondaryRequest)
				require.Error(t, (&Pair{PrimaryRequest: &Request{FromTarget: 3, ToTarget: 4}}).ReconcileRequestTargets(5, 2))
				return
			}
			blocked, reliable, limit := pair.RequestAdmission("primary", target, 5, false)
			assert.Equal(t, scenario.state == "pending" || scenario.state == "old-failure", blocked)
			if scenario.state == "before-scale" {
				assert.Equal(t, 1, limit)
				assert.Zero(t, reliable)
			} else if scenario.state == "primary-success" {
				assert.Zero(t, reliable)
			} else {
				assert.Equal(t, 1, reliable)
			}
		})
	}
}
func TestSecondaryPoolFailureBlocksPendingSecondaryPoolRequestReplay(t *testing.T) {
	controller, clock := newFailoverTestController(t)
	provider := &testProvider{controller: controller}
	failoverTestFailure(controller, "lin", "primary", clock.Now())
	require.NoError(t, provider.Refresh())
	var secondary *testGroup
	for _, candidate := range provider.NodeGroups() {
		if candidate.(*testGroup).Object().GetName() == "lin-secondary" {
			secondary = candidate.(*testGroup)
		}
	}
	require.NotNil(t, secondary)
	require.NoError(t, controller.failover.PrepareScaleRequest(secondary, 2))
	cluster, err := controller.managementClient.Resource(schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "clusters"}).Namespace("tenant").Get(context.Background(), "test", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = controller.failover.Store.Reconcile(context.Background(), cluster, func(state *State) error { state.Pairs["lin"].Secondary.Failed = true; return nil })
	require.NoError(t, err)
	require.False(t, secondary.GetCapacityPolicy().ScaleUpBlocked, "fixture must retain the stale admission snapshot")
	require.Error(t, secondary.IncreaseSize(2), "durable secondary failure must override cached intent replay permission")
	target, err := secondary.TargetSize()
	require.NoError(t, err)
	assert.Zero(t, target)
}

func TestFailedPrimaryPoolWritesDoNotIncreaseTargetOrReplayCommittedRequests(t *testing.T) {
	for _, failure := range []struct{ state, statement string }{
		{"persistence-denied", "DeniedIntentPersistencePreventsPrimaryPoolScaling"},
		{"scale-write-failed", "InterruptedScaleWriteRetriesItsPersistedIntentOnceAfterRestart"},
	} {
		t.Run(failure.statement, func(t *testing.T) {
			controller, clock := newFailoverTestController(t)
			provider := &testProvider{controller: controller}
			find := func(role string) *testGroup {
				for _, candidate := range provider.NodeGroups() {
					group := candidate.(*testGroup)
					if group.object.GetName() == "lin-"+role {
						return group
					}
				}
				t.Fatalf("missing role %s", role)
				return nil
			}
			failoverTestFailure(controller, "lin", "primary", clock.Now())
			require.NoError(t, provider.Refresh())
			require.NoError(t, find("secondary").IncreaseSize(2))
			require.NoError(t, provider.Refresh())
			require.Error(t, controller.failover.PrepareScaleRequest(find("secondary"), 1), "consumed fallback allowance cannot admit new secondary demand")
			primary := find("primary")
			if failure.state == "persistence-denied" {
				controller.managementClient.(*fakedynamic.FakeDynamicClient).PrependReactor("update", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewForbidden(ConfigMaps.GroupResource(), "state", fmt.Errorf("denied"))
				})
			} else {
				rejected := false
				controller.managementScaleClient.(*fakescale.FakeScaleClient).PrependReactor("update", "machinepools", func(action clienttesting.Action) (bool, runtime.Object, error) {
					if !rejected {
						rejected = true
						return true, nil, fmt.Errorf("scale write interrupted")
					}
					return false, nil, nil
				})
			}
			require.Error(t, primary.IncreaseSize(1))
			target, err := primary.TargetSize()
			require.NoError(t, err)
			assert.Equal(t, 3, target)
			cluster, err := controller.managementClient.Resource(schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "clusters"}).Namespace("tenant").Get(context.Background(), "test", metav1.GetOptions{})
			require.NoError(t, err)
			state, _, err := controller.failover.Store.Load(context.Background(), cluster)
			require.NoError(t, err)
			if failure.state == "persistence-denied" {
				assert.Nil(t, state.Pairs["lin"].PrimaryRequest)
				return
			}
			require.NotNil(t, state.Pairs["lin"].PrimaryRequest)
			assert.Equal(t, 3, state.Pairs["lin"].PrimaryRequest.FromTarget)
			assert.Equal(t, 4, state.Pairs["lin"].PrimaryRequest.ToTarget)
			require.NoError(t, controller.enableAzureFailover(false, 10*time.Second))
			clock.Step(31 * time.Second)
			controller.failover.Clock = clock
			require.NoError(t, provider.Refresh())
			require.Error(t, find("secondary").IncreaseSize(1))
			require.NoError(t, find("primary").IncreaseSize(1))
			require.Error(t, find("primary").IncreaseSize(1), "the same durable intent cannot replay a successful scale update")
			target, err = find("primary").TargetSize()
			require.NoError(t, err)
			assert.Equal(t, 4, target)
		})
	}
}
func TestNewDemandTriesPrimaryPoolBeforeAdditionalSecondaryPoolScaling(t *testing.T) {
	for _, pair := range []string{"lin", "win2"} {
		t.Run("FreshPrimaryFailurePermitsBoundedSecondaryScalingWithoutReplay/Pair="+pair, func(t *testing.T) {
			controller, clock := newFailoverTestController(t)
			provider := &testProvider{controller: controller}
			find := func(role string) *testGroup {
				for _, candidate := range provider.NodeGroups() {
					group := candidate.(*testGroup)
					if group.object.GetName() == pair+"-"+role {
						return group
					}
				}
				t.Fatalf("missing %s %s", pair, role)
				return nil
			}
			failoverTestFailure(controller, pair, "primary", clock.Now())
			require.NoError(t, provider.Refresh())
			primary, secondary := find("primary"), find("secondary")
			require.NoError(t, secondary.IncreaseSize(2))
			require.NoError(t, provider.Refresh())
			require.False(t, primary.GetCapacityPolicy().ScaleUpBlocked, "new demand must try primary after prior fallback")
			require.True(t, secondary.GetCapacityPolicy().ScaleUpBlocked, "old failure cannot route new demand to secondary")
			require.NoError(t, primary.IncreaseSize(1))
			require.NoError(t, provider.Refresh())
			assert.Equal(t, 1, primary.GetCapacityPolicy().ReliableUnregistered)
			pendingNode := testutils.BuildTestNode("new-primary-pending", 2000, 100000)
			pendingNode.Spec.ProviderID = "azure://new-primary-pending"
			pendingNode.CreationTimestamp = metav1.NewTime(clock.Now().Add(time.Second))
			testutils.SetNodeReadyState(pendingNode, false, clock.Now())
			require.NoError(t, controller.nodeInformer.GetStore().Add(pendingNode))
			poolResource := controller.managementClient.Resource(controller.machinePoolResource).Namespace("tenant")
			pool, err := poolResource.Get(context.Background(), pair+"-primary", metav1.GetOptions{})
			require.NoError(t, err)
			providerIDs, _, err := unstructured.NestedStringSlice(pool.Object, "spec", "providerIDList")
			require.NoError(t, err)
			require.NoError(t, unstructured.SetNestedStringSlice(pool.Object, append(providerIDs, pendingNode.Spec.ProviderID), "spec", "providerIDList"))
			_, err = poolResource.Update(context.Background(), pool, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.NoError(t, controller.machinePoolInformer.Informer().GetStore().Update(pool))
			require.NoError(t, provider.Refresh())
			assert.Zero(t, primary.GetCapacityPolicy().ReliableUnregistered, "registered arrival must not also be credited as unregistered")
			require.Error(t, secondary.IncreaseSize(1), "new primary request must get its opportunity first")
			failoverTestFailure(controller, pair, "primary", clock.Now())
			require.NoError(t, provider.Refresh())
			require.Error(t, secondary.IncreaseSize(1), "replayed old attempt cannot authorize fresh fallback")
			clock.Step(10 * time.Second)
			failure := &unstructured.Unstructured{Object: map[string]interface{}{"status": map[string]interface{}{"provisioningState": "Failed"}}}
			failure.SetNamespace("tenant")
			failure.SetUID("new-request-failure")
			failure.SetCreationTimestamp(metav1.NewTime(clock.Now()))
			failure.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "infrastructure.cluster.x-k8s.io/v1beta1", Kind: "AzureMachinePool", Name: pair + "-primary", UID: types.UID(pair + "-primary-amp")}})
			controller.failover.Observe(failure)
			require.NoError(t, provider.Refresh())
			require.NoError(t, secondary.IncreaseSize(1))
			target, err := primary.TargetSize()
			require.NoError(t, err)
			assert.Equal(t, 4, target)
			target, err = secondary.TargetSize()
			require.NoError(t, err)
			assert.Equal(t, 3, target)
		})
	}
}
