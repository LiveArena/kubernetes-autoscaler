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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	fakescale "k8s.io/client-go/scale/fake"
	"strings"
	"testing"
	"time"
)

func TestRedeploymentAndDeactivationPreserveStateWithoutTakingOverRetirement(t *testing.T) {
	for _, lifecycle := range []string{"active-redeploy", "opted-out-retained", "opted-out-removed"} {
		statement := map[string]string{
			"active-redeploy":    "ActiveRedeploymentPreservesStateAndDoesNotRepeatScaling",
			"opted-out-retained": "DeactivationPreservesRetainedCapacityWithoutNewRequestsOrCleanup",
			"opted-out-removed":  "DeactivationDoesNotResetStateAfterExternalPoolRemoval",
		}[lifecycle]
		t.Run(statement, func(t *testing.T) {
			controller, clock := newFailoverTestController(t)
			provider := &testProvider{controller: controller}
			failoverTestFailure(controller, "lin", "primary", clock.Now())
			require.NoError(t, provider.Refresh())
			for _, candidate := range provider.NodeGroups() {
				group := candidate.(*testGroup)
				if group.object.GetName() == "lin-secondary" {
					require.NoError(t, group.IncreaseSize(2))
				}
			}
			require.NoError(t, provider.Refresh())
			client := controller.managementClient.(*fakedynamic.FakeDynamicClient)
			maps := client.Resource(ConfigMaps).Namespace("tenant")
			state, err := maps.Get(context.Background(), "test-autoscaler-failover-state", metav1.GetOptions{})
			require.NoError(t, err)
			beforePools := map[string]*unstructured.Unstructured{}
			for _, candidate := range provider.NodeGroups() {
				group := candidate.(*testGroup)
				pool, err := client.Resource(controller.machinePoolResource).Namespace("tenant").Get(context.Background(), group.object.GetName(), metav1.GetOptions{})
				require.NoError(t, err)
				beforePools[pool.GetName()] = pool
			}
			if lifecycle == "active-redeploy" {
				require.NoError(t, controller.enableAzureFailover(false, 10*time.Second))
				controller.failover.Clock = clock
			} else {
				previous := controller.failover
				require.NoError(t, provider.Cleanup())
				assert.True(t, previous.Stopped.Load())
				controller.failover = nil
				require.NoError(t, maps.Delete(context.Background(), "test-autoscaler-failover-config", metav1.DeleteOptions{}))
				if lifecycle == "opted-out-removed" {
					for _, name := range []string{"lin-secondary", "win2-secondary"} {
						pool := beforePools[name].DeepCopy()
						require.NoError(t, unstructured.SetNestedField(pool.Object, int64(0), "spec", "replicas"))
						_, err := client.Resource(controller.machinePoolResource).Namespace("tenant").Update(context.Background(), pool, metav1.UpdateOptions{})
						require.NoError(t, err)
						providerIDs, _, err := unstructured.NestedStringSlice(pool.Object, "spec", "providerIDList")
						require.NoError(t, err)
						assert.Empty(t, providerIDs)
						require.NoError(t, client.Resource(controller.machinePoolResource).Namespace("tenant").Delete(context.Background(), name, metav1.DeleteOptions{}))
						require.NoError(t, controller.machinePoolInformer.Informer().GetStore().Delete(pool))
						delete(beforePools, name)
					}
				}
			}
			client.ClearActions()
			controller.managementScaleClient.(*fakescale.FakeScaleClient).ClearActions()
			for index := 0; index < 3; index++ {
				require.NoError(t, provider.Refresh())
			}
			groups := provider.NodeGroups()
			assert.Len(t, groups, len(beforePools))
			for _, candidate := range groups {
				group := candidate.(*testGroup)
				if strings.HasSuffix(group.object.GetName(), "-secondary") {
					assert.True(t, group.GetCapacityPolicy().ScaleUpBlocked)
					assert.False(t, group.GetCapacityPolicy().BlockUnregistered)
					require.Error(t, group.IncreaseSize(1))
					if group.object.GetName() == "lin-secondary" {
						target, err := group.TargetSize()
						require.NoError(t, err)
						assert.Equal(t, 2, target)
					}
				}
			}
			retained, err := maps.Get(context.Background(), "test-autoscaler-failover-state", metav1.GetOptions{})
			require.NoError(t, err)
			assert.Equal(t, state, retained, "deploy reconciliation must not reset or retire state")
			for name, before := range beforePools {
				after, err := client.Resource(controller.machinePoolResource).Namespace("tenant").Get(context.Background(), name, metav1.GetOptions{})
				require.NoError(t, err)
				assert.Equal(t, before, after)
			}
			for _, action := range client.Actions() {
				assert.NotEqual(t, "create", action.GetVerb())
				assert.NotEqual(t, "update", action.GetVerb())
				assert.NotEqual(t, "patch", action.GetVerb())
				assert.NotEqual(t, "delete", action.GetVerb(), "autoscaler must not take over retirement cleanup")
			}
			for _, action := range controller.managementScaleClient.(*fakescale.FakeScaleClient).Actions() {
				assert.NotEqual(t, "update", action.GetVerb())
				assert.NotEqual(t, "patch", action.GetVerb())
			}
		})
	}
}
func TestRegisteredIdentifierCannotBindReplacementResourcesBeforeItsRecordsArePurged(t *testing.T) {
	for _, scenario := range []struct {
		role, resourceKind string
		pendingRequest     bool
		changedTarget      bool
	}{
		{role: "primary", resourceKind: "MachinePool", pendingRequest: true},
		{role: "primary", resourceKind: "AzureMachinePool", pendingRequest: true},
		{role: "secondary", resourceKind: "MachinePool", pendingRequest: true},
		{role: "secondary", resourceKind: "AzureMachinePool", pendingRequest: true},
		{role: "primary", resourceKind: "MachinePool"},
		{role: "primary", resourceKind: "AzureMachinePool"},
		{role: "secondary", resourceKind: "MachinePool"},
		{role: "secondary", resourceKind: "AzureMachinePool"},
		{role: "primary", resourceKind: "MachinePool", pendingRequest: true, changedTarget: true},
	} {
		statement := map[string]string{"primary": "Primary", "secondary": "Secondary"}[scenario.role] + scenario.resourceKind + "ReplacementCannotReuseRegisteredIdentifier"
		if !scenario.pendingRequest {
			statement += "EvenWithoutOutstandingRequests"
		}
		if scenario.changedTarget {
			statement += "EvenWhenOldRequestTargetsDiffer"
		}
		t.Run(statement, func(t *testing.T) {
			controller, clock := newFailoverTestController(t)
			provider := &testProvider{controller: controller}
			ctx := context.Background()
			if scenario.pendingRequest {
				failoverTestFailure(controller, "lin", "primary", clock.Now())
				require.NoError(t, provider.Refresh())
				for _, candidate := range provider.NodeGroups() {
					group := candidate.(*testGroup)
					if group.object.GetName() == "lin-secondary" {
						require.NoError(t, group.IncreaseSize(2))
					}
				}
			}
			require.NoError(t, provider.Refresh())
			client := controller.managementClient.(*fakedynamic.FakeDynamicClient)
			maps := client.Resource(ConfigMaps).Namespace("tenant")
			if scenario.changedTarget {
				cluster, err := client.Resource(schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "clusters"}).Namespace("tenant").Get(ctx, "test", metav1.GetOptions{})
				require.NoError(t, err)
				_, err = controller.failover.Store.Reconcile(ctx, cluster, func(state *State) error {
					state.Pairs["lin"].PrimaryRequest = &Request{FromTarget: 3, ToTarget: 4, Started: metav1.NewTime(clock.Now())}
					return nil
				})
				require.NoError(t, err)
			}
			before, err := maps.Get(ctx, "test-autoscaler-failover-state", metav1.GetOptions{})
			require.NoError(t, err)
			pools := client.Resource(controller.machinePoolResource).Namespace("tenant")
			infrastructure := client.Resource(schema.GroupVersionResource{Group: AzureAPIGroup, Version: "v1beta1", Resource: "azuremachinepools"}).Namespace("tenant")
			name := "lin-" + scenario.role
			pool, err := pools.Get(ctx, name, metav1.GetOptions{})
			require.NoError(t, err)
			amp, err := infrastructure.Get(ctx, name, metav1.GetOptions{})
			require.NoError(t, err)
			if scenario.resourceKind == "MachinePool" {
				require.NoError(t, pools.Delete(ctx, name, metav1.DeleteOptions{}))
				require.NoError(t, controller.machinePoolInformer.Informer().GetStore().Delete(pool))
				pool.SetUID(types.UID(name + "-replacement"))
				if scenario.changedTarget {
					require.NoError(t, unstructured.SetNestedField(pool.Object, int64(2), "spec", "replicas"))
				}
				_, err = pools.Create(ctx, pool, metav1.CreateOptions{})
				require.NoError(t, err)
				require.NoError(t, controller.machinePoolInformer.Informer().GetStore().Add(pool))
				owners := amp.GetOwnerReferences()
				owners[0].UID = pool.GetUID()
				amp.SetOwnerReferences(owners)
				_, err = infrastructure.Update(ctx, amp, metav1.UpdateOptions{})
				require.NoError(t, err)
			} else {
				require.NoError(t, infrastructure.Delete(ctx, name, metav1.DeleteOptions{}))
				amp.SetUID(types.UID(name + "-replacement-amp"))
				_, err = infrastructure.Create(ctx, amp, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			for _, restarted := range []bool{false, true} {
				if restarted {
					require.NoError(t, provider.Cleanup())
					require.NoError(t, controller.enableAzureFailover(false, 10*time.Second))
					controller.failover.Clock = clock
				}
				client.ClearActions()
				controller.managementScaleClient.(*fakescale.FakeScaleClient).ClearActions()
				require.NoError(t, provider.Refresh())
				for _, candidate := range provider.NodeGroups() {
					group := candidate.(*testGroup)
					if group.object.GetAnnotations()[PairKey] != "lin" {
						if group.object.GetAnnotations()[RoleKey] == "primary" {
							assert.False(t, group.GetCapacityPolicy().ScaleUpBlocked, "an identifier conflict must not block the independent pair")
						}
						continue
					}
					capacity := group.GetCapacityPolicy()
					assert.True(t, capacity.ScaleUpBlocked)
					assert.True(t, capacity.RetainTarget)
					assert.Contains(t, capacity.Reason, "already registered")
					assert.Contains(t, capacity.Reason, "lin")
					require.ErrorContains(t, group.IncreaseSize(1), "already registered")
				}
				after, err := maps.Get(ctx, "test-autoscaler-failover-state", metav1.GetOptions{})
				require.NoError(t, err)
				assert.Equal(t, before, after)
				for _, action := range client.Actions() {
					assert.Equal(t, "get", action.GetVerb(), "resource replacement must not reset or rebind retained state")
				}
				assert.Empty(t, controller.managementScaleClient.(*fakescale.FakeScaleClient).Actions(), "old requests must not be replayed")
			}
		})
	}
}
func TestCompletedRetirementAllowsFreshActivationWithoutReplayingOldRequests(t *testing.T) {
	controller, clock := newFailoverTestController(t)
	provider := &testProvider{controller: controller}
	require.NoError(t, provider.Refresh())
	client := controller.managementClient.(*fakedynamic.FakeDynamicClient)
	ctx := context.Background()
	cluster, err := client.Resource(schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "clusters"}).Namespace("tenant").Get(ctx, "test", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = controller.failover.Store.Reconcile(ctx, cluster, func(state *State) error {
		for _, name := range []string{"lin", "win2"} {
			pair := state.Pairs[name]
			pair.Phase = Degraded
			pair.Primary.Failed = true
			pair.Primary.LastAttemptUID = types.UID(name + "-old-failure")
			pair.Primary.LastAttemptCreated = metav1.NewTime(clock.Now())
			pair.Primary.FailureEpoch = 3
			if name == "lin" {
				pair.PrimaryRequest = &Request{FromTarget: 3, ToTarget: 4, Started: metav1.NewTime(clock.Now())}
			} else {
				pair.FallbackAllowance = 2
				pair.SecondaryRequest = &Request{FromTarget: 0, ToTarget: 2, Started: metav1.NewTime(clock.Now())}
				pair.Secondary.Failed = true
				pair.Secondary.CheckInterval = 60
				pair.Secondary.NextCheck = metav1.NewTime(clock.Now().Add(time.Minute))
			}
		}
		return nil
	})
	require.NoError(t, err)
	oldWriter := controller.failover
	require.NoError(t, provider.Cleanup())
	controller.failover = nil
	maps := client.Resource(ConfigMaps).Namespace("tenant")
	require.NoError(t, maps.Delete(ctx, "test-autoscaler-failover-config", metav1.DeleteOptions{}))
	pools := client.Resource(controller.machinePoolResource).Namespace("tenant")
	infrastructure := client.Resource(schema.GroupVersionResource{Group: azureMachinePoolMachineApiGroup, Version: "v1beta1", Resource: "azuremachinepools"}).Namespace("tenant")
	retiredPools, retiredInfrastructure := map[string]*unstructured.Unstructured{}, map[string]*unstructured.Unstructured{}
	for _, name := range []string{"lin-secondary", "win2-secondary"} {
		pool, err := pools.Get(ctx, name, metav1.GetOptions{})
		require.NoError(t, err)
		target, _, err := unstructured.NestedInt64(pool.Object, "spec", "replicas")
		require.NoError(t, err)
		require.Zero(t, target)
		providerIDs, _, err := unstructured.NestedStringSlice(pool.Object, "spec", "providerIDList")
		require.NoError(t, err)
		require.Empty(t, providerIDs)
		amp, err := infrastructure.Get(ctx, name, metav1.GetOptions{})
		require.NoError(t, err)
		retiredPools[name], retiredInfrastructure[name] = pool, amp
		require.NoError(t, pools.Delete(ctx, name, metav1.DeleteOptions{}))
		require.NoError(t, controller.machinePoolInformer.Informer().GetStore().Delete(pool))
		require.NoError(t, infrastructure.Delete(ctx, name, metav1.DeleteOptions{}))
	}
	require.NoError(t, maps.Delete(ctx, "test-autoscaler-failover-state", metav1.DeleteOptions{}))
	client.ClearActions()
	controller.managementScaleClient.(*fakescale.FakeScaleClient).ClearActions()
	oldWriter.Refresh()
	require.NoError(t, provider.Refresh())
	for _, action := range client.Actions() {
		assert.Equal(t, "get", action.GetVerb(), "disabled/retired writers must not recreate state")
	}
	_, err = maps.Get(ctx, "test-autoscaler-failover-state", metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
	for name, retired := range retiredPools {
		pool := retired.DeepCopy()
		pool.SetUID(types.UID(name + "-reactivated"))
		pool.SetResourceVersion("1")
		_, err := pools.Create(ctx, pool, metav1.CreateOptions{})
		require.NoError(t, err)
		require.NoError(t, controller.machinePoolInformer.Informer().GetStore().Add(pool))
		amp := retiredInfrastructure[name].DeepCopy()
		amp.SetUID(types.UID(name + "-reactivated-amp"))
		owners := amp.GetOwnerReferences()
		owners[0].UID = pool.GetUID()
		amp.SetOwnerReferences(owners)
		_, err = infrastructure.Create(ctx, amp, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	writeFailoverTestConfiguration(t, client, cluster)
	require.NoError(t, controller.enableAzureFailover(false, 10*time.Second, oldWriter.NodeGroupDefaults))
	controller.failover.Clock = clock
	require.NoError(t, provider.Refresh())
	fresh, _, err := controller.failover.Store.Load(ctx, cluster)
	require.NoError(t, err)
	assert.Equal(t, cluster.GetUID(), fresh.ClusterUID)
	for _, name := range []string{"lin", "win2"} {
		pair := fresh.Pairs[name]
		require.NotNil(t, pair)
		assert.Equal(t, Healthy, pair.Phase)
		assert.Equal(t, types.UID(name+"-primary"), pair.Primary.PoolUID)
		assert.Equal(t, types.UID(name+"-secondary-reactivated"), pair.Secondary.PoolUID)
		assert.Equal(t, types.UID(name+"-secondary-reactivated-amp"), pair.Secondary.InfrastructureUID)
		assert.Nil(t, pair.PrimaryRequest)
		assert.Nil(t, pair.SecondaryRequest)
		assert.Zero(t, pair.FallbackAllowance)
		for _, role := range []Role{pair.Primary, pair.Secondary} {
			assert.False(t, role.Failed)
			assert.Empty(t, role.LastAttemptUID)
			assert.Zero(t, role.FailureEpoch)
			assert.Zero(t, role.CheckInterval)
			assert.True(t, role.NextCheck.IsZero())
		}
	}
	for _, action := range controller.managementScaleClient.(*fakescale.FakeScaleClient).Actions() {
		assert.NotEqual(t, "update", action.GetVerb(), "old intents must not produce a scale replay")
	}
	for _, candidate := range provider.NodeGroups() {
		group := candidate.(*testGroup)
		if group.object.GetName() == "lin-primary" {
			require.NoError(t, group.IncreaseSize(1), "completed retirement must allow a fresh request for the reused identifier")
		}
	}
}
