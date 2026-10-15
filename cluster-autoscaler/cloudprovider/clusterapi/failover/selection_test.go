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
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	fakescale "k8s.io/client-go/scale/fake"
	clienttesting "k8s.io/client-go/testing"
	"strings"
	"testing"
)

func TestOnlyConfiguredCurrentPoolsScaleWhenPhysicalGenerationsOverlap(t *testing.T) {
	controller, clock := newFailoverTestController(t)
	provider := &testProvider{controller: controller}
	resource := controller.managementClient.Resource(controller.machinePoolResource).Namespace("tenant")
	current, err := resource.Get(context.Background(), "lin-secondary", metav1.GetOptions{})
	require.NoError(t, err)
	old := current.DeepCopy()
	old.SetName("lin-secondary-old-hash")
	old.SetUID("old-pool-uid")
	old.SetResourceVersion("")
	require.NoError(t, unstructured.SetNestedField(old.Object, int64(2), "spec", "replicas"))
	_, err = resource.Create(context.Background(), old, metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, controller.machinePoolInformer.Informer().GetStore().Add(old))
	failoverTestFailure(controller, "lin", "primary", clock.Now())
	require.NoError(t, provider.Refresh())
	groups := provider.NodeGroups()
	require.Len(t, groups, 5)
	foundOld, foundCurrent := false, false
	for _, candidate := range groups {
		group := candidate.(*testGroup)
		switch group.object.GetName() {
		case old.GetName():
			foundOld = true
			assert.True(t, group.GetCapacityPolicy().ScaleUpBlocked)
			assert.False(t, group.GetCapacityPolicy().BlockUnregistered)
			target, err := group.TargetSize()
			require.NoError(t, err)
			assert.Equal(t, 2, target)
			require.Error(t, group.IncreaseSize(1))
		case current.GetName():
			foundCurrent = true
			assert.False(t, group.GetCapacityPolicy().ScaleUpBlocked)
			require.NoError(t, group.IncreaseSize(2))
		}
	}
	assert.True(t, foundOld && foundCurrent)
}
func TestRoleOverlapUsesExplicitSelectionAndAmbiguityBlocksScalingWithoutStateReset(t *testing.T) {
	for _, overlap := range []string{"primary", "secondary", "both"} {
		for _, unresolved := range []string{"none", "primary", "secondary"} {
			statement := "ExplicitSelectionAdmitsOnlyCurrentPoolsDespiteRoleOverlap"
			if unresolved != "none" {
				statement = "Unresolved" + map[string]string{"primary": "Primary", "secondary": "Secondary"}[unresolved] + "SelectionPreservesStateAndBlocksNewRequests"
			}
			t.Run(statement+"/ExtraPoolsForRole="+overlap, func(t *testing.T) {
				controller, clock := newFailoverTestController(t)
				renameFailoverTestPairIdentifiers(t, controller)
				provider := &testProvider{controller: controller}
				ctx := context.Background()
				resource := controller.managementClient.Resource(controller.machinePoolResource).Namespace("tenant")
				amps := controller.managementClient.Resource(schema.GroupVersionResource{Group: azureMachinePoolMachineApiGroup, Version: "v1beta1", Resource: "azuremachinepools"}).Namespace("tenant")
				for _, role := range []string{"primary", "secondary"} {
					if overlap != "both" && overlap != role {
						continue
					}
					current, err := resource.Get(ctx, "lin-"+role, metav1.GetOptions{})
					require.NoError(t, err)
					extra := current.DeepCopy()
					extra.SetName("z-extra-" + role)
					extra.SetUID(types.UID(extra.GetName()))
					extra.SetResourceVersion("1")
					require.NoError(t, unstructured.SetNestedField(extra.Object, int64(2), "spec", "replicas"))
					require.NoError(t, unstructured.SetNestedStringSlice(extra.Object, []string{}, "spec", "providerIDList"))
					require.NoError(t, unstructured.SetNestedStringMap(extra.Object, map[string]string{"apiVersion": "infrastructure.cluster.x-k8s.io/v1beta1", "kind": "AzureMachinePool", "name": extra.GetName()}, "spec", "template", "spec", "infrastructureRef"))
					_, err = resource.Create(ctx, extra, metav1.CreateOptions{})
					require.NoError(t, err)
					require.NoError(t, controller.machinePoolInformer.Informer().GetStore().Add(extra))
					amp, err := amps.Get(ctx, current.GetName(), metav1.GetOptions{})
					require.NoError(t, err)
					amp.SetName(extra.GetName())
					amp.SetUID(types.UID(extra.GetName() + "-amp"))
					amp.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: extra.GetAPIVersion(), Kind: machinePoolKind, Name: extra.GetName(), UID: extra.GetUID()}})
					_, err = amps.Create(ctx, amp, metav1.CreateOptions{})
					require.NoError(t, err)
					observation := &unstructured.Unstructured{Object: map[string]interface{}{"status": map[string]interface{}{"provisioningState": "Failed"}}}
					observation.SetNamespace("tenant")
					observation.SetUID(types.UID(extra.GetName() + "-failure"))
					observation.SetCreationTimestamp(metav1.NewTime(clock.Now()))
					observation.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: amp.GetAPIVersion(), Kind: "AzureMachinePool", Name: amp.GetName(), UID: amp.GetUID()}})
					controller.failover.Observe(observation)
				}
				require.NoError(t, provider.Refresh())
				cluster, err := controller.managementClient.Resource(schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "clusters"}).Namespace("tenant").Get(ctx, "test", metav1.GetOptions{})
				require.NoError(t, err)
				state, _, err := controller.failover.Store.Load(ctx, cluster)
				require.NoError(t, err)
				require.Contains(t, state.Pairs, "abc")
				assert.False(t, state.Pairs["abc"].Primary.Failed, "extra-primary failures must not degrade the selected primary")
				assert.False(t, state.Pairs["abc"].Secondary.Failed, "extra-secondary failures must not degrade the selected secondary")
				failoverTestFailure(controller, "lin", "primary", clock.Now())
				require.NoError(t, provider.Refresh())
				maps := controller.managementClient.Resource(ConfigMaps).Namespace("tenant")
				before, err := maps.Get(ctx, "test-autoscaler-failover-state", metav1.GetOptions{})
				require.NoError(t, err)
				configurationObject, err := maps.Get(ctx, "test-autoscaler-failover-config", metav1.GetOptions{})
				require.NoError(t, err)
				originalConfiguration := configurationObject.DeepCopy()
				if unresolved != "none" {
					encoded, _, err := unstructured.NestedString(configurationObject.Object, "data", "config.json")
					require.NoError(t, err)
					configuration := Configuration{}
					require.NoError(t, json.Unmarshal([]byte(encoded), &configuration))
					pair := configuration.Pairs["abc"]
					if unresolved == "primary" {
						pair.Primary.Name = "missing-primary"
					} else {
						pair.Secondary.Name = "missing-secondary"
					}
					configuration.Pairs["abc"] = pair
					data, err := json.Marshal(configuration)
					require.NoError(t, err)
					require.NoError(t, unstructured.SetNestedField(configurationObject.Object, string(data), "data", "config.json"))
					_, err = maps.Update(ctx, configurationObject, metav1.UpdateOptions{})
					require.NoError(t, err)
				}
				client := controller.managementClient.(*fakedynamic.FakeDynamicClient)
				client.ClearActions()
				controller.managementScaleClient.(*fakescale.FakeScaleClient).ClearActions()
				require.NoError(t, provider.Refresh())
				var selected *testGroup
				for _, candidate := range provider.NodeGroups() {
					group := candidate.(*testGroup)
					name := group.object.GetName()
					if name == "lin-secondary" {
						selected = group
					}
					if strings.HasPrefix(name, "z-extra-") || unresolved != "none" && strings.HasPrefix(name, "lin-") {
						policy := group.GetCapacityPolicy()
						assert.True(t, policy.ScaleUpBlocked)
						require.Error(t, group.IncreaseSize(1))
						if unresolved != "none" {
							assert.Contains(t, policy.Reason, "abc")
							assert.Contains(t, policy.Reason, unresolved)
							assert.Contains(t, policy.Reason, name)
						}
						if strings.HasPrefix(name, "z-extra-") {
							target, err := group.TargetSize()
							require.NoError(t, err)
							assert.Equal(t, 2, target)
						}
					}
				}
				require.NotNil(t, selected)
				if unresolved != "none" {
					after, err := maps.Get(ctx, "test-autoscaler-failover-state", metav1.GetOptions{})
					require.NoError(t, err)
					assert.Equal(t, before, after)
					for _, action := range client.Actions() {
						assert.Equal(t, "get", action.GetVerb(), "ambiguity must not write/reset/delete state or resources")
					}
					_, err = maps.Update(ctx, originalConfiguration, metav1.UpdateOptions{})
					require.NoError(t, err)
					require.NoError(t, provider.Refresh())
				}
				require.NoError(t, selected.IncreaseSize(2))
				for _, action := range controller.managementScaleClient.(*fakescale.FakeScaleClient).Actions() {
					if action.GetVerb() == "update" {
						assert.Equal(t, "lin-secondary", action.(clienttesting.UpdateAction).GetObject().(*autoscalingv1.Scale).Name)
					}
				}
				require.NoError(t, provider.Refresh())
				require.Error(t, selected.IncreaseSize(2), "restored selection cannot replay a committed request")
			})
		}
	}
}
