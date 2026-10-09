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
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
	"testing"
	"time"
)

func TestObservationOverflowStaysClosedUntilResynchronizationCompletes(t *testing.T) {
	for _, fault := range []string{"snapshot-error", "state-write-denied", "new-overflow"} {
		statement := map[string]string{
			"snapshot-error":     "FailedFailureSnapshotKeepsOverflowAndBufferedEvidence",
			"state-write-denied": "DeniedStatePersistenceKeepsOverflowAndBufferedEvidence",
			"new-overflow":       "OverflowDuringResynchronizationRequiresAnotherCompleteScan",
		}[fault]
		t.Run(statement, func(t *testing.T) {
			controller, clock := newFailoverTestController(t)
			provider := &testProvider{controller: controller}
			require.NoError(t, provider.Refresh())
			failoverTestFailure(controller, "lin", "primary", clock.Now())
			selected := controller.failover.Observations["tenant/lin-primary-amp"]
			noise := func(index int) *unstructured.Unstructured {
				name := fmt.Sprintf("noise-%d", index)
				attempt := &unstructured.Unstructured{Object: map[string]interface{}{"status": map[string]interface{}{"provisioningState": "Failed"}}}
				attempt.SetNamespace("tenant")
				attempt.SetUID(types.UID(string(selected.UID) + "-" + name))
				attempt.SetCreationTimestamp(metav1.NewTime(clock.Now()))
				attempt.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: AzureAPIGroup + "/v1beta1", Kind: "AzureMachinePool", Name: name, UID: types.UID(string(selected.OwnerUID) + "-" + name)}})
				return attempt
			}
			for index := 0; index < 64; index++ {
				controller.failover.Observe(noise(index))
			}
			require.True(t, controller.failover.Overflow)
			require.Len(t, controller.failover.Observations, 64)
			blocked, injected := true, false
			controller.failureVisitor = func(visit func(FailureObservation)) error {
				if blocked && fault == "snapshot-error" {
					return fmt.Errorf("failure snapshot unavailable")
				}
				visit(selected)
				if blocked && fault == "new-overflow" && !injected {
					injected = true
					controller.failover.Observe(noise(65))
				}
				return nil
			}
			if fault == "state-write-denied" {
				controller.managementClient.(*fakedynamic.FakeDynamicClient).PrependReactor("update", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
					if blocked {
						return true, nil, apierrors.NewForbidden(ConfigMaps.GroupResource(), "test", fmt.Errorf("denied"))
					}
					return false, nil, nil
				})
			}
			require.NoError(t, provider.Refresh())
			assert.True(t, controller.failover.Overflow)
			assert.NotEmpty(t, controller.failover.Observations)
			for _, candidate := range provider.NodeGroups() {
				group := candidate.(*testGroup)
				assert.True(t, group.GetCapacityPolicy().ScaleUpBlocked)
				require.Error(t, group.IncreaseSize(1))
			}
			blocked = false
			require.NoError(t, provider.Refresh())
			assert.False(t, controller.failover.Overflow)
			assert.Empty(t, controller.failover.Observations)
			for _, candidate := range provider.NodeGroups() {
				group := candidate.(*testGroup)
				if group.object.GetName() == "lin-secondary" {
					assert.False(t, group.GetCapacityPolicy().ScaleUpBlocked)
					require.NoError(t, group.IncreaseSize(2))
				}
			}
		})
	}
}

func TestPrimaryFitEvidenceDoesNotSurviveResetOrDiscoveryFailure(t *testing.T) {
	controller, _ := newFailoverTestController(t)
	provider := &testProvider{controller: controller}
	require.NoError(t, provider.Refresh())
	var secondary *testGroup
	for _, candidate := range provider.NodeGroups() {
		group := candidate.(*testGroup)
		if group.object.GetName() == "lin-secondary" {
			secondary = group
		}
	}
	require.NotNil(t, secondary)
	require.NoError(t, secondary.AllowPrimaryFitFallback("verified primary predicate rejection"))
	assert.False(t, secondary.GetCapacityPolicy().ScaleUpBlocked)
	secondary.ResetPrimaryFitFallback()
	assert.True(t, secondary.GetCapacityPolicy().ScaleUpBlocked)
	require.NoError(t, secondary.AllowPrimaryFitFallback("verified primary predicate rejection"))
	object := secondary.object.DeepCopy()
	annotations := object.GetAnnotations()
	annotations[nodeGroupMaxSizeAnnotationKey] = "invalid"
	object.SetAnnotations(annotations)
	require.NoError(t, controller.machinePoolInformer.Informer().GetStore().Update(object))
	require.NoError(t, provider.Refresh())
	assert.True(t, secondary.GetCapacityPolicy().ScaleUpBlocked)
	assert.False(t, secondary.GetCapacityPolicy().ConsiderPrimaryUnfit)
	assert.Empty(t, controller.failover.PrimaryFitFallbackReason(secondary.Id()))
	require.Error(t, secondary.AllowPrimaryFitFallback("old fit evidence"))
}
func TestUnverifiedOrFailedPairsBlockNewSecondaryPoolScaling(t *testing.T) {
	for _, fault := range []string{"read-denied", "write-denied", "old-generation", "both-zones", "discovery-failed"} {
		statement := map[string]string{
			"read-denied":      "DeniedStateReadsBlockSecondaryPoolScaling",
			"write-denied":     "DeniedStateWritesBlockSecondaryPoolScaling",
			"old-generation":   "UnresolvableConfiguredGenerationBlocksSecondaryPoolScaling",
			"both-zones":       "BothPoolFailuresBlockAdditionalSecondaryPoolScaling",
			"discovery-failed": "DiscoveryFailureClosesSecondaryPoolAdmission",
		}[fault]
		t.Run(statement, func(t *testing.T) {
			controller, clock := newFailoverTestController(t)
			provider := &testProvider{controller: controller}
			require.NoError(t, provider.Refresh())
			resource := controller.managementClient.Resource(controller.machinePoolResource).Namespace("tenant")
			pool, err := resource.Get(context.Background(), "lin-secondary", metav1.GetOptions{})
			require.NoError(t, err)
			require.NoError(t, unstructured.SetNestedField(pool.Object, int64(2), "spec", "replicas"))
			_, err = resource.Update(context.Background(), pool, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.NoError(t, controller.machinePoolInformer.Informer().GetStore().Update(pool))
			if fault == "old-generation" {
				configurationObject, err := controller.managementClient.Resource(ConfigMaps).Namespace("tenant").Get(context.Background(), "test-autoscaler-failover-config", metav1.GetOptions{})
				require.NoError(t, err)
				encoded, _, err := unstructured.NestedString(configurationObject.Object, "data", "config.json")
				require.NoError(t, err)
				configuration := Configuration{}
				require.NoError(t, json.Unmarshal([]byte(encoded), &configuration))
				pair := configuration.Pairs["lin"]
				pair.Secondary.Name = "new-secondary-generation"
				configuration.Pairs["lin"] = pair
				data, err := json.Marshal(configuration)
				require.NoError(t, err)
				require.NoError(t, unstructured.SetNestedField(configurationObject.Object, string(data), "data", "config.json"))
				_, err = controller.managementClient.Resource(ConfigMaps).Namespace("tenant").Update(context.Background(), configurationObject, metav1.UpdateOptions{})
				require.NoError(t, err)
			} else if fault == "both-zones" {
				failoverTestFailure(controller, "lin", "secondary", clock.Now())
			} else if fault == "discovery-failed" {
				annotations := pool.GetAnnotations()
				annotations[nodeGroupMaxSizeAnnotationKey] = "not-an-integer"
				pool = pool.DeepCopy()
				pool.SetAnnotations(annotations)
				require.NoError(t, controller.machinePoolInformer.Informer().GetStore().Update(pool))
			} else {
				verb := "get"
				if fault == "write-denied" {
					verb = "update"
				}
				controller.managementClient.(*fakedynamic.FakeDynamicClient).PrependReactor(verb, "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewForbidden(ConfigMaps.GroupResource(), "test", fmt.Errorf("denied"))
				})
			}
			failoverTestFailure(controller, "lin", "primary", clock.Now())
			require.NoError(t, provider.Refresh())
			if fault == "discovery-failed" {
				controller.failover.RLock()
				policy := controller.failover.Policies["MachinePool/tenant/lin-secondary"]
				controller.failover.RUnlock()
				assert.True(t, policy.ScaleUpBlocked)
				assert.Equal(t, "failover discovery failed", policy.Reason)
				return
			}
			for _, candidate := range provider.NodeGroups() {
				group := candidate.(*testGroup)
				if group.object.GetName() != "lin-secondary" {
					continue
				}
				policy := group.GetCapacityPolicy()
				assert.True(t, policy.ScaleUpBlocked)
				assert.Equal(t, fault == "both-zones", policy.BlockUnregistered)
				require.Error(t, group.IncreaseSize(1))
				target, err := group.TargetSize()
				require.NoError(t, err)
				assert.Equal(t, 2, target)
			}
		})
	}
}
func TestSecondaryPoolScalingHonorsItsLiveMaximumAndRejectsStaleAdmission(t *testing.T) {
	controller, clock := newFailoverTestController(t)
	provider := &testProvider{controller: controller}
	failoverTestFailure(controller, "lin", "primary", clock.Now())
	resource := controller.managementClient.Resource(controller.machinePoolResource).Namespace("tenant")
	pool, err := resource.Get(context.Background(), "lin-secondary", metav1.GetOptions{})
	require.NoError(t, err)
	annotations := pool.GetAnnotations()
	annotations[nodeGroupMaxSizeAnnotationKey] = "1"
	pool.SetAnnotations(annotations)
	_, err = resource.Update(context.Background(), pool, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, controller.machinePoolInformer.Informer().GetStore().Update(pool))
	require.NoError(t, provider.Refresh())
	for _, candidate := range provider.NodeGroups() {
		group := candidate.(*testGroup)
		if group.object.GetName() == "lin-primary" {
			assert.Equal(t, 4, group.MaxSize())
		}
		if group.object.GetName() != "lin-secondary" {
			continue
		}
		assert.Equal(t, 1, group.MaxSize())
		assert.False(t, group.GetCapacityPolicy().ScaleUpBlocked)
		require.Error(t, group.IncreaseSize(2))
		current, err := resource.Get(context.Background(), "lin-secondary", metav1.GetOptions{})
		require.NoError(t, err)
		changedAnnotations := current.GetAnnotations()
		changedAnnotations[nodeGroupMaxSizeAnnotationKey] = "0"
		current.SetAnnotations(changedAnnotations)
		_, err = resource.Update(context.Background(), current, metav1.UpdateOptions{})
		require.NoError(t, err)
		require.Error(t, group.IncreaseSize(1), "authoritative annotation change suppresses stale admission")
		changedAnnotations[nodeGroupMaxSizeAnnotationKey] = "1"
		current = current.DeepCopy()
		current.SetAnnotations(changedAnnotations)
		_, err = resource.Update(context.Background(), current, metav1.UpdateOptions{})
		require.NoError(t, err)
		target, err := group.TargetSize()
		require.NoError(t, err)
		assert.Zero(t, target)
		require.NoError(t, group.IncreaseSize(1))
		target, err = group.TargetSize()
		require.NoError(t, err)
		assert.Equal(t, 1, target)
		clock.Step(31 * time.Second)
		require.Error(t, group.IncreaseSize(1))
	}
}
