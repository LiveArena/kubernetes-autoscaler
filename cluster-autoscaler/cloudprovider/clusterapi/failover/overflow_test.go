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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestOverflowResynchronizationPreservesFailuresForLaterSelectedOwners(t *testing.T) {
	for _, version := range []string{ConfigVersion, LifecycleConfigVersion} {
		statement := "UnselectedLegacyOwnerRetainsFailureAfterInformerDeletion"
		if version == LifecycleConfigVersion {
			statement = "EmptyLifecycleSelectionStillResynchronizesCandidateOwners"
		}
		t.Run(statement, func(t *testing.T) {
			controller, clock := newFailoverTestController(t)
			ctx := context.Background()
			cluster, err := controller.managementClient.Resource(controller.machinePoolResource.GroupVersion().WithResource("clusters")).Namespace("tenant").Get(ctx, "test", metav1.GetOptions{})
			require.NoError(t, err)
			original, err := ReadConfiguration(ctx, controller.managementClient, cluster)
			require.NoError(t, err)
			maps := controller.managementClient.Resource(ConfigMaps).Namespace("tenant")
			write := func(configuration *Configuration) {
				object, err := maps.Get(ctx, "test-autoscaler-failover-config", metav1.GetOptions{})
				require.NoError(t, err)
				encoded, err := json.Marshal(configuration)
				require.NoError(t, err)
				require.NoError(t, unstructured.SetNestedField(object.Object, string(encoded), "data", "config.json"))
				_, err = maps.Update(ctx, object, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			selected := &Configuration{APIVersion: version, Cluster: original.Cluster, Pairs: map[string]ConfiguredPair{}}
			if version == ConfigVersion {
				selected.Pairs["lin"] = original.Pairs["lin"]
			}
			write(selected)
			failure := &unstructured.Unstructured{Object: map[string]interface{}{"status": map[string]interface{}{"provisioningState": "Failed"}}}
			failure.SetNamespace("tenant")
			failure.SetUID("unselected-primary-failure")
			failure.SetCreationTimestamp(metav1.NewTime(clock.Now()))
			failure.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: AzureAPIGroup + "/v1beta1", Kind: "AzureMachinePool", Name: "win2-primary", UID: "win2-primary-amp"}})
			for index := 0; index < 64; index++ {
				noise := failure.DeepCopy()
				name := fmt.Sprintf("noise-%d", index)
				noise.SetUID(types.UID(name + "-failure"))
				noise.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: AzureAPIGroup + "/v1beta1", Kind: "AzureMachinePool", Name: name, UID: types.UID(name)}})
				controller.failover.Observe(noise)
			}
			controller.failover.Observe(failure)
			require.True(t, controller.failover.Overflow)
			require.NotContains(t, controller.failover.Observations, "tenant/win2-primary-amp")
			visits := 0
			controller.failureVisitor = func(visit func(FailureObservation)) error {
				visits++
				observation, terminal := TerminalAMPMFailure(failure)
				require.True(t, terminal)
				visit(observation)
				return nil
			}
			controller.failover.Refresh()
			assert.Positive(t, visits, "even an empty selection must inspect candidate failure owners")
			assert.False(t, controller.failover.Overflow)
			assert.LessOrEqual(t, len(controller.failover.RecoveredObservations), 4, "recovery is bounded by the four current owner identities, not noise owners")
			controller.failureVisitor = func(func(FailureObservation)) error { return nil }
			unavailable := true
			controller.managementClient.(*fakedynamic.FakeDynamicClient).PrependReactor("get", "azuremachinepools", func(action clienttesting.Action) (bool, runtime.Object, error) {
				if unavailable && action.(clienttesting.GetAction).GetName() == "win2-primary" {
					return true, nil, apierrors.NewServiceUnavailable("transient owner discovery failure")
				}
				return false, nil, nil
			})
			controller.failover.Refresh()
			assert.Contains(t, controller.failover.RecoveredObservations, "tenant/win2-primary-amp", "incomplete discovery cannot erase already recovered evidence")
			unavailable = false
			original.APIVersion = version
			if version == LifecycleConfigVersion {
				for name, pair := range original.Pairs {
					pair.Intent = &PairIntent{State: "Creating", RegistrationID: "registration-" + name}
					original.Pairs[name] = pair
				}
			}
			write(original)
			controller.failover.Refresh()
			state, _, err := controller.failover.Store.Load(ctx, cluster)
			require.NoError(t, err)
			require.Contains(t, state.Pairs, "win2")
			assert.True(t, state.Pairs["win2"].Primary.Failed)
			assert.Equal(t, failure.GetUID(), state.Pairs["win2"].Primary.LastAttemptUID)
		})
	}
}

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
