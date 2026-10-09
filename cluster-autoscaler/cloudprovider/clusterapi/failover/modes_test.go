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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	fakescale "k8s.io/client-go/scale/fake"
	"testing"
	"time"
)

func TestOnlyDisabledActiveAndFreezeModesAreAccepted(t *testing.T) {
	for _, mode := range []string{"disabled", "active", "freeze"} {
		assert.NoError(t, ValidateMode(mode))
	}
	for _, mode := range []string{"", "enabled", "true", "Active"} {
		assert.Error(t, ValidateMode(mode))
	}
}
func TestSecondaryPoolScalingRequiresActiveModeWithoutHidingExistingCapacity(t *testing.T) {
	for _, mode := range []string{ModeDisabled, ModeActive, ModeFreeze} {
		statement := map[string]string{
			ModeDisabled: "DisabledModeBlocksSecondaryRequestsWithoutCreatingState",
			ModeActive:   "ActiveModePermitsFailureTriggeredSecondaryScaling",
			ModeFreeze:   "FrozenModeBlocksNewRequestsWithoutHidingExistingCapacity",
		}[mode]
		t.Run(statement, func(t *testing.T) {
			controller, clock := newFailoverTestController(t)
			provider := &testProvider{controller: controller}
			if mode == ModeFreeze {
				resource := controller.managementClient.Resource(controller.machinePoolResource).Namespace("tenant")
				pool, err := resource.Get(context.Background(), "lin-secondary", metav1.GetOptions{})
				require.NoError(t, err)
				require.NoError(t, unstructured.SetNestedField(pool.Object, int64(2), "spec", "replicas"))
				_, err = resource.Update(context.Background(), pool, metav1.UpdateOptions{})
				require.NoError(t, err)
				require.NoError(t, controller.machinePoolInformer.Informer().GetStore().Update(pool))
			}
			if mode == ModeDisabled {
				controller.failover = nil
			} else if mode == ModeFreeze {
				require.NoError(t, controller.enableAzureFailover(true, 10*time.Second))
				controller.failover.Clock = clock
			}
			if mode != ModeDisabled {
				failoverTestFailure(controller, "lin", "primary", clock.Now())
			}
			require.NoError(t, provider.Refresh())
			groups := provider.NodeGroups()
			require.Len(t, groups, 4)
			for _, candidate := range groups {
				group := candidate.(*testGroup)
				if group.object.GetName() != "lin-secondary" {
					continue
				}
				assert.Equal(t, mode != ModeActive, group.GetCapacityPolicy().ScaleUpBlocked)
				assert.False(t, group.GetCapacityPolicy().BlockUnregistered, "mode must not hide viable secondary arrivals")
				if mode == ModeFreeze {
					target, err := group.TargetSize()
					require.NoError(t, err)
					assert.Equal(t, 2, target)
				}
				if mode == ModeActive {
					require.NoError(t, group.IncreaseSize(2))
				} else {
					require.Error(t, group.IncreaseSize(1))
				}
			}
			if mode == ModeDisabled {
				_, err := controller.managementClient.Resource(ConfigMaps).Namespace("tenant").Get(context.Background(), "test-autoscaler-failover-state", metav1.GetOptions{})
				require.True(t, apierrors.IsNotFound(err))
			}
		})
	}
}
func TestInitialActivationCreatesStateWithoutChangingPoolsOrNodes(t *testing.T) {
	for _, mode := range []string{"disabled", "unconfigured", "configured"} {
		statement := map[string]string{
			"disabled":     "DisabledDeploymentDoesNotCreateFailoverState",
			"unconfigured": "UnconfiguredDeploymentDoesNotCreateFailoverState",
			"configured":   "FirstValidActivationCreatesOwnedHealthyStateWithoutScaling",
		}[mode]
		t.Run(statement, func(t *testing.T) {
			controller, _ := newFailoverTestController(t)
			provider := &testProvider{controller: controller}
			beforePools := map[string]*unstructured.Unstructured{}
			for _, candidate := range provider.NodeGroups() {
				group := candidate.(*testGroup)
				object := group.object.DeepCopy()
				if mode == "unconfigured" {
					annotations := object.GetAnnotations()
					delete(annotations, PairKey)
					delete(annotations, RoleKey)
					object.SetAnnotations(annotations)
					_, err := controller.managementClient.Resource(controller.machinePoolResource).Namespace("tenant").Update(context.Background(), object, metav1.UpdateOptions{})
					require.NoError(t, err)
					require.NoError(t, controller.machinePoolInformer.Informer().GetStore().Update(object))
				}
				beforePools[object.GetName()] = object
			}
			beforeNodes := map[string]*corev1.Node{}
			for _, object := range controller.nodeInformer.GetStore().List() {
				node := object.(*corev1.Node)
				beforeNodes[node.Name] = node.DeepCopy()
			}
			if mode == "disabled" {
				controller.failover = nil
			}
			maps := controller.managementClient.Resource(ConfigMaps).Namespace("tenant")
			_, err := maps.Get(context.Background(), "test-autoscaler-failover-state", metav1.GetOptions{})
			require.True(t, apierrors.IsNotFound(err))
			require.NoError(t, provider.Refresh())
			object, err := maps.Get(context.Background(), "test-autoscaler-failover-state", metav1.GetOptions{})
			if mode == "configured" {
				require.NoError(t, err)
				owners := object.GetOwnerReferences()
				require.Len(t, owners, 1)
				assert.Equal(t, types.UID("cluster-uid"), owners[0].UID)
				cluster, err := controller.managementClient.Resource(schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "clusters"}).Namespace("tenant").Get(context.Background(), "test", metav1.GetOptions{})
				require.NoError(t, err)
				state, _, err := controller.failover.Store.Load(context.Background(), cluster)
				require.NoError(t, err)
				assert.Equal(t, 2, state.Version)
				require.Len(t, state.Pairs, 2)
				for _, pair := range state.Pairs {
					assert.Equal(t, Healthy, pair.Phase)
					assert.Zero(t, pair.FallbackAllowance)
					assert.Nil(t, pair.PrimaryRequest)
					assert.Nil(t, pair.SecondaryRequest)
				}
				require.NoError(t, provider.Refresh())
			} else {
				require.True(t, apierrors.IsNotFound(err))
			}
			for name, before := range beforePools {
				after, err := controller.managementClient.Resource(controller.machinePoolResource).Namespace("tenant").Get(context.Background(), name, metav1.GetOptions{})
				require.NoError(t, err)
				assert.Equal(t, before, after, "state initialization must not modify pool %s", name)
			}
			for _, object := range controller.nodeInformer.GetStore().List() {
				node := object.(*corev1.Node)
				assert.Equal(t, beforeNodes[node.Name], node)
			}
			for _, action := range controller.managementScaleClient.(*fakescale.FakeScaleClient).Actions() {
				assert.NotEqual(t, "update", action.GetVerb(), "first initialization must not resize pools")
				assert.NotEqual(t, "patch", action.GetVerb(), "first initialization must not resize pools")
			}
		})
	}
}
