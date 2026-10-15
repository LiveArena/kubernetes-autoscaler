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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

type resourceResolverFunc func(schema.GroupKind, string, bool) (schema.GroupVersionResource, error)

func (resolve resourceResolverFunc) ResolveResource(kind schema.GroupKind, version string, refresh bool) (schema.GroupVersionResource, error) {
	return resolve(kind, version, refresh)
}

func TestRecordedRetirementResolvesCurrentRoutesBeforeDeclaringResourcesGone(t *testing.T) {
	environment, _ := newActiveLifecycleEnvironment(t, "lin")
	state, cluster := loadPairLifecycleState(t, environment)
	ctx := context.Background()
	oldRoute := schema.GroupVersionResource{Group: AzureAPIGroup, Version: "v1beta1", Resource: "azuremachinepools"}
	currentRoute := oldRoute
	currentRoute.Version = "v1beta2"
	for _, name := range []string{"lin-primary", "lin-secondary"} {
		infrastructure, err := environment.managementClient.Resource(oldRoute).Namespace("tenant").Get(ctx, name, metav1.GetOptions{})
		require.NoError(t, err)
		require.NoError(t, environment.managementClient.Resource(oldRoute).Namespace("tenant").Delete(ctx, name, metav1.DeleteOptions{}))
		infrastructure.SetAPIVersion(currentRoute.GroupVersion().String())
		_, err = environment.managementClient.Resource(currentRoute).Namespace("tenant").Create(ctx, infrastructure, metav1.CreateOptions{})
		require.NoError(t, err)
		require.NoError(t, environment.managementClient.Resource(environment.machinePoolResource).Namespace("tenant").Delete(ctx, name, metav1.DeleteOptions{}))
	}
	environment.failover.Store.Resolver = resourceResolverFunc(func(kind schema.GroupKind, version string, refresh bool) (schema.GroupVersionResource, error) {
		if kind.Kind == MachinePoolKind {
			return environment.machinePoolResource, nil
		}
		return currentRoute, nil
	})
	gone, err := environment.failover.Store.recordedPairResourcesGone(ctx, cluster, state.Pairs["lin"])
	require.NoError(t, err)
	assert.False(t, gone, "an absent old endpoint is not proof that the recorded AMP no longer exists")
}

func TestRegisteredMembersSurviveServedVersionChangesButRejectDifferentUIDs(t *testing.T) {
	environment, _ := newActiveLifecycleEnvironment(t, "lin")
	before, _ := loadPairLifecycleState(t, environment)
	ctx := context.Background()
	oldRoute := schema.GroupVersionResource{Group: AzureAPIGroup, Version: "v1beta1", Resource: "azuremachinepools"}
	currentRoute := oldRoute
	currentRoute.Version = "v1beta2"
	for _, name := range []string{"lin-primary", "lin-secondary", "win2-primary", "win2-secondary"} {
		infrastructure, err := environment.managementClient.Resource(oldRoute).Namespace("tenant").Get(ctx, name, metav1.GetOptions{})
		require.NoError(t, err)
		require.NoError(t, environment.managementClient.Resource(oldRoute).Namespace("tenant").Delete(ctx, name, metav1.DeleteOptions{}))
		infrastructure.SetAPIVersion(currentRoute.GroupVersion().String())
		_, err = environment.managementClient.Resource(currentRoute).Namespace("tenant").Create(ctx, infrastructure, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	environment.failover.Store.Resolver = resourceResolverFunc(func(kind schema.GroupKind, version string, refresh bool) (schema.GroupVersionResource, error) {
		if kind.Kind == MachinePoolKind {
			return environment.machinePoolResource, nil
		}
		return currentRoute, nil
	})
	environment.failover.Refresh()
	state, _ := loadPairLifecycleState(t, environment)
	require.Equal(t, currentRoute.GroupVersion().String(), state.Pairs["lin"].Primary.InfrastructureVersion)
	assert.Equal(t, before.Pairs["lin"].Primary.InfrastructureUID, state.Pairs["lin"].Primary.InfrastructureUID)
	assert.Equal(t, before.Pairs["lin"].Lifecycle.RegistrationID, state.Pairs["lin"].Lifecycle.RegistrationID)
	primary := findLifecycleGroup(t, environment, "lin-primary")
	require.NoError(t, primary.IncreaseSize(1))
	infrastructure, err := environment.managementClient.Resource(currentRoute).Namespace("tenant").Get(ctx, "lin-primary", metav1.GetOptions{})
	require.NoError(t, err)
	infrastructure.SetUID("replacement-infrastructure")
	_, err = environment.managementClient.Resource(currentRoute).Namespace("tenant").Update(ctx, infrastructure, metav1.UpdateOptions{})
	require.NoError(t, err)
	environment.failover.Refresh()
	assert.True(t, primary.GetCapacityPolicy().ScaleUpBlocked)
	state, _ = loadPairLifecycleState(t, environment)
	assert.Equal(t, before.Pairs["lin"].Primary.InfrastructureUID, state.Pairs["lin"].Primary.InfrastructureUID)
}

func TestSelectedPoolMetadataUsesServedRoutesInsteadOfTheRecordedVersion(t *testing.T) {
	environment, _ := newActiveLifecycleEnvironment(t, "lin")
	state, cluster := loadPairLifecycleState(t, environment)
	ctx := context.Background()
	configuration, err := ReadConfiguration(ctx, environment.managementClient, cluster)
	require.NoError(t, err)
	current := environment.machinePoolResource
	current.Version = "v1beta3"
	for _, name := range []string{"lin-primary", "lin-secondary", "win2-primary", "win2-secondary"} {
		pool, err := environment.managementClient.Resource(environment.machinePoolResource).Namespace("tenant").Get(ctx, name, metav1.GetOptions{})
		require.NoError(t, err)
		require.NoError(t, environment.managementClient.Resource(environment.machinePoolResource).Namespace("tenant").Delete(ctx, name, metav1.DeleteOptions{}))
		pool.SetAPIVersion(current.GroupVersion().String())
		_, err = environment.managementClient.Resource(current).Namespace("tenant").Create(ctx, pool, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	environment.failover.Store.Resolver = resourceResolverFunc(func(kind schema.GroupKind, version string, refresh bool) (schema.GroupVersionResource, error) {
		if kind.Kind == MachinePoolKind {
			return current, nil
		}
		return schema.GroupVersionResource{Group: AzureAPIGroup, Version: "v1beta1", Resource: "azuremachinepools"}, nil
	})
	resolved, err := environment.failover.ResolveConfiguredPools(ctx, cluster, configuration)
	require.NoError(t, err)
	assert.Equal(t, state.Pairs["lin"].Primary.PoolUID, resolved["lin-primary"])
	maximum, err := environment.failover.PrimaryAtMaximum(ctx, cluster, "lin", state.Pairs["lin"].Primary.PoolUID)
	require.NoError(t, err)
	assert.False(t, maximum)
}

func TestResourceReadsRefreshOnlyObsoleteRoutesWithoutHidingFailures(t *testing.T) {
	for _, scenario := range []struct{ fault, statement string }{
		{"route-changed", "ObsoleteRouteRefreshesOnceAndFindsTheSameNamedResource"},
		{"object-absent", "GenuineObjectAbsenceDoesNotTryAnotherIdentity"},
		{"forbidden", "PermissionDenialDoesNotTriggerDiscoveryFallback"},
		{"transport", "TransportFailureDoesNotTriggerDiscoveryFallback"},
		{"discovery-error", "FailedDiscoveryCannotProveResourceAbsence"},
	} {
		t.Run(scenario.statement, func(t *testing.T) {
			environment, _ := newFailoverTestController(t)
			ctx := context.Background()
			client := environment.managementClient.(*fakedynamic.FakeDynamicClient)
			oldRoute := schema.GroupVersionResource{Group: AzureAPIGroup, Version: "v1beta1", Resource: "azuremachinepools"}
			currentRoute := oldRoute
			currentRoute.Version = "v1beta2"
			infrastructure, err := client.Resource(oldRoute).Namespace("tenant").Get(ctx, "lin-primary", metav1.GetOptions{})
			require.NoError(t, err)
			if scenario.fault == "route-changed" || scenario.fault == "object-absent" || scenario.fault == "discovery-error" {
				require.NoError(t, client.Resource(oldRoute).Namespace("tenant").Delete(ctx, "lin-primary", metav1.DeleteOptions{}))
			}
			if scenario.fault == "route-changed" {
				infrastructure.SetAPIVersion(currentRoute.GroupVersion().String())
				_, err := client.Resource(currentRoute).Namespace("tenant").Create(ctx, infrastructure, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			failure := fmt.Errorf("transport unavailable")
			if scenario.fault == "forbidden" || scenario.fault == "transport" {
				if scenario.fault == "forbidden" {
					failure = apierrors.NewForbidden(oldRoute.GroupResource(), "lin-primary", nil)
				}
				client.PrependReactor("get", "azuremachinepools", func(clienttesting.Action) (bool, runtime.Object, error) { return true, nil, failure })
			}
			refreshes := 0
			environment.failover.Store.Resolver = resourceResolverFunc(func(kind schema.GroupKind, version string, refresh bool) (schema.GroupVersionResource, error) {
				if refresh {
					refreshes++
					if scenario.fault == "discovery-error" {
						return schema.GroupVersionResource{}, failure
					}
					if scenario.fault == "route-changed" {
						return currentRoute, nil
					}
				}
				return oldRoute, nil
			})
			client.ClearActions()
			object, err := environment.failover.Store.readResource(ctx, schema.GroupKind{Group: AzureAPIGroup, Kind: "AzureMachinePool"}, oldRoute.GroupVersion().String(), "tenant", "lin-primary")
			if scenario.fault == "route-changed" {
				require.NoError(t, err)
				assert.Equal(t, infrastructure.GetUID(), object.GetUID())
				assert.Len(t, client.Actions(), 2)
				assert.Equal(t, 1, refreshes)
			} else {
				require.Error(t, err)
				assert.Len(t, client.Actions(), 1)
				if scenario.fault == "object-absent" {
					assert.True(t, apierrors.IsNotFound(err))
					assert.Equal(t, 1, refreshes)
				} else {
					assert.False(t, apierrors.IsNotFound(err))
					require.ErrorIs(t, err, failure)
					assert.Equal(t, map[bool]int{true: 1, false: 0}[scenario.fault == "discovery-error"], refreshes)
				}
			}
		})
	}
}
