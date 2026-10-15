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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	fakescale "k8s.io/client-go/scale/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestScanReusesConfigurationWithoutClusterReadsOrUnpairedCapacityLookups(t *testing.T) {
	environment, _ := newActiveLifecycleEnvironment(t, "lin")
	ctx := context.Background()
	client := environment.managementClient.(*fakedynamic.FakeDynamicClient)
	for _, role := range []string{"primary", "secondary"} {
		pool, err := client.Resource(environment.machinePoolResource).Namespace("tenant").Get(ctx, "win2-"+role, metav1.GetOptions{})
		require.NoError(t, err)
		annotations := pool.GetAnnotations()
		delete(annotations, PairKey)
		delete(annotations, RoleKey)
		pool.SetAnnotations(annotations)
		_, err = client.Resource(environment.machinePoolResource).Namespace("tenant").Update(ctx, pool, metav1.UpdateOptions{})
		require.NoError(t, err)
		require.NoError(t, environment.machinePoolInformer.store.Update(pool))
	}
	client.ClearActions()
	scales := environment.managementScaleClient.(*fakescale.FakeScaleClient)
	scales.ClearActions()
	environment.failover.Refresh()
	clusterReads, configReads, unpairedInfrastructureReads := 0, 0, 0
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" {
			continue
		}
		name := action.(clienttesting.GetAction).GetName()
		switch action.GetResource().Resource {
		case "clusters":
			clusterReads++
		case "configmaps":
			if name == "test-autoscaler-failover-config" {
				configReads++
			}
		case "azuremachinepools":
			if name == "win2-primary" || name == "win2-secondary" {
				unpairedInfrastructureReads++
			}
		}
	}
	assert.Zero(t, clusterReads, "parent identity comes from pool metadata, not a new Cluster permission")
	assert.LessOrEqual(t, configReads, 2, "reuse read-only config but keep the store's authoritative intent check")
	assert.Zero(t, unpairedInfrastructureReads)
	for _, action := range scales.Actions() {
		if action.GetVerb() == "get" {
			assert.NotContains(t, action.(clienttesting.GetAction).GetName(), "win2-", "unpaired scope discovery does not need capacity reads")
		}
	}
	setPairLifecycleIntent(t, environment, "lin", "Retiring", "registration-lin", false)
	require.Error(t, findLifecycleGroup(t, environment, "lin-primary").IncreaseSize(1), "scan memoization cannot leak into authoritative write admission")
}

func TestFailoverRegistrationScalingAndRetirementRequireNoClusterAccess(t *testing.T) {
	environment, _ := newFailoverTestController(t)
	ctx := context.Background()
	client := environment.managementClient.(*fakedynamic.FakeDynamicClient)
	clusters := environment.machinePoolResource.GroupVersion().WithResource("clusters")
	cluster, err := client.Resource(clusters).Namespace("tenant").Get(ctx, "test", metav1.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, client.Tracker().Delete(clusters, "tenant", "test"))
	clusterRequests := 0
	client.PrependReactor("*", "clusters", func(action clienttesting.Action) (bool, runtime.Object, error) {
		clusterRequests++
		return true, nil, apierrors.NewForbidden(clusters.GroupResource(), "test", nil)
	})
	setPairLifecycleIntent(t, environment, "lin", "Creating", "registration-lin", false)
	environment.failover.Refresh()
	state, _, err := environment.failover.Store.Load(ctx, cluster)
	require.NoError(t, err)
	require.Contains(t, state.Pairs, "lin")
	assert.Equal(t, "Creating", state.Pairs["lin"].Lifecycle.State)
	setPairLifecycleIntent(t, environment, "lin", "Active", "registration-lin", false)
	environment.failover.Refresh()
	primary := findLifecycleGroup(t, environment, "lin-primary")
	require.NoError(t, primary.IncreaseSize(1))
	setPairLifecycleIntent(t, environment, "lin", "Retiring", "registration-lin", false)
	require.Error(t, primary.IncreaseSize(1), "removing parent reads must not bypass authoritative retirement admission")
	environment.failover.Refresh()
	retirePairFixtureResources(t, environment, "lin")
	setPairLifecycleIntent(t, environment, "lin", "Retiring", "registration-lin", true)
	environment.failover.Refresh()
	state, _, err = environment.failover.Store.Load(ctx, cluster)
	require.NoError(t, err)
	assert.Equal(t, "Retired", state.Pairs["lin"].Lifecycle.State)
	assert.True(t, state.Pairs["lin"].Lifecycle.OperationsSettled)
	assert.Zero(t, clusterRequests, "no phase may require get, list, watch or write access to Cluster objects")
}

func TestParentMetadataStillRejectsMalformedOrInconsistentPoolOwners(t *testing.T) {
	for _, scenario := range []struct{ fault, statement string }{
		{"missing-owner", "MissingParentReferenceIsRejected"},
		{"missing-uid", "MissingParentUIDIsRejected"},
		{"missing-name", "MissingParentNameIsRejected"},
		{"wrong-group", "ForeignParentAPIGroupIsRejected"},
		{"missing-version", "MissingParentAPIVersionIsRejected"},
		{"ambiguous-owner", "AmbiguousParentReferencesAreRejected"},
		{"cluster-name-mismatch", "PoolClusterNameMustMatchItsParentReference"},
		{"deleting-pool", "DeletingPoolIsRejectedWithoutInspectingParentState"},
	} {
		t.Run(scenario.statement, func(t *testing.T) {
			environment, _ := newFailoverTestController(t)
			group := findLifecycleGroup(t, environment, "lin-primary")
			pool := group.Object().DeepCopy()
			owners := pool.GetOwnerReferences()
			switch scenario.fault {
			case "missing-owner":
				owners = nil
			case "missing-uid":
				owners[0].UID = ""
			case "missing-name":
				owners[0].Name = ""
			case "wrong-group":
				owners[0].APIVersion = "other.example/v1"
			case "missing-version":
				owners[0].APIVersion = "cluster.x-k8s.io/"
			case "ambiguous-owner":
				owners = append(owners, owners[0])
			case "cluster-name-mismatch":
				require.NoError(t, unstructured.SetNestedField(pool.Object, "another-cluster", "spec", "clusterName"))
			case "deleting-pool":
				now := metav1.Now()
				pool.SetDeletionTimestamp(&now)
			}
			pool.SetOwnerReferences(owners)
			group.object = pool
			_, err := environment.failover.owningCluster(group)
			require.Error(t, err)
		})
	}
}
