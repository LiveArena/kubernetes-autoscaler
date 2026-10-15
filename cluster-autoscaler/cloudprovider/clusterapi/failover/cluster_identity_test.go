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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestConfigurationOwnerIdentityAllowsEquivalentClusterAPIVersions(t *testing.T) {
	for _, scenario := range []struct {
		statement, poolVersion, configVersion, configUID string
		valid                                            bool
	}{
		{"LegacyConfigOwnerMatchesConvertedPoolOwner", "cluster.x-k8s.io/v1beta2", "cluster.x-k8s.io/v1beta1", "cluster-uid", true},
		{"ConvertedConfigOwnerMatchesLegacyPoolOwner", "cluster.x-k8s.io/v1beta1", "cluster.x-k8s.io/v1beta2", "cluster-uid", true},
		{"DifferentClusterUIDRemainsRejectedAcrossVersions", "cluster.x-k8s.io/v1beta2", "cluster.x-k8s.io/v1beta1", "other-cluster", false},
		{"ForeignClusterAPIGroupRemainsRejected", "cluster.x-k8s.io/v1beta2", "other.example/v1beta2", "cluster-uid", false},
	} {
		t.Run(scenario.statement, func(t *testing.T) {
			environment, _ := newFailoverTestController(t)
			ctx := context.Background()
			cluster, err := environment.managementClient.Resource(environment.machinePoolResource.GroupVersion().WithResource("clusters")).Namespace("tenant").Get(ctx, "test", metav1.GetOptions{})
			require.NoError(t, err)
			cluster.SetAPIVersion(scenario.poolVersion)
			maps := environment.managementClient.Resource(ConfigMaps).Namespace("tenant")
			object, err := maps.Get(ctx, "test-autoscaler-failover-config", metav1.GetOptions{})
			require.NoError(t, err)
			owners := object.GetOwnerReferences()
			owners[0].APIVersion = scenario.configVersion
			owners[0].UID = types.UID(scenario.configUID)
			object.SetOwnerReferences(owners)
			_, err = maps.Update(ctx, object, metav1.UpdateOptions{})
			require.NoError(t, err)
			configuration, err := ReadConfiguration(ctx, environment.managementClient, cluster)
			if scenario.valid {
				require.NoError(t, err, "API representation versions do not change Cluster identity")
				require.NotNil(t, configuration)
			} else {
				require.Error(t, err)
				assert.Nil(t, configuration)
			}
		})
	}
}

func TestMixedClusterOwnerVersionsSupportRegistrationScalingAndStateReload(t *testing.T) {
	environment, _ := newFailoverTestController(t)
	ctx := context.Background()
	client := environment.managementClient.(*fakedynamic.FakeDynamicClient)
	clusters := environment.machinePoolResource.GroupVersion().WithResource("clusters")
	cluster, err := client.Resource(clusters).Namespace("tenant").Get(ctx, "test", metav1.GetOptions{})
	require.NoError(t, err)
	clusterRequests := 0
	client.PrependReactor("*", "clusters", func(action clienttesting.Action) (bool, runtime.Object, error) {
		clusterRequests++
		return true, nil, apierrors.NewForbidden(clusters.GroupResource(), "test", nil)
	})
	setPairLifecycleIntent(t, environment, "lin", "Creating", "registration-lin", false)
	maps := client.Resource(ConfigMaps).Namespace("tenant")
	configuration, err := maps.Get(ctx, "test-autoscaler-failover-config", metav1.GetOptions{})
	require.NoError(t, err)
	owners := configuration.GetOwnerReferences()
	owners[0].APIVersion = "cluster.x-k8s.io/v1beta1"
	configuration.SetOwnerReferences(owners)
	_, err = maps.Update(ctx, configuration, metav1.UpdateOptions{})
	require.NoError(t, err)
	for _, name := range []string{"lin-secondary", "win2-secondary"} {
		pools := client.Resource(environment.machinePoolResource).Namespace("tenant")
		pool, err := pools.Get(ctx, name, metav1.GetOptions{})
		require.NoError(t, err)
		owners := pool.GetOwnerReferences()
		owners[0].APIVersion = "cluster.x-k8s.io/v1beta1"
		pool.SetOwnerReferences(owners)
		_, err = pools.Update(ctx, pool, metav1.UpdateOptions{})
		require.NoError(t, err)
		require.NoError(t, environment.machinePoolInformer.store.Update(pool))
	}
	environment.failover.Refresh()
	state, _, err := environment.failover.Store.Load(ctx, cluster)
	require.NoError(t, err)
	require.Contains(t, state.Pairs, "lin")
	assert.Equal(t, "Creating", state.Pairs["lin"].Lifecycle.State)
	setPairLifecycleIntent(t, environment, "lin", "Active", "registration-lin", false)
	environment.failover.Refresh()
	primary := findLifecycleGroup(t, environment, "lin-primary")
	require.NoError(t, primary.IncreaseSize(1))
	cluster.SetAPIVersion("cluster.x-k8s.io/v1beta1")
	state, _, err = environment.failover.Store.Load(ctx, cluster)
	require.NoError(t, err)
	assert.Equal(t, "Active", state.Pairs["lin"].Lifecycle.State)
	require.NotNil(t, state.Pairs["lin"].PrimaryRequest)
	assert.Equal(t, 4, state.Pairs["lin"].PrimaryRequest.ToTarget)
	changedUID := cluster.DeepCopy()
	changedUID.SetUID("another-cluster")
	_, _, err = environment.failover.Store.Load(ctx, changedUID)
	require.Error(t, err, "a version change is not permission to accept another Cluster UID")
	assert.Zero(t, clusterRequests)
}

func TestClusterOwnerIdentityRejectsMalformedOrForeignReferences(t *testing.T) {
	environment, _ := newFailoverTestController(t)
	cluster, err := environment.failover.owningCluster(findLifecycleGroup(t, environment, "lin-primary"))
	require.NoError(t, err)
	for _, scenario := range []struct {
		statement string
		owner     metav1.OwnerReference
	}{
		{"DifferentClusterUIDIsRejected", metav1.OwnerReference{APIVersion: "cluster.x-k8s.io/v1beta1", Kind: "Cluster", Name: "test", UID: "other"}},
		{"ForeignClusterAPIGroupIsRejected", metav1.OwnerReference{APIVersion: "other.example/v1beta1", Kind: "Cluster", Name: "test", UID: cluster.GetUID()}},
		{"MissingOwnerAPIVersionIsRejected", metav1.OwnerReference{APIVersion: "cluster.x-k8s.io/", Kind: "Cluster", Name: "test", UID: cluster.GetUID()}},
		{"MissingOwnerAPIGroupIsRejected", metav1.OwnerReference{APIVersion: "v1beta1", Kind: "Cluster", Name: "test", UID: cluster.GetUID()}},
		{"DifferentOwnerKindIsRejected", metav1.OwnerReference{APIVersion: "cluster.x-k8s.io/v1beta1", Kind: "Other", Name: "test", UID: cluster.GetUID()}},
		{"DifferentOwnerNameIsRejected", metav1.OwnerReference{APIVersion: "cluster.x-k8s.io/v1beta1", Kind: "Cluster", Name: "other", UID: cluster.GetUID()}},
		{"MissingOwnerUIDIsRejected", metav1.OwnerReference{APIVersion: "cluster.x-k8s.io/v1beta1", Kind: "Cluster", Name: "test"}},
	} {
		t.Run(scenario.statement, func(t *testing.T) {
			assert.False(t, matchesClusterOwner(scenario.owner, cluster))
		})
	}
}
