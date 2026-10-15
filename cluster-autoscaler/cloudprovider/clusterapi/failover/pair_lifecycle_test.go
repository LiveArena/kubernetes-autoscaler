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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

func TestLifecycleVersionIsIndependentOfTheStateEnvelopeVersion(t *testing.T) {
	environment, _ := newActiveLifecycleEnvironment(t, "lin")
	state, cluster := loadPairLifecycleState(t, environment)
	assert.Equal(t, 1, state.Version)
	encoded, err := json.Marshal(state.Pairs["lin"].Lifecycle)
	require.NoError(t, err)
	var lifecycle map[string]interface{}
	require.NoError(t, json.Unmarshal(encoded, &lifecycle))
	assert.Equal(t, float64(1), lifecycle["version"])
	if _, found := lifecycle["version"]; found {
		lifecycle["version"] = 2
		invalid, err := json.Marshal(lifecycle)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(invalid, state.Pairs["lin"].Lifecycle))
		require.Error(t, state.Validate(cluster.GetUID()), "unsupported lifecycle versions must be rejected independently")
		assert.Equal(t, 1, state.Version)
	}
}

func TestCreatingRegistersFreshPairBeforeActiveAdmission(t *testing.T) {
	environment, _ := newFailoverTestController(t)
	provider := &testProvider{controller: environment}
	setPairLifecycleIntent(t, environment, "lin", "Creating", "registration-lin", false)
	require.NoError(t, provider.Refresh())
	state, _ := loadPairLifecycleState(t, environment)
	require.Equal(t, 1, state.Version)
	encoded, err := json.Marshal(state.Pairs["lin"])
	require.NoError(t, err)
	var record map[string]interface{}
	require.NoError(t, json.Unmarshal(encoded, &record))
	lifecycle, found := record["lifecycle"].(map[string]interface{})
	require.True(t, found)
	assert.Equal(t, "Creating", lifecycle["state"])
	assert.Equal(t, "registration-lin", lifecycle["registrationID"])
	for _, candidate := range provider.NodeGroups() {
		group := candidate.(*testGroup)
		assert.True(t, group.GetCapacityPolicy().ScaleUpBlocked)
		require.Error(t, group.IncreaseSize(1))
	}
	setPairLifecycleIntent(t, environment, "lin", "Active", "registration-lin", false)
	require.NoError(t, provider.Refresh())
	for _, candidate := range provider.NodeGroups() {
		group := candidate.(*testGroup)
		if group.object.GetName() == "lin-primary" {
			assert.False(t, group.GetCapacityPolicy().ScaleUpBlocked)
			require.NoError(t, group.IncreaseSize(1))
		}
	}
}

func TestActiveIntentCannotRegisterAnAbsentPair(t *testing.T) {
	environment, _ := newFailoverTestController(t)
	provider := &testProvider{controller: environment}
	setPairLifecycleIntent(t, environment, "lin", "Active", "registration-lin", false)
	require.NoError(t, provider.Refresh())
	state, _ := loadPairLifecycleState(t, environment)
	assert.NotContains(t, state.Pairs, "lin")
	for _, candidate := range provider.NodeGroups() {
		group := candidate.(*testGroup)
		if group.object.GetAnnotations()[PairKey] == "lin" {
			assert.True(t, group.GetCapacityPolicy().ScaleUpBlocked)
			require.Error(t, group.IncreaseSize(1))
		}
	}
}

func TestIncompleteCreatingPairDoesNotBlockAnotherActivePair(t *testing.T) {
	environment, _ := newFailoverTestController(t)
	provider := &testProvider{controller: environment}
	setPairLifecycleIntent(t, environment, "lin", "Creating", "registration-lin", false)
	require.NoError(t, provider.Refresh())
	setPairLifecycleIntent(t, environment, "win2", "Active", "registration-win2", false)
	ctx := context.Background()
	pools := environment.managementClient.Resource(environment.machinePoolResource).Namespace("tenant")
	pool, err := pools.Get(ctx, "lin-secondary", metav1.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, pools.Delete(ctx, "lin-secondary", metav1.DeleteOptions{}))
	require.NoError(t, environment.machinePoolInformer.GetStore().Delete(pool))
	require.NoError(t, provider.Refresh())
	for _, candidate := range provider.NodeGroups() {
		group := candidate.(*testGroup)
		if group.object.GetName() == "win2-primary" {
			assert.False(t, group.GetCapacityPolicy().ScaleUpBlocked)
			require.NoError(t, group.IncreaseSize(1))
		} else if group.object.GetAnnotations()[PairKey] == "lin" {
			assert.True(t, group.GetCapacityPolicy().ScaleUpBlocked)
		}
	}
}

func TestLifecycleActivationDoesNotSilentlyMigrateLegacyState(t *testing.T) {
	environment, _ := newFailoverTestController(t)
	provider := &testProvider{controller: environment}
	require.NoError(t, provider.Refresh())
	before, _ := loadPairLifecycleState(t, environment)
	setPairLifecycleIntent(t, environment, "lin", "Creating", "registration-lin", false)
	require.NoError(t, provider.Refresh())
	after, _ := loadPairLifecycleState(t, environment)
	assert.Equal(t, before, after)
	for _, candidate := range provider.NodeGroups() {
		group := candidate.(*testGroup)
		assert.True(t, group.GetCapacityPolicy().ScaleUpBlocked)
	}
}

func TestCompletedTargetedPurgeAllowsFreshRegistrationWithoutPriorGenerationReplay(t *testing.T) {
	environment, clock := newActiveLifecycleEnvironment(t, "lin", "win2")
	provider := &testProvider{controller: environment}
	ctx := context.Background()
	failoverTestFailure(environment, "lin", "primary", clock.Now())
	require.NoError(t, provider.Refresh())
	old, cluster := loadPairLifecycleState(t, environment)
	require.True(t, old.Pairs["lin"].Primary.Failed)
	data, err := json.Marshal(old.Pairs["lin"])
	require.NoError(t, err)
	captured := &Pair{}
	require.NoError(t, json.Unmarshal(data, captured))
	pools := environment.managementClient.Resource(environment.machinePoolResource).Namespace("tenant")
	infrastructure := environment.managementClient.Resource(schema.GroupVersionResource{Group: AzureAPIGroup, Version: "v1beta1", Resource: "azuremachinepools"}).Namespace("tenant")
	poolCopies, infrastructureCopies := map[string]*unstructured.Unstructured{}, map[string]*unstructured.Unstructured{}
	for _, role := range []string{"primary", "secondary"} {
		pool, err := pools.Get(ctx, "lin-"+role, metav1.GetOptions{})
		require.NoError(t, err)
		amp, err := infrastructure.Get(ctx, pool.GetName(), metav1.GetOptions{})
		require.NoError(t, err)
		poolCopies[role], infrastructureCopies[role] = pool.DeepCopy(), amp.DeepCopy()
	}
	setPairLifecycleIntent(t, environment, "lin", "Retiring", "registration-lin", false)
	require.NoError(t, provider.Refresh())
	retirePairFixtureResources(t, environment, "lin")
	setPairLifecycleIntent(t, environment, "lin", "Retiring", "registration-lin", true)
	require.NoError(t, provider.Refresh())
	purgeLifecycleFixtureRecord(t, environment, "lin")
	require.NoError(t, environment.enableAzureFailover(false, 10*time.Second))
	environment.failover.Clock = clock
	failoverTestFailure(environment, "lin", "primary", clock.Now())
	for _, role := range []string{"primary", "secondary"} {
		pool := poolCopies[role]
		pool.SetUID(types.UID("fresh-lin-" + role))
		pool.SetResourceVersion("")
		_, err = pools.Create(ctx, pool, metav1.CreateOptions{})
		require.NoError(t, err)
		require.NoError(t, environment.machinePoolInformer.GetStore().Add(pool))
		amp := infrastructureCopies[role]
		amp.SetUID(types.UID("fresh-lin-" + role + "-amp"))
		owners := amp.GetOwnerReferences()
		owners[0].UID = pool.GetUID()
		amp.SetOwnerReferences(owners)
		_, err = infrastructure.Create(ctx, amp, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	setPairLifecycleIntent(t, environment, "lin", "Creating", "registration-lin-fresh", false)
	require.NoError(t, provider.Refresh())
	fresh, _ := loadPairLifecycleState(t, environment)
	require.Equal(t, "registration-lin-fresh", fresh.Pairs["lin"].Lifecycle.RegistrationID)
	assert.False(t, fresh.Pairs["lin"].Primary.Failed)
	assert.Zero(t, fresh.Pairs["lin"].FallbackAllowance)
	assert.Empty(t, fresh.Pairs["lin"].Primary.LastAttemptUID)
	assert.Equal(t, old.Pairs["win2"], fresh.Pairs["win2"])
	_, err = environment.failover.Store.Reconcile(ctx, cluster, func(state *State) error {
		state.Pairs["lin"] = captured
		return nil
	})
	require.Error(t, err)
	setPairLifecycleIntent(t, environment, "lin", "Active", "registration-lin-fresh", false)
	require.NoError(t, provider.Refresh())
	require.NoError(t, findLifecycleGroup(t, environment, "lin-primary").IncreaseSize(1))
}
