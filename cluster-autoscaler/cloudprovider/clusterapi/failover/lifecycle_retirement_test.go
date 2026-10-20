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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fakescale "k8s.io/client-go/scale/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestRetiringIntentDeniesCachedGrowthWithoutStoppingOtherPairs(t *testing.T) {
	environment, _ := newActiveLifecycleEnvironment(t, "lin", "win2")
	provider := &testProvider{controller: environment}
	cached := findLifecycleGroup(t, environment, "lin-primary")
	setPairLifecycleIntent(t, environment, "lin", "Retiring", "registration-lin", false)
	require.Error(t, cached.IncreaseSize(1), "cached Active admission must not bypass current Retiring intent")
	require.NoError(t, provider.Refresh())
	state, _ := loadPairLifecycleState(t, environment)
	assert.Equal(t, "Retiring", state.Pairs["lin"].Lifecycle.State)
	assert.Nil(t, state.Pairs["lin"].PrimaryRequest)
	for _, candidate := range provider.NodeGroups() {
		group := candidate.(*testGroup)
		if group.object.GetAnnotations()[PairKey] == "lin" {
			capacity := group.GetCapacityPolicy()
			assert.True(t, capacity.ScaleUpBlocked)
			assert.False(t, capacity.ConsiderPrimaryUnfit)
			assert.False(t, capacity.BlockUnregistered, "retirement must not hide viable declared capacity")
			require.Error(t, group.IncreaseSize(1))
		} else if group.object.GetName() == "win2-primary" {
			assert.False(t, group.GetCapacityPolicy().ScaleUpBlocked)
			require.NoError(t, group.IncreaseSize(1))
		}
	}
	state, _ = loadPairLifecycleState(t, environment)
	assert.False(t, state.Pairs["win2"].Lifecycle.OperationsSettled, "persisted request intent must immediately clear the settled status")
}

func TestMissingObjectsRequireExplicitRetirementCompletionAndDoNotBlockActiveReplacement(t *testing.T) {
	environment, _ := newActiveLifecycleEnvironment(t, "lin", "win2")
	provider := &testProvider{controller: environment}
	setPairLifecycleIntent(t, environment, "lin", "Retiring", "registration-lin", false)
	require.NoError(t, provider.Refresh())
	retirePairFixtureResources(t, environment, "lin")
	require.NoError(t, provider.Refresh())
	state, _ := loadPairLifecycleState(t, environment)
	assert.Equal(t, "Retiring", state.Pairs["lin"].Lifecycle.State, "absence is not retirement confirmation")
	assert.False(t, findLifecycleGroup(t, environment, "win2-primary").GetCapacityPolicy().ScaleUpBlocked)
	setPairLifecycleIntent(t, environment, "lin", "Retiring", "registration-lin", true)
	require.NoError(t, provider.Refresh())
	state, _ = loadPairLifecycleState(t, environment)
	assert.Equal(t, "Retired", state.Pairs["lin"].Lifecycle.State)
	assert.True(t, state.Pairs["lin"].Lifecycle.OperationsSettled)
	assert.Contains(t, state.Pairs, "win2")
}

func TestRetiringPairsReconcileDurableRequestsBeforeReportingOperationsSettled(t *testing.T) {
	for _, committed := range []bool{false, true} {
		statement := "UnconfirmedRequestRemainsUnsettledDuringRetirement"
		if committed {
			statement = "AuthoritativeCommittedTargetSettlesRetiringRequest"
		}
		t.Run(statement, func(t *testing.T) {
			environment, clock := newActiveLifecycleEnvironment(t, "lin")
			provider := &testProvider{controller: environment}
			_, cluster := loadPairLifecycleState(t, environment)
			_, err := environment.failover.Store.Reconcile(context.Background(), cluster, func(state *State) error {
				state.Pairs["lin"].PrimaryRequest = &Request{FromTarget: 3, ToTarget: 4, Started: metav1.NewTime(clock.Now()), ReadyAtStart: 1}
				return nil
			})
			require.NoError(t, err)
			if committed {
				resource := environment.managementClient.Resource(environment.machinePoolResource).Namespace("tenant")
				pool, err := resource.Get(context.Background(), "lin-primary", metav1.GetOptions{})
				require.NoError(t, err)
				require.NoError(t, unstructured.SetNestedField(pool.Object, int64(4), "spec", "replicas"))
				_, err = resource.Update(context.Background(), pool, metav1.UpdateOptions{})
				require.NoError(t, err)
				require.NoError(t, environment.machinePoolInformer.GetStore().Update(pool))
			}
			setPairLifecycleIntent(t, environment, "lin", "Retiring", "registration-lin", false)
			require.NoError(t, provider.Refresh())
			state, _ := loadPairLifecycleState(t, environment)
			assert.Equal(t, committed, state.Pairs["lin"].Lifecycle.OperationsSettled)
			if committed {
				assert.Nil(t, state.Pairs["lin"].PrimaryRequest)
			} else {
				assert.NotNil(t, state.Pairs["lin"].PrimaryRequest)
			}
		})
	}
}

func TestRetirementWaitsForAnAdmittedLocalScaleOperation(t *testing.T) {
	environment, _ := newActiveLifecycleEnvironment(t, "lin")
	provider := &testProvider{controller: environment}
	primary := findLifecycleGroup(t, environment, "lin-primary")
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	environment.managementScaleClient.(*fakescale.FakeScaleClient).PrependReactor("update", "machinepools", func(action clienttesting.Action) (bool, runtime.Object, error) {
		close(entered)
		<-release
		return true, nil, fmt.Errorf("request outcome was not confirmed")
	})
	go func() { done <- primary.IncreaseSize(1) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("scale operation did not reach the controlled boundary")
	}
	defer func() {
		close(release)
		<-done
	}()
	setPairLifecycleIntent(t, environment, "lin", "Retiring", "registration-lin", false)
	retirePairFixtureResources(t, environment, "lin")
	setPairLifecycleIntent(t, environment, "lin", "Retiring", "registration-lin", true)
	require.NoError(t, provider.Refresh())
	state, _ := loadPairLifecycleState(t, environment)
	assert.Equal(t, "Retiring", state.Pairs["lin"].Lifecycle.State)
	assert.False(t, state.Pairs["lin"].Lifecycle.OperationsSettled)
}

func TestUnpairedReplacementAllowsFinalRetirementAfterRestart(t *testing.T) {
	environment, clock := newFailoverTestController(t)
	provider := &testProvider{controller: environment}
	ctx := context.Background()
	setPairLifecycleIntent(t, environment, "lin", "Creating", "registration-lin", false)
	require.NoError(t, provider.Refresh())
	pools := environment.managementClient.Resource(environment.machinePoolResource).Namespace("tenant")
	infrastructure := environment.managementClient.Resource(schema.GroupVersionResource{Group: AzureAPIGroup, Version: "v1beta1", Resource: "azuremachinepools"}).Namespace("tenant")
	pool, err := pools.Get(ctx, "lin-primary", metav1.GetOptions{})
	require.NoError(t, err)
	amp, err := infrastructure.Get(ctx, "lin-primary", metav1.GetOptions{})
	require.NoError(t, err)
	for _, name := range []string{"lin", "win2"} {
		setPairLifecycleIntent(t, environment, name, "Retiring", "registration-"+name, false)
	}
	require.NoError(t, provider.Refresh())
	for _, name := range []string{"lin", "win2"} {
		retirePairFixtureResources(t, environment, name)
		setPairLifecycleIntent(t, environment, name, "Retiring", "registration-"+name, true)
	}
	pool = pool.DeepCopy()
	pool.SetName("unpaired-replacement")
	pool.SetUID("fresh-unpaired")
	pool.SetResourceVersion("")
	annotations := pool.GetAnnotations()
	delete(annotations, PairKey)
	delete(annotations, RoleKey)
	pool.SetAnnotations(annotations)
	require.NoError(t, unstructured.SetNestedStringMap(pool.Object, map[string]string{"apiVersion": AzureAPIGroup + "/v1beta1", "kind": "AzureMachinePool", "name": "unpaired-replacement"}, "spec", "template", "spec", "infrastructureRef"))
	require.NoError(t, unstructured.SetNestedStringSlice(pool.Object, []string{}, "spec", "providerIDList"))
	_, err = pools.Create(ctx, pool, metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, environment.machinePoolInformer.GetStore().Add(pool))
	amp = amp.DeepCopy()
	amp.SetName("unpaired-replacement")
	amp.SetUID("fresh-unpaired-amp")
	amp.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: pool.GetAPIVersion(), Kind: MachinePoolKind, Name: pool.GetName(), UID: pool.GetUID()}})
	_, err = infrastructure.Create(ctx, amp, metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, environment.enableAzureFailover(false, 10*time.Second))
	environment.failover.Clock = clock
	require.NoError(t, provider.Refresh())
	state, _ := loadPairLifecycleState(t, environment)
	for _, name := range []string{"lin", "win2"} {
		assert.Equal(t, "Retired", state.Pairs[name].Lifecycle.State)
	}
	groups := provider.NodeGroups()
	require.Len(t, groups, 1)
	assert.False(t, groups[0].(*testGroup).GetCapacityPolicy().ScaleUpBlocked)
}
