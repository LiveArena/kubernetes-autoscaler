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
	"k8s.io/apimachinery/pkg/runtime/schema"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestLifecycleCASRetryCannotRestoreAnActiveSnapshotAfterConcurrentRetirement(t *testing.T) {
	environment, _ := newActiveLifecycleEnvironment(t, "lin", "win2")
	before, cluster := loadPairLifecycleState(t, environment)
	encoded, err := json.Marshal(before.Pairs["lin"])
	require.NoError(t, err)
	captured := &Pair{}
	require.NoError(t, json.Unmarshal(encoded, captured))
	captured.Primary.ObservedTarget++
	client := environment.managementClient.(*fakedynamic.FakeDynamicClient)
	maps := client.Resource(ConfigMaps).Namespace("tenant")
	configurationObject, err := maps.Get(context.Background(), "test-autoscaler-failover-config", metav1.GetOptions{})
	require.NoError(t, err)
	configurationPayload, _, err := unstructured.NestedString(configurationObject.Object, "data", "config.json")
	require.NoError(t, err)
	configuration := Configuration{}
	require.NoError(t, json.Unmarshal([]byte(configurationPayload), &configuration))
	configured := configuration.Pairs["lin"]
	configured.Intent = &PairIntent{State: "Retiring", RegistrationID: "registration-lin", ResourceRetirementComplete: true}
	configuration.Pairs["lin"] = configured
	configurationJSON, err := json.Marshal(configuration)
	require.NoError(t, err)
	require.NoError(t, unstructured.SetNestedField(configurationObject.Object, string(configurationJSON), "data", "config.json"))
	conflicted, callbacks := false, 0
	client.PrependReactor("update", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		object := action.(clienttesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		if object.GetName() != "test-autoscaler-failover-state" || conflicted {
			return false, nil, nil
		}
		conflicted = true
		require.NoError(t, client.Tracker().Update(ConfigMaps, configurationObject, "tenant"))
		for _, role := range []string{"primary", "secondary"} {
			require.NoError(t, client.Tracker().Delete(environment.machinePoolResource, "tenant", "lin-"+role))
			require.NoError(t, client.Tracker().Delete(schema.GroupVersionResource{Group: AzureAPIGroup, Version: "v1beta1", Resource: "azuremachinepools"}, "tenant", "lin-"+role))
		}
		stored, err := client.Tracker().Get(ConfigMaps, "tenant", "test-autoscaler-failover-state")
		require.NoError(t, err)
		retiredObject := stored.(*unstructured.Unstructured).DeepCopy()
		retiredState := &State{}
		payload, _, err := unstructured.NestedString(retiredObject.Object, "data", "state.json")
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal([]byte(payload), retiredState))
		retiredState.Pairs["lin"].Lifecycle.State = "Retired"
		retiredState.Pairs["lin"].Lifecycle.OperationsSettled = true
		data, err := json.Marshal(retiredState)
		require.NoError(t, err)
		require.NoError(t, unstructured.SetNestedField(retiredObject.Object, string(data), "data", "state.json"))
		require.NoError(t, client.Tracker().Update(ConfigMaps, retiredObject, "tenant"))
		return true, nil, apierrors.NewConflict(ConfigMaps.GroupResource(), object.GetName(), fmt.Errorf("concurrent retirement"))
	})
	_, err = environment.failover.Store.Reconcile(context.Background(), cluster, func(state *State) error {
		callbacks++
		state.Pairs["lin"] = captured
		return nil
	})
	require.Error(t, err)
	assert.True(t, conflicted)
	assert.Equal(t, 2, callbacks)
	after, _ := loadPairLifecycleState(t, environment)
	assert.Equal(t, "Retired", after.Pairs["lin"].Lifecycle.State)
	assert.Equal(t, before.Pairs["win2"], after.Pairs["win2"])
}

func TestLifecycleRegistrationRejectsAnInfrastructureReferenceChangedAfterObservation(t *testing.T) {
	environment, _ := newFailoverTestController(t)
	provider := &testProvider{controller: environment}
	setPairLifecycleIntent(t, environment, "lin", "Creating", "registration-lin", false)
	require.NoError(t, provider.Refresh())
	state, cluster := loadPairLifecycleState(t, environment)
	ctx := context.Background()
	configuration, err := ReadConfiguration(ctx, environment.managementClient, cluster)
	require.NoError(t, err)
	pools := environment.managementClient.Resource(environment.machinePoolResource).Namespace("tenant")
	pool, err := pools.Get(ctx, "lin-primary", metav1.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, unstructured.SetNestedStringMap(pool.Object, map[string]string{"apiVersion": AzureAPIGroup + "/v1beta1", "kind": "AzureMachinePool", "name": "different-current-amp"}, "spec", "template", "spec", "infrastructureRef"))
	_, err = pools.Update(ctx, pool, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.Error(t, environment.failover.Store.validateLifecycleBinding(ctx, cluster, "lin", configuration.Pairs["lin"], state.Pairs["lin"]), "an owned but no longer referenced AMP cannot establish a current registration")
}

func TestLifecycleFlagCannotDeclareRetiredWhileRecordedResourcesStillExist(t *testing.T) {
	environment, _ := newActiveLifecycleEnvironment(t, "lin")
	setPairLifecycleIntent(t, environment, "lin", "Retiring", "registration-lin", true)
	_, cluster := loadPairLifecycleState(t, environment)
	_, err := environment.failover.Store.Reconcile(context.Background(), cluster, func(state *State) error {
		state.Pairs["lin"].Lifecycle.State = "Retired"
		state.Pairs["lin"].Lifecycle.OperationsSettled = true
		return nil
	})
	require.Error(t, err, "setting status is not evidence of physical resource retirement")
}

func TestLifecycleIntentRejectsUnregisteredCallbacksEvenWhenAllRecordsAreAbsent(t *testing.T) {
	environment, _ := newFailoverTestController(t)
	ctx := context.Background()
	cluster, err := environment.managementClient.Resource(schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "clusters"}).Namespace("tenant").Get(ctx, "test", metav1.GetOptions{})
	require.NoError(t, err)
	maps := environment.managementClient.Resource(ConfigMaps).Namespace("tenant")
	object, err := maps.Get(ctx, "test-autoscaler-failover-config", metav1.GetOptions{})
	require.NoError(t, err)
	configuration := Configuration{APIVersion: LifecycleConfigVersion, Cluster: ClusterIdentity{Name: "test", Namespace: "tenant", UID: cluster.GetUID()}, Pairs: map[string]ConfiguredPair{}}
	encoded, err := json.Marshal(configuration)
	require.NoError(t, err)
	require.NoError(t, unstructured.SetNestedField(object.Object, string(encoded), "data", "config.json"))
	_, err = maps.Update(ctx, object, metav1.UpdateOptions{})
	require.NoError(t, err)
	_, err = environment.failover.Store.Reconcile(ctx, cluster, func(state *State) error {
		state.Pairs["old-registration"] = &Pair{Phase: Healthy}
		return nil
	})
	require.Error(t, err, "absence cannot authorize a stale ordinary-state callback under lifecycle intent")
	state, _, err := environment.failover.Store.Load(ctx, cluster)
	require.NoError(t, err)
	assert.Empty(t, state.Pairs)
}

func TestStaleStateCallbacksCannotReactivateRetiredOrRecreatePurgedPairs(t *testing.T) {
	for _, purged := range []bool{false, true} {
		statement := "RetiredRegistrationRejectsStaleActivation"
		if purged {
			statement = "PurgedRegistrationRejectsStaleRecordCreation"
		}
		t.Run(statement, func(t *testing.T) {
			environment, _ := newActiveLifecycleEnvironment(t, "lin", "win2")
			provider := &testProvider{controller: environment}
			active, cluster := loadPairLifecycleState(t, environment)
			encoded, err := json.Marshal(active.Pairs["lin"])
			require.NoError(t, err)
			captured := &Pair{}
			require.NoError(t, json.Unmarshal(encoded, captured))
			setPairLifecycleIntent(t, environment, "lin", "Retiring", "registration-lin", false)
			require.NoError(t, provider.Refresh())
			retirePairFixtureResources(t, environment, "lin")
			setPairLifecycleIntent(t, environment, "lin", "Retiring", "registration-lin", true)
			require.NoError(t, provider.Refresh())
			retired, _ := loadPairLifecycleState(t, environment)
			require.Equal(t, "Retired", retired.Pairs["lin"].Lifecycle.State)
			if purged {
				purgeLifecycleFixtureRecord(t, environment, "lin")
			}
			before, _ := loadPairLifecycleState(t, environment)
			_, err = environment.failover.Store.Reconcile(context.Background(), cluster, func(state *State) error {
				state.Version = StateVersion
				state.Pairs["lin"] = captured
				return nil
			})
			require.Error(t, err, "a stale callback must not resurrect an old registration")
			after, _ := loadPairLifecycleState(t, environment)
			assert.Equal(t, before, after)
			assert.Contains(t, after.Pairs, "win2")
		})
	}
}
