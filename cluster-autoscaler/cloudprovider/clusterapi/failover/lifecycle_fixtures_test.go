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

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clocktesting "k8s.io/utils/clock/testing"
)

func setPairLifecycleIntent(t *testing.T, environment *testEnvironment, name, state, registration string, complete bool) {
	t.Helper()
	ctx := context.Background()
	maps := environment.managementClient.Resource(ConfigMaps).Namespace("tenant")
	object, err := maps.Get(ctx, "test-autoscaler-failover-config", metav1.GetOptions{})
	require.NoError(t, err)
	encoded, _, err := unstructured.NestedString(object.Object, "data", "config.json")
	require.NoError(t, err)
	configuration := Configuration{}
	require.NoError(t, json.Unmarshal([]byte(encoded), &configuration))
	configuration.APIVersion = LifecycleConfigVersion
	if _, found := configuration.Pairs[name]; !found {
		configuration.Pairs[name] = ConfiguredPair{Primary: ConfiguredPool{Name: name + "-primary"}, Secondary: ConfiguredPool{Name: name + "-secondary"}}
	}
	for identifier, pair := range configuration.Pairs {
		if pair.Intent == nil {
			pair.Intent = &PairIntent{State: "Creating", RegistrationID: "registration-" + identifier}
		}
		if identifier == name {
			pair.Intent = &PairIntent{State: state, RegistrationID: registration, ResourceRetirementComplete: complete}
		}
		configuration.Pairs[identifier] = pair
	}
	data, err := json.Marshal(configuration)
	require.NoError(t, err)
	require.NoError(t, unstructured.SetNestedField(object.Object, string(data), "data", "config.json"))
	_, err = maps.Update(ctx, object, metav1.UpdateOptions{})
	require.NoError(t, err)
}

func loadPairLifecycleState(t *testing.T, environment *testEnvironment) (*State, *unstructured.Unstructured) {
	t.Helper()
	cluster, err := environment.managementClient.Resource(schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "clusters"}).Namespace("tenant").Get(context.Background(), "test", metav1.GetOptions{})
	require.NoError(t, err)
	state, _, err := environment.failover.Store.Load(context.Background(), cluster)
	require.NoError(t, err)
	return state, cluster
}

func retirePairFixtureResources(t *testing.T, environment *testEnvironment, name string) {
	t.Helper()
	ctx := context.Background()
	pools := environment.managementClient.Resource(environment.machinePoolResource).Namespace("tenant")
	infrastructure := environment.managementClient.Resource(schema.GroupVersionResource{Group: AzureAPIGroup, Version: "v1beta1", Resource: "azuremachinepools"}).Namespace("tenant")
	for _, role := range []string{"primary", "secondary"} {
		pool, err := pools.Get(ctx, name+"-"+role, metav1.GetOptions{})
		require.NoError(t, err)
		require.NoError(t, pools.Delete(ctx, pool.GetName(), metav1.DeleteOptions{}))
		require.NoError(t, environment.machinePoolInformer.GetStore().Delete(pool))
		require.NoError(t, infrastructure.Delete(ctx, pool.GetName(), metav1.DeleteOptions{}))
	}
	for _, object := range environment.nodeInformer.GetStore().List() {
		node := object.(*corev1.Node)
		if node.Name == name+"-primary" || node.Name == name+"-secondary" {
			require.NoError(t, environment.nodeInformer.GetStore().Delete(node))
		}
	}
}

func purgeLifecycleFixtureRecord(t *testing.T, environment *testEnvironment, name string) {
	t.Helper()
	ctx := context.Background()
	state, _ := loadPairLifecycleState(t, environment)
	require.Equal(t, "Retired", state.Pairs[name].Lifecycle.State)
	maps := environment.managementClient.Resource(ConfigMaps).Namespace("tenant")
	configurationObject, err := maps.Get(ctx, "test-autoscaler-failover-config", metav1.GetOptions{})
	require.NoError(t, err)
	encoded, _, err := unstructured.NestedString(configurationObject.Object, "data", "config.json")
	require.NoError(t, err)
	configuration := Configuration{}
	require.NoError(t, json.Unmarshal([]byte(encoded), &configuration))
	delete(configuration.Pairs, name)
	data, err := json.Marshal(configuration)
	require.NoError(t, err)
	require.NoError(t, unstructured.SetNestedField(configurationObject.Object, string(data), "data", "config.json"))
	_, err = maps.Update(ctx, configurationObject, metav1.UpdateOptions{})
	require.NoError(t, err)
	delete(state.Pairs, name)
	object, err := maps.Get(ctx, "test-autoscaler-failover-state", metav1.GetOptions{})
	require.NoError(t, err)
	data, err = json.Marshal(state)
	require.NoError(t, err)
	require.NoError(t, unstructured.SetNestedField(object.Object, string(data), "data", "state.json"))
	_, err = maps.Update(ctx, object, metav1.UpdateOptions{})
	require.NoError(t, err)
}

func newActiveLifecycleEnvironment(t *testing.T, identifiers ...string) (*testEnvironment, *clocktesting.FakeClock) {
	t.Helper()
	environment, clock := newFailoverTestController(t)
	provider := &testProvider{controller: environment}
	setPairLifecycleIntent(t, environment, "lin", "Creating", "registration-lin", false)
	require.NoError(t, provider.Refresh())
	for _, identifier := range identifiers {
		setPairLifecycleIntent(t, environment, identifier, "Active", "registration-"+identifier, false)
	}
	require.NoError(t, provider.Refresh())
	return environment, clock
}

func findLifecycleGroup(t *testing.T, environment *testEnvironment, name string) *testGroup {
	t.Helper()
	groups, err := environment.NodeGroups()
	require.NoError(t, err)
	for _, candidate := range groups {
		group := candidate.(*testGroup)
		if group.object.GetName() == name {
			return group
		}
	}
	t.Fatalf("lifecycle fixture group %s is missing", name)
	return nil
}
