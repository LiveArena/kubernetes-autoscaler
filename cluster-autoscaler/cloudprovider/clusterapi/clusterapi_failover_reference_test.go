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

package clusterapi

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/clusterapi/failover"
	fakediscovery "k8s.io/client-go/discovery/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestActualProviderRegistersVersionlessInfrastructureReferencesUsingDiscovery(t *testing.T) {
	controller, _ := newFailoverTestController(t)
	controller.managementDiscoveryClient = &fakediscovery.FakeDiscovery{Fake: &clienttesting.Fake{Resources: []*metav1.APIResourceList{{GroupVersion: failover.AzureAPIGroup + "/v1beta1", APIResources: []metav1.APIResource{{Name: "azuremachinepools", Kind: "AzureMachinePool", Namespaced: true}}}}}}
	ctx := context.Background()
	pools := controller.managementClient.Resource(controller.machinePoolResource).Namespace("tenant")
	for _, candidate := range (&provider{controller: controller}).NodeGroups() {
		pool, err := pools.Get(ctx, candidate.(*nodegroup).Object().GetName(), metav1.GetOptions{})
		require.NoError(t, err)
		ref, _, err := unstructured.NestedStringMap(pool.Object, "spec", "template", "spec", "infrastructureRef")
		require.NoError(t, err)
		delete(ref, "apiVersion")
		ref["apiGroup"] = failover.AzureAPIGroup
		require.NoError(t, unstructured.SetNestedStringMap(pool.Object, ref, "spec", "template", "spec", "infrastructureRef"))
		_, err = pools.Update(ctx, pool, metav1.UpdateOptions{})
		require.NoError(t, err)
		require.NoError(t, controller.machinePoolInformer.Informer().GetStore().Update(pool))
	}
	maps := controller.managementClient.Resource(failover.ConfigMaps).Namespace("tenant")
	setIntent := func(desired string) {
		object, err := maps.Get(ctx, "test-autoscaler-failover-config", metav1.GetOptions{})
		require.NoError(t, err)
		encoded, _, err := unstructured.NestedString(object.Object, "data", "config.json")
		require.NoError(t, err)
		configuration := failover.Configuration{}
		require.NoError(t, json.Unmarshal([]byte(encoded), &configuration))
		configuration.APIVersion = failover.LifecycleConfigVersion
		for name, pair := range configuration.Pairs {
			pair.Intent = &failover.PairIntent{State: desired, RegistrationID: "registration-" + name}
			configuration.Pairs[name] = pair
		}
		payload, err := json.Marshal(configuration)
		require.NoError(t, err)
		require.NoError(t, unstructured.SetNestedField(object.Object, string(payload), "data", "config.json"))
		_, err = maps.Update(ctx, object, metav1.UpdateOptions{})
		require.NoError(t, err)
	}
	setIntent("Creating")
	provider := &provider{controller: controller}
	require.NoError(t, provider.Refresh())
	for _, pair := range []string{"lin", "win2"} {
		fixture := &failoverOrchestratorFixture{t: t, pair: pair, controller: controller}
		state := fixture.pairState()
		require.Equal(t, "Creating", state.Lifecycle.State)
		assert.Equal(t, failover.AzureAPIGroup+"/v1beta1", state.Primary.InfrastructureVersion)
	}
	setIntent("Active")
	require.NoError(t, provider.Refresh())
	for _, candidate := range provider.NodeGroups() {
		group := candidate.(*nodegroup)
		if group.Object().GetName() == "lin-primary" {
			pool, err := pools.Get(ctx, group.Object().GetName(), metav1.GetOptions{})
			require.NoError(t, err)
			require.NoError(t, unstructured.SetNestedField(pool.Object, "other.infrastructure.example", "spec", "template", "spec", "infrastructureRef", "apiGroup"))
			_, err = pools.Update(ctx, pool, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.Error(t, group.IncreaseSize(1))
			require.NoError(t, unstructured.SetNestedField(pool.Object, failover.AzureAPIGroup, "spec", "template", "spec", "infrastructureRef", "apiGroup"))
			_, err = pools.Update(ctx, pool, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.NoError(t, group.IncreaseSize(1))
		}
	}
}
