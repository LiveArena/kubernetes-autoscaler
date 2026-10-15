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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/clusterapi/failover"
	"k8s.io/client-go/discovery"
	fakediscovery "k8s.io/client-go/discovery/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestFailoverDiscoveryCachesRoutesUntilExplicitRefresh(t *testing.T) {
	client := &fakediscovery.FakeDiscovery{Fake: &clienttesting.Fake{Resources: []*metav1.APIResourceList{{GroupVersion: failover.AzureAPIGroup + "/v1beta1", APIResources: []metav1.APIResource{{Name: "azuremachinepools", Kind: "AzureMachinePool", Namespaced: true}}}}}}
	resolver := &failoverResourceResolver{discovery: func() discovery.DiscoveryInterface { return client }}
	kind := schema.GroupKind{Group: failover.AzureAPIGroup, Kind: "AzureMachinePool"}
	first, err := resolver.ResolveResource(kind, "", false)
	require.NoError(t, err)
	calls := len(client.Actions())
	second, err := resolver.ResolveResource(kind, "", false)
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.Len(t, client.Actions(), calls, "known routes must not rediscover on each read")
	client.Resources[0].GroupVersion = failover.AzureAPIGroup + "/v1beta2"
	current, err := resolver.ResolveResource(kind, "v1beta1", true)
	require.NoError(t, err)
	assert.Equal(t, "v1beta2", current.Version)
	assert.Greater(t, len(client.Actions()), calls)
}

func TestFailoverDiscoveryRefreshesOnceWhenACachedKindIsMissing(t *testing.T) {
	client := &fakediscovery.FakeDiscovery{Fake: &clienttesting.Fake{Resources: []*metav1.APIResourceList{{GroupVersion: "cluster.x-k8s.io/v1beta2", APIResources: []metav1.APIResource{{Name: "machinepools", Kind: "MachinePool", Namespaced: true}}}}}}
	resolver := &failoverResourceResolver{discovery: func() discovery.DiscoveryInterface { return client }}
	_, err := resolver.ResolveResource(schema.GroupKind{Group: "cluster.x-k8s.io", Kind: "MachinePool"}, "", false)
	require.NoError(t, err)
	client.Resources = append(client.Resources, &metav1.APIResourceList{GroupVersion: failover.AzureAPIGroup + "/v1beta2", APIResources: []metav1.APIResource{{Name: "azuremachinepools", Kind: "AzureMachinePool", Namespaced: true}}})
	route, err := resolver.ResolveResource(schema.GroupKind{Group: failover.AzureAPIGroup, Kind: "AzureMachinePool"}, "", false)
	require.NoError(t, err, "a stale discovery snapshot must not permanently hide a newly served kind")
	assert.Equal(t, "v1beta2", route.Version)
}
