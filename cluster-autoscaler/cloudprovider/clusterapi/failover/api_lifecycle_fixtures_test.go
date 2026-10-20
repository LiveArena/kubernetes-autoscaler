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
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/scale"
	"k8s.io/client-go/tools/cache"
)

type lifecycleAPIFixture struct {
	api            *disposableAPIFixture
	environment    *testEnvironment
	configuration  Configuration
	groups         map[string]*testGroup
	infrastructure map[string]*unstructured.Unstructured
	scoped         *rest.Config
}

func newLifecycleAPIFixture(t *testing.T) *lifecycleAPIFixture {
	t.Helper()
	api := newDisposableAPIFixture(t)
	rules := []interface{}{
		map[string]interface{}{"apiGroups": []interface{}{""}, "resources": []interface{}{"configmaps"}, "resourceNames": []interface{}{"test-autoscaler-failover-config"}, "verbs": []interface{}{"get"}},
		map[string]interface{}{"apiGroups": []interface{}{""}, "resources": []interface{}{"configmaps"}, "resourceNames": []interface{}{"test-autoscaler-failover-state"}, "verbs": []interface{}{"get", "update", "patch"}},
		map[string]interface{}{"apiGroups": []interface{}{""}, "resources": []interface{}{"configmaps"}, "verbs": []interface{}{"create"}},
		map[string]interface{}{"apiGroups": []interface{}{"cluster.x-k8s.io"}, "resources": []interface{}{"machinepools"}, "verbs": []interface{}{"get"}},
		map[string]interface{}{"apiGroups": []interface{}{"cluster.x-k8s.io"}, "resources": []interface{}{"machinepools/scale"}, "verbs": []interface{}{"get", "update", "patch"}},
		map[string]interface{}{"apiGroups": []interface{}{AzureAPIGroup}, "resources": []interface{}{"azuremachinepools"}, "verbs": []interface{}{"get"}},
	}
	role := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": map[string]interface{}{"name": "lifecycle-scoped", "namespace": api.namespace.GetName()}, "rules": rules}}
	_, err := api.client.Resource(schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}).Namespace(api.namespace.GetName()).Create(api.context, role, metav1.CreateOptions{})
	require.NoError(t, err)
	binding := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": map[string]interface{}{"name": "lifecycle-scoped", "namespace": api.namespace.GetName()}, "roleRef": map[string]interface{}{"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": role.GetName()}, "subjects": []interface{}{map[string]interface{}{"apiGroup": "rbac.authorization.k8s.io", "kind": "User", "name": "inf761-scoped"}}}}
	_, err = api.client.Resource(schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}).Namespace(api.namespace.GetName()).Create(api.context, binding, metav1.CreateOptions{})
	require.NoError(t, err)
	scoped := rest.CopyConfig(api.configuration)
	scoped.BearerToken = "inf761-scoped"
	client, err := dynamic.NewForConfig(scoped)
	require.NoError(t, err)
	scaleClient, err := scale.NewForConfig(scoped, restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(api.discovery)), dynamic.LegacyAPIPathResolverFunc, scale.NewDiscoveryScaleKindResolver(api.discovery))
	require.NoError(t, err)
	environment := &testEnvironment{managementClient: client, managementScaleClient: scaleClient, machinePoolResource: schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "machinepools"}, machinePoolInformer: &testInformer{store: cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})}, nodeInformer: &testInformer{store: cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})}}
	require.NoError(t, environment.enableAzureFailover(false, time.Second))
	t.Cleanup(func() { environment.failover.StopWriter() })
	fixture := &lifecycleAPIFixture{api: api, environment: environment, scoped: scoped, groups: map[string]*testGroup{}, infrastructure: map[string]*unstructured.Unstructured{}, configuration: Configuration{APIVersion: LifecycleConfigVersion, Cluster: ClusterIdentity{Name: api.cluster.GetName(), Namespace: api.cluster.GetNamespace(), UID: api.cluster.GetUID()}, Pairs: map[string]ConfiguredPair{}}}
	fixture.writeConfiguration(t)
	_, err = client.Resource(environment.machinePoolResource.GroupVersion().WithResource("clusters")).Namespace(api.cluster.GetNamespace()).Get(api.context, api.cluster.GetName(), metav1.GetOptions{})
	require.True(t, apierrors.IsForbidden(err), "lifecycle operations must run without Cluster read permission: %v", err)
	return fixture
}

func (fixture *lifecycleAPIFixture) writeConfiguration(t *testing.T) {
	t.Helper()
	maps := fixture.api.client.Resource(ConfigMaps).Namespace(fixture.api.cluster.GetNamespace())
	object, err := maps.Get(fixture.api.context, "test-autoscaler-failover-config", metav1.GetOptions{})
	creating := object == nil
	if creating {
		object = &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap"}}
		object.SetName("test-autoscaler-failover-config")
		object.SetNamespace(fixture.api.cluster.GetNamespace())
		object.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: fixture.api.cluster.GetAPIVersion(), Kind: "Cluster", Name: fixture.api.cluster.GetName(), UID: fixture.api.cluster.GetUID()}})
	} else {
		require.NoError(t, err)
	}
	encoded, err := json.Marshal(fixture.configuration)
	require.NoError(t, err)
	require.NoError(t, unstructured.SetNestedField(object.Object, string(encoded), "data", "config.json"))
	if creating {
		_, err = maps.Create(fixture.api.context, object, metav1.CreateOptions{})
	} else {
		_, err = maps.Update(fixture.api.context, object, metav1.UpdateOptions{})
	}
	require.NoError(t, err)
}

func (fixture *lifecycleAPIFixture) addPair(t *testing.T, name, registration string) {
	t.Helper()
	for _, role := range []string{"primary", "secondary"} {
		poolName := name + "-" + role
		pool := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "cluster.x-k8s.io/v1beta2", "kind": MachinePoolKind, "spec": map[string]interface{}{"clusterName": fixture.api.cluster.GetName(), "replicas": int64(0)}, "status": map[string]interface{}{"replicas": int64(0)}}}
		pool.SetName(poolName)
		pool.SetNamespace(fixture.api.cluster.GetNamespace())
		pool.SetAnnotations(map[string]string{PairKey: name, RoleKey: role, MaxSizeAnnotationKey: "4"})
		pool.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: fixture.api.cluster.GetAPIVersion(), Kind: "Cluster", Name: fixture.api.cluster.GetName(), UID: fixture.api.cluster.GetUID()}})
		require.NoError(t, unstructured.SetNestedStringMap(pool.Object, map[string]string{"apiVersion": AzureAPIGroup + "/v1beta2", "kind": "AzureMachinePool", "name": poolName}, "spec", "template", "spec", "infrastructureRef"))
		created, err := fixture.api.client.Resource(fixture.environment.machinePoolResource).Namespace(pool.GetNamespace()).Create(fixture.api.context, pool, metav1.CreateOptions{})
		require.NoError(t, err)
		infrastructure := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": AzureAPIGroup + "/v1beta2", "kind": "AzureMachinePool"}}
		infrastructure.SetName(poolName)
		infrastructure.SetNamespace(pool.GetNamespace())
		infrastructure.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: created.GetAPIVersion(), Kind: MachinePoolKind, Name: created.GetName(), UID: created.GetUID()}})
		infrastructure, err = fixture.api.client.Resource(schema.GroupVersionResource{Group: AzureAPIGroup, Version: "v1beta2", Resource: "azuremachinepools"}).Namespace(pool.GetNamespace()).Create(fixture.api.context, infrastructure, metav1.CreateOptions{})
		require.NoError(t, err)
		fixture.infrastructure[poolName] = infrastructure
		require.NoError(t, fixture.environment.machinePoolInformer.store.Add(created))
		fixture.groups[poolName], err = newTestGroup(fixture.environment, created)
		require.NoError(t, err)
	}
	fixture.configuration.Pairs[name] = ConfiguredPair{Primary: ConfiguredPool{Name: name + "-primary"}, Secondary: ConfiguredPool{Name: name + "-secondary"}, Intent: &PairIntent{State: "Creating", RegistrationID: registration}}
	fixture.writeConfiguration(t)
}

func (fixture *lifecycleAPIFixture) intent(t *testing.T, name, state string, complete bool) {
	t.Helper()
	pair := fixture.configuration.Pairs[name]
	pair.Intent.State, pair.Intent.ResourceRetirementComplete = state, complete
	fixture.configuration.Pairs[name] = pair
	fixture.writeConfiguration(t)
}

func (fixture *lifecycleAPIFixture) state(t *testing.T) *State {
	t.Helper()
	state, _, err := fixture.environment.failover.Store.Load(fixture.api.context, fixture.api.cluster)
	require.NoError(t, err)
	return state
}

func (fixture *lifecycleAPIFixture) retireResources(t *testing.T, name string) {
	t.Helper()
	for _, role := range []string{"primary", "secondary"} {
		poolName := name + "-" + role
		require.NoError(t, fixture.api.client.Resource(fixture.environment.machinePoolResource).Namespace(fixture.api.cluster.GetNamespace()).Delete(fixture.api.context, poolName, metav1.DeleteOptions{}))
		require.NoError(t, fixture.api.client.Resource(schema.GroupVersionResource{Group: AzureAPIGroup, Version: "v1beta2", Resource: "azuremachinepools"}).Namespace(fixture.api.cluster.GetNamespace()).Delete(fixture.api.context, poolName, metav1.DeleteOptions{}))
		require.NoError(t, fixture.environment.machinePoolInformer.store.Delete(fixture.groups[poolName].object))
	}
}
