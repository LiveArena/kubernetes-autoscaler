package failover

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/scale"
	clocktesting "k8s.io/utils/clock/testing"
	"os"
	"testing"
	"time"
)

func failoverTestCRD(plural, kind string, withScale bool) *unstructured.Unstructured {
	version := map[string]interface{}{"name": "v1beta2", "served": true, "storage": true, "schema": map[string]interface{}{"openAPIV3Schema": map[string]interface{}{"type": "object", "properties": map[string]interface{}{"spec": map[string]interface{}{"type": "object", "x-kubernetes-preserve-unknown-fields": true, "properties": map[string]interface{}{"replicas": map[string]interface{}{"type": "integer", "format": "int32", "minimum": int64(0)}}}, "status": map[string]interface{}{"type": "object", "properties": map[string]interface{}{"replicas": map[string]interface{}{"type": "integer", "format": "int32"}}}}}}}
	if withScale {
		version["subresources"] = map[string]interface{}{"scale": map[string]interface{}{"specReplicasPath": ".spec.replicas", "statusReplicasPath": ".status.replicas"}}
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition", "metadata": map[string]interface{}{"name": plural + ".cluster.x-k8s.io"}, "spec": map[string]interface{}{"group": "cluster.x-k8s.io", "scope": "Namespaced", "names": map[string]interface{}{"plural": plural, "singular": plural[:len(plural)-1], "kind": kind, "listKind": kind + "List"}, "versions": []interface{}{version}}}}
}
func TestDisposableAPIEnforcesScopedPermissionsStateCASAndNonreplayedScaleWrites(t *testing.T) {
	endpoint := os.Getenv("INF761_TEST_API")
	if endpoint == "" || os.Getenv("INF761_TEST_API_DISPOSABLE") != "1" {
		t.Skip("requires an explicitly authorized disposable API via INF761_TEST_API and INF761_TEST_API_DISPOSABLE=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	configuration := &rest.Config{Host: endpoint, BearerToken: "inf761-admin", TLSClientConfig: rest.TLSClientConfig{Insecure: true}, Timeout: 10 * time.Second}
	client, err := dynamic.NewForConfig(configuration)
	require.NoError(t, err)
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(configuration)
	require.NoError(t, err)
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, time.Minute, true, func(ctx context.Context) (bool, error) {
		_, err := discoveryClient.ServerVersion()
		return err == nil, nil
	}))
	crds := client.Resource(schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"})
	infrastructureDefinition := failoverTestCRD("azuremachinepools", "AzureMachinePool", false)
	infrastructureDefinition.SetName("azuremachinepools." + azureMachinePoolMachineApiGroup)
	require.NoError(t, unstructured.SetNestedField(infrastructureDefinition.Object, azureMachinePoolMachineApiGroup, "spec", "group"))
	for _, definition := range []*unstructured.Unstructured{failoverTestCRD("clusters", "Cluster", false), failoverTestCRD("machinepools", "MachinePool", true), infrastructureDefinition} {
		_, err := crds.Create(ctx, definition, metav1.CreateOptions{})
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = crds.Delete(context.Background(), definition.GetName(), metav1.DeleteOptions{})
		})
	}
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		resources, err := discoveryClient.ServerResourcesForGroupVersion("cluster.x-k8s.io/v1beta2")
		if err != nil {
			return false, nil
		}
		for _, resource := range resources.APIResources {
			if resource.Name == "machinepools/scale" {
				return true, nil
			}
		}
		return false, nil
	}))
	namespaces := client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "namespaces"})
	namespace, err := namespaces.Create(ctx, &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]interface{}{"generateName": "inf761-"}}}, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = namespaces.Delete(context.Background(), namespace.GetName(), metav1.DeleteOptions{})
	})
	cluster := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "cluster.x-k8s.io/v1beta2", "kind": "Cluster", "metadata": map[string]interface{}{"name": "test", "namespace": namespace.GetName()}}}
	clusterResource := schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "clusters"}
	cluster, err = client.Resource(clusterResource).Namespace(namespace.GetName()).Create(ctx, cluster, metav1.CreateOptions{})
	require.NoError(t, err)
	store := &Store{Client: client}
	state, err := store.Reconcile(ctx, cluster, func(state *State) error {
		state.Pairs["lin"] = &Pair{Phase: Degraded, Primary: Role{Failed: true, ObservedTarget: 3}, FallbackAllowance: 2}
		return nil
	})
	require.NoError(t, err)
	loaded, object, err := (&Store{Client: client}).Load(ctx, cluster)
	require.NoError(t, err)
	assert.Equal(t, state, loaded)
	require.NotEmpty(t, object.GetResourceVersion())
	configMaps := client.Resource(ConfigMaps).Namespace(namespace.GetName())
	changed := object.DeepCopy()
	changed.SetAnnotations(map[string]string{"test": "concurrent"})
	_, err = configMaps.Update(ctx, changed, metav1.UpdateOptions{})
	require.NoError(t, err)
	_, err = configMaps.Update(ctx, object, metav1.UpdateOptions{})
	require.True(t, apierrors.IsConflict(err), "stale real resourceVersion must conflict: %v", err)
	attempts := 0
	_, err = store.Reconcile(ctx, cluster, func(state *State) error {
		attempts++
		if attempts == 1 {
			concurrent, err := configMaps.Get(ctx, object.GetName(), metav1.GetOptions{})
			if err != nil {
				return err
			}
			concurrent.SetAnnotations(map[string]string{"test": "force-cas-conflict"})
			if _, err := configMaps.Update(ctx, concurrent, metav1.UpdateOptions{}); err != nil {
				return err
			}
		}
		state.Pairs["lin"].Secondary.ObservedTarget = 2
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, attempts)
	deniedConfiguration := rest.CopyConfig(configuration)
	deniedConfiguration.BearerToken = "inf761-denied"
	deniedClient, err := dynamic.NewForConfig(deniedConfiguration)
	require.NoError(t, err)
	_, _, err = (&Store{Client: deniedClient}).Load(ctx, cluster)
	require.True(t, apierrors.IsForbidden(err), "real RBAC must deny reads: %v", err)
	_, err = deniedClient.Resource(ConfigMaps).Namespace(namespace.GetName()).Update(ctx, changed, metav1.UpdateOptions{})
	require.True(t, apierrors.IsForbidden(err), "real RBAC must deny writes: %v", err)
	rules := []interface{}{map[string]interface{}{"apiGroups": []interface{}{""}, "resources": []interface{}{"configmaps"}, "resourceNames": []interface{}{"test-autoscaler-failover-config"}, "verbs": []interface{}{"get", "list", "watch"}}, map[string]interface{}{"apiGroups": []interface{}{""}, "resources": []interface{}{"configmaps"}, "resourceNames": []interface{}{"test-autoscaler-failover-state"}, "verbs": []interface{}{"get", "list", "watch", "update", "patch"}}, map[string]interface{}{"apiGroups": []interface{}{""}, "resources": []interface{}{"configmaps"}, "verbs": []interface{}{"create"}}, map[string]interface{}{"apiGroups": []interface{}{"cluster.x-k8s.io"}, "resources": []interface{}{"clusters", "machinepools"}, "verbs": []interface{}{"get", "list", "watch"}}, map[string]interface{}{"apiGroups": []interface{}{"cluster.x-k8s.io"}, "resources": []interface{}{"machinepools/scale"}, "verbs": []interface{}{"get", "update", "patch"}}, map[string]interface{}{"apiGroups": []interface{}{azureMachinePoolMachineApiGroup}, "resources": []interface{}{"azuremachinepools"}, "verbs": []interface{}{"get", "list", "watch"}}}
	role := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": map[string]interface{}{"name": "failover-scoped", "namespace": namespace.GetName()}, "rules": rules}}
	_, err = client.Resource(schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}).Namespace(namespace.GetName()).Create(ctx, role, metav1.CreateOptions{})
	require.NoError(t, err)
	binding := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": map[string]interface{}{"name": "failover-scoped", "namespace": namespace.GetName()}, "roleRef": map[string]interface{}{"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "failover-scoped"}, "subjects": []interface{}{map[string]interface{}{"apiGroup": "rbac.authorization.k8s.io", "kind": "User", "name": "inf761-scoped"}}}}
	_, err = client.Resource(schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}).Namespace(namespace.GetName()).Create(ctx, binding, metav1.CreateOptions{})
	require.NoError(t, err)
	scopedConfiguration := rest.CopyConfig(configuration)
	scopedConfiguration.BearerToken = "inf761-scoped"
	scopedClient, err := dynamic.NewForConfig(scopedConfiguration)
	require.NoError(t, err)
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		_, _, err := (&Store{Client: scopedClient}).Load(ctx, cluster)
		return err == nil, nil
	}))
	_, err = scopedClient.Resource(ConfigMaps).Namespace(namespace.GetName()).Get(ctx, "unrelated", metav1.GetOptions{})
	require.True(t, apierrors.IsForbidden(err), "name-scoped role must not read unrelated configmaps: %v", err)
	_, err = scopedClient.Resource(schema.GroupVersionResource{Group: "coordination.k8s.io", Version: "v1", Resource: "leases"}).Namespace(namespace.GetName()).Get(ctx, "test-autoscaler-failover", metav1.GetOptions{})
	require.True(t, apierrors.IsForbidden(err), "no management Lease permission is needed: %v", err)
	store = &Store{Client: scopedClient}
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	poolResource := schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "machinepools"}
	pool := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "cluster.x-k8s.io/v1beta2", "kind": "MachinePool", "metadata": map[string]interface{}{"name": "lin-secondary", "namespace": namespace.GetName()}, "spec": map[string]interface{}{"replicas": int64(0), "clusterName": "test"}, "status": map[string]interface{}{"replicas": int64(0)}}}
	pool.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: cluster.GetAPIVersion(), Kind: "Cluster", Name: cluster.GetName(), UID: cluster.GetUID()}})
	pool.SetAnnotations(map[string]string{nodeGroupMaxSizeAnnotationKey: "4", PairKey: "lin", RoleKey: "secondary"})
	writeFailoverTestConfiguration(t, client, cluster)
	_, err = ReadConfiguration(ctx, scopedClient, cluster)
	require.NoError(t, err)
	configObject, err := client.Resource(ConfigMaps).Namespace(namespace.GetName()).Get(ctx, "test-autoscaler-failover-config", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = scopedClient.Resource(ConfigMaps).Namespace(namespace.GetName()).Update(ctx, configObject, metav1.UpdateOptions{})
	require.True(t, apierrors.IsForbidden(err), "configuration must remain read-only: %v", err)
	err = scopedClient.Resource(ConfigMaps).Namespace(namespace.GetName()).Delete(ctx, "test-autoscaler-failover-state", metav1.DeleteOptions{})
	require.True(t, apierrors.IsForbidden(err), "state must not require delete permission: %v", err)
	require.NoError(t, unstructured.SetNestedStringMap(pool.Object, map[string]string{"apiVersion": azureMachinePoolMachineApiGroup + "/v1beta2", "kind": "AzureMachinePool", "name": "secondary"}, "spec", "template", "spec", "infrastructureRef"))
	pool, err = client.Resource(poolResource).Namespace(namespace.GetName()).Create(ctx, pool, metav1.CreateOptions{})
	require.NoError(t, err)
	for _, reference := range []struct{ name, pair, role string }{{"lin-primary", "lin", "primary"}, {"win2-primary", "win2", "primary"}, {"win2-secondary", "win2", "secondary"}} {
		other := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "cluster.x-k8s.io/v1beta2", "kind": "MachinePool", "spec": map[string]interface{}{"replicas": int64(0), "clusterName": "test"}, "status": map[string]interface{}{"replicas": int64(0)}}}
		other.SetName(reference.name)
		other.SetNamespace(namespace.GetName())
		other.SetAnnotations(map[string]string{PairKey: reference.pair, RoleKey: reference.role, nodeGroupMaxSizeAnnotationKey: "4"})
		other.SetOwnerReferences(pool.GetOwnerReferences())
		_, err := client.Resource(poolResource).Namespace(namespace.GetName()).Create(ctx, other, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	infrastructure := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": azureMachinePoolMachineApiGroup + "/v1beta2", "kind": "AzureMachinePool", "metadata": map[string]interface{}{"name": "secondary", "namespace": namespace.GetName()}}}
	infrastructure.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: pool.GetAPIVersion(), Kind: machinePoolKind, Name: pool.GetName(), UID: pool.GetUID()}})
	infrastructureResource := client.Resource(schema.GroupVersionResource{Group: azureMachinePoolMachineApiGroup, Version: "v1beta2", Resource: "azuremachinepools"}).Namespace(namespace.GetName())
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := infrastructureResource.Create(ctx, infrastructure, metav1.CreateOptions{})
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return err == nil, err
	}))
	scaleClient, err := scale.NewForConfig(scopedConfiguration, restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(discoveryClient)), dynamic.LegacyAPIPathResolverFunc, scale.NewDiscoveryScaleKindResolver(discoveryClient))
	require.NoError(t, err)
	clock := clocktesting.NewFakeClock(now)
	controller := &testEnvironment{managementClient: scopedClient, managementScaleClient: scaleClient, machinePoolResource: poolResource}
	group, err := newTestGroup(controller, pool)
	require.NoError(t, err)
	target := 0
	controller.failover = &Policy{Environment: controller, Store: store, Clock: clock, Policies: map[string]cloudprovider.NodeGroupCapacityPolicy{group.Id(): {ExpectedTarget: &target}}}
	require.NoError(t, group.IncreaseSize(2))
	require.Error(t, group.IncreaseSize(2), "same admission cannot replay the scale write")
	actual, err := group.TargetSize()
	require.NoError(t, err)
	assert.Equal(t, 2, actual)
	loaded, _, err = store.Load(ctx, cluster)
	require.NoError(t, err)
	require.NotNil(t, loaded.Pairs["lin"].SecondaryRequest)
	assert.Equal(t, 2, loaded.Pairs["lin"].SecondaryRequest.ToTarget)
	_, err = store.Reconcile(ctx, cluster, func(state *State) error {
		return state.Pairs["lin"].ReconcileRequestTargets(3, actual)
	})
	require.NoError(t, err)
	previousWriter := controller.failover
	previousWriter.StopWriter()
	target = 2
	require.Error(t, group.IncreaseSize(1), "stopped former writer cannot issue a scale update")
	controller.failover = &Policy{Environment: controller, Store: store, Clock: clock, Policies: map[string]cloudprovider.NodeGroupCapacityPolicy{group.Id(): {ExpectedTarget: &target}}}
	loaded, _, err = (&Store{Client: client}).Load(ctx, cluster)
	require.NoError(t, err)
	assert.True(t, loaded.Pairs["lin"].Primary.Failed)
	assert.Equal(t, 2, loaded.Pairs["lin"].Secondary.ObservedTarget)
	controller.failover.Policies[group.Id()] = cloudprovider.NodeGroupCapacityPolicy{ExpectedTarget: &target, ScaleUpBlocked: true, Reason: "no residual demand after reload"}
	require.Error(t, group.IncreaseSize(1))
	actual, err = group.TargetSize()
	require.NoError(t, err)
	assert.Equal(t, 2, actual)
	replacement := cluster.DeepCopy()
	replacement.SetUID("different-cluster")
	_, _, err = store.Load(ctx, replacement)
	require.Error(t, err)
	t.Logf("real API CAS/reload/RBAC/lease/scale contract passed in isolated namespace %s", namespace.GetName())
	assert.NoError(t, ctx.Err(), fmt.Sprintf("API test exceeded its bounded deadline: %v", ctx.Err()))
}
