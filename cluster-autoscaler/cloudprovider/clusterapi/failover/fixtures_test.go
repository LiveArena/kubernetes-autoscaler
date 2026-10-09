package failover

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/client-go/dynamic"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	fakescale "k8s.io/client-go/scale/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	clocktesting "k8s.io/utils/clock/testing"
)

func newFailoverTestController(t *testing.T) (*testEnvironment, *clocktesting.FakeClock) {
	t.Helper()
	ctx := context.Background()
	resource := schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "machinepools"}
	client := fakedynamic.NewSimpleDynamicClient(runtime.NewScheme())
	environment := &testEnvironment{managementClient: client, machinePoolResource: resource, machinePoolInformer: &testInformer{store: cache.NewIndexer(cache.MetaNamespaceKeyFunc, nil)}, nodeInformer: &testInformer{store: cache.NewIndexer(cache.MetaNamespaceKeyFunc, nil)}}
	cluster := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "cluster.x-k8s.io/v1beta2", "kind": "Cluster"}}
	cluster.SetName("test")
	cluster.SetNamespace("tenant")
	cluster.SetUID("cluster-uid")
	_, err := client.Resource(schema.GroupVersionResource{Group: resource.Group, Version: resource.Version, Resource: "clusters"}).Namespace("tenant").Create(ctx, cluster, metav1.CreateOptions{})
	require.NoError(t, err)
	for _, pair := range []string{"lin", "win2"} {
		for _, role := range []string{"primary", "secondary"} {
			name := pair + "-" + role
			target, minimum := int64(0), "0"
			ids := []string{}
			if role == "primary" {
				target, minimum = 3, "1"
				ids = append(ids, "azure://"+name)
				node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name)}, Spec: corev1.NodeSpec{ProviderID: ids[0]}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
				require.NoError(t, environment.nodeInformer.store.Add(node))
			}
			pool := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "cluster.x-k8s.io/v1beta2", "kind": MachinePoolKind, "spec": map[string]interface{}{"replicas": target, "clusterName": "test", "providerIDList": stringSlice(ids)}}}
			pool.SetName(name)
			pool.SetNamespace("tenant")
			pool.SetUID(types.UID(name))
			pool.SetResourceVersion("1")
			pool.SetAnnotations(map[string]string{PairKey: pair, RoleKey: role, nodeGroupMinSizeAnnotationKey: minimum, MaxSizeAnnotationKey: "4"})
			pool.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: cluster.GetAPIVersion(), Kind: "Cluster", Name: cluster.GetName(), UID: cluster.GetUID()}})
			require.NoError(t, unstructured.SetNestedStringMap(pool.Object, map[string]string{"apiVersion": AzureAPIGroup + "/v1beta1", "kind": "AzureMachinePool", "name": name}, "spec", "template", "spec", "infrastructureRef"))
			_, err = client.Resource(resource).Namespace("tenant").Create(ctx, pool, metav1.CreateOptions{})
			require.NoError(t, err)
			require.NoError(t, environment.machinePoolInformer.store.Add(pool))
			amp := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": AzureAPIGroup + "/v1beta1", "kind": "AzureMachinePool"}}
			amp.SetName(name)
			amp.SetNamespace("tenant")
			amp.SetUID(types.UID(name + "-amp"))
			amp.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: pool.GetAPIVersion(), Kind: MachinePoolKind, Name: name, UID: pool.GetUID()}})
			_, err = client.Resource(schema.GroupVersionResource{Group: AzureAPIGroup, Version: "v1beta1", Resource: "azuremachinepools"}).Namespace("tenant").Create(ctx, amp, metav1.CreateOptions{})
			require.NoError(t, err)
		}
	}
	environment.managementScaleClient = newTestScaleClient(t, environment)
	writeFailoverTestConfiguration(t, client, cluster)
	require.NoError(t, environment.enableAzureFailover(false, 10*time.Second, config.NodeGroupAutoscalingOptions{MaxNodeProvisionTime: 45 * time.Minute}))
	clock := clocktesting.NewFakeClock(time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC))
	environment.failover.Clock = clock
	return environment, clock
}

func stringSlice(values []string) []interface{} {
	result := make([]interface{}, 0, len(values))
	for _, value := range values {
		result = append(result, value)
	}
	return result
}

func writeFailoverTestConfiguration(t *testing.T, client dynamic.Interface, cluster *unstructured.Unstructured) {
	t.Helper()
	configuration := Configuration{APIVersion: ConfigVersion, Cluster: ClusterIdentity{Name: cluster.GetName(), Namespace: cluster.GetNamespace(), UID: cluster.GetUID()}, Pairs: map[string]ConfiguredPair{"lin": {Primary: ConfiguredPool{Name: "lin-primary"}, Secondary: ConfiguredPool{Name: "lin-secondary"}}, "win2": {Primary: ConfiguredPool{Name: "win2-primary"}, Secondary: ConfiguredPool{Name: "win2-secondary"}}}}
	data, err := json.Marshal(configuration)
	require.NoError(t, err)
	object := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap", "data": map[string]interface{}{"config.json": string(data)}}}
	object.SetName(cluster.GetName() + "-autoscaler-failover-config")
	object.SetNamespace(cluster.GetNamespace())
	object.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: cluster.GetAPIVersion(), Kind: "Cluster", Name: cluster.GetName(), UID: cluster.GetUID()}})
	_, err = client.Resource(ConfigMaps).Namespace(cluster.GetNamespace()).Create(context.Background(), object, metav1.CreateOptions{})
	require.NoError(t, err)
}

func failoverTestFailure(environment *testEnvironment, pair, role string, now time.Time) {
	object := &unstructured.Unstructured{Object: map[string]interface{}{"status": map[string]interface{}{"provisioningState": "Failed"}}}
	object.SetNamespace("tenant")
	object.SetUID(types.UID(pair + "-" + role + "-failure"))
	object.SetCreationTimestamp(metav1.NewTime(now))
	object.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: AzureAPIGroup + "/v1beta1", Kind: "AzureMachinePool", Name: pair + "-" + role, UID: types.UID(pair + "-" + role + "-amp")}})
	environment.failover.Observe(object)
}

func newTestScaleClient(t *testing.T, environment *testEnvironment) *fakescale.FakeScaleClient {
	client := &fakescale.FakeScaleClient{}
	client.AddReactor("*", "machinepools", func(action clienttesting.Action) (bool, runtime.Object, error) {
		name := ""
		var requested *autoscalingv1.Scale
		switch typed := action.(type) {
		case clienttesting.GetAction:
			name = typed.GetName()
		case clienttesting.UpdateAction:
			requested = typed.GetObject().(*autoscalingv1.Scale)
			name = requested.Name
		default:
			return true, nil, unexpectedTestScale(action.GetVerb())
		}
		resource := environment.managementClient.Resource(environment.machinePoolResource).Namespace(action.GetNamespace())
		pool, err := resource.Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			return true, nil, err
		}
		target, _, _ := unstructured.NestedInt64(pool.Object, "spec", "replicas")
		if requested != nil {
			if requested.ResourceVersion != pool.GetResourceVersion() {
				return true, nil, apierrors.NewConflict(environment.machinePoolResource.GroupResource(), name, unexpectedTestScale("stale scale"))
			}
			target = int64(requested.Spec.Replicas)
			require.NoError(t, unstructured.SetNestedField(pool.Object, target, "spec", "replicas"))
			version, _ := strconv.Atoi(pool.GetResourceVersion())
			pool.SetResourceVersion(strconv.Itoa(version + 1))
			_, err = resource.Update(context.Background(), pool, metav1.UpdateOptions{})
			if err != nil {
				return true, nil, err
			}
		}
		return true, &autoscalingv1.Scale{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: action.GetNamespace(), ResourceVersion: pool.GetResourceVersion()}, Spec: autoscalingv1.ScaleSpec{Replicas: int32(target)}}, nil
	})
	return client
}
