package clusterapi

import "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/clusterapi/failover"

import (
	"context"
	"encoding/json"
	"fmt"
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
	"k8s.io/client-go/dynamic/dynamicinformer"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/informers"
	fakekube "k8s.io/client-go/kubernetes/fake"
	fakescale "k8s.io/client-go/scale/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	clocktesting "k8s.io/utils/clock/testing"
	"strconv"
	"testing"
	"time"
)

func newFailoverTestController(t *testing.T) (*machineController, *clocktesting.FakeClock) {
	t.Helper()
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	client := fakedynamic.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "machines"}: "MachineList", {Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "machinepools"}: "MachinePoolList", {Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "machinesets"}: "MachineSetList", {Group: azureMachinePoolMachineApiGroup, Version: "v1beta1", Resource: "azuremachinepools"}: "AzureMachinePoolList", {Group: azureMachinePoolMachineApiGroup, Version: "v1beta1", Resource: resourceNameAzureMachinePoolMachine}: "AzureMachinePoolMachineList"})
	stop := make(chan struct{})
	t.Cleanup(func() {
		close(stop)
	})
	cluster := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "cluster.x-k8s.io/v1beta2", "kind": "Cluster"}}
	cluster.SetName("test")
	cluster.SetNamespace("tenant")
	cluster.SetUID("cluster-uid")
	_, err := client.Resource(schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "clusters"}).Namespace("tenant").Create(context.Background(), cluster, metav1.CreateOptions{})
	require.NoError(t, err)
	factory := dynamicinformer.NewDynamicSharedInformerFactory(client, 0)
	poolResource := schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "machinepools"}
	nodeInformer := informers.NewSharedInformerFactory(fakekube.NewSimpleClientset(), 0).Core().V1().Nodes().Informer()
	require.NoError(t, nodeInformer.AddIndexers(cache.Indexers{nodeProviderIDIndex: indexNodeByProviderID}))
	controller := &machineController{managementClient: client, managementInformerFactory: factory, nodeInformer: nodeInformer, stopChannel: stop, machinePoolResource: poolResource, machinePoolsAvailable: true, machinePoolInformer: factory.ForResource(poolResource), machineInformer: factory.ForResource(schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "machines"}), machineSetInformer: factory.ForResource(schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "machinesets"})}
	require.NoError(t, controller.machinePoolInformer.Informer().GetIndexer().AddIndexers(cache.Indexers{machinePoolProviderIDIndex: indexMachinePoolByProviderID}))
	require.NoError(t, controller.machineInformer.Informer().GetIndexer().AddIndexers(cache.Indexers{machineProviderIDIndex: indexMachineByProviderID}))
	scaleClient := &fakescale.FakeScaleClient{}
	scaleClient.AddReactor("*", "machinepools", func(action clienttesting.Action) (bool, runtime.Object, error) {
		name, namespace := "", action.GetNamespace()
		var requested *autoscalingv1.Scale
		switch action := action.(type) {
		case clienttesting.GetAction:
			name = action.GetName()
		case clienttesting.UpdateAction:
			requested = action.GetObject().(*autoscalingv1.Scale)
			name, namespace = requested.Name, requested.Namespace
		default:
			return true, nil, fmt.Errorf("unexpected scale action %s", action.GetVerb())
		}
		pool, err := client.Resource(poolResource).Namespace(namespace).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			return true, nil, err
		}
		target, _, _ := unstructured.NestedInt64(pool.Object, "spec", "replicas")
		if requested != nil {
			if requested.ResourceVersion != pool.GetResourceVersion() {
				return true, nil, apierrors.NewConflict(poolResource.GroupResource(), name, fmt.Errorf("stale scale"))
			}
			target = int64(requested.Spec.Replicas)
			require.NoError(t, unstructured.SetNestedField(pool.Object, target, "spec", "replicas"))
			version, _ := strconv.Atoi(pool.GetResourceVersion())
			pool.SetResourceVersion(strconv.Itoa(version + 1))
			_, err = client.Resource(poolResource).Namespace(namespace).Update(context.Background(), pool, metav1.UpdateOptions{})
			if err != nil {
				return true, nil, err
			}
		}
		return true, &autoscalingv1.Scale{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, ResourceVersion: pool.GetResourceVersion()}, Spec: autoscalingv1.ScaleSpec{Replicas: int32(target)}}, nil
	})
	controller.managementScaleClient = scaleClient
	extension := &AzureControllerExtension{azureMachinePoolMachineAvailable: true, azureMachinePoolMachineInformer: factory.ForResource(schema.GroupVersionResource{Group: azureMachinePoolMachineApiGroup, Version: "v1beta1", Resource: resourceNameAzureMachinePoolMachine})}
	require.NoError(t, extension.azureMachinePoolMachineInformer.Informer().AddIndexers(cache.Indexers{machineProviderIDIndex: indexMachineByProviderID}))
	controller.azureIntegration = &AzureIntegration{extension: extension, lookup: &AzureMachineLookup{controller: controller, extension: extension}}
	for _, pair := range []string{"lin", "win2"} {
		for _, role := range []string{"primary", "secondary"} {
			name := pair + "-" + role
			target, minimum := int64(0), "0"
			providerIDs := []string{}
			if role == "primary" {
				target, minimum = 3, "1"
				providerIDs = append(providerIDs, "azure://"+name)
				node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name)}, Spec: corev1.NodeSpec{ProviderID: providerIDs[0]}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
				require.NoError(t, nodeInformer.GetStore().Add(node))
			}
			pool := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "cluster.x-k8s.io/v1beta2", "kind": "MachinePool"}}
			pool.SetName(name)
			pool.SetNamespace("tenant")
			pool.SetUID(types.UID(name))
			pool.SetResourceVersion("1")
			pool.SetAnnotations(map[string]string{failover.PairKey: pair, failover.RoleKey: role, nodeGroupMinSizeAnnotationKey: minimum, nodeGroupMaxSizeAnnotationKey: "4", "capacity.cluster-autoscaler.kubernetes.io/cpu": "4", "capacity.cluster-autoscaler.kubernetes.io/memory": "4Gi"})
			pool.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: cluster.GetAPIVersion(), Kind: "Cluster", Name: cluster.GetName(), UID: cluster.GetUID()}})
			require.NoError(t, unstructured.SetNestedField(pool.Object, target, "spec", "replicas"))
			require.NoError(t, unstructured.SetNestedField(pool.Object, "test", "spec", "clusterName"))
			require.NoError(t, unstructured.SetNestedStringSlice(pool.Object, providerIDs, "spec", "providerIDList"))
			require.NoError(t, unstructured.SetNestedStringMap(pool.Object, map[string]string{"apiVersion": "infrastructure.cluster.x-k8s.io/v1beta1", "kind": "AzureMachinePool", "name": name}, "spec", "template", "spec", "infrastructureRef"))
			_, err := client.Resource(poolResource).Namespace("tenant").Create(context.Background(), pool, metav1.CreateOptions{})
			require.NoError(t, err)
			require.NoError(t, controller.machinePoolInformer.Informer().GetStore().Add(pool))
			amp := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "infrastructure.cluster.x-k8s.io/v1beta1", "kind": "AzureMachinePool", "status": map[string]interface{}{"capacity": map[string]interface{}{"cpu": "4", "memory": "4Gi"}}}}
			amp.SetName(name)
			amp.SetNamespace("tenant")
			amp.SetUID(types.UID(name + "-amp"))
			amp.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: pool.GetAPIVersion(), Kind: machinePoolKind, Name: name, UID: pool.GetUID()}})
			_, err = client.Resource(schema.GroupVersionResource{Group: azureMachinePoolMachineApiGroup, Version: "v1beta1", Resource: "azuremachinepools"}).Namespace("tenant").Create(context.Background(), amp, metav1.CreateOptions{})
			require.NoError(t, err)
		}
	}
	writeFailoverTestConfiguration(t, client, cluster)
	require.NoError(t, controller.enableAzureFailover(false, 10*time.Second))
	clock := clocktesting.NewFakeClock(now)
	controller.failover.Clock = clock
	controller.failover.NodeGroupDefaults = config.NodeGroupAutoscalingOptions{MaxNodeProvisionTime: 45 * time.Minute}
	return controller, clock
}
func writeFailoverTestConfiguration(t *testing.T, client dynamic.Interface, cluster *unstructured.Unstructured) {
	t.Helper()
	configuration := failover.Configuration{APIVersion: failover.ConfigVersion, Cluster: failover.ClusterIdentity{Name: cluster.GetName(), Namespace: cluster.GetNamespace(), UID: cluster.GetUID()}, Pairs: map[string]failover.ConfiguredPair{"lin": {Primary: failover.ConfiguredPool{Name: "lin-primary"}, Secondary: failover.ConfiguredPool{Name: "lin-secondary"}}, "win2": {Primary: failover.ConfiguredPool{Name: "win2-primary"}, Secondary: failover.ConfiguredPool{Name: "win2-secondary"}}}}
	encoded, err := json.Marshal(configuration)
	require.NoError(t, err)
	object := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap", "data": map[string]interface{}{"config.json": string(encoded)}}}
	object.SetName(cluster.GetName() + "-autoscaler-failover-config")
	object.SetNamespace(cluster.GetNamespace())
	object.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: cluster.GetAPIVersion(), Kind: "Cluster", Name: cluster.GetName(), UID: cluster.GetUID()}})
	_, err = client.Resource(failover.ConfigMaps).Namespace(cluster.GetNamespace()).Create(context.Background(), object, metav1.CreateOptions{})
	require.NoError(t, err)
}
func failoverTestFailure(controller *machineController, pair, role string, now time.Time) {
	machine := &unstructured.Unstructured{Object: map[string]interface{}{"status": map[string]interface{}{"provisioningState": "Failed"}}}
	machine.SetUID(types.UID(pair + "-" + role + "-failure"))
	machine.SetNamespace("tenant")
	machine.SetCreationTimestamp(metav1.NewTime(now))
	machine.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "infrastructure.cluster.x-k8s.io/v1beta1", Kind: "AzureMachinePool", Name: pair + "-" + role, UID: types.UID(pair + "-" + role + "-amp")}})
	controller.failover.Observe(machine)
}
