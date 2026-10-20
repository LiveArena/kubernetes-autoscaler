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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/clusterapi/failover"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/orchestrator"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	testutils "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	fakekube "k8s.io/client-go/kubernetes/fake"
	fakescale "k8s.io/client-go/scale/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	clocktesting "k8s.io/utils/clock/testing"
)

type failoverOrchestratorFixture struct {
	t                  *testing.T
	pair               string
	disableAccounting  bool
	controller         *machineController
	clock              *clocktesting.FakeClock
	provider           *provider
	templates          map[string]*framework.NodeInfo
	nodes              []*corev1.Node
	occupied           *corev1.Pod
	boundPods, pending []*corev1.Pod
	options            config.AutoscalingOptions
	registry           *clusterstate.ClusterStateRegistry
	engine             *orchestrator.ScaleUpOrchestrator
	filterPods         func() []*corev1.Pod
	attempts           dynamic.ResourceInterface
}

func newFailoverOrchestratorFixture(t *testing.T, pair string, disableAccounting bool) *failoverOrchestratorFixture {
	fixture := &failoverOrchestratorFixture{t: t, pair: pair, disableAccounting: disableAccounting}
	fixture.controller, fixture.clock = newFailoverTestController(t)
	fixture.provider = &provider{controller: fixture.controller, resourceLimiter: cloudprovider.NewResourceLimiter(map[string]int64{}, map[string]int64{})}
	fixture.templates = map[string]*framework.NodeInfo{}
	fixture.nodes = []*corev1.Node{}
	for _, object := range fixture.controller.nodeInformer.GetStore().List() {
		original := object.(*corev1.Node)
		node := testutils.BuildTestNode(original.Name, 2000, 100000)
		node.Spec.ProviderID = original.Spec.ProviderID
		node.UID = original.UID
		node.Labels["test/workload"] = original.Name
		os := "windows"
		if strings.HasPrefix(original.Name, "lin-") {
			os = "linux"
			node.Status.Capacity["nvidia.com/gpu"] = apiresource.MustParse("1")
			node.Status.Allocatable["nvidia.com/gpu"] = apiresource.MustParse("1")
		}
		node.Labels[corev1.LabelOSStable] = os
		testutils.SetNodeReadyState(node, true, fixture.clock.Now().Add(-time.Minute))
		require.NoError(t, fixture.controller.nodeInformer.GetStore().Update(node))
		fixture.nodes = append(fixture.nodes, node)
		workload := strings.TrimSuffix(node.Name, "-primary")
		for _, role := range []string{"primary", "secondary"} {
			template := node.DeepCopy()
			template.Name = workload + "-" + role + "-template"
			fixture.templates["MachinePool/tenant/"+workload+"-"+role] = framework.NewTestNodeInfo(template)
		}
	}
	fixture.occupied = testutils.BuildTestPod("occupied", 1800, 100)
	fixture.occupied.Spec.NodeName = fixture.pair + "-primary"
	fixture.boundPods = []*corev1.Pod{fixture.occupied}
	fixture.pending = []*corev1.Pod{testutils.BuildTestPod("pending-1", 1400, 100), testutils.BuildTestPod("pending-2", 1400, 100)}
	for _, pod := range fixture.pending {
		pod.Spec.NodeSelector = map[string]string{"test/workload": fixture.pair + "-primary", corev1.LabelOSStable: fixture.templates["MachinePool/tenant/"+fixture.pair+"-primary"].Node().Labels[corev1.LabelOSStable]}
		if fixture.pair == "lin" {
			pod.Spec.Containers[0].Resources.Requests["nvidia.com/gpu"] = apiresource.MustParse("1")
		}
	}
	fixture.options = config.AutoscalingOptions{EstimatorName: estimator.BinpackingEstimatorName, MaxNodesTotal: 20, MaxCoresTotal: 100, MaxMemoryTotal: 10000000, MaxNodeGroupBinpackingDuration: time.Minute, MaxBinpackingTime: time.Minute, NodeGroupDefaults: config.NodeGroupAutoscalingOptions{MaxNodeProvisionTime: 45 * time.Minute}}
	fixture.registry, fixture.engine, fixture.filterPods = fixture.newScan()
	fixture.attempts = fixture.controller.managementClient.Resource(schema.GroupVersionResource{Group: azureMachinePoolMachineApiGroup, Version: "v1beta1", Resource: resourceNameAzureMachinePoolMachine}).Namespace("tenant")
	return fixture
}
func (fixture *failoverOrchestratorFixture) writes() int {
	count := 0
	for _, action := range fixture.controller.managementScaleClient.(*fakescale.FakeScaleClient).Actions() {
		if action.GetVerb() == "update" || action.GetVerb() == "patch" {
			count++
		}
	}
	return count
}
func (fixture *failoverOrchestratorFixture) observeAttempt(name string) *unstructured.Unstructured {
	t := fixture.t
	attempt := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "infrastructure.cluster.x-k8s.io/v1beta1", "kind": "AzureMachinePoolMachine", "status": map[string]interface{}{"provisioningState": "Failed"}}}
	attempt.SetName(name)
	attempt.SetNamespace("tenant")
	attempt.SetUID(types.UID(name))
	attempt.SetCreationTimestamp(metav1.NewTime(fixture.clock.Now()))
	attempt.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "infrastructure.cluster.x-k8s.io/v1beta1", Kind: "AzureMachinePool", Name: fixture.pair + "-primary", UID: types.UID(fixture.pair + "-primary-amp")}})
	_, err := fixture.attempts.Create(context.Background(), attempt, metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, wait.PollUntilContextTimeout(context.Background(), time.Millisecond, time.Second, true, func(ctx context.Context) (bool, error) {
		fixture.controller.failover.RLock()
		defer fixture.controller.failover.RUnlock()
		observation := fixture.controller.failover.Observations["tenant/"+fixture.pair+"-primary-amp"]
		return observation.UID == attempt.GetUID(), nil
	}))
	return attempt
}
func (fixture *failoverOrchestratorFixture) restart() {
	t := fixture.t
	retainedClient := fixture.controller.managementClient
	restarted := &machineController{managementClient: retainedClient, managementScaleClient: fixture.controller.managementScaleClient, machinePoolResource: fixture.controller.machinePoolResource, machinePoolsAvailable: true, stopChannel: fixture.controller.stopChannel, managementInformerFactory: dynamicinformer.NewDynamicSharedInformerFactory(retainedClient, 0)}
	restarted.managementDiscoveryClient = fixture.controller.managementDiscoveryClient
	restarted.machinePoolInformer = restarted.managementInformerFactory.ForResource(fixture.controller.machinePoolResource)
	require.NoError(t, restarted.machinePoolInformer.Informer().GetIndexer().AddIndexers(cache.Indexers{machinePoolProviderIDIndex: indexMachinePoolByProviderID}))
	restarted.machineSetInformer = restarted.managementInformerFactory.ForResource(schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "machinesets"})
	restarted.machineInformer = restarted.managementInformerFactory.ForResource(schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "machines"})
	require.NoError(t, restarted.machineInformer.Informer().GetIndexer().AddIndexers(cache.Indexers{machineProviderIDIndex: indexMachineByProviderID}))
	extension := &AzureControllerExtension{azureMachinePoolMachineAvailable: true, azureMachinePoolMachineInformer: restarted.managementInformerFactory.ForResource(schema.GroupVersionResource{Group: azureMachinePoolMachineApiGroup, Version: "v1beta1", Resource: resourceNameAzureMachinePoolMachine})}
	restarted.azureIntegration = &AzureIntegration{extension: extension, lookup: &AzureMachineLookup{controller: restarted, extension: extension}}
	for _, nodeGroup := range fixture.provider.NodeGroups() {
		pool, err := retainedClient.Resource(fixture.controller.machinePoolResource).Namespace("tenant").Get(context.Background(), nodeGroup.(*nodegroup).scalableResource.Name(), metav1.GetOptions{})
		require.NoError(t, err)
		require.NoError(t, restarted.machinePoolInformer.Informer().GetStore().Add(pool))
	}
	restarted.nodeInformer = informers.NewSharedInformerFactory(fakekube.NewSimpleClientset(), 0).Core().V1().Nodes().Informer()
	require.NoError(t, restarted.nodeInformer.AddIndexers(cache.Indexers{nodeProviderIDIndex: indexNodeByProviderID}))
	for _, node := range fixture.nodes {
		require.NoError(t, restarted.nodeInformer.GetStore().Add(node.DeepCopy()))
	}
	require.NoError(t, restarted.enableAzureFailover(false, 10*time.Second, fixture.options.NodeGroupDefaults))
	fixture.clock.Step(31 * time.Second)
	restarted.failover.Clock = fixture.clock
	fixture.controller = restarted
	fixture.provider.controller = fixture.controller
	fixture.registry, fixture.engine, fixture.filterPods = fixture.newScan()
}
func (fixture *failoverOrchestratorFixture) registerReady(role string, count int) {
	t := fixture.t
	resource := fixture.controller.managementClient.Resource(fixture.controller.machinePoolResource).Namespace("tenant")
	pool, err := resource.Get(context.Background(), fixture.pair+"-"+role, metav1.GetOptions{})
	require.NoError(t, err)
	providerIDs, _, err := unstructured.NestedStringSlice(pool.Object, "spec", "providerIDList")
	require.NoError(t, err)
	baseIndex := len(providerIDs)
	for index := 0; index < count; index++ {
		name := fmt.Sprintf("%s-%s-ready-%d", fixture.pair, role, baseIndex+index)
		node := fixture.templates["MachinePool/tenant/"+fixture.pair+"-"+role].Node().DeepCopy()
		node.Name = name
		node.UID = types.UID(name)
		node.Spec.ProviderID = "azure://" + name
		node.Labels[corev1.LabelHostname] = name
		testutils.SetNodeReadyState(node, true, fixture.clock.Now())
		require.NoError(t, fixture.controller.nodeInformer.GetStore().Add(node))
		fixture.nodes = append(fixture.nodes, node)
		providerIDs = append(providerIDs, node.Spec.ProviderID)
	}
	require.NoError(t, unstructured.SetNestedStringSlice(pool.Object, providerIDs, "spec", "providerIDList"))
	_, err = resource.Update(context.Background(), pool, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, fixture.controller.machinePoolInformer.Informer().GetStore().Update(pool))
}

func (fixture *failoverOrchestratorFixture) pendingPod(name string) *corev1.Pod {
	pod := testutils.BuildTestPod(name, 1400, 100)
	pod.Spec.NodeSelector = map[string]string{"test/workload": fixture.pair + "-primary", corev1.LabelOSStable: fixture.templates["MachinePool/tenant/"+fixture.pair+"-primary"].Node().Labels[corev1.LabelOSStable]}
	if fixture.pair == "lin" {
		pod.Spec.Containers[0].Resources.Requests["nvidia.com/gpu"] = apiresource.MustParse("1")
	}
	return pod
}

func (fixture *failoverOrchestratorFixture) setMaximum(role, maximum string) {
	fixture.t.Helper()
	pools := fixture.controller.managementClient.Resource(fixture.controller.machinePoolResource).Namespace("tenant")
	pool, err := pools.Get(context.Background(), fixture.pair+"-"+role, metav1.GetOptions{})
	require.NoError(fixture.t, err)
	annotations := pool.GetAnnotations()
	annotations[nodeGroupMaxSizeAnnotationKey] = maximum
	pool.SetAnnotations(annotations)
	_, err = pools.Update(context.Background(), pool, metav1.UpdateOptions{})
	require.NoError(fixture.t, err)
	require.NoError(fixture.t, fixture.controller.machinePoolInformer.Informer().GetStore().Update(pool))
}

func (fixture *failoverOrchestratorFixture) lastWrite() *autoscalingv1.Scale {
	fixture.t.Helper()
	var last *autoscalingv1.Scale
	for _, action := range fixture.controller.managementScaleClient.(*fakescale.FakeScaleClient).Actions() {
		if action.GetVerb() == "update" {
			last = action.(clienttesting.UpdateAction).GetObject().(*autoscalingv1.Scale)
		}
	}
	require.NotNil(fixture.t, last, "expected a scale write")
	return last
}

func (fixture *failoverOrchestratorFixture) pairState() *failover.Pair {
	fixture.t.Helper()
	ctx := context.Background()
	cluster, err := fixture.controller.managementClient.Resource(schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "clusters"}).Namespace("tenant").Get(ctx, "test", metav1.GetOptions{})
	require.NoError(fixture.t, err)
	state, _, err := fixture.controller.failover.Store.Load(ctx, cluster)
	require.NoError(fixture.t, err)
	require.Contains(fixture.t, state.Pairs, fixture.pair)
	return state.Pairs[fixture.pair]
}
