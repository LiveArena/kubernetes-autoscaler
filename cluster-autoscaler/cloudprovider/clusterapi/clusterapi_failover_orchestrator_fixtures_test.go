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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/actuation"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/deletiontracker"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/orchestrator"
	coretest "k8s.io/autoscaler/cluster-autoscaler/core/test"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupconfig"
	processorstest "k8s.io/autoscaler/cluster-autoscaler/processors/test"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	simulatoroptions "k8s.io/autoscaler/cluster-autoscaler/simulator/options"
	kubeutils "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	testutils "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	fakekube "k8s.io/client-go/kubernetes/fake"
	fakescale "k8s.io/client-go/scale/fake"
	"k8s.io/client-go/tools/cache"
	clocktesting "k8s.io/utils/clock/testing"
	"strings"
	"testing"
	"time"
)

type failoverOrchestratorFixture struct {
	t                  *testing.T
	pair, outcome      string
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

func newFailoverOrchestratorFixture(t *testing.T, pair, outcome string, disableAccounting bool) *failoverOrchestratorFixture {
	fixture := &failoverOrchestratorFixture{t: t, pair: pair, outcome: outcome, disableAccounting: disableAccounting}
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
func (fixture *failoverOrchestratorFixture) newScan() (*clusterstate.ClusterStateRegistry, *orchestrator.ScaleUpOrchestrator, func() []*corev1.Pod) {
	t := fixture.t
	context, err := coretest.NewScaleTestAutoscalingContext(fixture.options, fakekube.NewSimpleClientset(), nil, fixture.provider, nil, nil)
	require.NoError(t, err)
	daemonsets, err := kubeutils.NewTestDaemonSetLister(nil)
	require.NoError(t, err)
	allNodes := kubeutils.NewTestNodeLister(fixture.nodes)
	context.ListerRegistry = kubeutils.NewListerRegistry(allNodes, allNodes, kubeutils.NewTestPodLister(append([]*corev1.Pod{fixture.occupied}, fixture.pending...)), kubeutils.NewTestPodDisruptionBudgetLister(nil), daemonsets, nil, nil, nil, nil)
	processors := processorstest.NewTestProcessors(&context)
	context.ScaleDownActuator = actuation.NewActuator(&context, processors.ScaleStateNotifier, deletiontracker.NewNodeDeletionTracker(0), simulatoroptions.NodeDeleteOptions{}, nil, processors.NodeGroupConfigProcessor)
	fixture.registry = clusterstate.NewClusterStateRegistry(fixture.provider, clusterstate.ClusterStateRegistryConfig{OkTotalUnreadyCount: 10, MaxTotalUnreadyPercentage: 100}, context.LogRecorder, coretest.NewBackoff(), nodegroupconfig.NewDefaultNodeGroupConfigProcessor(fixture.options.NodeGroupDefaults), processors.AsyncNodeGroupStateChecker)
	processors.ScaleStateNotifier.Register(fixture.registry)
	builder, err := estimator.NewEstimatorBuilder(estimator.BinpackingEstimatorName, estimator.NewThresholdBasedEstimationLimiter(nil), estimator.NewDecreasingPodOrderer(), nil)
	require.NoError(t, err)
	orchestrator := orchestrator.New()
	orchestrator.Initialize(&context, processors, fixture.registry, builder, taints.TaintConfig{})
	fixture.filterPods = func() []*corev1.Pod {
		allNodes.SetNodes(fixture.nodes)
		require.NoError(t, context.ClusterSnapshot.SetClusterState(fixture.nodes, fixture.boundPods, nil))
		counts, exclusions := fixture.registry.GetUpcomingNodes()
		placeholderIndex := 0
		for group, count := range counts {
			for index := 0; index < count; index++ {
				nodeInfo, err := simulator.SanitizedNodeInfo(fixture.templates[group], fmt.Sprintf("upcoming-%d", placeholderIndex))
				require.NoError(t, err)
				require.NoError(t, context.ClusterSnapshot.AddNodeInfo(nodeInfo))
				placeholderIndex++
			}
		}
		for _, names := range exclusions {
			for _, name := range names {
				require.NoError(t, context.ClusterSnapshot.RemoveNodeInfo(name))
			}
		}
		residual, err := processors.PodListProcessor.Process(&context, fixture.pending)
		require.NoError(t, err)
		t.Logf("placeholders=%d residualPods=%d", placeholderIndex, len(residual))
		return residual
	}
	return fixture.registry, orchestrator, fixture.filterPods
}
func (fixture *failoverOrchestratorFixture) scan() int {
	t := fixture.t
	require.NoError(t, fixture.provider.Refresh())
	if fixture.disableAccounting {
		fixture.controller.failover.Lock()
		id := "MachinePool/tenant/" + fixture.pair + "-primary"
		policy := fixture.controller.failover.Policies[id]
		policy.BlockUnregistered = false
		fixture.controller.failover.Policies[id] = policy
		fixture.controller.failover.Unlock()
	}
	require.NoError(t, fixture.registry.UpdateNodes(fixture.nodes, nil, fixture.clock.Now()))
	counts, exclusions := fixture.registry.GetUpcomingNodes()
	incoming, err := fixture.engine.UpcomingNodes(fixture.templates)
	assert.NoError(t, err)
	t.Logf("scan=%s counts=%v excluded=%v orchestratorIncoming=%d", fixture.clock.Now().Format(time.RFC3339), counts, exclusions, len(incoming))
	residual := fixture.filterPods()
	if len(residual) > 0 {
		_, err = fixture.engine.ScaleUp(residual, fixture.nodes, nil, fixture.templates, false)
		require.NoError(t, err)
	}
	fixture.clock.Step(10 * time.Second)
	return len(residual)
}
func (fixture *failoverOrchestratorFixture) restart() {
	t := fixture.t
	retainedClient := fixture.controller.managementClient
	restarted := &machineController{managementClient: retainedClient, managementScaleClient: fixture.controller.managementScaleClient, machinePoolResource: fixture.controller.machinePoolResource, machinePoolsAvailable: true, stopChannel: fixture.controller.stopChannel, managementInformerFactory: dynamicinformer.NewDynamicSharedInformerFactory(retainedClient, 0)}
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
