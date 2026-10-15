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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/actuation"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/deletiontracker"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/orchestrator"
	coretest "k8s.io/autoscaler/cluster-autoscaler/core/test"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupconfig"
	processorstest "k8s.io/autoscaler/cluster-autoscaler/processors/test"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	simulatoroptions "k8s.io/autoscaler/cluster-autoscaler/simulator/options"
	kubeutils "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	fakekube "k8s.io/client-go/kubernetes/fake"
	fakescale "k8s.io/client-go/scale/fake"
	clienttesting "k8s.io/client-go/testing"
)

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
	engine := orchestrator.New()
	engine.Initialize(&context, processors, fixture.registry, builder, taints.TaintConfig{})
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
	return fixture.registry, engine, fixture.filterPods
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

func (fixture *failoverOrchestratorFixture) expectHealthyBootstrap() {
	fixture.t.Helper()
	assert.Zero(fixture.t, fixture.scan())
	assert.Zero(fixture.t, fixture.writes())
	counts, _ := fixture.registry.GetUpcomingNodes()
	assert.Equal(fixture.t, 2, counts["MachinePool/tenant/"+fixture.pair+"-primary"])
}

func (fixture *failoverOrchestratorFixture) supplyFailedPrimaryDemand() {
	t := fixture.t
	t.Helper()
	fixture.expectHealthyBootstrap()
	failed := fixture.observeAttempt("primary-failed")
	assert.Equal(t, 2, fixture.scan())
	assert.Equal(t, 1, fixture.writes())
	counts, _ := fixture.registry.GetUpcomingNodes()
	assert.Zero(t, counts["MachinePool/tenant/"+fixture.pair+"-primary"])
	for _, action := range fixture.controller.managementScaleClient.(*fakescale.FakeScaleClient).Actions() {
		if action.GetVerb() == "update" {
			requested := action.(clienttesting.UpdateAction).GetObject().(*autoscalingv1.Scale)
			assert.Equal(t, fixture.pair+"-secondary", requested.Name)
			assert.Equal(t, int32(2), requested.Spec.Replicas)
		}
	}
	for index := 0; index < 100; index++ {
		fixture.controller.failover.Observe(failed.DeepCopy())
	}
	require.NoError(t, fixture.attempts.Delete(context.Background(), failed.GetName(), metav1.DeleteOptions{}))
	assert.Zero(t, fixture.scan())
	assert.Equal(t, 1, fixture.writes())
	counts, _ = fixture.registry.GetUpcomingNodes()
	assert.Equal(t, 2, counts["MachinePool/tenant/"+fixture.pair+"-secondary"])
	replacement := fixture.observeAttempt("primary-replacement")
	assert.Zero(t, fixture.scan())
	assert.Equal(t, 1, fixture.writes())
	require.NoError(t, fixture.attempts.Delete(context.Background(), replacement.GetName(), metav1.DeleteOptions{}))
	fixture.restart()
	assert.Zero(t, fixture.scan())
	assert.Equal(t, 1, fixture.writes())
	fixture.registerReady("secondary", 2)
}
