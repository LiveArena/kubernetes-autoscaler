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
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	testutils "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	fakescale "k8s.io/client-go/scale/fake"
	clienttesting "k8s.io/client-go/testing"
	"strings"
	"testing"
	"time"
)

func TestResidualDemandScalingSurvivesRestartWithoutDuplicatePoolRequests(t *testing.T) {
	for _, pair := range []string{"lin", "win2"} {
		for _, outcome := range []string{"initial", "primary-first", "secondary-first", "secondary-maximum", "primary-maximum", "primary-unfit", "both-unfit", "freeze-unfit", "mixed-pending"} {
			for _, disableAccounting := range []bool{false, true} {
				if disableAccounting && outcome != "initial" {
					continue
				}
				statement := map[string]string{
					"initial":           "FailureRevealsResidualDemandAndRestartDoesNotReplayFallback",
					"primary-first":     "NewDemandTriesPrimaryAndPrimarySuccessPreventsAnotherFallback",
					"secondary-first":   "FreshPrimaryFailurePermitsIndependentSecondaryCapacity",
					"secondary-maximum": "SecondaryMaximumRejectsTheFullRequestWithoutReducingPrimaryIntent",
					"primary-maximum":   "PrimaryMaximumPermitsFittingSecondaryDemand",
					"primary-unfit":     "UnfitPrimaryPermitsFittingSecondaryDemand",
					"both-unfit":        "DemandThatFitsNeitherPoolDoesNotRequestCapacity",
					"freeze-unfit":      "UnfitPrimaryDoesNotBypassFrozenSecondaryAdmission",
					"mixed-pending":     "IndependentSecondaryOnlyDemandDoesNotCancelOrReplayThePrimaryTrial",
				}[outcome]
				if disableAccounting {
					statement = "RemovingBlockedGapAccountingPreventsCanonicalFallback"
				}
				t.Run(fmt.Sprintf("%s/Pair=%s", statement, pair), func(t *testing.T) {
					fixture := newFailoverOrchestratorFixture(t, pair, outcome, disableAccounting)
					assert.Zero(t, fixture.scan())
					assert.Zero(t, fixture.writes())
					counts, _ := fixture.registry.GetUpcomingNodes()
					assert.Equal(t, 2, counts["MachinePool/tenant/"+fixture.pair+"-primary"])
					failed := fixture.observeAttempt("primary-failed")
					if fixture.disableAccounting {
						assert.Zero(t, fixture.scan())
						assert.Zero(t, fixture.writes())
						return
					}
					assert.Equal(t, 2, fixture.scan())
					assert.Equal(t, 1, fixture.writes())
					counts, _ = fixture.registry.GetUpcomingNodes()
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
					if fixture.outcome == "mixed-pending" {
						for index, pod := range fixture.pending {
							bound := pod.DeepCopy()
							bound.Spec.NodeName = fmt.Sprintf("%s-secondary-ready-%d", fixture.pair, index)
							fixture.boundPods = append(fixture.boundPods, bound)
						}
						fixture.pending = nil
					}
					assert.Zero(t, fixture.scan())
					if fixture.outcome != "initial" {
						setMaximum := func(role string, maximum string) {
							resource := fixture.controller.managementClient.Resource(fixture.controller.machinePoolResource).Namespace("tenant")
							pool, err := resource.Get(context.Background(), fixture.pair+"-"+role, metav1.GetOptions{})
							require.NoError(t, err)
							annotations := pool.GetAnnotations()
							annotations[nodeGroupMaxSizeAnnotationKey] = maximum
							pool.SetAnnotations(annotations)
							_, err = resource.Update(context.Background(), pool, metav1.UpdateOptions{})
							require.NoError(t, err)
							require.NoError(t, fixture.controller.machinePoolInformer.Informer().GetStore().Update(pool))
						}
						if fixture.outcome == "primary-maximum" {
							setMaximum("primary", "3")
						} else if fixture.outcome == "secondary-maximum" {
							setMaximum("secondary", "2")
						} else if fixture.outcome == "mixed-pending" {
							setMaximum("primary", "6")
						}
						additional := testutils.BuildTestPod("new-demand", 1400, 100)
						additional.Spec.NodeSelector = map[string]string{"test/workload": fixture.pair + "-primary", corev1.LabelOSStable: fixture.templates["MachinePool/tenant/"+fixture.pair+"-primary"].Node().Labels[corev1.LabelOSStable]}
						if fixture.pair == "lin" {
							additional.Spec.Containers[0].Resources.Requests["nvidia.com/gpu"] = apiresource.MustParse("1")
						}
						additional.Name = "new-demand"
						additional.UID = "new-demand"
						if fixture.outcome == "mixed-pending" {
							fixture.templates["MachinePool/tenant/"+fixture.pair+"-primary"].Node().Labels["test/primary-only"] = "true"
							additional.Spec.NodeSelector["test/primary-only"] = "true"
						}
						fixture.pending = append(fixture.pending, additional)
						if fixture.outcome == "primary-unfit" || fixture.outcome == "both-unfit" || fixture.outcome == "freeze-unfit" {
							fixture.templates["MachinePool/tenant/"+fixture.pair+"-primary"].Node().Labels["test/workload"] = "incompatible-primary"
							if fixture.outcome == "both-unfit" {
								fixture.templates["MachinePool/tenant/"+fixture.pair+"-secondary"].Node().Labels["test/workload"] = "incompatible-secondary"
							} else if fixture.outcome == "freeze-unfit" {
								require.NoError(t, fixture.controller.enableAzureFailover(true, 10*time.Second, fixture.options.NodeGroupDefaults))
								fixture.controller.failover.Clock = fixture.clock
							}
							assert.Equal(t, 1, fixture.scan())
							wantedWrites := 2
							if fixture.outcome != "primary-unfit" {
								wantedWrites = 1
							}
							assert.Equal(t, wantedWrites, fixture.writes())
							if fixture.outcome == "primary-unfit" {
								cluster, err := fixture.controller.managementClient.Resource(schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "clusters"}).Namespace("tenant").Get(context.Background(), "test", metav1.GetOptions{})
								require.NoError(t, err)
								state, _, err := fixture.controller.failover.Store.Load(context.Background(), cluster)
								require.NoError(t, err)
								require.NotNil(t, state.Pairs[fixture.pair].SecondaryRequest)
								assert.NotEmpty(t, state.Pairs[fixture.pair].SecondaryRequest.PrimaryUnavailableReason)
								assert.Zero(t, fixture.scan())
								assert.Equal(t, wantedWrites, fixture.writes())
								fixture.restart()
								assert.Zero(t, fixture.scan())
								assert.Equal(t, wantedWrites, fixture.writes())
							}
							return
						}
						assert.Equal(t, 1, fixture.scan())
						assert.Equal(t, 2, fixture.writes())
						lastWrite := func() *autoscalingv1.Scale {
							var last *autoscalingv1.Scale
							for _, action := range fixture.controller.managementScaleClient.(*fakescale.FakeScaleClient).Actions() {
								if action.GetVerb() == "update" {
									last = action.(clienttesting.UpdateAction).GetObject().(*autoscalingv1.Scale)
								}
							}
							return last
						}
						if fixture.outcome == "primary-maximum" {
							assert.Equal(t, fixture.pair+"-secondary", lastWrite().Name)
							assert.Equal(t, int32(3), lastWrite().Spec.Replicas)
							assert.Zero(t, fixture.scan())
							return
						}
						assert.Equal(t, fixture.pair+"-primary", lastWrite().Name)
						assert.Equal(t, int32(4), lastWrite().Spec.Replicas)
						if fixture.outcome == "mixed-pending" {
							fixture.templates["MachinePool/tenant/"+fixture.pair+"-secondary"].Node().Labels["test/secondary-only"] = "true"
							secondaryOnly := additional.DeepCopy()
							secondaryOnly.Name = "secondary-only-demand"
							secondaryOnly.UID = "secondary-only-demand"
							delete(secondaryOnly.Spec.NodeSelector, "test/primary-only")
							secondaryOnly.Spec.NodeSelector["test/secondary-only"] = "true"
							fixture.pending = append(fixture.pending, secondaryOnly)
							require.Equal(t, 1, fixture.scan())
							require.Equal(t, 3, fixture.writes(), "secondary-only residual must not wait for an unrelated primary trial")
							assert.Equal(t, fixture.pair+"-secondary", lastWrite().Name)
							assert.Equal(t, int32(3), lastWrite().Spec.Replicas)
							cluster, err := fixture.controller.managementClient.Resource(schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "clusters"}).Namespace("tenant").Get(context.Background(), "test", metav1.GetOptions{})
							require.NoError(t, err)
							state, _, err := fixture.controller.failover.Store.Load(context.Background(), cluster)
							require.NoError(t, err)
							require.NotNil(t, state.Pairs[fixture.pair].PrimaryRequest, "independent primary intent must survive")
							assert.Equal(t, 4, state.Pairs[fixture.pair].PrimaryRequest.ToTarget)
							require.NotNil(t, state.Pairs[fixture.pair].SecondaryRequest)
							assert.NotEmpty(t, state.Pairs[fixture.pair].SecondaryRequest.PrimaryUnavailableReason)
							assert.Zero(t, fixture.scan())
							assert.Equal(t, 3, fixture.writes())
							fixture.restart()
							assert.Zero(t, fixture.scan())
							assert.Equal(t, 3, fixture.writes())
							return
						}
						fixture.restart()
						assert.Zero(t, fixture.scan())
						assert.Equal(t, 2, fixture.writes())
						if fixture.outcome == "primary-first" {
							fixture.registerReady("primary", 1)
							assert.Zero(t, fixture.scan())
							assert.Equal(t, 2, fixture.writes())
							return
						}
						newFailure := fixture.observeAttempt("new-demand-failure")
						assert.Equal(t, 1, fixture.scan())
						if fixture.outcome == "secondary-maximum" {
							assert.Equal(t, 2, fixture.writes())
							assert.Equal(t, fixture.pair+"-primary", lastWrite().Name)
							assert.Equal(t, int32(4), lastWrite().Spec.Replicas)
							return
						}
						assert.Equal(t, 3, fixture.writes())
						assert.Equal(t, fixture.pair+"-secondary", lastWrite().Name)
						assert.Equal(t, int32(3), lastWrite().Spec.Replicas)
						require.NoError(t, fixture.attempts.Delete(context.Background(), newFailure.GetName(), metav1.DeleteOptions{}))
						fixture.registerReady("secondary", 1)
						assert.Zero(t, fixture.scan())
						fixture.registerReady("primary", 1)
						assert.Zero(t, fixture.scan())
						assert.Equal(t, 3, fixture.writes())
						return
					}
					fixture.registerReady("primary", 2)
					assert.Zero(t, fixture.scan())
					assert.Zero(t, fixture.scan())
					assert.Equal(t, 1, fixture.writes())
					for _, candidate := range fixture.provider.NodeGroups() {
						group := candidate.(*nodegroup)
						if !strings.HasPrefix(group.scalableResource.Name(), fixture.pair+"-") {
							assert.False(t, group.GetCapacityPolicy().BlockUnregistered)
							assert.Equal(t, strings.HasSuffix(group.scalableResource.Name(), "-secondary"), group.GetCapacityPolicy().ScaleUpBlocked)
						}
						if group.scalableResource.Name() == fixture.pair+"-primary" {
							target, err := group.TargetSize()
							require.NoError(t, err)
							assert.Equal(t, 3, target)
							assert.False(t, group.GetCapacityPolicy().BlockUnregistered)
						}
						if group.scalableResource.Name() == fixture.pair+"-secondary" {
							target, err := group.TargetSize()
							require.NoError(t, err)
							assert.Equal(t, 2, target)
							assert.True(t, group.GetCapacityPolicy().ScaleUpBlocked)
						}
					}
				})
			}
		}
	}
}
