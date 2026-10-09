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
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"testing"
	"time"
)

func TestPrimaryPoolTrialDeadlineOnlyReleasesUnmetCommittedCapacity(t *testing.T) {
	started := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	deadline := metav1.NewTime(started.Add(45 * time.Minute))
	for _, scenario := range []string{"before", "at", "unwritten", "partial-ready", "registered-arrivals", "success-at-deadline"} {
		statement := map[string]string{
			"before":              "TrialDoesNotExpireBeforeItsDeadline",
			"at":                  "TrialExpiresAtItsDeadlineWithoutReducingPrimaryTarget",
			"unwritten":           "UnwrittenTrialDoesNotAuthorizeFallback",
			"partial-ready":       "PartialPrimaryReadinessReducesFallbackAllowance",
			"registered-arrivals": "ViableRegisteredArrivalsDoNotCreateFallbackDebt",
			"success-at-deadline": "PrimarySuccessAtDeadlinePreventsFallback",
		}[scenario]
		t.Run(statement, func(t *testing.T) {
			pair := &Pair{Phase: Healthy, PrimaryRequest: &Request{FromTarget: 3, ToTarget: 5, Started: metav1.NewTime(started), Deadline: &deadline, ReadyAtStart: 1, ReadyFingerprint: ReadyFingerprint([]string{"occupied"})}}
			primary := &Member{Target: 5, Ready: []string{"occupied"}, FailedNodes: map[string]bool{}, NotReadyCreated: map[string]metav1.Time{}}
			now := deadline.Time
			if scenario == "before" {
				now = now.Add(-time.Second)
			} else if scenario == "unwritten" {
				primary.Target = 3
			} else if scenario == "partial-ready" {
				primary.Ready = append(primary.Ready, "new-primary")
			} else if scenario == "registered-arrivals" {
				primary.NotReadyCreated["new-primary-1"] = metav1.NewTime(started.Add(time.Minute))
				primary.NotReadyCreated["new-primary-2"] = metav1.NewTime(started.Add(time.Minute))
			} else if scenario == "success-at-deadline" {
				primary.Ready = append(primary.Ready, "new-primary-1", "new-primary-2")
				pair.CompletePrimaryArrival(primary.Ready, primary.Target)
			}
			expired := pair.ExpirePrimaryTrial(primary, now)
			if scenario == "before" || scenario == "unwritten" || scenario == "success-at-deadline" {
				assert.False(t, expired)
				assert.Zero(t, pair.FallbackAllowance)
				return
			}
			assert.True(t, expired)
			assert.Equal(t, Degraded, pair.Phase)
			assert.True(t, pair.Primary.Failed)
			assert.Nil(t, pair.PrimaryRequest)
			wanted := 2
			if scenario == "partial-ready" {
				wanted = 1
			} else if scenario == "registered-arrivals" {
				wanted = 0
			}
			assert.Equal(t, wanted, pair.FallbackAllowance)
			assert.Equal(t, 5, primary.Target)
			assert.False(t, pair.ExpirePrimaryTrial(primary, now.Add(time.Hour)))
			assert.Equal(t, wanted, pair.FallbackAllowance)
		})
	}
}
func TestPrimaryPoolTrialDeadlineSurvivesRestartAndEnablesFallbackOnce(t *testing.T) {
	controller, clock := newFailoverTestController(t)
	provider := &testProvider{controller: controller}
	find := func(role string) *testGroup {
		for _, candidate := range provider.NodeGroups() {
			group := candidate.(*testGroup)
			if group.object.GetName() == "lin-"+role {
				return group
			}
		}
		t.Fatalf("missing %s", role)
		return nil
	}
	failoverTestFailure(controller, "lin", "primary", clock.Now())
	require.NoError(t, provider.Refresh())
	require.NoError(t, find("secondary").IncreaseSize(2))
	require.NoError(t, provider.Refresh())
	require.NoError(t, find("primary").IncreaseSize(1))
	require.NoError(t, provider.Refresh())
	require.True(t, find("secondary").GetCapacityPolicy().ScaleUpBlocked)
	cluster, err := controller.managementClient.Resource(schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "clusters"}).Namespace("tenant").Get(context.Background(), "test", metav1.GetOptions{})
	require.NoError(t, err)
	state, _, err := controller.failover.Store.Load(context.Background(), cluster)
	require.NoError(t, err)
	require.NotNil(t, state.Pairs["lin"].PrimaryRequest.Deadline)
	wantedDeadline := clock.Now().Add(45 * time.Minute)
	assert.True(t, wantedDeadline.Equal(state.Pairs["lin"].PrimaryRequest.Deadline.Time))
	require.NoError(t, controller.enableAzureFailover(false, 10*time.Second))
	controller.failover.Clock = clock
	clock.Step(46 * time.Minute)
	require.NoError(t, provider.Refresh())
	require.False(t, find("secondary").GetCapacityPolicy().ScaleUpBlocked, "provisioning deadline must admit still-unmet fallback without a terminal event")
	require.NoError(t, find("secondary").IncreaseSize(1))
	require.Error(t, find("secondary").IncreaseSize(1))
	target, err := find("primary").TargetSize()
	require.NoError(t, err)
	assert.Equal(t, 4, target)
}
func TestPrimaryPoolTrialDurationUsesGroupOptionsAndRejectsNonPositiveValues(t *testing.T) {
	for _, scenario := range []string{"default", "override", "invalid"} {
		statement := map[string]string{
			"default":  "TrialUsesTheDefaultProvisioningDuration",
			"override": "TrialUsesThePoolProvisioningDurationOverride",
			"invalid":  "NonPositiveProvisioningDurationIsRejected",
		}[scenario]
		t.Run(statement, func(t *testing.T) {
			controller, _ := newFailoverTestController(t)
			groups, err := controller.nodeGroups()
			require.NoError(t, err)
			group := groups[0].(*testGroup)
			wanted := 45 * time.Minute
			if scenario == "override" {
				group.options = map[string]string{config.DefaultMaxNodeProvisionTimeKey: "7m"}
				wanted = 7 * time.Minute
			} else if scenario == "invalid" {
				controller.failover.NodeGroupDefaults.MaxNodeProvisionTime = 0
			}
			duration, err := controller.failover.PrimaryTrialDuration(group)
			if scenario == "invalid" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, wanted, duration)
			}
		})
	}
}
