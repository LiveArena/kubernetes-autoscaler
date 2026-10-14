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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	fakescale "k8s.io/client-go/scale/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestScanReusesClusterAndConfigurationReadsWithoutResolvingUnpairedCapacity(t *testing.T) {
	environment, _ := newActiveLifecycleEnvironment(t, "lin")
	ctx := context.Background()
	client := environment.managementClient.(*fakedynamic.FakeDynamicClient)
	for _, role := range []string{"primary", "secondary"} {
		pool, err := client.Resource(environment.machinePoolResource).Namespace("tenant").Get(ctx, "win2-"+role, metav1.GetOptions{})
		require.NoError(t, err)
		annotations := pool.GetAnnotations()
		delete(annotations, PairKey)
		delete(annotations, RoleKey)
		pool.SetAnnotations(annotations)
		_, err = client.Resource(environment.machinePoolResource).Namespace("tenant").Update(ctx, pool, metav1.UpdateOptions{})
		require.NoError(t, err)
		require.NoError(t, environment.machinePoolInformer.store.Update(pool))
	}
	client.ClearActions()
	scales := environment.managementScaleClient.(*fakescale.FakeScaleClient)
	scales.ClearActions()
	environment.failover.Refresh()
	clusterReads, configReads, unpairedInfrastructureReads := 0, 0, 0
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" {
			continue
		}
		name := action.(clienttesting.GetAction).GetName()
		switch action.GetResource().Resource {
		case "clusters":
			clusterReads++
		case "configmaps":
			if name == "test-autoscaler-failover-config" {
				configReads++
			}
		case "azuremachinepools":
			if name == "win2-primary" || name == "win2-secondary" {
				unpairedInfrastructureReads++
			}
		}
	}
	assert.Equal(t, 1, clusterReads, "one trusted Cluster read per scan is enough for all members")
	assert.LessOrEqual(t, configReads, 2, "reuse read-only config but keep the store's authoritative intent check")
	assert.Zero(t, unpairedInfrastructureReads)
	for _, action := range scales.Actions() {
		if action.GetVerb() == "get" {
			assert.NotContains(t, action.(clienttesting.GetAction).GetName(), "win2-", "unpaired scope discovery does not need capacity reads")
		}
	}
	setPairLifecycleIntent(t, environment, "lin", "Retiring", "registration-lin", false)
	require.Error(t, findLifecycleGroup(t, environment, "lin-primary").IncreaseSize(1), "scan memoization cannot leak into authoritative write admission")
}
