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
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"strings"
	"testing"
	"time"
)

func TestStateAcceptsMoreThanTwoOpaquePairIdentifiers(t *testing.T) {
	state := &State{Version: StateVersion, ClusterUID: "cluster", Pairs: map[string]*Pair{"abc": {Phase: Healthy}, "batch": {Phase: Healthy}, "extra": {Phase: Healthy}, "worker.gpu": {Phase: Healthy}}}
	require.NoError(t, state.Validate("cluster"))
}
func TestPairIdentifiersUseBoundedNonemptyLabelSyntax(t *testing.T) {
	for _, identifier := range []string{"abc", "Batch_2", "worker.gpu", strings.Repeat("a", 63)} {
		assert.True(t, ValidPairIdentifier(identifier), identifier)
	}
	for _, identifier := range []string{"", " primary", "abc/def", "abc\n", strings.Repeat("a", 64)} {
		assert.False(t, ValidPairIdentifier(identifier), identifier)
	}
}
func renameFailoverTestPairIdentifiers(t *testing.T, controller *testEnvironment) {
	t.Helper()
	ctx := context.Background()
	resource := controller.managementClient.Resource(controller.machinePoolResource).Namespace("tenant")
	for _, object := range controller.machinePoolInformer.Informer().GetStore().List() {
		pool := object.(*unstructured.Unstructured).DeepCopy()
		annotations := pool.GetAnnotations()
		identifier := map[string]string{"lin": "abc", "win2": "batch"}[annotations[PairKey]]
		annotations[PairKey] = identifier
		pool.SetAnnotations(annotations)
		_, err := resource.Update(ctx, pool, metav1.UpdateOptions{})
		require.NoError(t, err)
		require.NoError(t, controller.machinePoolInformer.Informer().GetStore().Update(pool))
	}
	maps := controller.managementClient.Resource(ConfigMaps).Namespace("tenant")
	object, err := maps.Get(ctx, "test-autoscaler-failover-config", metav1.GetOptions{})
	require.NoError(t, err)
	encoded, _, err := unstructured.NestedString(object.Object, "data", "config.json")
	require.NoError(t, err)
	configuration := Configuration{}
	require.NoError(t, json.Unmarshal([]byte(encoded), &configuration))
	configuration.Pairs = map[string]ConfiguredPair{"abc": configuration.Pairs["lin"], "batch": configuration.Pairs["win2"]}
	data, err := json.Marshal(configuration)
	require.NoError(t, err)
	require.NoError(t, unstructured.SetNestedField(object.Object, string(data), "data", "config.json"))
	_, err = maps.Update(ctx, object, metav1.UpdateOptions{})
	require.NoError(t, err)
}
func TestPairIdentifiersDoNotChangePoolAdmissionRules(t *testing.T) {
	for _, mode := range []string{"active", "freeze", "disabled", "single-pair"} {
		statement := map[string]string{
			"active":      "ArbitraryIdentifiersPermitActiveFailureTriggeredScaling",
			"freeze":      "ArbitraryIdentifiersDoNotBypassFrozenAdmission",
			"disabled":    "ArbitraryIdentifiersDoNotBypassDisabledAdmission",
			"single-pair": "SingleDeclaredPairDoesNotRequireDeploymentSpecificIdentifiers",
		}[mode]
		t.Run(statement, func(t *testing.T) {
			controller, clock := newFailoverTestController(t)
			renameFailoverTestPairIdentifiers(t, controller)
			provider := &testProvider{controller: controller}
			ctx := context.Background()
			cluster, err := controller.managementClient.Resource(schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "clusters"}).Namespace("tenant").Get(ctx, "test", metav1.GetOptions{})
			require.NoError(t, err)
			if mode == "single-pair" {
				maps := controller.managementClient.Resource(ConfigMaps).Namespace("tenant")
				object, err := maps.Get(ctx, "test-autoscaler-failover-config", metav1.GetOptions{})
				require.NoError(t, err)
				configuration := Configuration{APIVersion: ConfigVersion, Cluster: ClusterIdentity{Name: "test", Namespace: "tenant", UID: cluster.GetUID()}, Pairs: map[string]ConfiguredPair{"abc": {Primary: ConfiguredPool{Name: "lin-primary"}, Secondary: ConfiguredPool{Name: "lin-secondary"}}}}
				data, err := json.Marshal(configuration)
				require.NoError(t, err)
				require.NoError(t, unstructured.SetNestedField(object.Object, string(data), "data", "config.json"))
				_, err = maps.Update(ctx, object, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			configuration, err := ReadConfiguration(ctx, controller.managementClient, cluster)
			require.NoError(t, err)
			require.Contains(t, configuration.Pairs, "abc")
			assert.NotContains(t, configuration.Pairs, "lin")
			if mode == "disabled" {
				controller.failover.StopWriter()
				controller.failover = nil
			} else if mode == "freeze" {
				require.NoError(t, controller.enableAzureFailover(true, 10*time.Second))
				controller.failover.Clock = clock
			}
			if mode != "disabled" {
				failoverTestFailure(controller, "lin", "primary", clock.Now())
			}
			require.NoError(t, provider.Refresh())
			var secondary *testGroup
			for _, candidate := range provider.NodeGroups() {
				group := candidate.(*testGroup)
				if group.object.GetName() == "lin-secondary" {
					secondary = group
				}
			}
			require.NotNil(t, secondary)
			if mode == "disabled" || mode == "freeze" {
				assert.True(t, secondary.GetCapacityPolicy().ScaleUpBlocked)
				require.Error(t, secondary.IncreaseSize(1))
			} else {
				assert.False(t, secondary.GetCapacityPolicy().ScaleUpBlocked)
				require.NoError(t, secondary.IncreaseSize(2))
				state, _, err := controller.failover.Store.Load(ctx, cluster)
				require.NoError(t, err)
				require.Contains(t, state.Pairs, "abc")
				assert.Equal(t, types.UID("lin-primary"), state.Pairs["abc"].Primary.PoolUID)
				assert.Equal(t, types.UID("lin-secondary"), state.Pairs["abc"].Secondary.PoolUID)
			}
		})
	}
}
