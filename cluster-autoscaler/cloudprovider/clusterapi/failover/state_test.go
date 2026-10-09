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
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
	"strings"
	"testing"
)

func TestStatePersistenceSurvivesRestartAndFailsClosedOnUnsafeUpdates(t *testing.T) {
	ctx := context.Background()
	cluster := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "cluster.x-k8s.io/v1beta2", "kind": "Cluster"}}
	cluster.SetName("test")
	cluster.SetNamespace("tenant")
	cluster.SetUID("cluster-uid")
	client := fakedynamic.NewSimpleDynamicClient(runtime.NewScheme())
	store := &Store{Client: client}
	state, err := store.Reconcile(ctx, cluster, func(state *State) error {
		state.Pairs["lin"] = &Pair{Phase: Degraded, Primary: Role{Failed: true}}
		state.Pairs["batch"] = &Pair{Phase: Healthy}
		state.Pairs["extra"] = &Pair{Phase: Healthy}
		return nil
	})
	require.NoError(t, err)
	assert.True(t, state.Pairs["lin"].Primary.Failed)
	restarted := &Store{Client: client}
	loaded, _, err := restarted.Load(ctx, cluster)
	require.NoError(t, err)
	assert.Len(t, loaded.Pairs, 3)
	assert.Equal(t, state, loaded)
	conflicts := 0
	client.PrependReactor("update", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		conflicts++
		if conflicts == 1 {
			return true, nil, apierrors.NewConflict(ConfigMaps.GroupResource(), "test", fmt.Errorf("concurrent writer"))
		}
		return false, nil, nil
	})
	_, err = store.Reconcile(ctx, cluster, func(state *State) error {
		state.Pairs["lin"].Primary.ObservedTarget = 3
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, conflicts)
	cluster.SetUID("replacement-cluster")
	_, _, err = restarted.Load(ctx, cluster)
	require.Error(t, err)
	cluster.SetUID("cluster-uid")
	client.PrependReactor("update", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(ConfigMaps.GroupResource(), "test", fmt.Errorf("denied"))
	})
	state, err = store.Reconcile(ctx, cluster, func(state *State) error {
		state.Pairs["lin"].Secondary.Failed = true
		return nil
	})
	require.Error(t, err)
	assert.Nil(t, state)
}

func TestStateReaderRejectsMalformedAmbiguousForeignOrOversizedState(t *testing.T) {
	for _, scenario := range []struct{ statement, encoded string }{
		{"MalformedJSONIsRejected", "{"},
		{"UnknownSchemaVersionIsRejected", `{"version":99,"clusterUID":"cluster-uid","pairs":{}}`},
		{"UnsupportedLegacySchemaIsRejectedWithoutReset", `{"version":1,"clusterUID":"cluster-uid","pairs":{}}`},
		{"ForeignClusterStateIsRejected", `{"version":2,"clusterUID":"different","pairs":{}}`},
		{"NullPairStateIsRejected", `{"version":2,"clusterUID":"cluster-uid","pairs":{"lin":null}}`},
		{"InconsistentDegradationStateIsRejected", `{"version":2,"clusterUID":"cluster-uid","pairs":{"lin":{"phase":"Degraded"}}}`},
		{"UnknownStateFieldsAreRejected", `{"version":2,"clusterUID":"cluster-uid","pairs":{},"unknown":true}`},
		{"TrailingStateJSONIsRejected", `{"version":2,"clusterUID":"cluster-uid","pairs":{}} {}`},
		{"DuplicateSchemaVersionFieldsAreRejected", `{"version":1,"version":2,"clusterUID":"cluster-uid","pairs":{}}`},
		{"DuplicatePairKeysAreRejected", `{"version":2,"clusterUID":"cluster-uid","pairs":{"abc":{"phase":"Degraded","primary":{"failed":true}},"abc":{"phase":"Healthy"}}}`},
		{"DuplicateRoleFieldsAreRejected", `{"version":2,"clusterUID":"cluster-uid","pairs":{"abc":{"phase":"Healthy","primary":{"failed":true},"primary":{"failed":false}}}}`},
		{"OversizedValidStateIsRejected", strings.Repeat(" ", StateLimit) + `{"version":2,"clusterUID":"cluster-uid","pairs":{}}`},
	} {
		t.Run(scenario.statement, func(t *testing.T) {
			cluster := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "cluster.x-k8s.io/v1beta2", "kind": "Cluster"}}
			cluster.SetName("test")
			cluster.SetNamespace("tenant")
			cluster.SetUID("cluster-uid")
			object := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap", "data": map[string]interface{}{"state.json": scenario.encoded}}}
			object.SetName("test-autoscaler-failover-state")
			object.SetNamespace("tenant")
			object.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: cluster.GetAPIVersion(), Kind: "Cluster", Name: cluster.GetName(), UID: cluster.GetUID()}})
			store := &Store{Client: fakedynamic.NewSimpleDynamicClient(runtime.NewScheme(), object)}
			state, _, err := store.Load(context.Background(), cluster)
			require.Error(t, err)
			assert.Nil(t, state)
		})
	}
}
func TestCancelledStateReconciliationCannotWriteOrStartAnotherAPIOperation(t *testing.T) {
	for _, boundary := range []string{"before-read", "after-read", "before-write", "conflict"} {
		statement := map[string]string{
			"before-read":  "CancellationBeforeReadPreventsAllAPIOperations",
			"after-read":   "CancellationAfterReadPreventsStateWrites",
			"before-write": "CancellationDuringReconciliationPreventsStateWrites",
			"conflict":     "CancellationAfterConflictPreventsAnotherRetry",
		}[boundary]
		t.Run(statement, func(t *testing.T) {
			controller, _ := newFailoverTestController(t)
			provider := &testProvider{controller: controller}
			require.NoError(t, provider.Refresh())
			client := controller.managementClient.(*fakedynamic.FakeDynamicClient)
			cluster, err := client.Resource(schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "clusters"}).Namespace("tenant").Get(context.Background(), "test", metav1.GetOptions{})
			require.NoError(t, err)
			before, _, err := controller.failover.Store.Load(context.Background(), cluster)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if boundary == "before-read" {
				cancel()
			} else if boundary == "after-read" {
				client.PrependReactor("get", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
					cancel()
					return false, nil, nil
				})
			} else if boundary == "conflict" {
				client.PrependReactor("update", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
					cancel()
					return true, nil, apierrors.NewConflict(ConfigMaps.GroupResource(), "test-autoscaler-failover-state", fmt.Errorf("paused writer lost its state revision"))
				})
			}
			client.ClearActions()
			result, err := controller.failover.Store.Reconcile(ctx, cluster, func(state *State) error {
				state.Pairs["lin"].Primary.ObservedTarget++
				if boundary == "before-write" {
					cancel()
				}
				return nil
			})
			require.ErrorIs(t, err, context.Canceled)
			assert.Nil(t, result)
			wantedCalls := 1
			if boundary == "before-read" {
				wantedCalls = 0
			} else if boundary == "conflict" {
				wantedCalls = 2
			}
			assert.Len(t, client.Actions(), wantedCalls, "cancelled writer must not start another API operation")
			after, _, err := controller.failover.Store.Load(context.Background(), cluster)
			require.NoError(t, err)
			assert.Equal(t, before, after)
		})
	}
}
