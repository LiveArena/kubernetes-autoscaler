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
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
	"strings"
	"testing"
)

func TestConfigurationReaderAcceptsOnlyCompleteUnambiguousOwnedPairs(t *testing.T) {
	for _, fault := range []string{"valid", "multiple-pairs", "empty", "oversized", "owner", "identity", "version", "incomplete", "duplicate", "duplicate-role-field", "duplicate-pair-key", "unknown-field", "malformed", "trailing", "denied"} {
		statement := map[string]string{
			"valid":                "ValidOwnedConfigurationIsReadWithoutMutation",
			"multiple-pairs":       "AdditionalDistinctPairsAreReadWithoutMutation",
			"empty":                "ConfigurationWithoutPairsIsRejected",
			"oversized":            "OversizedValidConfigurationIsRejected",
			"owner":                "ForeignClusterOwnerIsRejected",
			"identity":             "StaleClusterIdentityIsRejected",
			"version":              "UnsupportedConfigurationVersionIsRejected",
			"incomplete":           "MissingDeclaredRoleIsRejected",
			"duplicate":            "ReusedPoolReferenceIsRejected",
			"duplicate-role-field": "DuplicateRoleFieldsAreRejected",
			"duplicate-pair-key":   "DuplicatePairKeysAreRejected",
			"unknown-field":        "UnknownConfigurationFieldsAreRejected",
			"malformed":            "MalformedConfigurationJSONIsRejected",
			"trailing":             "TrailingConfigurationJSONIsRejected",
			"denied":               "DeniedConfigurationReadReturnsNoAdmissionData",
		}[fault]
		t.Run(statement, func(t *testing.T) {
			cluster := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "cluster.x-k8s.io/v1beta2", "kind": "Cluster"}}
			cluster.SetName("test")
			cluster.SetNamespace("tenant")
			cluster.SetUID("cluster-uid")
			configuration := Configuration{APIVersion: ConfigVersion, Cluster: ClusterIdentity{Name: "test", Namespace: "tenant", UID: "cluster-uid"}, Pairs: map[string]ConfiguredPair{"lin": {Primary: ConfiguredPool{Name: "lin-primary"}, Secondary: ConfiguredPool{Name: "lin-secondary"}}, "win2": {Primary: ConfiguredPool{Name: "win2-primary"}, Secondary: ConfiguredPool{Name: "win2-secondary"}}}}
			switch fault {
			case "multiple-pairs":
				configuration.Pairs["batch"] = ConfiguredPair{Primary: ConfiguredPool{Name: "batch-primary"}, Secondary: ConfiguredPool{Name: "batch-secondary"}}
			case "empty":
				configuration.Pairs = map[string]ConfiguredPair{}
			case "identity":
				configuration.Cluster.UID = "other"
			case "version":
				configuration.APIVersion = "aiproducer.com/worker-failover/v99"
			case "incomplete":
				configuration.Pairs["win2"] = ConfiguredPair{Primary: ConfiguredPool{Name: "win2-primary"}}
			case "duplicate":
				configuration.Pairs["lin"] = ConfiguredPair{Primary: ConfiguredPool{Name: "lin-primary"}, Secondary: ConfiguredPool{Name: "lin-primary"}}
			}
			encoded, err := json.Marshal(configuration)
			require.NoError(t, err)
			payload := string(encoded)
			if fault == "oversized" {
				payload = strings.Repeat(" ", StateLimit) + payload
			} else if fault == "unknown-field" {
				payload = strings.TrimSuffix(payload, "}") + `,"mode":"active"}`
			} else if fault == "malformed" {
				payload = "{"
			} else if fault == "trailing" {
				payload += " {}"
			} else if fault == "duplicate-role-field" {
				payload = strings.Replace(payload, `"primary":{"name":"lin-primary"}`, `"primary":{"name":"unselected-primary"},"primary":{"name":"lin-primary"}`, 1)
			} else if fault == "duplicate-pair-key" {
				payload = strings.Replace(payload, `"pairs":{`, `"pairs":{"lin":{"primary":{"name":"unselected-primary"},"secondary":{"name":"unselected-secondary"}},`, 1)
			}
			object := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap", "data": map[string]interface{}{"config.json": payload}}}
			object.SetName("test-autoscaler-failover-config")
			object.SetNamespace("tenant")
			ownerUID := cluster.GetUID()
			if fault == "owner" {
				ownerUID = "other"
			}
			object.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: cluster.GetAPIVersion(), Kind: "Cluster", Name: cluster.GetName(), UID: ownerUID}})
			client := fakedynamic.NewSimpleDynamicClient(runtime.NewScheme(), object)
			if fault == "denied" {
				client.PrependReactor("get", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewForbidden(ConfigMaps.GroupResource(), object.GetName(), fmt.Errorf("denied"))
				})
			}
			actual, err := ReadConfiguration(context.Background(), client, cluster)
			if fault == "valid" || fault == "multiple-pairs" {
				require.NoError(t, err)
				assert.Equal(t, configuration, *actual)
			} else {
				require.Error(t, err)
				assert.Nil(t, actual)
			}
			require.Len(t, client.Actions(), 1)
			action := client.Actions()[0].(clienttesting.GetAction)
			assert.Equal(t, "get", action.GetVerb())
			assert.Equal(t, "tenant", action.GetNamespace())
			assert.Equal(t, "test-autoscaler-failover-config", action.GetName())
		})
	}
}
func TestMissingOrChangedConfigurationBlocksSecondaryPoolScaling(t *testing.T) {
	for _, fault := range []string{"missing-config", "wrong-clusterName", "missing-reference", "changed-after-admission", "other-pair-incomplete"} {
		statement := map[string]string{
			"missing-config":          "MissingConfigurationBlocksSecondaryPoolScaling",
			"wrong-clusterName":       "MismatchedPoolClusterBlocksSecondaryPoolScaling",
			"missing-reference":       "MissingConfiguredPoolBlocksSecondaryPoolScaling",
			"changed-after-admission": "ConfigurationChangesInvalidateCachedScalingAdmission",
			"other-pair-incomplete":   "IncompleteSharedPairConfigurationBlocksScaling",
		}[fault]
		t.Run(statement, func(t *testing.T) {
			controller, clock := newFailoverTestController(t)
			provider := &testProvider{controller: controller}
			failoverTestFailure(controller, "lin", "primary", clock.Now())
			require.NoError(t, provider.Refresh())
			var secondary *testGroup
			for _, candidate := range provider.NodeGroups() {
				if group := candidate.(*testGroup); group.object.GetName() == "lin-secondary" {
					secondary = group
				}
			}
			require.NotNil(t, secondary)
			require.False(t, secondary.GetCapacityPolicy().ScaleUpBlocked)
			maps := controller.managementClient.Resource(ConfigMaps).Namespace("tenant")
			if fault == "missing-config" {
				require.NoError(t, maps.Delete(context.Background(), "test-autoscaler-failover-config", metav1.DeleteOptions{}))
			} else if fault == "wrong-clusterName" {
				resource := controller.managementClient.Resource(controller.machinePoolResource).Namespace("tenant")
				pool, err := resource.Get(context.Background(), "lin-secondary", metav1.GetOptions{})
				require.NoError(t, err)
				require.NoError(t, unstructured.SetNestedField(pool.Object, "different-cluster", "spec", "clusterName"))
				_, err = resource.Update(context.Background(), pool, metav1.UpdateOptions{})
				require.NoError(t, err)
				require.NoError(t, controller.machinePoolInformer.Informer().GetStore().Update(pool))
			} else {
				object, err := maps.Get(context.Background(), "test-autoscaler-failover-config", metav1.GetOptions{})
				require.NoError(t, err)
				encoded, _, err := unstructured.NestedString(object.Object, "data", "config.json")
				require.NoError(t, err)
				configuration := Configuration{}
				require.NoError(t, json.Unmarshal([]byte(encoded), &configuration))
				name := "lin"
				if fault == "other-pair-incomplete" {
					name = "win2"
				}
				pair := configuration.Pairs[name]
				pair.Secondary.Name = "not-current-secondary"
				configuration.Pairs[name] = pair
				encodedBytes, err := json.Marshal(configuration)
				require.NoError(t, err)
				require.NoError(t, unstructured.SetNestedField(object.Object, string(encodedBytes), "data", "config.json"))
				_, err = maps.Update(context.Background(), object, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			if fault != "changed-after-admission" && fault != "other-pair-incomplete" {
				require.NoError(t, provider.Refresh())
				require.True(t, secondary.GetCapacityPolicy().ScaleUpBlocked)
			}
			require.Error(t, secondary.IncreaseSize(2))
			target, err := secondary.TargetSize()
			require.NoError(t, err)
			assert.Zero(t, target)
		})
	}
}
