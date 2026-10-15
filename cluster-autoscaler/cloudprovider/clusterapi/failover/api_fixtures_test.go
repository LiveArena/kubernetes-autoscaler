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
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

type disposableAPIFixture struct {
	context       context.Context
	configuration *rest.Config
	client        dynamic.Interface
	discovery     discovery.DiscoveryInterface
	namespace     *unstructured.Unstructured
	cluster       *unstructured.Unstructured
}

func failoverTestCRD(plural, kind string, withScale bool) *unstructured.Unstructured {
	version := map[string]interface{}{"name": "v1beta2", "served": true, "storage": true, "schema": map[string]interface{}{"openAPIV3Schema": map[string]interface{}{"type": "object", "properties": map[string]interface{}{"spec": map[string]interface{}{"type": "object", "x-kubernetes-preserve-unknown-fields": true, "properties": map[string]interface{}{"replicas": map[string]interface{}{"type": "integer", "minimum": int64(0)}}}, "status": map[string]interface{}{"type": "object", "properties": map[string]interface{}{"replicas": map[string]interface{}{"type": "integer"}}}}}}}
	if withScale {
		version["subresources"] = map[string]interface{}{"scale": map[string]interface{}{"specReplicasPath": ".spec.replicas", "statusReplicasPath": ".status.replicas"}}
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition", "metadata": map[string]interface{}{"name": plural + ".cluster.x-k8s.io"}, "spec": map[string]interface{}{"group": "cluster.x-k8s.io", "scope": "Namespaced", "names": map[string]interface{}{"plural": plural, "singular": plural[:len(plural)-1], "kind": kind, "listKind": kind + "List"}, "versions": []interface{}{version}}}}
}

func newDisposableAPIFixture(t *testing.T) *disposableAPIFixture {
	t.Helper()
	endpoint := os.Getenv("INF761_TEST_API")
	if testing.Short() || endpoint == "" || os.Getenv("INF761_TEST_API_DISPOSABLE") != "1" {
		t.Skip("requires an explicitly authorized disposable API via INF761_TEST_API and INF761_TEST_API_DISPOSABLE=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	configuration := &rest.Config{Host: endpoint, BearerToken: "inf761-admin", TLSClientConfig: rest.TLSClientConfig{Insecure: true}, Timeout: 10 * time.Second, QPS: 50, Burst: 100}
	client, err := dynamic.NewForConfig(configuration)
	require.NoError(t, err)
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(configuration)
	require.NoError(t, err)
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, time.Minute, true, func(ctx context.Context) (bool, error) {
		_, err := discoveryClient.ServerVersion()
		return err == nil, nil
	}))
	crds := client.Resource(schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"})
	infrastructureDefinition := failoverTestCRD("azuremachinepools", "AzureMachinePool", false)
	infrastructureDefinition.SetName("azuremachinepools." + AzureAPIGroup)
	require.NoError(t, unstructured.SetNestedField(infrastructureDefinition.Object, AzureAPIGroup, "spec", "group"))
	for _, definition := range []*unstructured.Unstructured{failoverTestCRD("clusters", "Cluster", false), failoverTestCRD("machinepools", "MachinePool", true), infrastructureDefinition} {
		_, err := crds.Create(ctx, definition, metav1.CreateOptions{})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, crds.Delete(context.Background(), definition.GetName(), metav1.DeleteOptions{}))
			require.NoError(t, wait.PollUntilContextTimeout(context.Background(), 50*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
				_, err := crds.Get(ctx, definition.GetName(), metav1.GetOptions{})
				return apierrors.IsNotFound(err), nil
			}))
		})
	}
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		resources, err := discoveryClient.ServerResourcesForGroupVersion("cluster.x-k8s.io/v1beta2")
		if err != nil {
			return false, nil
		}
		for _, resource := range resources.APIResources {
			if resource.Name == "machinepools/scale" {
				return true, nil
			}
		}
		return false, nil
	}))
	namespaces := client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "namespaces"})
	namespace, err := namespaces.Create(ctx, &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]interface{}{"generateName": "inf761-"}}}, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, namespaces.Delete(context.Background(), namespace.GetName(), metav1.DeleteOptions{}))
	})
	cluster := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "cluster.x-k8s.io/v1beta2", "kind": "Cluster", "metadata": map[string]interface{}{"name": "test", "namespace": namespace.GetName()}}}
	cluster, err = client.Resource(schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "clusters"}).Namespace(namespace.GetName()).Create(ctx, cluster, metav1.CreateOptions{})
	require.NoError(t, err)
	return &disposableAPIFixture{context: ctx, configuration: configuration, client: client, discovery: discoveryClient, namespace: namespace, cluster: cluster}
}
