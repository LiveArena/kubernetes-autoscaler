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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

type scanReadCacheKey struct{}

type scanClusterRead struct {
	cluster *unstructured.Unstructured
	err     error
}

type scanConfigurationRead struct {
	configuration *Configuration
	err           error
}

type scanReadCache struct {
	clusters       map[string]scanClusterRead
	configurations map[string]scanConfigurationRead
}

func withScanReads(ctx context.Context) context.Context {
	return context.WithValue(ctx, scanReadCacheKey{}, &scanReadCache{clusters: map[string]scanClusterRead{}, configurations: map[string]scanConfigurationRead{}})
}

func readScanCluster(ctx context.Context, client dynamic.Interface, resource schema.GroupVersionResource, namespace, name string) (*unstructured.Unstructured, error) {
	cache, _ := ctx.Value(scanReadCacheKey{}).(*scanReadCache)
	key := resource.String() + "/" + namespace + "/" + name
	if cache != nil {
		if read, found := cache.clusters[key]; found {
			return read.cluster, read.err
		}
	}
	cluster, err := client.Resource(resource).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if cache != nil {
		cache.clusters[key] = scanClusterRead{cluster: cluster, err: err}
	}
	return cluster, err
}

func readScanConfiguration(ctx context.Context, client dynamic.Interface, cluster *unstructured.Unstructured) (*Configuration, error) {
	cache, _ := ctx.Value(scanReadCacheKey{}).(*scanReadCache)
	key := cluster.GetNamespace() + "/" + string(cluster.GetUID())
	if cache != nil {
		if read, found := cache.configurations[key]; found {
			return read.configuration, read.err
		}
	}
	configuration, err := ReadConfiguration(ctx, client, cluster)
	if cache != nil {
		cache.configurations[key] = scanConfigurationRead{configuration: configuration, err: err}
	}
	return configuration, err
}
