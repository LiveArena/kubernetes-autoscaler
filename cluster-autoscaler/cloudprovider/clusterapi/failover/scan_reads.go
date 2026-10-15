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

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
)

type scanReadCacheKey struct{}

type scanConfigurationRead struct {
	configuration *Configuration
	err           error
}

type scanReadCache struct {
	configurations map[string]scanConfigurationRead
}

func withScanReads(ctx context.Context) context.Context {
	return context.WithValue(ctx, scanReadCacheKey{}, &scanReadCache{configurations: map[string]scanConfigurationRead{}})
}

func readScanConfiguration(ctx context.Context, client dynamic.Interface, cluster *unstructured.Unstructured) (*Configuration, error) {
	cache, _ := ctx.Value(scanReadCacheKey{}).(*scanReadCache)
	key := cluster.GetAPIVersion() + "/" + cluster.GetNamespace() + "/" + cluster.GetName() + "/" + string(cluster.GetUID())
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
