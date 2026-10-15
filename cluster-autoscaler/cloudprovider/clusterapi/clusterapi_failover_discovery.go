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
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/restmapper"
)

type failoverResourceResolver struct {
	mutex     sync.Mutex
	discovery func() discovery.DiscoveryInterface
	mapper    *restmapper.DeferredDiscoveryRESTMapper
}

func (resolver *failoverResourceResolver) ResolveResource(kind schema.GroupKind, version string, refresh bool) (schema.GroupVersionResource, error) {
	resolver.mutex.Lock()
	defer resolver.mutex.Unlock()
	if resolver.mapper == nil {
		client := resolver.discovery()
		if client == nil {
			return schema.GroupVersionResource{}, fmt.Errorf("management API discovery is unavailable")
		}
		resolver.mapper = restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(client))
	} else if refresh {
		resolver.mapper.Reset()
	}
	lookup := func() (*meta.RESTMapping, error) {
		if version != "" {
			mapping, err := resolver.mapper.RESTMapping(kind, version)
			if !meta.IsNoMatchError(err) {
				return mapping, err
			}
		}
		return resolver.mapper.RESTMapping(kind)
	}
	mapping, err := lookup()
	if meta.IsNoMatchError(err) && !refresh {
		resolver.mapper.Reset()
		mapping, err = lookup()
	}
	if err != nil {
		return schema.GroupVersionResource{}, err
	}
	if mapping.Scope.Name() != meta.RESTScopeNameNamespace || mapping.Resource.Group != kind.Group || mapping.GroupVersionKind.Kind != kind.Kind {
		return schema.GroupVersionResource{}, fmt.Errorf("discovery returned an incompatible resource for %s", kind)
	}
	return mapping.Resource, nil
}
