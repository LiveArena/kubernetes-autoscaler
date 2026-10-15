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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func (store *Store) resourceRoute(kind schema.GroupKind, apiVersion string, refresh bool) (schema.GroupVersionResource, error) {
	plural := map[string]string{MachinePoolKind: "machinepools", "AzureMachinePool": "azuremachinepools"}[kind.Kind]
	if kind.Group == "" || plural == "" || kind.Kind == "AzureMachinePool" && kind.Group != AzureAPIGroup {
		return schema.GroupVersionResource{}, fmt.Errorf("unsupported failover resource identity %s", kind)
	}
	hint, err := schema.ParseGroupVersion(apiVersion)
	if err != nil || hint.Group != kind.Group || hint.Version == "" {
		return schema.GroupVersionResource{}, fmt.Errorf("invalid failover resource route %q for %s", apiVersion, kind)
	}
	if store.Resolver == nil {
		return hint.WithResource(plural), nil
	}
	route, err := store.Resolver.ResolveResource(kind, hint.Version, refresh)
	if err != nil {
		return schema.GroupVersionResource{}, err
	}
	if route.Group != kind.Group || route.Version == "" || route.Resource != plural {
		return schema.GroupVersionResource{}, fmt.Errorf("discovery returned an incompatible route for %s", kind)
	}
	return route, nil
}

func (store *Store) readResource(ctx context.Context, kind schema.GroupKind, apiVersion, namespace, name string) (*unstructured.Unstructured, error) {
	route, err := store.resourceRoute(kind, apiVersion, false)
	if err != nil {
		return nil, err
	}
	object, err := store.Client.Resource(route).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) && store.Resolver != nil {
		current, discoveryErr := store.resourceRoute(kind, apiVersion, true)
		if discoveryErr != nil {
			return nil, fmt.Errorf("cannot confirm absence of %s %s/%s: %w", kind, namespace, name, discoveryErr)
		}
		if current != route {
			object, err = store.Client.Resource(current).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		}
	}
	if err != nil {
		return nil, err
	}
	actual, parseErr := schema.ParseGroupVersion(object.GetAPIVersion())
	if parseErr != nil || actual.Group != kind.Group || actual.Version == "" || object.GetKind() != kind.Kind || object.GetNamespace() != namespace || object.GetName() != name {
		return nil, fmt.Errorf("resource response does not match %s %s/%s", kind, namespace, name)
	}
	return object, nil
}
