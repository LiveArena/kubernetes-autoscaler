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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func (store *Store) validateLifecycleBinding(ctx context.Context, cluster *unstructured.Unstructured, name string, configured ConfiguredPair, pair *Pair) error {
	for role, reference := range map[string]ConfiguredPool{"primary": configured.Primary, "secondary": configured.Secondary} {
		record := pair.Primary
		if role == "secondary" {
			record = pair.Secondary
		}
		version, err := schema.ParseGroupVersion(record.PoolVersion)
		if err != nil || version.Group == "" || record.PoolName != reference.Name {
			return fmt.Errorf("invalid registered pool reference for pair %q", name)
		}
		pool, err := store.Client.Resource(version.WithResource("machinepools")).Namespace(cluster.GetNamespace()).Get(ctx, reference.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		annotations := pool.GetAnnotations()
		if pool.GetUID() != record.PoolUID || pool.GetKind() != MachinePoolKind || !pool.GetDeletionTimestamp().IsZero() || annotations[PairKey] != name || annotations[RoleKey] != role {
			return fmt.Errorf("current physical member does not match pair %q registration", name)
		}
		clusterName, found, err := unstructured.NestedString(pool.Object, "spec", "clusterName")
		if err != nil || !found || clusterName != cluster.GetName() || pool.GetAPIVersion() != record.PoolVersion {
			return fmt.Errorf("registered pool Cluster or API identity changed")
		}
		reference, found, err := unstructured.NestedStringMap(pool.Object, "spec", "template", "spec", "infrastructureRef")
		if err != nil || !found || reference["kind"] != "AzureMachinePool" || reference["name"] != record.InfrastructureName || reference["namespace"] != "" && reference["namespace"] != cluster.GetNamespace() {
			return fmt.Errorf("registered pool infrastructure reference changed")
		}
		registered, err := schema.ParseGroupVersion(record.InfrastructureVersion)
		if err != nil || registered.Group != AzureAPIGroup || registered.Version == "" {
			return fmt.Errorf("invalid registered infrastructure API version")
		}
		referenced, err := infrastructureReferenceVersion(reference, func(group string) (string, error) { return registered.Version, nil })
		if err != nil || referenced != registered {
			return fmt.Errorf("registered pool infrastructure API identity changed")
		}
		owned := false
		for _, owner := range pool.GetOwnerReferences() {
			owned = owned || owner.Kind == "Cluster" && owner.Name == cluster.GetName() && owner.UID == cluster.GetUID() && owner.APIVersion == cluster.GetAPIVersion()
		}
		if !owned {
			return fmt.Errorf("registered member has a different Cluster owner")
		}
		version, err = schema.ParseGroupVersion(record.InfrastructureVersion)
		if err != nil || version.Group != AzureAPIGroup || record.InfrastructureName == "" {
			return fmt.Errorf("invalid registered infrastructure identity")
		}
		infrastructure, err := store.Client.Resource(version.WithResource("azuremachinepools")).Namespace(cluster.GetNamespace()).Get(ctx, record.InfrastructureName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if infrastructure.GetUID() != record.InfrastructureUID || !infrastructure.GetDeletionTimestamp().IsZero() {
			return fmt.Errorf("registered infrastructure UID changed")
		}
		owned = false
		for _, owner := range infrastructure.GetOwnerReferences() {
			owned = owned || owner.Kind == MachinePoolKind && owner.Name == pool.GetName() && owner.UID == pool.GetUID()
		}
		if !owned {
			return fmt.Errorf("registered infrastructure has a different pool owner")
		}
	}
	return nil
}
