package failover

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

func (policy *Policy) AssertCurrentPool(ctx context.Context, group Group) error {
	member, err := policy.Member(ctx, group)
	if err != nil {
		return err
	}
	configuration, err := ReadConfiguration(ctx, policy.Store.Client, member.Cluster)
	if err != nil {
		return err
	}
	resolved, err := policy.ResolveConfiguredPools(ctx, member.Cluster, configuration)
	if err != nil {
		return err
	}
	annotations := group.Object().GetAnnotations()
	pair, found := configuration.Pairs[annotations[PairKey]]
	if !found {
		return fmt.Errorf("pool is not in current failover configuration")
	}
	reference := pair.Primary
	if annotations[RoleKey] == "secondary" {
		reference = pair.Secondary
	} else if annotations[RoleKey] != "primary" {
		return fmt.Errorf("invalid configured role")
	}
	if reference.Name != group.Object().GetName() || resolved[reference.Name] != group.Object().GetUID() {
		return fmt.Errorf("pool generation changed in failover configuration")
	}
	return nil
}
func (policy *Policy) ResolveConfiguredPools(ctx context.Context, cluster *unstructured.Unstructured, configuration *Configuration) (map[string]types.UID, error) {
	resolved := map[string]types.UID{}
	resource := policy.Store.Client.Resource(policy.Environment.MachinePoolResource()).Namespace(cluster.GetNamespace())
	for name, pair := range configuration.Pairs {
		for role, reference := range map[string]ConfiguredPool{"primary": pair.Primary, "secondary": pair.Secondary} {
			pool, err := resource.Get(ctx, reference.Name, metav1.GetOptions{})
			if err != nil {
				return nil, fmt.Errorf("pair %q role %s reference %q cannot be resolved: %w", name, role, reference.Name, err)
			}
			clusterName, found, err := unstructured.NestedString(pool.Object, "spec", "clusterName")
			annotations := pool.GetAnnotations()
			if err != nil || !found || clusterName != cluster.GetName() || pool.GetKind() != MachinePoolKind || pool.GetUID() == "" || !pool.GetDeletionTimestamp().IsZero() || annotations[PairKey] != name || annotations[RoleKey] != role {
				return nil, fmt.Errorf("configured pool %s has invalid identity or role", reference.Name)
			}
			owned := false
			for _, owner := range pool.GetOwnerReferences() {
				if owner.Kind == "Cluster" && owner.APIVersion == cluster.GetAPIVersion() && owner.Name == cluster.GetName() && owner.UID == cluster.GetUID() {
					owned = true
				}
			}
			if !owned {
				return nil, fmt.Errorf("configured pool %s has a different Cluster owner", reference.Name)
			}
			resolved[reference.Name] = pool.GetUID()
		}
	}
	return resolved, nil
}
