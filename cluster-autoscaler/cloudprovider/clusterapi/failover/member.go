package failover

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Member combines a validated pool identity, current target and workload capacity observations.
type Member struct {
	Group           Group
	Cluster         *unstructured.Unstructured
	Infrastructure  *unstructured.Unstructured
	Ready           []string
	FailedNodes     map[string]bool
	NotReadyCreated map[string]metav1.Time
	Target          int
}

// Member resolves live ownership and records usable workload nodes for a discovered group.
func (policy *Policy) Member(ctx context.Context, group Group) (*Member, error) {
	pool := group.Object()
	if pool.GetKind() != MachinePoolKind || pool.GetUID() == "" || !pool.GetDeletionTimestamp().IsZero() {
		return nil, fmt.Errorf("failover requires an active MachinePool with UID")
	}
	var cluster *unstructured.Unstructured
	for _, owner := range pool.GetOwnerReferences() {
		if owner.Kind != "Cluster" || owner.UID == "" {
			continue
		}
		version, err := schema.ParseGroupVersion(owner.APIVersion)
		if err != nil || version.Group != policy.Environment.MachinePoolResource().Group {
			return nil, fmt.Errorf("invalid owning Cluster reference")
		}
		cluster, err = policy.Store.Client.Resource(version.WithResource("clusters")).Namespace(pool.GetNamespace()).Get(ctx, owner.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if cluster.GetUID() != owner.UID || !cluster.GetDeletionTimestamp().IsZero() {
			return nil, fmt.Errorf("stale or deleting owning Cluster")
		}
		break
	}
	if cluster == nil {
		return nil, fmt.Errorf("missing owning Cluster reference")
	}
	clusterName, found, err := unstructured.NestedString(pool.Object, "spec", "clusterName")
	if err != nil || !found || clusterName != cluster.GetName() {
		return nil, fmt.Errorf("MachinePool clusterName does not match its owning Cluster")
	}
	ref, found, err := unstructured.NestedStringMap(pool.Object, "spec", "template", "spec", "infrastructureRef")
	if err != nil || !found || ref["kind"] != "AzureMachinePool" || ref["name"] == "" || (ref["namespace"] != "" && ref["namespace"] != pool.GetNamespace()) {
		return nil, fmt.Errorf("invalid AzureMachinePool infrastructure reference")
	}
	version, err := schema.ParseGroupVersion(ref["apiVersion"])
	if err != nil || version.Group != AzureAPIGroup {
		return nil, fmt.Errorf("invalid infrastructure API version")
	}
	infrastructure, err := policy.Store.Client.Resource(version.WithResource("azuremachinepools")).Namespace(pool.GetNamespace()).Get(ctx, ref["name"], metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	owned := false
	for _, owner := range infrastructure.GetOwnerReferences() {
		if owner.Kind == MachinePoolKind && owner.UID == pool.GetUID() && owner.Name == pool.GetName() {
			owned = true
		}
	}
	if !owned || infrastructure.GetUID() == "" || !infrastructure.GetDeletionTimestamp().IsZero() {
		return nil, fmt.Errorf("stale, unowned, or deleting AzureMachinePool")
	}
	target, err := group.Replicas()
	if err != nil {
		return nil, err
	}
	member := &Member{Group: group, Cluster: cluster, Infrastructure: infrastructure, Target: target, FailedNodes: map[string]bool{}, NotReadyCreated: map[string]metav1.Time{}}
	providerIDs, err := group.ProviderIDs()
	if err != nil {
		return nil, err
	}
	for _, providerID := range providerIDs {
		node, err := policy.Environment.FindNodeByProviderID(providerID)
		if err != nil {
			return nil, err
		}
		if node == nil || !node.DeletionTimestamp.IsZero() {
			continue
		}
		ready := false
		for _, condition := range node.Status.Conditions {
			ready = ready || condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue
		}
		if ready {
			member.Ready = append(member.Ready, string(node.UID)+"/"+node.Name)
		} else {
			member.NotReadyCreated[node.Name] = node.CreationTimestamp
			machine, err := policy.Environment.FindMachineByProviderID(providerID)
			if err != nil {
				return nil, err
			}
			if machine != nil && machine.GetKind() == "AzureMachinePoolMachine" {
				observation, terminal := TerminalAMPMFailure(machine)
				if terminal && observation.Namespace == infrastructure.GetNamespace() && observation.OwnerName == infrastructure.GetName() && observation.OwnerUID == infrastructure.GetUID() {
					member.FailedNodes[node.Name] = true
				}
			}
		}
	}
	return member, nil
}
