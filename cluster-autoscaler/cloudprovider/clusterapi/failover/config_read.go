package failover

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	strictjson "sigs.k8s.io/json"
	"strings"
)

func ReadConfiguration(ctx context.Context, client dynamic.Interface, cluster *unstructured.Unstructured) (*Configuration, error) {
	object, err := client.Resource(ConfigMaps).Namespace(cluster.GetNamespace()).Get(ctx, cluster.GetName()+"-autoscaler-failover-config", metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if object.GetNamespace() != cluster.GetNamespace() || !object.GetDeletionTimestamp().IsZero() {
		return nil, fmt.Errorf("invalid failover configuration namespace or lifecycle")
	}
	owners := 0
	for _, owner := range object.GetOwnerReferences() {
		if owner.Kind == "Cluster" {
			if owner.Name != cluster.GetName() || owner.UID != cluster.GetUID() || owner.APIVersion != cluster.GetAPIVersion() {
				return nil, fmt.Errorf("failover configuration has a different Cluster owner")
			}
			owners++
		}
	}
	if owners != 1 || cluster.GetUID() == "" {
		return nil, fmt.Errorf("failover configuration requires one actual Cluster owner")
	}
	encoded, found, err := unstructured.NestedString(object.Object, "data", "config.json")
	if err != nil || !found || len(encoded) > StateLimit {
		return nil, fmt.Errorf("missing, invalid, or oversized failover configuration")
	}
	configuration := &Configuration{}
	strictErrors, err := strictjson.UnmarshalStrict([]byte(encoded), configuration, strictjson.DisallowDuplicateFields, strictjson.DisallowUnknownFields)
	if err != nil {
		return nil, fmt.Errorf("decode failover configuration: %w", err)
	}
	if len(strictErrors) > 0 {
		return nil, fmt.Errorf("invalid failover configuration fields: %v", strictErrors)
	}
	identity := configuration.Cluster
	if configuration.APIVersion != ConfigVersion || identity.Name != cluster.GetName() || identity.Namespace != cluster.GetNamespace() || identity.UID != cluster.GetUID() {
		return nil, fmt.Errorf("unsupported or stale failover configuration identity")
	}
	if len(configuration.Pairs) == 0 || len(configuration.Pairs) > PairLimit {
		return nil, fmt.Errorf("failover configuration requires between one and %d complete pairs", PairLimit)
	}
	names := map[string]bool{}
	for name, pair := range configuration.Pairs {
		if !ValidPairIdentifier(name) {
			return nil, fmt.Errorf("invalid failover pair identifier %q", name)
		}
		for _, pool := range []ConfiguredPool{pair.Primary, pair.Secondary} {
			if pool.Name == "" || strings.Contains(pool.Name, "/") || names[pool.Name] {
				return nil, fmt.Errorf("missing or ambiguous failover pool reference")
			}
			names[pool.Name] = true
		}
	}
	return configuration, nil
}
