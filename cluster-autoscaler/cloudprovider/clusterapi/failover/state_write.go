package failover

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/retry"
)

// Reconcile applies a validated state update and retries conflicting management API writes.
// The update callback may run more than once when a conflict is retried.
func (store *Store) Reconcile(ctx context.Context, cluster *unstructured.Unstructured, update func(*State) error) (*State, error) {
	var result *State
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		state, object, err := store.Load(ctx, cluster)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		before, err := json.Marshal(state)
		if err != nil {
			return err
		}
		if err := update(state); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := state.Validate(cluster.GetUID()); err != nil {
			return err
		}
		encoded, err := json.Marshal(state)
		if err != nil || len(encoded) > StateLimit {
			return fmt.Errorf("failover state cannot be encoded within its bound")
		}
		if object != nil && bytes.Equal(before, encoded) {
			result = state
			return nil
		}
		create := object == nil
		if create {
			object = &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap"}}
			object.SetName(cluster.GetName() + "-autoscaler-failover-state")
			object.SetNamespace(cluster.GetNamespace())
			object.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: cluster.GetAPIVersion(), Kind: "Cluster", Name: cluster.GetName(), UID: cluster.GetUID()}})
		}
		if err := unstructured.SetNestedField(object.Object, string(encoded), "data", "state.json"); err != nil {
			return err
		}
		client := store.Client.Resource(ConfigMaps).Namespace(cluster.GetNamespace())
		if err := ctx.Err(); err != nil {
			return err
		}
		if create {
			_, err = client.Create(ctx, object, metav1.CreateOptions{})
			if apierrors.IsAlreadyExists(err) {
				return apierrors.NewConflict(ConfigMaps.GroupResource(), object.GetName(), err)
			}
		} else {
			_, err = client.Update(ctx, object, metav1.UpdateOptions{})
		}
		if err == nil {
			result = state
		}
		return err
	})
	return result, err
}
