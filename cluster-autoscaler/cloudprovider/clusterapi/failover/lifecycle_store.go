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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func (store *Store) validateLifecycleUpdate(ctx context.Context, cluster *unstructured.Unstructured, encodedBefore []byte, after *State) error {
	before := &State{}
	if err := json.Unmarshal(encodedBefore, before); err != nil {
		return err
	}
	var configuration *Configuration
	var err error
	if !containsLifecycleRecords(before) && !containsLifecycleRecords(after) {
		newRegistration := false
		for name := range after.Pairs {
			newRegistration = newRegistration || before.Pairs[name] == nil
		}
		if !newRegistration {
			return nil
		}
		configuration, err = ReadConfiguration(ctx, store.Client, cluster)
		if apierrors.IsNotFound(err) || err == nil && configuration.APIVersion == ConfigVersion {
			return nil
		}
	} else {
		configuration, err = ReadConfiguration(ctx, store.Client, cluster)
	}
	if err != nil || configuration.APIVersion != LifecycleConfigVersion {
		return fmt.Errorf("lifecycle state requires authoritative configuration v2: %v", err)
	}
	for name, previous := range before.Pairs {
		if after.Pairs[name] == nil {
			return fmt.Errorf("autoscaler reconciliation cannot purge pair %q", name)
		}
		current := after.Pairs[name]
		if previous.Lifecycle == nil && current.Lifecycle == nil {
			continue
		}
		if previous.Lifecycle == nil || current.Lifecycle == nil || previous.Lifecycle.Version != current.Lifecycle.Version || previous.Lifecycle.RegistrationID != current.Lifecycle.RegistrationID || lifecycleOrder(current.Lifecycle.State) < lifecycleOrder(previous.Lifecycle.State) {
			return fmt.Errorf("pair %q lifecycle cannot move backward or change registration", name)
		}
		if !sameMemberBinding(previous.Primary, current.Primary) || !sameMemberBinding(previous.Secondary, current.Secondary) {
			return fmt.Errorf("pair %q registered member identities cannot change", name)
		}
	}
	for name, current := range after.Pairs {
		previous := before.Pairs[name]
		if previous != nil {
			oldJSON, _ := json.Marshal(previous)
			newJSON, _ := json.Marshal(current)
			if bytes.Equal(oldJSON, newJSON) {
				continue
			}
			if previous.Lifecycle == nil || current.Lifecycle == nil {
				return fmt.Errorf("unregistered lifecycle conversion is not supported for pair %q", name)
			}
			if previous.Lifecycle.State == "Retired" {
				return fmt.Errorf("retired pair %q cannot be changed by stale reconciliation", name)
			}
		}
		if current.Lifecycle == nil {
			return fmt.Errorf("new pair %q requires versioned lifecycle registration", name)
		}
		configured, found := configuration.Pairs[name]
		if !found || configured.Intent == nil || configured.Intent.RegistrationID != current.Lifecycle.RegistrationID {
			return fmt.Errorf("pair %q registration is absent or superseded in current intent", name)
		}
		intent := configured.Intent
		if previous == nil {
			if intent.State != "Creating" || current.Lifecycle.State != "Creating" {
				return fmt.Errorf("absent pair %q requires a fresh explicit Creating registration", name)
			}
			if err := store.validateLifecycleBinding(ctx, cluster, name, configured, current); err != nil {
				return err
			}
		} else if current.Lifecycle.State != intent.State && !(current.Lifecycle.State == "Retired" && intent.State == "Retiring" && intent.ResourceRetirementComplete) {
			return fmt.Errorf("pair %q update conflicts with current lifecycle intent", name)
		}
		if current.Lifecycle.State == "Retired" {
			if !current.Lifecycle.OperationsSettled || current.PrimaryRequest != nil || current.SecondaryRequest != nil {
				return fmt.Errorf("retired pair %q still has unresolved operations", name)
			}
			gone, err := store.recordedPairResourcesGone(ctx, cluster, current)
			if err != nil || !gone {
				return fmt.Errorf("retired pair %q has unconfirmed recorded-resource retirement: %v", name, err)
			}
		}
	}
	return nil
}

func lifecycleOrder(state string) int {
	return map[string]int{"Creating": 0, "Active": 1, "Retiring": 2, "Retired": 3}[state]
}

func containsLifecycleRecords(state *State) bool {
	for _, pair := range state.Pairs {
		if pair != nil && pair.Lifecycle != nil {
			return true
		}
	}
	return false
}

func sameMemberBinding(previous, current Role) bool {
	return previous.PoolUID == current.PoolUID && previous.InfrastructureUID == current.InfrastructureUID && previous.PoolName == current.PoolName && previous.PoolVersion == current.PoolVersion && previous.InfrastructureName == current.InfrastructureName && previous.InfrastructureVersion == current.InfrastructureVersion
}
