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

import "fmt"

// LifecycleStateVersion identifies the independently versioned PairLifecycle JSON format.
const LifecycleStateVersion = 1

// PairLifecycle reports one registration's rollout state independently of failure policy.
type PairLifecycle struct {
	Version           int    `json:"version"`
	State             string `json:"state"`
	RegistrationID    string `json:"registrationID"`
	OperationsSettled bool   `json:"operationsSettled"`
	Reason            string `json:"reason"`
}

func validLifecycleState(state string) bool {
	return state == "Creating" || state == "Active" || state == "Retiring" || state == "Retired"
}

func (policy *Policy) reconcilePairLifecycle(state *State, name string, primary, secondary *Member) error {
	configuration, err := ReadConfiguration(policy.RequestContext(), policy.Store.Client, primary.Cluster)
	if err != nil {
		return err
	}
	if configuration.APIVersion == ConfigVersion {
		if pair := state.Pairs[name]; pair != nil && pair.Lifecycle != nil {
			return fmt.Errorf("lifecycle state cannot be used with legacy configuration")
		}
		return nil
	}
	configured, found := configuration.Pairs[name]
	if !found || configured.Intent == nil {
		return fmt.Errorf("pair registration is absent from current lifecycle intent")
	}
	intent := configured.Intent
	pair := state.Pairs[name]
	if pair == nil {
		if intent.State != "Creating" {
			return fmt.Errorf("an absent pair requires explicit Creating registration")
		}
		pair = &Pair{Phase: Healthy, Lifecycle: &PairLifecycle{Version: LifecycleStateVersion, State: "Creating", RegistrationID: intent.RegistrationID, OperationsSettled: true}}
		state.Pairs[name] = pair
	}
	if pair.Lifecycle == nil || pair.Lifecycle.RegistrationID != intent.RegistrationID {
		return fmt.Errorf("registered pair generation does not match current intent")
	}
	previous := pair.Lifecycle.State
	if previous == "Retired" || previous == "Retiring" && intent.State != "Retiring" || previous == "Active" && intent.State == "Creating" {
		return fmt.Errorf("pair lifecycle cannot move backward or reactivate a retired registration")
	}
	for role, member := range map[string]*Member{"primary": primary, "secondary": secondary} {
		record := &pair.Primary
		if role == "secondary" {
			record = &pair.Secondary
		}
		record.PoolName = member.Group.Object().GetName()
		record.PoolVersion = member.Group.Object().GetAPIVersion()
		record.InfrastructureName = member.Infrastructure.GetName()
		record.InfrastructureVersion = member.Infrastructure.GetAPIVersion()
	}
	pair.Lifecycle.State = intent.State
	pair.Lifecycle.OperationsSettled = pair.PrimaryRequest == nil && pair.SecondaryRequest == nil
	pair.Lifecycle.Reason = ""
	return nil
}
