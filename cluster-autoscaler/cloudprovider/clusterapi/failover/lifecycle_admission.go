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

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func (policy *Policy) assertPairLifecycleAdmission(ctx context.Context, cluster *unstructured.Unstructured, name string, pair *Pair) error {
	configuration, err := ReadConfiguration(ctx, policy.Store.Client, cluster)
	if err != nil {
		return err
	}
	if configuration.APIVersion == ConfigVersion && pair.Lifecycle == nil {
		return nil
	}
	configured, found := configuration.Pairs[name]
	if configuration.APIVersion != LifecycleConfigVersion || !found || configured.Intent == nil || pair.Lifecycle == nil || configured.Intent.State != "Active" || pair.Lifecycle.State != "Active" || configured.Intent.RegistrationID != pair.Lifecycle.RegistrationID {
		return fmt.Errorf("pair lifecycle is not currently Active for this registration")
	}
	return nil
}

func (policy *Policy) assertCurrentLifecycleAdmission(group Group) error {
	ctx := policy.RequestContext()
	member, err := policy.Member(ctx, group)
	if err != nil {
		return err
	}
	state, _, err := policy.Store.Load(ctx, member.Cluster)
	if err != nil {
		return err
	}
	annotations := group.Object().GetAnnotations()
	pair := state.Pairs[annotations[PairKey]]
	if pair == nil {
		return fmt.Errorf("pair registration is absent before scale write")
	}
	record := pair.Primary
	if annotations[RoleKey] == "secondary" {
		record = pair.Secondary
	}
	if record.PoolUID != member.Group.Object().GetUID() || record.InfrastructureUID != member.Infrastructure.GetUID() {
		return fmt.Errorf("pair member identity changed before scale write")
	}
	return policy.assertPairLifecycleAdmission(ctx, member.Cluster, annotations[PairKey], pair)
}
