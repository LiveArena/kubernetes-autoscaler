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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sort"
	"strings"
	"time"
)

// FailureObservation identifies a terminal AMPM attempt and its AzureMachinePool owner.
type FailureObservation struct {
	Namespace string
	OwnerName string
	OwnerUID  types.UID
	UID       types.UID
	Created   metav1.Time
}

// TerminalAMPMFailure extracts a nondeleting Failed attempt with usable owner identity.
func TerminalAMPMFailure(machine *unstructured.Unstructured) (FailureObservation, bool) {
	state, _, err := unstructured.NestedString(machine.Object, "status", "provisioningState")
	created := machine.GetCreationTimestamp()
	if err != nil || state != "Failed" || !machine.GetDeletionTimestamp().IsZero() || machine.GetUID() == "" || created.IsZero() {
		return FailureObservation{}, false
	}
	for _, owner := range machine.GetOwnerReferences() {
		if owner.Kind == "AzureMachinePool" && strings.HasPrefix(owner.APIVersion, AzureAPIGroup+"/") && owner.UID != "" {
			return FailureObservation{Namespace: machine.GetNamespace(), OwnerName: owner.Name, OwnerUID: owner.UID, UID: machine.GetUID(), Created: created}, true
		}
	}
	return FailureObservation{}, false
}

// ReadyFingerprint hashes Ready identities independently of their input order.
func ReadyFingerprint(ready []string) string {
	identities := append([]string(nil), ready...)
	sort.Strings(identities)
	encoded, _ := json.Marshal(identities)
	return fmt.Sprintf("%x", sha256.Sum256(encoded))
}

// ObserveFailure advances the role's failure watermark only for a fresh attempt.
func (role *Role) ObserveFailure(observation FailureObservation, ready []string, now time.Time, scanInterval time.Duration) bool {
	if observation.UID == "" || observation.Created.IsZero() || observation.UID == role.LastAttemptUID || observation.Created.Before(&role.LastAttemptCreated) {
		return false
	}
	if observation.Created.Equal(&role.LastAttemptCreated) && string(observation.UID) <= string(role.LastAttemptUID) {
		return false
	}
	role.Failed = true
	role.LastAttemptUID = observation.UID
	role.LastAttemptCreated = observation.Created
	role.FailureEpoch++
	role.ReadyCountAtFailure = len(ready)
	role.ReadyFingerprintAtFailure = ReadyFingerprint(ready)
	if role.CheckInterval == 0 {
		role.CheckInterval = min(15*60, max(1, int64(scanInterval/time.Second)))
		role.NextCheck = metav1.NewTime(now.Add(time.Duration(role.CheckInterval) * time.Second))
	}
	return true
}

func (policy *Policy) resyncPairObservations(primary, secondary *Member, retained map[string]FailureObservation) (map[string]FailureObservation, error) {
	observations := map[string]FailureObservation{}
	for _, member := range []*Member{primary, secondary} {
		key := member.Cluster.GetNamespace() + "/" + string(member.Infrastructure.GetUID())
		if observation, found := retained[key]; found {
			observations[key] = observation
		}
	}
	err := policy.Environment.VisitFailures(func(observation FailureObservation) {
		for _, member := range []*Member{primary, secondary} {
			if observation.Namespace != member.Cluster.GetNamespace() || observation.OwnerUID != member.Infrastructure.GetUID() || observation.OwnerName != member.Infrastructure.GetName() {
				continue
			}
			key := observation.Namespace + "/" + string(observation.OwnerUID)
			previous, found := observations[key]
			if !found || observation.Created.After(previous.Created.Time) || observation.Created.Equal(&previous.Created) && observation.UID > previous.UID {
				observations[key] = observation
			}
		}
	})
	return observations, err
}
