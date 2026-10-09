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

type FailureObservation struct {
	Namespace string
	OwnerName string
	OwnerUID  types.UID
	UID       types.UID
	Created   metav1.Time
}

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
func ReadyFingerprint(ready []string) string {
	identities := append([]string(nil), ready...)
	sort.Strings(identities)
	encoded, _ := json.Marshal(identities)
	return fmt.Sprintf("%x", sha256.Sum256(encoded))
}
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
