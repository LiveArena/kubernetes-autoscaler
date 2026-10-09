package failover

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

const (
	StateVersion = 2
	StateLimit   = 64 * 1024
	Healthy      = "Healthy"
	Degraded     = "Degraded"
	Recovering   = "Recovering"
	PairKey      = "aiproducer.com/failover-pair"
	RoleKey      = "aiproducer.com/failover-role"
)

var ConfigMaps = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}

type State struct {
	Version    int              `json:"version"`
	ClusterUID types.UID        `json:"clusterUID"`
	Pairs      map[string]*Pair `json:"pairs"`
}
type Pair struct {
	Phase             string      `json:"phase"`
	RecoveryScans     int         `json:"recoveryScans"`
	RecoveryScanTime  metav1.Time `json:"recoveryScanTime"`
	Primary           Role        `json:"primary"`
	Secondary         Role        `json:"secondary"`
	FallbackAllowance int         `json:"fallbackAllowance"`
	PrimaryRequest    *Request    `json:"primaryRequest,omitempty"`
	SecondaryRequest  *Request    `json:"secondaryRequest,omitempty"`
}
type Role struct {
	PoolUID                   types.UID   `json:"poolUID"`
	InfrastructureUID         types.UID   `json:"infrastructureUID"`
	Failed                    bool        `json:"failed"`
	LastAttemptUID            types.UID   `json:"lastAttemptUID"`
	LastAttemptCreated        metav1.Time `json:"lastAttemptCreated"`
	FailureEpoch              int64       `json:"failureEpoch"`
	ReadyCountAtFailure       int         `json:"readyCountAtFailure"`
	ReadyFingerprintAtFailure string      `json:"readyFingerprintAtFailure"`
	ObservedTarget            int         `json:"observedTarget"`
	NextCheck                 metav1.Time `json:"nextCheck"`
	CheckInterval             int64       `json:"checkIntervalSeconds"`
}
type Store struct{ Client dynamic.Interface }

func NewState(cluster *unstructured.Unstructured) *State {
	return &State{Version: StateVersion, ClusterUID: cluster.GetUID(), Pairs: map[string]*Pair{}}
}
