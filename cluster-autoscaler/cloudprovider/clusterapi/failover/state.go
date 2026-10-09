package failover

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// StateVersion and related constants define persistence, lifecycle and pairing metadata.
const (
	StateVersion = 2
	StateLimit   = 64 * 1024
	Healthy      = "Healthy"
	Degraded     = "Degraded"
	Recovering   = "Recovering"
	PairKey      = "aiproducer.com/failover-pair"
	RoleKey      = "aiproducer.com/failover-role"
)

// ConfigMaps identifies the management API resource used for configuration and state.
var ConfigMaps = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}

// State contains Cluster-scoped durable records for registered failover pairs.
type State struct {
	Version    int              `json:"version"`
	ClusterUID types.UID        `json:"clusterUID"`
	Pairs      map[string]*Pair `json:"pairs"`
}
// Pair holds request allowances and recovery bookkeeping for one failover identifier.
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
// Role tracks physical identity, failure evidence and recheck timing for one pair member.
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
// Store reads and reconciles Cluster-owned state through the management API.
type Store struct{ Client dynamic.Interface }

// NewState initializes empty failover state bound to the Cluster UID.
func NewState(cluster *unstructured.Unstructured) *State {
	return &State{Version: StateVersion, ClusterUID: cluster.GetUID(), Pairs: map[string]*Pair{}}
}
