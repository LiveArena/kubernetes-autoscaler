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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// StateVersion identifies the outer persisted failover-state format introduced by this feature.
const StateVersion = 1

// StateLimit bounds the encoded persisted state and configuration payloads.
const StateLimit = 64 * 1024

// State contains Cluster-scoped durable records for registered failover pairs.
type State struct {
	Version    int              `json:"version"`
	ClusterUID types.UID        `json:"clusterUID"`
	Pairs      map[string]*Pair `json:"pairs"`
}

// Pair failure phases describe provisioning health independently of rollout lifecycle.
const (
	Healthy    = "Healthy"
	Degraded   = "Degraded"
	Recovering = "Recovering"
)

// Pair holds request allowances and recovery bookkeeping for one failover identifier.
type Pair struct {
	Lifecycle         *PairLifecycle `json:"lifecycle,omitempty"`
	Phase             string         `json:"phase"`
	RecoveryScans     int            `json:"recoveryScans"`
	RecoveryScanTime  metav1.Time    `json:"recoveryScanTime"`
	Primary           Role           `json:"primary"`
	Secondary         Role           `json:"secondary"`
	FallbackAllowance int            `json:"fallbackAllowance"`
	PrimaryRequest    *Request       `json:"primaryRequest,omitempty"`
	SecondaryRequest  *Request       `json:"secondaryRequest,omitempty"`
}

// Role tracks physical identity, failure evidence and recheck timing for one pair member.
type Role struct {
	PoolName                  string      `json:"poolName,omitempty"`
	PoolVersion               string      `json:"poolVersion,omitempty"`
	InfrastructureName        string      `json:"infrastructureName,omitempty"`
	InfrastructureVersion     string      `json:"infrastructureVersion,omitempty"`
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
type Store struct {
	Client   dynamic.Interface
	Resolver ResourceResolver
}

// NewState initializes empty failover state bound to the Cluster UID.
func NewState(cluster *unstructured.Unstructured) *State {
	return &State{Version: StateVersion, ClusterUID: cluster.GetUID(), Pairs: map[string]*Pair{}}
}
