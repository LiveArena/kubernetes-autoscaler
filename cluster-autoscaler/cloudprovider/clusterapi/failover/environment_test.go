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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/scale"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/clock"
)

type testInformer struct{ store cache.Indexer }

func (informer *testInformer) Informer() *testInformer { return informer }
func (informer *testInformer) GetStore() cache.Indexer { return informer.store }

type testEnvironment struct {
	managementClient      dynamic.Interface
	managementScaleClient scale.ScalesGetter
	machinePoolResource   schema.GroupVersionResource
	machinePoolInformer   *testInformer
	nodeInformer          *testInformer
	failover              *Policy
}

func (environment *testEnvironment) NodeGroups() ([]cloudprovider.NodeGroup, error) {
	var result []cloudprovider.NodeGroup
	for _, value := range environment.machinePoolInformer.store.List() {
		pool := value.(*unstructured.Unstructured)
		group, err := newTestGroup(environment, pool)
		if err != nil {
			return nil, err
		}
		result = append(result, group)
	}
	return result, nil
}

func (environment *testEnvironment) nodeGroups() ([]cloudprovider.NodeGroup, error) {
	return environment.NodeGroups()
}
func (environment *testEnvironment) MachinePoolResource() schema.GroupVersionResource {
	return environment.machinePoolResource
}

func (environment *testEnvironment) FindNodeByProviderID(id string) (*corev1.Node, error) {
	for _, value := range environment.nodeInformer.store.List() {
		node := value.(*corev1.Node)
		if node.Spec.ProviderID == id {
			return node.DeepCopy(), nil
		}
	}
	return nil, nil
}

func (environment *testEnvironment) FindMachineByProviderID(string) (*unstructured.Unstructured, error) {
	return nil, nil
}

func (environment *testEnvironment) enableAzureFailover(frozen bool, interval time.Duration, defaults ...config.NodeGroupAutoscalingOptions) error {
	options := config.NodeGroupAutoscalingOptions{MaxNodeProvisionTime: 45 * time.Minute}
	if environment.failover != nil {
		options = environment.failover.NodeGroupDefaults
		environment.failover.StopWriter()
	}
	if len(defaults) > 0 {
		options = defaults[0]
	}
	ctx, cancel := context.WithCancel(context.Background())
	environment.failover = &Policy{Environment: environment, Store: &Store{Client: environment.managementClient}, Clock: clock.RealClock{}, ScanInterval: interval, Frozen: frozen, NodeGroupDefaults: options, WriterContext: ctx, CancelWriter: cancel, Policies: map[string]cloudprovider.NodeGroupCapacityPolicy{}, Observations: map[string]FailureObservation{}}
	return nil
}

type testProvider struct{ controller *testEnvironment }

func (provider *testProvider) Refresh() error {
	if provider.controller.failover != nil {
		provider.controller.failover.Refresh()
	}
	return nil
}
func (provider *testProvider) Cleanup() error {
	if provider.controller.failover != nil {
		provider.controller.failover.StopWriter()
	}
	return nil
}
func (provider *testProvider) NodeGroups() []cloudprovider.NodeGroup {
	groups, err := provider.controller.NodeGroups()
	if err != nil {
		return nil
	}
	return groups
}

func unexpectedTestScale(verb string) error { return fmt.Errorf("unexpected scale action %s", verb) }
