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

package clusterapi

import (
	"context"
	"fmt"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/clusterapi/failover"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/clock"
	"time"
)

func (controller *machineController) enableAzureFailover(frozen bool, scanInterval time.Duration, defaults ...config.NodeGroupAutoscalingOptions) error {
	if controller.azureIntegration == nil || !controller.azureIntegration.extension.azureMachinePoolMachineAvailable {
		return fmt.Errorf("Azure failover requires the AMPM informer")
	}
	policy := &failover.Policy{Environment: controller, Store: &failover.Store{Client: controller.managementClient}, Clock: clock.RealClock{}, ScanInterval: scanInterval, Frozen: frozen, Policies: map[string]cloudprovider.NodeGroupCapacityPolicy{}, Observations: map[string]failover.FailureObservation{}}
	if controller.failover != nil {
		policy.NodeGroupDefaults = controller.failover.NodeGroupDefaults
	}
	if len(defaults) > 0 {
		policy.NodeGroupDefaults = defaults[0]
	}
	policy.WriterContext, policy.CancelWriter = context.WithCancel(context.Background())
	handler, err := controller.azureIntegration.extension.azureMachinePoolMachineInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: policy.Observe, UpdateFunc: func(previous, current interface{}) {
		policy.Observe(current)
	}})
	if err == nil {
		if controller.failover != nil {
			controller.failover.StopWriter()
		}
		policy.Handler = handler
		controller.failover = policy
		go func() {
			select {
			case <-controller.stopChannel:
				policy.StopWriter()
			case <-policy.WriterContext.Done():
			}
		}()
	} else {
		policy.CancelWriter()
	}
	return err
}
func (ng *nodegroup) increaseSizeWithFailoverPolicy(delta int) (bool, error) {
	writer := ng.machineController.failover
	if writer == nil {
		if policy := ng.GetCapacityPolicy(); policy.ScaleUpBlocked {
			return true, fmt.Errorf("scale-up admission blocked: %s", policy.Reason)
		}
		return false, nil
	}
	return writer.IncreaseSize(ng, delta, ng.machineController.managementScaleClient)
}
func (ng *nodegroup) ResetPrimaryFitFallback() {
	if policy := ng.machineController.failover; policy != nil {
		policy.ResetPrimaryFitFallback(ng.Id())
	}
}
func (ng *nodegroup) AllowPrimaryFitFallback(reason string) error {
	policy := ng.machineController.failover
	if policy == nil || reason == "" {
		return fmt.Errorf("primary-fit fallback is disabled")
	}
	return policy.AllowPrimaryFitFallback(ng.Id(), reason)
}
