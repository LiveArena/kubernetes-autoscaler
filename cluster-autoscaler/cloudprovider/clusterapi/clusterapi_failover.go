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
func (group *nodegroup) ResetPrimaryFitFallback() {
	if policy := group.machineController.failover; policy != nil {
		policy.ResetPrimaryFitFallback(group.Id())
	}
}
func (group *nodegroup) AllowPrimaryFitFallback(reason string) error {
	policy := group.machineController.failover
	if policy == nil || reason == "" {
		return fmt.Errorf("primary-fit fallback is disabled")
	}
	return policy.AllowPrimaryFitFallback(group.Id(), reason)
}
