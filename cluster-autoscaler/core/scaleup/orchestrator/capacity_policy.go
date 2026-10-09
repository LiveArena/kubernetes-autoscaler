package orchestrator

import (
	"fmt"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/equivalence"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/resource"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/klog/v2"
)

func (orchestrator *ScaleUpOrchestrator) preparePrimaryFitFallback(groups []cloudprovider.NodeGroup, templates map[string]*framework.NodeInfo, limits resource.Limits, pods []*equivalence.PodGroup, now time.Time) {
	byID := map[string]cloudprovider.NodeGroup{}
	for _, group := range groups {
		byID[group.Id()] = group
		if extension, ok := group.(cloudprovider.NodeGroupPrimaryFitFallback); ok {
			extension.ResetPrimaryFitFallback()
		}
	}
	for _, secondary := range groups {
		policy := cloudprovider.GetNodeGroupCapacityPolicy(secondary)
		if !policy.ConsiderPrimaryUnfit {
			continue
		}
		extension, supported := secondary.(cloudprovider.NodeGroupPrimaryFitFallback)
		primary := byID[policy.PrimaryNodeGroupID]
		template := templates[policy.PrimaryNodeGroupID]
		if !supported || primary == nil || template == nil || orchestrator.isNodeGroupReadyToScaleUp(primary, now) != nil {
			continue
		}
		reason := ""
		if exceeded := orchestrator.IsNodeGroupResourceExceeded(limits, primary, template, 1); exceeded != nil {
			if _, hardLimit := exceeded.(*MaxResourceLimitReached); !hardLimit {
				continue
			}
			reason = fmt.Sprintf("primary resource limits prevent scaling: %v", exceeded)
		} else if len(orchestrator.SchedulablePodGroups(pods, primary, template)) == 0 {
			verified := len(pods) > 0
			for _, podGroup := range pods {
				failure := podGroup.SchedulingErrors[primary.Id()]
				if schedulingFailure, typed := failure.(clustersnapshot.SchedulingError); !typed || schedulingFailure.Type() == clustersnapshot.SchedulingInternalError {
					verified = false
				}
			}
			if verified {
				reason = "primary cannot fit the current residual demand"
			}
		}
		if reason != "" {
			if err := extension.AllowPrimaryFitFallback(reason); err != nil {
				klog.V(4).Infof("Secondary %s rejected primary-fit exception: %v", secondary.Id(), err)
			} else {
				klog.V(4).Infof("Secondary %s may serve residual demand: %s", secondary.Id(), reason)
			}
		}
	}
}
