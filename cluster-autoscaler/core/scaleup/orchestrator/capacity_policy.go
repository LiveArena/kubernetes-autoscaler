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

package orchestrator

import (
	"fmt"
	"maps"
	"slices"
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
		} else if orchestrator.primaryCannotFit(pods, primary, template) {
			reason = "primary cannot fit the current residual demand"
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

func (orchestrator *ScaleUpOrchestrator) primaryCannotFit(pods []*equivalence.PodGroup, primary cloudprovider.NodeGroup, template *framework.NodeInfo) bool {
	probes := make([]*equivalence.PodGroup, len(pods))
	for index, original := range pods {
		probe := *original
		probe.SchedulableGroups = slices.Clone(original.SchedulableGroups)
		probe.SchedulingErrors = maps.Clone(original.SchedulingErrors)
		probes[index] = &probe
	}
	if len(probes) == 0 || len(orchestrator.SchedulablePodGroups(probes, primary, template)) != 0 {
		return false
	}
	for _, probe := range probes {
		failure, typed := probe.SchedulingErrors[primary.Id()].(clustersnapshot.SchedulingError)
		if !typed || failure.Type() == clustersnapshot.SchedulingInternalError {
			return false
		}
	}
	return true
}
