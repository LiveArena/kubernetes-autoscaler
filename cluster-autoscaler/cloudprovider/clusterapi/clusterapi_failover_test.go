package clusterapi

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"testing"
	"time"
)

func TestOnlyLiveAMPMsOwnedBySelectedInfrastructureExcludeRegisteredCapacity(t *testing.T) {
	for _, scenario := range []string{"selected-owner", "wrong-owner", "deleting"} {
		statement := map[string]string{
			"selected-owner": "SelectedInfrastructureFailureExcludesItsRegisteredNode",
			"wrong-owner":    "OtherInfrastructureFailureDoesNotExcludeRegisteredCapacity",
			"deleting":       "DeletingMachinesDoNotExcludeRegisteredCapacity",
		}[scenario]
		t.Run(statement, func(t *testing.T) {
			controller, clock := newFailoverTestController(t)
			var group *nodegroup
			for _, candidate := range (&provider{controller: controller}).NodeGroups() {
				if candidate.(*nodegroup).Object().GetName() == "lin-primary" {
					group = candidate.(*nodegroup)
				}
			}
			require.NotNil(t, group)
			node, err := controller.FindNodeByProviderID("azure://lin-primary")
			require.NoError(t, err)
			require.NotNil(t, node)
			node.Status.Conditions[0].Status = "False"
			require.NoError(t, controller.nodeInformer.GetStore().Update(node))
			machine := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": azureMachinePoolMachineApiGroup + "/v1beta1", "kind": "AzureMachinePoolMachine", "spec": map[string]interface{}{"providerID": "azure://lin-primary"}, "status": map[string]interface{}{"provisioningState": "Failed"}}}
			machine.SetName("registered-failure")
			machine.SetNamespace("tenant")
			machine.SetUID("registered-failure-uid")
			machine.SetCreationTimestamp(metav1.NewTime(clock.Now()))
			ownerUID := types.UID("lin-primary-amp")
			if scenario == "wrong-owner" {
				ownerUID = "another-generation-amp"
			} else if scenario == "deleting" {
				machine.SetDeletionTimestamp(&metav1.Time{Time: clock.Now()})
			}
			machine.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: azureMachinePoolMachineApiGroup + "/v1beta1", Kind: "AzureMachinePool", Name: "lin-primary", UID: ownerUID}})
			require.NoError(t, controller.azureIntegration.extension.azureMachinePoolMachineInformer.Informer().GetStore().Add(machine))
			member, err := controller.failover.Member(context.Background(), group)
			require.NoError(t, err)
			assert.Equal(t, scenario == "selected-owner", member.FailedNodes[node.Name], "only a live terminal AMPM owned by the selected AMP may exclude registered capacity")
			assert.Contains(t, member.NotReadyCreated, node.Name)
		})
	}
}

func TestPrimaryFailureEnablesSecondaryCapacityWithoutReplayOrPrimaryTargetReduction(t *testing.T) {
	for _, pair := range []string{"lin", "win2"} {
		t.Run("PrimaryFailurePreservesIntentAndOtherPairPreference/Pair="+pair, func(t *testing.T) {
			controller, clock := newFailoverTestController(t)
			provider := &provider{controller: controller}
			require.NoError(t, provider.Refresh())
			findGroup := func(name string) *nodegroup {
				for _, group := range provider.NodeGroups() {
					if group.(*nodegroup).scalableResource.Name() == name {
						return group.(*nodegroup)
					}
				}
				t.Fatalf("group %s missing", name)
				return nil
			}
			primary, secondary := findGroup(pair+"-primary"), findGroup(pair+"-secondary")
			assert.False(t, primary.GetCapacityPolicy().BlockUnregistered)
			assert.True(t, secondary.GetCapacityPolicy().ScaleUpBlocked)
			failed := &unstructured.Unstructured{Object: map[string]interface{}{"status": map[string]interface{}{"provisioningState": "Failed"}}}
			failed.SetUID("failed-attempt")
			failed.SetNamespace("tenant")
			failed.SetCreationTimestamp(metav1.NewTime(clock.Now()))
			failed.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "infrastructure.cluster.x-k8s.io/v1beta1", Kind: "AzureMachinePool", Name: pair + "-primary", UID: types.UID(pair + "-primary-amp")}})
			for index := 0; index < 100; index++ {
				controller.failover.Observe(failed)
			}
			require.NoError(t, provider.Refresh())
			assert.True(t, primary.GetCapacityPolicy().BlockUnregistered)
			assert.Empty(t, controller.failover.Observations)
			assert.False(t, secondary.GetCapacityPolicy().ScaleUpBlocked)
			require.NoError(t, secondary.IncreaseSize(2))
			assert.Error(t, secondary.IncreaseSize(2))
			assert.Error(t, primary.DecreaseTargetSize(-2))
			target, err := primary.TargetSize()
			require.NoError(t, err)
			assert.Equal(t, 3, target)
			target, err = secondary.TargetSize()
			require.NoError(t, err)
			assert.Equal(t, 2, target)
			require.NoError(t, controller.enableAzureFailover(true, 10*time.Second))
			clock.Step(31 * time.Second)
			controller.failover.Clock = clock
			require.NoError(t, provider.Refresh())
			assert.True(t, primary.GetCapacityPolicy().BlockUnregistered)
			assert.True(t, secondary.GetCapacityPolicy().ScaleUpBlocked)
			target, err = secondary.TargetSize()
			require.NoError(t, err)
			assert.Equal(t, 2, target)
			other := "lin"
			if pair == "lin" {
				other = "win2"
			}
			assert.False(t, findGroup(other+"-primary").GetCapacityPolicy().BlockUnregistered)
			assert.True(t, findGroup(other+"-secondary").GetCapacityPolicy().ScaleUpBlocked)
		})
	}
}
