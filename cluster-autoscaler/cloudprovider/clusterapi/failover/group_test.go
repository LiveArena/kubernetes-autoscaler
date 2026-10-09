package failover

import (
	"context"
	"fmt"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/config"
)

const nodeGroupMinSizeAnnotationKey = "cluster.x-k8s.io/cluster-api-autoscaler-node-group-min-size"
const nodeGroupMaxSizeAnnotationKey = MaxSizeAnnotationKey
const machinePoolKind = MachinePoolKind
const azureMachinePoolMachineApiGroup = AzureAPIGroup

type testGroup struct {
	cloudprovider.NodeGroup
	environment *testEnvironment
	object      *unstructured.Unstructured
	options     map[string]string
}

func newTestGroup(environment *testEnvironment, object *unstructured.Unstructured) (*testGroup, error) {
	maximum, err := strconv.Atoi(object.GetAnnotations()[MaxSizeAnnotationKey])
	if err != nil {
		return nil, err
	}
	minimum, _ := strconv.Atoi(object.GetAnnotations()[nodeGroupMinSizeAnnotationKey])
	group := testprovider.NewTestNodeGroup("MachinePool/"+object.GetNamespace()+"/"+object.GetName(), maximum, minimum, 0, true, false, "", nil, nil)
	return &testGroup{NodeGroup: group, environment: environment, object: object}, nil
}

func (group *testGroup) Object() *unstructured.Unstructured { return group.object }
func (group *testGroup) Resource() (schema.GroupVersionResource, error) {
	return group.environment.machinePoolResource, nil
}
func (group *testGroup) Replicas() (int, error) { return group.TargetSize() }
func (group *testGroup) TargetSize() (int, error) {
	object, err := group.environment.managementClient.Resource(group.environment.machinePoolResource).Namespace(group.object.GetNamespace()).Get(context.Background(), group.object.GetName(), metav1.GetOptions{})
	if err != nil {
		return 0, err
	}
	count, _, err := unstructured.NestedInt64(object.Object, "spec", "replicas")
	return int(count), err
}

func (group *testGroup) ProviderIDs() ([]string, error) {
	values, _, err := unstructured.NestedStringSlice(group.object.Object, "spec", "providerIDList")
	return values, err
}
func (group *testGroup) GetOptions(defaults config.NodeGroupAutoscalingOptions) (*config.NodeGroupAutoscalingOptions, error) {
	if value := group.options[config.DefaultMaxNodeProvisionTimeKey]; value != "" {
		duration, err := time.ParseDuration(value)
		if err != nil {
			return nil, err
		}
		defaults.MaxNodeProvisionTime = duration
	}
	return &defaults, nil
}

func (group *testGroup) GetCapacityPolicy() cloudprovider.NodeGroupCapacityPolicy {
	if group.environment.failover == nil {
		return DisabledCapacity(group.object)
	}
	return group.environment.failover.Capacity(group)
}

func (group *testGroup) IncreaseSize(delta int) error {
	if group.environment.failover == nil {
		return fmt.Errorf("failover disabled")
	}
	_, err := group.environment.failover.IncreaseSize(group, delta, group.environment.managementScaleClient)
	return err
}
func (group *testGroup) ResetPrimaryFitFallback() {
	group.environment.failover.ResetPrimaryFitFallback(group.Id())
}
func (group *testGroup) AllowPrimaryFitFallback(reason string) error {
	return group.environment.failover.AllowPrimaryFitFallback(group.Id(), reason)
}
