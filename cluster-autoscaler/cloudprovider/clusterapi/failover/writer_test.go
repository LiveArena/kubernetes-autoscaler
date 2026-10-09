package failover

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	fakescale "k8s.io/client-go/scale/fake"
	clienttesting "k8s.io/client-go/testing"
	"testing"
	"time"
)

func TestStoppingWriterCancelsAndDrainsLocalOperationsBeforeReturning(t *testing.T) {
	for _, operation := range []string{"refresh", "intent", "scale", "scale-after-intent"} {
		statement := map[string]string{
			"refresh":            "ShutdownWaitsForInFlightStateReconciliation",
			"intent":             "ShutdownWaitsForInFlightIntentPersistence",
			"scale":              "ShutdownWaitsForInFlightScaleWrites",
			"scale-after-intent": "CancelledIntentPersistenceCannotStartAnotherScaleWrite",
		}[operation]
		t.Run(statement, func(t *testing.T) {
			controller, clock := newFailoverTestController(t)
			provider := &testProvider{controller: controller}
			require.NoError(t, provider.Refresh())
			failoverTestFailure(controller, "lin", "primary", clock.Now())
			if operation != "refresh" {
				require.NoError(t, provider.Refresh())
			}
			entered, release := make(chan struct{}), make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			block := func(action clienttesting.Action) (bool, runtime.Object, error) {
				close(entered)
				<-release
				return false, nil, nil
			}
			if operation == "scale" {
				controller.managementScaleClient.(*fakescale.FakeScaleClient).PrependReactor("update", "machinepools", block)
			} else {
				controller.managementClient.(*fakedynamic.FakeDynamicClient).PrependReactor("update", "configmaps", block)
			}
			var secondary *testGroup
			for _, candidate := range provider.NodeGroups() {
				group := candidate.(*testGroup)
				if group.object.GetName() == "lin-secondary" {
					secondary = group
				}
			}
			require.NotNil(t, secondary)
			finished := make(chan error, 1)
			go func() {
				if operation == "refresh" {
					finished <- provider.Refresh()
				} else if operation == "intent" {
					finished <- controller.failover.PrepareScaleRequest(secondary, 2)
				} else {
					finished <- secondary.IncreaseSize(2)
				}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("feature operation did not enter the controlled API boundary")
			}
			drained := make(chan error, 1)
			go func() {
				drained <- provider.Cleanup()
			}()
			select {
			case <-controller.failover.RequestContext().Done():
			case <-time.After(5 * time.Second):
				t.Fatal("cleanup did not cancel the feature writer")
			}
			select {
			case err := <-drained:
				assert.NoError(t, err)
				t.Error("cleanup returned before the in-flight feature operation drained")
			default:
			}
			close(release)
			select {
			case <-finished:
			case <-time.After(5 * time.Second):
				t.Fatal("feature operation did not complete after release")
			}
			select {
			case err := <-drained:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				if !t.Failed() {
					t.Fatal("cleanup did not finish after the operation drained")
				}
			}
			if operation == "scale-after-intent" {
				for _, action := range controller.managementScaleClient.(*fakescale.FakeScaleClient).Actions() {
					assert.NotEqual(t, "update", action.GetVerb(), "cancelled intent must not start a new scale update")
				}
			}
			require.Error(t, secondary.IncreaseSize(1))
		})
	}
}
func TestStoppedWriterCannotRefreshStateOrScaleSecondaryPools(t *testing.T) {
	controller, clock := newFailoverTestController(t)
	provider := &testProvider{controller: controller}
	failoverTestFailure(controller, "lin", "primary", clock.Now())
	require.NoError(t, provider.Refresh())
	require.NoError(t, provider.Cleanup())
	for _, candidate := range provider.NodeGroups() {
		group := candidate.(*testGroup)
		if group.object.GetName() == "lin-secondary" {
			require.Error(t, group.IncreaseSize(2))
		}
	}
	actions := len(controller.managementClient.(*fakedynamic.FakeDynamicClient).Actions())
	require.NoError(t, provider.Refresh())
	assert.Equal(t, actions, len(controller.managementClient.(*fakedynamic.FakeDynamicClient).Actions()))
	for _, action := range controller.managementClient.(*fakedynamic.FakeDynamicClient).Actions() {
		assert.NotEqual(t, "leases", action.GetResource().Resource)
	}
}
