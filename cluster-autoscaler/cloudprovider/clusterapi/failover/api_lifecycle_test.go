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
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/scale"
)

type lifecycleAPIRoundTripper func(*http.Request) (*http.Response, error)

func (transport lifecycleAPIRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestDisposableAPIRegistersRealMemberIdentitiesAndIsolatesRetiringAdmission(t *testing.T) {
	fixture := newLifecycleAPIFixture(t)
	fixture.addPair(t, "first", "first-registration")
	fixture.addPair(t, "second", "second-registration")
	fixture.environment.failover.Refresh()
	state := fixture.state(t)
	require.Len(t, state.Pairs, 2)
	assert.Equal(t, StateVersion, state.Version)
	for _, name := range []string{"first", "second"} {
		pair := state.Pairs[name]
		require.NotNil(t, pair.Lifecycle)
		assert.Equal(t, LifecycleStateVersion, pair.Lifecycle.Version)
		assert.Equal(t, "Creating", pair.Lifecycle.State)
		assert.Equal(t, fixture.groups[name+"-primary"].object.GetUID(), pair.Primary.PoolUID)
		assert.Equal(t, fixture.infrastructure[name+"-secondary"].GetUID(), pair.Secondary.InfrastructureUID)
		require.Error(t, fixture.groups[name+"-primary"].IncreaseSize(1))
		fixture.intent(t, name, "Active", false)
	}
	fixture.environment.failover.Refresh()
	cached := fixture.groups["first-primary"]
	fixture.intent(t, "first", "Retiring", false)
	require.Error(t, cached.IncreaseSize(1), "cached Active admission must obey current authoritative intent")
	fixture.environment.failover.Refresh()
	require.NoError(t, fixture.groups["second-primary"].IncreaseSize(1))
	firstTarget, err := cached.TargetSize()
	require.NoError(t, err)
	secondTarget, err := fixture.groups["second-primary"].TargetSize()
	require.NoError(t, err)
	assert.Zero(t, firstTarget)
	assert.Equal(t, 1, secondTarget)
	state = fixture.state(t)
	assert.Equal(t, "Retiring", state.Pairs["first"].Lifecycle.State)
	assert.False(t, state.Pairs["second"].Lifecycle.OperationsSettled)
}

func TestDisposableAPIRetirementWaitsForUnconfirmedAndInflightScaleOperations(t *testing.T) {
	fixture := newLifecycleAPIFixture(t)
	for _, name := range []string{"first", "second"} {
		fixture.addPair(t, name, name+"-registration")
	}
	fixture.environment.failover.Refresh()
	for _, name := range []string{"first", "second"} {
		fixture.intent(t, name, "Active", false)
	}
	fixture.environment.failover.Refresh()
	require.NoError(t, fixture.groups["second-primary"].IncreaseSize(1))
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	configuration := rest.CopyConfig(fixture.scoped)
	configuration.WrapTransport = func(delegate http.RoundTripper) http.RoundTripper {
		return lifecycleAPIRoundTripper(func(request *http.Request) (*http.Response, error) {
			if request.Method == http.MethodPut && strings.HasSuffix(request.URL.Path, "/scale") {
				close(entered)
				select {
				case <-release:
				case <-request.Context().Done():
					return nil, request.Context().Err()
				}
			}
			return delegate.RoundTrip(request)
		})
	}
	scaleClient, err := scale.NewForConfig(configuration, restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(fixture.api.discovery)), dynamic.LegacyAPIPathResolverFunc, scale.NewDiscoveryScaleKindResolver(fixture.api.discovery))
	require.NoError(t, err)
	fixture.environment.managementScaleClient = scaleClient
	resume := sync.OnceFunc(func() { close(release) })
	finished := false
	defer func() {
		resume()
		if !finished {
			select {
			case <-done:
			case <-fixture.api.context.Done():
				t.Error("admitted scale operation did not return")
			}
		}
	}()
	go func() { done <- fixture.groups["first-primary"].IncreaseSize(1) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("scale operation did not reach the controlled HTTP boundary")
	}
	for _, name := range []string{"first", "second"} {
		fixture.intent(t, name, "Retiring", false)
	}
	fixture.environment.failover.Refresh()
	state := fixture.state(t)
	assert.False(t, state.Pairs["first"].Lifecycle.OperationsSettled)
	require.NotNil(t, state.Pairs["first"].PrimaryRequest)
	assert.True(t, state.Pairs["second"].Lifecycle.OperationsSettled)
	assert.Nil(t, state.Pairs["second"].PrimaryRequest, "real committed target must reconcile")
	fixture.retireResources(t, "first")
	fixture.environment.failover.Refresh()
	assert.Equal(t, "Retiring", fixture.state(t).Pairs["first"].Lifecycle.State, "absence alone does not confirm retirement")
	fixture.intent(t, "first", "Retiring", true)
	fixture.environment.failover.Refresh()
	assert.Equal(t, "Retiring", fixture.state(t).Pairs["first"].Lifecycle.State, "admitted HTTP call still prevents completion")
	resume()
	select {
	case err := <-done:
		finished = true
		require.Error(t, err, "deleted resource must not receive the delayed scale write")
	case <-fixture.api.context.Done():
		t.Fatal("delayed scale call did not return")
	}
	fixture.environment.failover.Refresh()
	state = fixture.state(t)
	assert.Equal(t, "Retired", state.Pairs["first"].Lifecycle.State)
	assert.True(t, state.Pairs["first"].Lifecycle.OperationsSettled)
	assert.Nil(t, state.Pairs["first"].PrimaryRequest)
}

func TestDisposableAPIConflictPurgeAndReloadCannotResurrectAnOldRegistration(t *testing.T) {
	fixture := newLifecycleAPIFixture(t)
	for _, name := range []string{"first", "second"} {
		fixture.addPair(t, name, name+"-registration")
	}
	fixture.environment.failover.Refresh()
	for _, name := range []string{"first", "second"} {
		fixture.intent(t, name, "Active", false)
	}
	fixture.environment.failover.Refresh()
	before := fixture.state(t)
	captured := *before.Pairs["first"]
	captured.Primary.ObservedTarget++
	configuration := rest.CopyConfig(fixture.scoped)
	var concurrent sync.Once
	conflicts := 0
	configuration.WrapTransport = func(delegate http.RoundTripper) http.RoundTripper {
		return lifecycleAPIRoundTripper(func(request *http.Request) (*http.Response, error) {
			if request.Method == http.MethodPut && strings.HasSuffix(request.URL.Path, "/configmaps/test-autoscaler-failover-state") {
				concurrent.Do(func() {
					fixture.intent(t, "first", "Retiring", false)
					fixture.environment.failover.Refresh()
				})
			}
			response, err := delegate.RoundTrip(request)
			if response != nil && response.StatusCode == http.StatusConflict {
				conflicts++
			}
			return response, err
		})
	}
	client, err := dynamic.NewForConfig(configuration)
	require.NoError(t, err)
	attempts := 0
	_, err = (&Store{Client: client}).Reconcile(fixture.api.context, fixture.api.cluster, func(state *State) error {
		attempts++
		state.Pairs["first"] = &captured
		return nil
	})
	require.Error(t, err)
	assert.Equal(t, 1, conflicts, "the API server, not a fake reactor, must reject the stale resourceVersion")
	assert.Equal(t, 2, attempts)
	assert.Equal(t, "Retiring", fixture.state(t).Pairs["first"].Lifecycle.State)
	fixture.retireResources(t, "first")
	fixture.intent(t, "first", "Retiring", true)
	fixture.environment.failover.Refresh()
	state, object, err := fixture.environment.failover.Store.Load(fixture.api.context, fixture.api.cluster)
	require.NoError(t, err)
	require.Equal(t, "Retired", state.Pairs["first"].Lifecycle.State)
	require.True(t, state.Pairs["first"].Lifecycle.OperationsSettled)
	delete(fixture.configuration.Pairs, "first")
	fixture.writeConfiguration(t)
	delete(state.Pairs, "first")
	purged := object.DeepCopy()
	encoded, err := json.Marshal(state)
	require.NoError(t, err)
	require.NoError(t, unstructured.SetNestedField(purged.Object, string(encoded), "data", "state.json"))
	maps := fixture.api.client.Resource(ConfigMaps).Namespace(fixture.api.cluster.GetNamespace())
	_, err = maps.Update(fixture.api.context, purged, metav1.UpdateOptions{})
	require.NoError(t, err)
	_, err = maps.Update(fixture.api.context, object, metav1.UpdateOptions{})
	require.True(t, apierrors.IsConflict(err), "stale external purge updates must preserve live resourceVersion")
	require.NoError(t, fixture.environment.enableAzureFailover(false, time.Second))
	fixture.environment.failover.Refresh()
	assert.Equal(t, before.Pairs["second"], fixture.state(t).Pairs["second"])
	_, err = fixture.environment.failover.Store.Reconcile(fixture.api.context, fixture.api.cluster, func(state *State) error {
		state.Pairs["first"] = &captured
		return nil
	})
	require.Error(t, err)
	assert.NotContains(t, fixture.state(t).Pairs, "first")
	fixture.addPair(t, "first", "fresh-registration")
	fixture.environment.failover.Refresh()
	fresh := fixture.state(t).Pairs["first"]
	require.NotNil(t, fresh)
	assert.NotEqual(t, before.Pairs["first"].Primary.PoolUID, fresh.Primary.PoolUID)
	assert.NotEqual(t, before.Pairs["first"].Secondary.InfrastructureUID, fresh.Secondary.InfrastructureUID)
	assert.Equal(t, "fresh-registration", fresh.Lifecycle.RegistrationID)
	assert.Equal(t, "Creating", fresh.Lifecycle.State)
	assert.Equal(t, Healthy, fresh.Phase)
	assert.Zero(t, fresh.FallbackAllowance)
	assert.Nil(t, fresh.PrimaryRequest)
	assert.Nil(t, fresh.SecondaryRequest)
	fixture.intent(t, "first", "Active", false)
	fixture.environment.failover.Refresh()
	require.NoError(t, fixture.groups["first-primary"].IncreaseSize(1))
}
