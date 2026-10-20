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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInfrastructureReferencesUseExplicitVersionsOrTrustedDiscovery(t *testing.T) {
	for _, reference := range []struct {
		statement string
		fields    map[string]string
		wanted    string
		calls     int
	}{
		{"LegacyVersionRemainsExplicitWithoutDiscovery", map[string]string{"apiVersion": AzureAPIGroup + "/v1beta1"}, "v1beta1", 0},
		{"VersionlessGroupUsesDiscoveredVersion", map[string]string{"apiGroup": AzureAPIGroup}, "v1beta2", 1},
		{"MatchingGroupDoesNotOverrideAnExplicitVersion", map[string]string{"apiGroup": AzureAPIGroup, "apiVersion": AzureAPIGroup + "/v1beta1"}, "v1beta1", 0},
	} {
		t.Run(reference.statement, func(t *testing.T) {
			calls := 0
			version, err := infrastructureReferenceVersion(reference.fields, func(group string) (string, error) {
				calls++
				assert.Equal(t, AzureAPIGroup, group)
				return "v1beta2", nil
			})
			require.NoError(t, err)
			assert.Equal(t, AzureAPIGroup, version.Group)
			assert.Equal(t, reference.wanted, version.Version)
			assert.Equal(t, reference.calls, calls)
		})
	}
}

func TestInvalidInfrastructureVersionsAndUnavailableDiscoveryFailClosed(t *testing.T) {
	for _, reference := range []struct {
		statement string
		fields    map[string]string
	}{
		{"MissingGroupAndVersionAreNotGuessed", map[string]string{}},
		{"ForeignVersionlessGroupIsRejected", map[string]string{"apiGroup": "other.infrastructure.example"}},
		{"ForeignExplicitVersionIsRejected", map[string]string{"apiVersion": "other.infrastructure.example/v1"}},
		{"ContradictoryGroupAndVersionAreRejected", map[string]string{"apiGroup": "other.infrastructure.example", "apiVersion": AzureAPIGroup + "/v1beta1"}},
		{"ExplicitVersionMustIncludeTheAPIGroup", map[string]string{"apiVersion": "v1beta1"}},
		{"ExplicitVersionMustNotBeEmpty", map[string]string{"apiVersion": AzureAPIGroup + "/"}},
	} {
		t.Run(reference.statement, func(t *testing.T) {
			_, err := infrastructureReferenceVersion(reference.fields, nil)
			require.Error(t, err)
		})
	}
	t.Run("VersionlessGroupRequiresDiscoverySupport", func(t *testing.T) {
		_, err := infrastructureReferenceVersion(map[string]string{"apiGroup": AzureAPIGroup}, nil)
		require.Error(t, err)
	})
	t.Run("DiscoveryErrorsAreRetained", func(t *testing.T) {
		failure := fmt.Errorf("discovery unavailable")
		_, err := infrastructureReferenceVersion(map[string]string{"apiGroup": AzureAPIGroup}, func(string) (string, error) { return "", failure })
		require.ErrorIs(t, err, failure)
	})
	for _, version := range []string{"", "v1/bogus"} {
		t.Run("MalformedDiscoveredVersionIsRejected/Version="+version, func(t *testing.T) {
			_, err := infrastructureReferenceVersion(map[string]string{"apiGroup": AzureAPIGroup}, func(string) (string, error) { return version, nil })
			require.Error(t, err)
		})
	}
}
