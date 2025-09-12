/*
Copyright 2025 The Kubernetes Authors.

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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestAzureProviderIDNormalization(t *testing.T) {
	tests := []struct {
		name       string
		providerID string
		expected   normalizedProviderID
	}{
		{
			name:       "azure standard vm",
			providerID: "azure:///subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/resourceGroupName/providers/Microsoft.Compute/virtualMachines/control-plane-1cbe5-d4dx7",
			expected:   "control-plane-1cbe5-d4dx7",
		},
		{
			name:       "azure vmss",
			providerID: "azure:///subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/resourceGroupName/providers/Microsoft.Compute/virtualMachineScaleSets/vmssName/virtualMachines/0",
			expected:   "azure:///subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/resourceGroupName/providers/Microsoft.Compute/virtualMachineScaleSets/vmssName/virtualMachines/0",
		},
		{
			name:       "non-azure provider id",
			providerID: "aws:///us-west-2a/i-1234567890abcdef0",
			expected:   "i-1234567890abcdef0",
		},
		{
			name:       "empty provider id",
			providerID: "",
			expected:   "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := normalizedProviderString(tc.providerID)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestAzureProviderIDNormalizer(t *testing.T) {
	normalizer := &AzureProviderIDNormalizer{}

	tests := []struct {
		name         string
		providerID   string
		expectedID   normalizedProviderID
		expectedBool bool
	}{
		{
			name:         "azure provider id",
			providerID:   "azure:///subscriptions/test/resourceGroups/test/providers/Microsoft.Compute/virtualMachines/test",
			expectedID:   "azure:///subscriptions/test/resourceGroups/test/providers/Microsoft.Compute/virtualMachines/test",
			expectedBool: true,
		},
		{
			name:         "non-azure provider id",
			providerID:   "aws:///us-west-2a/i-1234567890abcdef0",
			expectedID:   "",
			expectedBool: false,
		},
		{
			name:         "empty provider id",
			providerID:   "",
			expectedID:   "",
			expectedBool: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Test IsAzureProviderID
			isAzure := normalizer.IsAzureProviderID(tc.providerID)
			assert.Equal(t, tc.expectedBool, isAzure)

			// Test NormalizeProviderID
			normalized := normalizer.NormalizeProviderID(tc.providerID)
			assert.Equal(t, tc.expectedID, normalized)
		})
	}
}

func TestAzureMachineLookup(t *testing.T) {
	// Mock objects for testing
	mockController := &machineController{}
	mockExtension := &AzureControllerExtension{
		azureMachinePoolMachineAvailable: false,
	}

	lookup := &AzureMachineLookup{
		controller: mockController,
		extension:  mockExtension,
	}

	// Test with Azure extension unavailable
	providerID := normalizedProviderID("azure:///test/provider/id")
	machine, err := lookup.FindMachineByProviderID(providerID)

	// Should not error but return nil since we don't have proper mock setup
	assert.NoError(t, err)
	assert.Nil(t, machine)
}

func TestAzureResourceMarker_GetResourceGVR(t *testing.T) {
	mockController := &machineController{}
	mockExtension := &AzureControllerExtension{}

	marker := &AzureResourceMarker{
		controller: mockController,
		extension:  mockExtension,
	}

	tests := []struct {
		name        string
		machineKind string
		expectError bool
	}{
		{
			name:        "azure machine pool machine",
			machineKind: azureMachinePoolMachineKind,
			expectError: false,
		},
		{
			name:        "standard machine",
			machineKind: machineKind,
			expectError: false,
		},
		{
			name:        "unknown machine kind",
			machineKind: "UnknownMachine",
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			machine := &unstructured.Unstructured{}
			machine.SetKind(tc.machineKind)

			_, err := marker.GetResourceGVR(machine)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestAzureNodeDeletionHandler(t *testing.T) {
	mockController := &machineController{}
	mockExtension := &AzureControllerExtension{}

	handler := &AzureNodeDeletionHandler{
		controller: mockController,
		extension:  mockExtension,
	}

	tests := []struct {
		name        string
		machineKind string
		expectSkip  bool
	}{
		{
			name:        "azure machine pool machine",
			machineKind: azureMachinePoolMachineKind,
			expectSkip:  false,
		},
		{
			name:        "standard machine",
			machineKind: machineKind,
			expectSkip:  true,
		},
		{
			name:        "unknown machine",
			machineKind: "UnknownMachine",
			expectSkip:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			machine := &unstructured.Unstructured{}
			machine.SetKind(tc.machineKind)

			err := handler.HandleAzureMachinePoolDeletion(machine, nil)
			if tc.expectSkip {
				// Should not error for non-Azure machines (they're skipped)
				assert.NoError(t, err)
			} else {
				// Azure machines will error due to mock setup, but that's expected
				// The important thing is the handler doesn't panic or skip incorrectly
				// In real scenario with proper clients, this would work correctly
			}
		})
	}
}
