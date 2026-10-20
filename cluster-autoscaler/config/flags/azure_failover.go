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

package flags

import (
	"flag"
	"fmt"
)

type azureFailoverModeValue string

func (value *azureFailoverModeValue) String() string {
	return string(*value)
}

func (value *azureFailoverModeValue) Set(mode string) error {
	if mode != "disabled" && mode != "active" && mode != "freeze" {
		return fmt.Errorf("azure-machinepool-failover-mode must be disabled, active, or freeze")
	}
	*value = azureFailoverModeValue(mode)
	return nil
}

func newAzureMachinePoolFailoverModeFlag() *azureFailoverModeValue {
	value := azureFailoverModeValue("disabled")
	flag.Var(&value, "azure-machinepool-failover-mode", "Azure MachinePool failover mode: disabled, active, or freeze. ClusterAPI only")
	return &value
}
