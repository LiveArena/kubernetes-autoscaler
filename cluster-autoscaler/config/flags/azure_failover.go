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
