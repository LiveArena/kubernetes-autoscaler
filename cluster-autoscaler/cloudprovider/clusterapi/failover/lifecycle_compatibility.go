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
	"fmt"
)

func validateLegacyConfigurationFields(encoded string) error {
	var object struct {
		Pairs map[string]map[string]json.RawMessage `json:"pairs"`
	}
	if err := json.Unmarshal([]byte(encoded), &object); err != nil {
		return err
	}
	for _, pair := range object.Pairs {
		if _, found := pair["intent"]; found {
			return fmt.Errorf("lifecycle fields require configuration v2")
		}
	}
	return nil
}

func validateStateFields(encoded string) error {
	var object struct {
		Pairs map[string]map[string]json.RawMessage `json:"pairs"`
	}
	if err := json.Unmarshal([]byte(encoded), &object); err != nil {
		return err
	}
	for _, pair := range object.Pairs {
		if lifecycle, found := pair["lifecycle"]; found && string(lifecycle) == "null" {
			return fmt.Errorf("lifecycle must be omitted or contain a versioned record")
		}
	}
	return nil
}
