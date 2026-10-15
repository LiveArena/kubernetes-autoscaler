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

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func infrastructureReferenceVersion(reference map[string]string, preferred func(string) (string, error)) (schema.GroupVersion, error) {
	group := reference["apiGroup"]
	if group != "" && group != AzureAPIGroup {
		return schema.GroupVersion{}, fmt.Errorf("invalid infrastructure API group %q", group)
	}
	if explicit := reference["apiVersion"]; explicit != "" {
		version, err := schema.ParseGroupVersion(explicit)
		if err != nil || version.Group != AzureAPIGroup || version.Version == "" || group != "" && group != version.Group {
			return schema.GroupVersion{}, fmt.Errorf("invalid infrastructure API version %q for group %q", explicit, group)
		}
		return version, nil
	}
	if group == "" || preferred == nil {
		return schema.GroupVersion{}, fmt.Errorf("versionless infrastructure reference requires an API group and management discovery")
	}
	version, err := preferred(group)
	if err != nil {
		return schema.GroupVersion{}, fmt.Errorf("resolve infrastructure API group %q: %w", group, err)
	}
	parsed, err := schema.ParseGroupVersion(group + "/" + version)
	if err != nil || parsed.Group != group || parsed.Version == "" {
		return schema.GroupVersion{}, fmt.Errorf("invalid discovered infrastructure API version %q", version)
	}
	return parsed, nil
}

func (policy *Policy) infrastructureVersion(reference map[string]string) (schema.GroupVersion, error) {
	var preferred func(string) (string, error)
	if resolver, supported := policy.Environment.(InfrastructureVersionResolver); supported {
		preferred = resolver.PreferredInfrastructureVersion
	}
	return infrastructureReferenceVersion(reference, preferred)
}
