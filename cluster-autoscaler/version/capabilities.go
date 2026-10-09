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

package version

import (
	"encoding/json"
	"io"
	"net/http"
	"runtime/debug"
	"strconv"
)

// CapabilitiesAPIVersion identifies the capability-report JSON contract.
const CapabilitiesAPIVersion = "aiproducer.com/autoscaler-capabilities/v1"

// AzureMachinePoolFailoverCapability identifies compiled Azure MachinePool failover support.
const AzureMachinePoolFailoverCapability = "azure-machinepool-failover-v1"

// CapabilityReport separates compiled support, source metadata and configured runtime settings.
type CapabilityReport struct {
	APIVersion   string                  `json:"apiVersion"`
	Version      string                  `json:"version"`
	Capabilities []string                `json:"capabilities"`
	Source       CapabilitySource        `json:"source"`
	Configured   CapabilityConfiguration `json:"configured"`
}

// CapabilitySource reports embedded build metadata, which may be incomplete or dirty.
type CapabilitySource struct {
	Status    string `json:"status"`
	VCS       string `json:"vcs"`
	Revision  string `json:"revision"`
	Modified  *bool  `json:"modified"`
	GoVersion string `json:"goVersion"`
}

// CapabilityConfiguration describes supplied settings without evaluating runtime readiness.
type CapabilityConfiguration struct {
	CloudProvider  string `json:"cloudProvider"`
	FailoverMode   string `json:"azureMachinePoolFailoverMode"`
	LeaderElection bool   `json:"leaderElection"`
	Readiness      string `json:"readiness"`
}

// NewCapabilityReport reports compiled support and supplied settings without connecting to Kubernetes.
func NewCapabilityReport(provider, mode string, leaderElection bool) CapabilityReport {
	build, _ := debug.ReadBuildInfo()
	return capabilityReport(build, provider, mode, leaderElection)
}

func capabilityReport(build *debug.BuildInfo, provider, mode string, leaderElection bool) CapabilityReport {
	report := CapabilityReport{
		APIVersion:   CapabilitiesAPIVersion,
		Version:      ClusterAutoscalerVersion,
		Capabilities: []string{AzureMachinePoolFailoverCapability},
		Source:       CapabilitySource{Status: "unknown"},
		Configured:   CapabilityConfiguration{CloudProvider: provider, FailoverMode: mode, LeaderElection: leaderElection, Readiness: "not-evaluated"},
	}
	if build == nil {
		return report
	}
	report.Source.GoVersion = build.GoVersion
	for _, setting := range build.Settings {
		switch setting.Key {
		case "vcs":
			report.Source.VCS = setting.Value
		case "vcs.revision":
			report.Source.Revision = setting.Value
		case "vcs.modified":
			if modified, err := strconv.ParseBool(setting.Value); err == nil {
				report.Source.Modified = &modified
			}
		}
	}
	if report.Source.VCS != "" && report.Source.Revision != "" && report.Source.Modified != nil {
		report.Source.Status = "available"
	}
	return report
}

// WriteCapabilities encodes the capability report as JSON.
func WriteCapabilities(writer io.Writer, report CapabilityReport) error {
	return json.NewEncoder(writer).Encode(report)
}

// CapabilityHandler serves the report as non-cacheable JSON for GET requests only.
func CapabilityHandler(report CapabilityReport) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.Header().Set("Allow", http.MethodGet)
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Cache-Control", "no-store")
		_ = WriteCapabilities(writer, report)
	}
}
