package version

import (
	"encoding/json"
	"io"
	"net/http"
	"runtime/debug"
	"strconv"
)

const CapabilitiesAPIVersion = "aiproducer.com/autoscaler-capabilities/v1"
const AzureMachinePoolFailoverCapability = "azure-machinepool-failover-v1"

type CapabilityReport struct {
	APIVersion   string                  `json:"apiVersion"`
	Version      string                  `json:"version"`
	Capabilities []string                `json:"capabilities"`
	Source       CapabilitySource        `json:"source"`
	Configured   CapabilityConfiguration `json:"configured"`
}

type CapabilitySource struct {
	Status    string `json:"status"`
	VCS       string `json:"vcs"`
	Revision  string `json:"revision"`
	Modified  *bool  `json:"modified"`
	GoVersion string `json:"goVersion"`
}

type CapabilityConfiguration struct {
	CloudProvider  string `json:"cloudProvider"`
	FailoverMode   string `json:"azureMachinePoolFailoverMode"`
	LeaderElection bool   `json:"leaderElection"`
	Readiness      string `json:"readiness"`
}

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

func WriteCapabilities(writer io.Writer, report CapabilityReport) error {
	return json.NewEncoder(writer).Encode(report)
}

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
