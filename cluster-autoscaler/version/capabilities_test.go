package version

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCapabilityReportUsesEmbeddedSourceWithoutClaimingRuntimeReadiness(t *testing.T) {
	report := NewCapabilityReport("clusterapi", "freeze", true)
	build, _ := debug.ReadBuildInfo()
	assert.Equal(t, capabilityReport(build, "clusterapi", "freeze", true), report)
	assert.Equal(t, "not-evaluated", report.Configured.Readiness)
	assert.Equal(t, []string{AzureMachinePoolFailoverCapability}, report.Capabilities)
}

func TestCapabilitySourceMetadataDistinguishesCleanDirtyAndUnknownBuilds(t *testing.T) {
	for _, scenario := range []string{"missing", "clean", "dirty", "incomplete", "invalid"} {
		statement := map[string]string{
			"missing":    "MissingBuildMetadataIsReportedAsUnknown",
			"clean":      "CompleteCleanBuildMetadataIsReportedAsAvailableAndUnmodified",
			"dirty":      "CompleteDirtyBuildMetadataIsReportedAsAvailableAndModified",
			"incomplete": "IncompleteBuildMetadataIsNotAssumedClean",
			"invalid":    "InvalidModifiedMetadataIsNotAssumedClean",
		}[scenario]
		t.Run(statement, func(t *testing.T) {
			var build *debug.BuildInfo
			if scenario != "missing" {
				modified := "false"
				if scenario == "dirty" {
					modified = "true"
				} else if scenario == "invalid" {
					modified = "unknown"
				}
				build = &debug.BuildInfo{GoVersion: "go1.24.0", Settings: []debug.BuildSetting{{Key: "vcs", Value: "git"}, {Key: "vcs.revision", Value: "source-revision"}, {Key: "vcs.modified", Value: modified}}}
				if scenario == "incomplete" {
					build.Settings = build.Settings[:2]
				}
			}
			report := capabilityReport(build, "clusterapi", "disabled", true)
			assert.Equal(t, []string{AzureMachinePoolFailoverCapability}, report.Capabilities)
			assert.Equal(t, "not-evaluated", report.Configured.Readiness)
			if scenario == "clean" || scenario == "dirty" {
				assert.Equal(t, "available", report.Source.Status)
				require.NotNil(t, report.Source.Modified)
				assert.Equal(t, scenario == "dirty", *report.Source.Modified)
			} else {
				assert.Equal(t, "unknown", report.Source.Status)
				assert.Nil(t, report.Source.Modified)
			}
		})
	}
}

func TestCapabilityJSONMatchesHTTPAndRejectsNonGETRequests(t *testing.T) {
	for _, mode := range []string{"disabled", "active", "freeze"} {
		t.Run("HTTPMatchesJSONForConfiguredMode="+mode, func(t *testing.T) {
			report := capabilityReport(nil, "clusterapi", mode, true)
			var cli bytes.Buffer
			require.NoError(t, WriteCapabilities(&cli, report))
			response := httptest.NewRecorder()
			CapabilityHandler(report)(response, httptest.NewRequest(http.MethodGet, "/capabilities", nil))
			assert.Equal(t, http.StatusOK, response.Code)
			assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
			assert.Equal(t, "no-store", response.Header().Get("Cache-Control"))
			assert.Equal(t, cli.String(), response.Body.String())
			decoded := CapabilityReport{}
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded))
			assert.Equal(t, report, decoded)
			assert.Equal(t, CapabilitiesAPIVersion, decoded.APIVersion)
		})
	}
	response := httptest.NewRecorder()
	CapabilityHandler(capabilityReport(nil, "clusterapi", "active", false))(response, httptest.NewRequest(http.MethodPost, "/capabilities", nil))
	assert.Equal(t, http.StatusMethodNotAllowed, response.Code)
	assert.Equal(t, http.MethodGet, response.Header().Get("Allow"))
}
