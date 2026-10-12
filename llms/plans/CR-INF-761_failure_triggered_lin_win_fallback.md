---
id: CR-INF-761
title: Failure-triggered primary/secondary worker failover
status: approved-for-local-implementation
date: 2026-10-07
updated: 2026-10-12
requestor: Louis Hansen
tracking-issue: INF-761
branch: feature/inf-761-implement-failure-triggered-linwin-fallback-in-cluster
implementation-base: 130a28dbc6e85420ca96b64d1d6d075445eff6f7
---

# Change Request: Failure-Triggered Primary/Secondary Worker Failover

## Purpose and Status

Implement demand-driven fallback between explicitly selected primary and secondary
Azure MachinePools when primary provisioning cannot supply eligible workload demand.
The feature preserves primary desired replicas for CAPZ retries and permits usable
capacity from either role to satisfy workloads.

This CR records what was decided, why, and how the local implementation satisfies
those decisions. It is the autoscaler workstream for
[INF-761](https://linear.app/aiproducer/issue/INF-761), under
[INF-759](https://linear.app/aiproducer/issue/INF-759). External governance is
CR-INF-759 and ADR-0004 in `LiveArena/tfapply`; the
[implementation handoff](https://linear.app/aiproducer/document/inf-761-cluster-autoscaler-implementation-handoff-8fadf700b148)
provides consumer context. This CR does not amend the sibling repository.

Local implementation is approved and present. Release provenance, consumer
acceptance and deployed behavior are separate gates. Implementation approval does
not authorize commits, pushes, image publication, live fault injection or tenant
mutation. No tested release or successful live retirement is claimed here.

## Scope and Ownership

| Owner | Responsibility | Rationale |
|---|---|---|
| Cluster Autoscaler | Demand-driven replica requests, capacity accounting, pair admission, durable failover/lifecycle reconciliation and ordinary safe scale-down | One demand-driven writer avoids competing replica decisions |
| CAPZ | Azure allocation, primary retry and remediation | Fallback must not replace the infrastructure controller's functioning allocation/retry behavior |
| tfapply | Tenant settings, fresh pool generations, zone/SKU validation, templates, Spot resolution, chart lifecycle, image/arguments, management RBAC and existing pool-retirement/targeted-purge orchestration | Deployment and resource cleanup remain with the existing owning workstream |

The implementation preserves existing discovery, provider-ID normalization,
resize/deletion behavior and templates. Unconfigured groups, disabled baseline
deployments and other providers retain their ordinary behavior.

Leadership redesign, HA/Lease changes, a global autoscaler pause, distributed
fencing/acknowledgement protocols, infrastructure cleanup controllers and forced
workload migration are outside scope. Existing elected-process and operational
safety mechanisms remain authoritative. This work does not change tenant minima,
substitute GPU SKUs, search a third zone or perform unrelated upgrades/fixes.

## Implemented Decisions

### Failure Evidence and Incoming Capacity

**Decision:** qualifying evidence is a live, nondeleting AMPM with
`status.provisioningState == "Failed"`, usable attempt identity/timestamp and the
selected AzureMachinePool owner. Namespace, Cluster ownership, MachinePool
infrastructure reference and physical UIDs are validated before applying it.
Pending, bootstrap, deletion, silence and missing selection are not failures.

**Rationale:** a failed desired-to-Ready gap must not masquerade as nodes expected
to arrive. Reducing replicas or treating destructive instance cleanup as evidence
would cancel primary intent instead of correcting demand accounting.

The optional capacity policy adjusts the shared upcoming-node accounting consumed
by both snapshot pod filtering and scale-up orchestration. Failed unregistered
capacity is not credited; mapped terminally failed registered nodes are excluded.
Real Ready nodes and independently viable registered arrivals remain usable.
Accounting changes do not mutate replicas, provider-ID lists or scale-up records.

Failure watermarks make repeated/replacement observations idempotent. The event
buffer retains at most 64 AMP-owner keys. Overflow closes admission until selected
pairs are reconciled using the synchronized management informer snapshot. Resync
retains at most two selected-role observations per pair, preserves newer events,
and clears overflow only after successful reconciliation with no newer dropped
event. Snapshot/state failure keeps admission closed rather than losing evidence.

### Primary-First Demand and Bounded Requests

**Decision:** fresh fitting residual demand gets a bounded primary provisioning
opportunity when primary has headroom. An old degraded latch does not permanently
route new demand to secondary.

**Rationale:** fallback supplies unmet demand without becoming sticky or producing
replica debt across scans, attempts or restart.

Request intent is persisted before the scale write. The fresh primary increment
gets bounded incoming credit separately from the old failed gap, without counting
registered arrivals twice. Its deadline uses the existing per-group
`MaxNodeProvisionTime`, including overrides; the CLI default is 15 minutes.
Deadlines survive restart. Qualifying later failure or deadline expiry releases
only the applicable unmet increment for fallback. Current primary saturation or
verified scheduler-fit/hard-limit exclusions can also justify scoped fallback;
internal predicate errors cannot.

Before/after targets distinguish unwritten retry from a committed request.
Unexpected target changes close admission. Allowances are bounded rather than
added for every failure, and committed secondary requests are reconciled without
replay. Secondary failure does not veto healthy-primary new-demand scaling.
Independently justified secondary-only intent survives primary recovery.

**Primary desired replicas remain intact even after secondary Ready capacity
covers the demand.** CAPZ can continue retrying primary. Clearing a failover request
record or allowance is not cancellation of the pool target. Either usable arrival
can satisfy the workload; late excess capacity remains eligible for safe scale-down.
Optional retain-target guards prevent generic failed-request cleanup and target
repair from cancelling this intent beyond the ordinary provisioning timeout;
they do not veto legitimate safe scale-down.

### Limits, Recovery and Scale-Down

**Decision:** ordinary scheduler predicates, health/backoff, group/global resource
limits, minima, maxima and scale-up controls remain authoritative.

Secondary expansion requires the full estimated increment. A request that cannot
fit the group's headroom, cluster-wide limits or final balancing capacity is
rejected, not truncated. The orchestrator checks this where it still knows the
original estimate; the provider sees only the final delta. Nonparticipating
groups retain their ordinary partial-scaling behavior.

Tfapply initializes fallback sizing from the corresponding primary settings;
fallback initial target/minimum are zero. The autoscaler honors each pool's current
`cluster.x-k8s.io/cluster-api-autoscaler-node-group-max-size` annotation. Workload
compatibility comes from rendered constraints, not pair names or assumed equal
Spot/taint/topology settings. Absent/null fallback Spot settings inherit the
primary's resolved choice; tfapply-owned boolean overrides affect fallback only.

Recovery requires new positive primary readiness and retained target completion
on two distinct failure-free scans. Silence, deletion and cooldown do not prove
recovery. Secondary failure rechecks start from the scan interval and double to a
15-minute ceiling, with timing persisted. Neither role's retries create copies
of already outstanding failed capacity.

For equivalent safely removable surplus within a pair, the planner prefers
secondary removal. Existing eligibility, simulation, PDB/drain/storage safeguards,
timing and minima remain unchanged. Empty/risky removal classes and unrelated pair
positions are preserved. An unused late primary can be removed before a secondary
hosting protected work; primary spare preference is not indefinite retention.

## Configuration and Selection Contract

**Decision:** pair identifiers are opaque, generation-specific keys. Explicit
configuration selects exactly one named primary and one named secondary.

**Rationale:** physical overlap during rollover must not create implicit winners,
extra admission slots or fallback fan-out. OS/GPU scheduling belongs to templates
and predicates, not identifier interpretation.

Pair/registration identifiers use nonempty Kubernetes label-value syntax
(1-63 characters). There is no fixed pair-count limit. Extra physical generations
remain discoverable but are not additional candidates for paired growth. Missing
or unverifiable selection blocks admission and preserves state instead of
resetting records, deleting resources or inferring a winner.

Selected MachinePools have these annotations:

| Annotation | Value |
|---|---|
| `aiproducer.com/failover-pair` | Opaque pair identifier |
| `aiproducer.com/failover-role` | `primary` or `secondary` |

Authoritative configuration is the exact-name, Cluster-owned management ConfigMap
`<cluster>-autoscaler-failover-config` in the tenant/management namespace, with
`data["config.json"]`. The reader validates JSON identity, namespace/Cluster owner,
complete unique references, live `spec.clusterName`, role metadata, infrastructure
ownership and physical UIDs. Duplicate/unknown fields, malformed data and
unsupported versions are rejected.

The supported wire contracts are pairing-only
`aiproducer.com/worker-failover/v1` and lifecycle-intent
`aiproducer.com/worker-failover/v2`. V1 requires at least one complete pair and
does not accept lifecycle intent. V2 requires explicit intent for every declared
pair and permits an empty `pairs` object. A v2 example is:

```json
{
  "apiVersion": "aiproducer.com/worker-failover/v2",
  "cluster": {
    "name": "managed-cluster",
    "namespace": "tenant",
    "uid": "actual-cluster-uid"
  },
  "pairs": {
    "generation-key": {
      "primary": { "name": "fresh-primary" },
      "secondary": { "name": "fresh-secondary" },
      "intent": {
        "state": "Creating",
        "registrationID": "registration-for-this-introduction",
        "resourceRetirementComplete": false
      }
    }
  }
}
```

V2 handles incomplete/missing members per pair when shared configuration is valid,
so a retiring or creating generation does not stop another Active pair. Invalid
shared ownership/configuration closes that configuration's admission scope.
The pairing-only v1 path requires its shared declared selection to be valid.

## Durable State and Version Ownership

**Decision:** persist bounded Cluster-scoped state in the Cluster-owned management
ConfigMap `<cluster>-autoscaler-failover-state`, `data["state.json"]`.

**Rationale:** failure evidence, before/after request intent, deadlines and
registration identity must survive restart without being applied to another
physical generation. Observation disappearance alone cannot erase those facts.

Both raw configuration and state JSON strings have separate 64 KiB (65,536-byte)
bounds, excluding ConfigMap metadata. Whitespace counts on reads. State writes
check encoded JSON before saving. Oversize input/update is rejected without
truncation or automatic purge. Retained records consume the same state budget.
There is no per-pod debt or unbounded attempt history: each role has an attempt
watermark/readiness fingerprint, and each pair has bounded allowance plus at most
one primary and one secondary request.

| Version | Field/Contract | Definition Owner |
|---|---|---|
| `StateVersion = 1` | Outer `State.version` | State definition in `state.go` |
| `LifecycleStateVersion = 1` | Optional nested `PairLifecycle.version` | Lifecycle definition in `pair_lifecycle.go` |
| Configuration APIs v1/v2 | `Configuration.apiVersion`, selecting pairing-only or lifecycle-intent wire shape | `config.go` |
| Capability-report API v1 | Report `apiVersion` | `version/capabilities.go` |
| Pool/AMP API versions | External Kubernetes `apiVersion` for exact resource lookup | Immutable registered member bindings |

The outer state and nested lifecycle are newly introduced formats with independent
initial version 1. Future version increments follow actual supported-format
changes, not development iterations. A genuinely pre-existing unversioned format
would be treated as baseline 1 and its changed format as 2. Unsupported versions,
null lifecycle records, orphaned lifecycle binding fields and incomplete lifecycle
identities fail closed.

State writes use resourceVersion/CAS retry and validate current lifecycle intent
and immutable bindings. Config, state and scale updates are separate transactions;
this is conservative replay protection, not atomic exactly-once execution. No
automatic record conversion or migration is provided.

## Pair Lifecycle and Record Retirement

**Decision:** lifecycle is separate from the failure-policy phase
Healthy/Degraded/Recovering. Tfapply owns desired rollout intent; the autoscaler
owns observed registration and operation-completion status.

**Rationale:** old and replacement generations must overlap safely without
mixing their history or requiring a new infrastructure-cleanup system.

Normal introduction/replacement uses two fresh physical members. A Pool or AMP
UID already registered to any retained pair cannot be reused under another key
or opposite role. An unchanged redeploy keeps its registration. Adoption of an
existing unpaired pool is outside the normal rollover contract; tfapply must
supply fresh generations rather than relabel retained members.

V2 desired states are Creating, Active and Retiring. `registrationID` identifies
one explicit introduction; it changes for fresh re-registration after purge.
It is not a leadership epoch or distributed fence. `resourceRetirementComplete`
is valid only with Retiring and is tfapply's confirmation of ordinary resource
retirement. Observed `lifecycle` contains `version`, `state`, `registrationID`,
`operationsSettled` and `reason`:

| Lifecycle | Implemented Behavior |
|---|---|
| Creating | Complete live identities register fresh state; incomplete selection stays closed. No new paired growth; CAPZ may provision declared replicas and a zero-node secondary is valid |
| Active | Matching registered generation uses ordinary failover admission and preserves durable state across unchanged redeploy/restart |
| Retiring | Deny new increases and fit exceptions for this ID; preserve capacity, unresolved evidence and ordinary safe scale-down while reconciling admitted operations |
| Retired | Requires ordinary-retirement confirmation, absence of both recorded Pool/AMP objects and settled ID operations; no autoscaler reactivation, and eligible for external targeted purge |
| Purged | Absence after external purge, not accumulated tombstones. Fresh explicit Creating with new registration/current members can initialize without old history |

### Admission and Completion Safety

Admission is revalidated during durable-intent preparation and immediately before
the scale update. Local scale/intent operations are counted per group through the
existing writer lifecycle. Persisting a request immediately marks operations
unsettled. Retiring reconciles authoritative committed targets; unconfirmed
requests remain unresolved. Confirmed ordinary retirement with recorded resources
absent can settle remaining intent only after local admitted calls return.

Every lifecycle-aware store attempt checks forward-only state, current
registration and immutable names/API versions/UIDs. New-key creation checks
authoritative intent even when no lifecycle records remain. Under v2 intent,
absence permits only fresh Creating registration with live binding validation.
Stale observations/callbacks and CAS retries cannot restore Retired/purged history
or replace a fresh registration with a captured old record.

Missing resources, omitted selection, a displayed lifecycle flag or local
cancellation alone do not prove retirement. `operationsSettled` is not proof of
CAPZ allocation/workload/storage cleanup or another process's remote outcome.
After restart, paired or validated unpaired replacement pools recover exact
owning Cluster scope. Without a discoverable owning pool scope, retirement-status
reconciliation waits instead of guessing or listing ConfigMaps broadly. Unpaired
admission does not depend on that status reconciliation.

### External Purge Sequence

Tfapply uses the same existing retirement process and completion criteria as for
equivalent unpaired pools. Keep active/freeze mode while lifecycle intent/status
is being reconciled. Its authorized completion path:

1. Supplies Retiring intent and completes ordinary workload/resource retirement.
2. Confirms resource completion and observes matching Retired/settled status.
3. Removes that registration from authoritative intent.
4. Removes only the confirmed retired pair entry using live identity and
   resourceVersion/precondition checks, preserving other pairs' records.

Final global disable follows completed external retirement. No autoscaler state
deletion permission, automatic identifier pruning or infrastructure cleanup is
introduced. Identifier reuse requires full prior record/operation retirement and
purge, even with no remaining failure allowance. A new key is not proof that old
resources were cleaned up. Fresh re-registration requires current fresh members
and a new registration identity; existing ordinary records are not silently adopted.

## Runtime Modes and Deployment

The single mode flag is `--azure-machinepool-failover-mode=disabled|active|freeze`,
default disabled. Invalid values are rejected; mode is not duplicated in JSON.

| Mode | Behavior | Rationale |
|---|---|---|
| disabled | No failover/lifecycle writer; recognized secondary growth is blocked while existing capacity remains visible | Capable code alone must preserve the unactivated baseline |
| active | Reconcile selected pairs and apply demand-driven policy | Enable fallback only under verified current identity and durable admission |
| freeze | Continue observation, state/lifecycle reconciliation, accounting and normal safe scale-down; deny new secondary requests | Retire capacity without erasing state or hiding running workloads |

Pair-specific Creating/Retiring/Retired admission applies during active/freeze
reconciliation. Active/freeze require AMPM availability/initial synchronization
and existing autoscaler leader election. Similar-group balancing is rejected
for these modes because it could bypass selected-pair admission.

Installing capable code with topology disabled requires no pair configuration
or feature state and leaves ordinary autoscaling unchanged. Activation verifies
the capable deployment before enabling fresh pools and pairing. Active redeploy
preserves state. Deactivation uses freeze/pair retirement and external cleanup;
leftover objects/state are incomplete retirement, not a clean disabled baseline.
Later activation follows completed retirement/purge and explicit fresh registration.
An old incapable image with active secondaries is not an approved rollback.

## Management Access and Capability Verification

Configuration/state use the management client; workload nodes/pods use the
workload-side discovery/snapshot. Configuration is read by exact-name GET on scans
and before admitted writes. Reuse the prepared management permissions:

- Configuration ConfigMap: exact-name read-only access.
- State ConfigMap: exact-name read/update/patch and tenant-scoped create, because
  Kubernetes cannot restrict create by object name.
- Existing owning-Cluster, MachinePool/scale, AMP and AMPM discovery/write access.

Prepared name-scoped list/watch permissions may remain, but the config/state
reader does not require namespace-wide list/watch. Any name-scoped list/watch
must use an exact `metadata.name` selector. No configuration write, state delete
or additional management Lease permission is required.

CLI `--capabilities` emits JSON and exits before connecting to Kubernetes.
Diagnostic HTTP `GET /capabilities` uses
`aiproducer.com/autoscaler-capabilities/v1`. Compiled identifiers are
`azure-machinepool-failover-v1` and `azure-machinepool-pair-lifecycle-v1`.
Lifecycle-intent consumption requires the lifecycle capability as well as failover
support. Reporting separates compiled support, version, embedded source metadata
and configured provider/mode/election. Readiness is `not-evaluated`; unknown or
dirty source metadata is not clean released-image proof.

Tfapply must verify approved immutable image/source and the running capable
deployment, then independently validate live configuration/rollout/readiness.
Flags, guessed tags, version strings, ConfigMaps, manual capability booleans or
CLI inspection alone do not prove the running deployment is ready. Capability
JSON is not a retirement acknowledgement. Consumer acceptance of this contract
is a delivery gate, not permission to bypass activation checks.

## Architecture and Maintenance

Feature policy, state and lifecycle logic live under
`cluster-autoscaler/cloudprovider/clusterapi/failover`. The package's
`Group`/`Environment` boundary adapts parent discovery, resource access and
synchronized AMPM snapshots without importing the parent package. Controller/
informer registration and workload lookup remain in thin parent adapters.

Optional capacity-policy hooks integrate shared accounting, scale-up and paired
scale-down ordering. Required cloud-provider interfaces and upstream scheduler,
estimator, resize and scale-down algorithms are not replaced. This limits future
upstream merge cost while keeping both demand consumers consistent.

Definitions stay with their owning types/contracts: state format/size with State,
lifecycle format with PairLifecycle, pairing metadata and modes with configuration,
and resource identifiers with the provider adapter. Implementation files near
100 lines and test files near 250 are responsibility-review guidelines, not hard
caps. Tests are grouped by registration, retirement and store safety with shared
fixtures; assertions and concurrency boundaries remain visible. New Go source
uses the repository's standard Apache boilerplate and exported APIs use GoDoc.

## Verification and Delivery Boundaries

Executable tests are the behavior evidence. Provider/store/scheduler regressions
exercise accounting sensitivity, residual demand, request limits, replay/restart,
recovery, pair isolation, lifecycle transitions, in-flight operation settlement,
stale callbacks, CAS conflicts and fresh registration after simulated external
purge. Linux GPU and Windows fixtures exercise the same role-based algorithm.
These offline results do not prove real Azure allocation or resource retirement.

Required local gates use Linux Docker and Go 1.24.0. If Docker is unavailable,
pause for owner agreement before host-toolchain alternatives. From the
`cluster-autoscaler` module, the affected regression gate is:

```sh
go test -race -p 2 -count=1 -vet=atomic,bool,buildtags,directive,errorsas,ifaceassert,nilfunc,slog,stringintconv,tests ./cloudprovider/clusterapi ./cloudprovider/clusterapi/failover ./clusterstate ./core ./core/scaleup/orchestrator ./core/scaledown/planner ./config/flags ./version
```

The vet selection follows the module Makefile. Build with `CGO_ENABLED=0`,
`go build -mod=readonly -p 2`, and run applicable golint, gofmt, repository
boilerplate and patch-whitespace checks. Affected race suites, golint and repository
boilerplate have passed locally. The final release source still requires a full
executable build and capability smoke check; a checked working tree is not a
reviewed release commit or image.

The disposable real-API gate is an opt-in integration suite, not a dependency of
ordinary unit tests. Its four race-enabled tests connect to an externally
provisioned, explicitly authorized disposable Kubernetes API with etcd storage
and RBAC. The repository does not include a test Dockerfile, container image
definition or pinned image versions. Environment creation, image/toolchain
selection, server logs and final environment teardown remain outside the test
source. Use Docker-first tooling; no Azure credentials, CAPZ controller, actual
workers or workload/storage retirement are required.

Supply the endpoint and disposable marker in the test environment, then run
from the `cluster-autoscaler` module:

```sh
make test-failover-api
```

The Make target requires `INF761_TEST_API` and `INF761_TEST_API_DISPOSABLE=1`;
absence is a failed gate rather than a passing skipped run. Ordinary Go suites
skip these tests without opt-in or under `go test -short`. Tests create/delete
their namespaces and cluster-scoped CRDs, so this must not be a shared or
production API. Final control-plane/storage teardown belongs to its external
owner. Repeatability is checked within that disposable environment with:

```sh
go test -race -p 2 -count=2 -shuffle=on -vet=atomic,bool,buildtags,directive,errorsas,ifaceassert,nilfunc,slog,stringintconv,tests -run '^TestDisposableAPI' ./cloudprovider/clusterapi/failover
```

Coverage checks real server-assigned bindings, Creating/Active/Retiring admission,
independent-pair growth, committed/unconfirmed/in-flight scale calls, actual
resourceVersion conflicts, targeted external purge, reload and fresh registration.
The HTTP barriers control ordering while forwarding to the real API; no fake
conflicts replace server responses. Synthetic retirement/purge is performed by
a separate test actor, not a production cleanup controller.

Public test-only admin/denied/scoped identities are in
[api-tokens.csv](../../cluster-autoscaler/cloudprovider/clusterapi/failover/test-fixtures/api-tokens.csv).
Configure the externally owned API's test authentication using that read-only
fixture (for example, `--token-auth-file` pointing to its supplied path).
**Never use these identities with a production API.** Neither this gate nor local operation tracking proves
distributed fencing, Azure allocation, workload/storage cleanup or remote
cancellation outcomes.

Remaining delivery gates are tfapply's fresh whole-pair/unpaired rollover and
targeted-purge integration, consumer/image/version acceptance, final tested commit
and immutable image provenance, and explicitly authorized deployed Azure/GPU/
Windows/storage/lifecycle acceptance. Complete crash, delayed-cache, placement
and mixed-demand permutations are not exhaustively covered. These verification
limits do not authorize new cleanup/leadership functionality or unsafe rollout.