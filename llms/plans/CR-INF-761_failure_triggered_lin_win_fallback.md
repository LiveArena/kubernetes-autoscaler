---
id: CR-INF-761
title: Failure-triggered Linux GPU and Windows fallback
status: approved-for-local-implementation
date: 2026-10-07
requestor: Louis Hansen
tracking-issue: INF-761
branch: feature/inf-761-implement-failure-triggered-linwin-fallback-in-cluster
implementation-base: 130a28dbc6e85420ca96b64d1d6d075445eff6f7
---

# Change Request: Failure-Triggered Linux GPU and Windows Fallback

## Status and Authorization

This is the autoscaler implementation proposal for
[INF-761](https://linear.app/aiproducer/issue/INF-761), under
[INF-759](https://linear.app/aiproducer/issue/INF-759).
It records implementation approval, not implementation, test results, or release.

Louis Hansen approved this CR and the CR-0040/ADR-0003 approach for local
implementation in this conversation on 2026-10-07. Approval does not authorize
commits, pushes, image publication, live fault injection, or infrastructure
mutation. Cross-repository interfaces remain subject to the agreement below.

The ticket branch was created from the existing `livearena/release` HEAD above.
The starting worktree was clean; no fetch, rebase, reset, or upstream substitution
was performed. This base also matches the ticket-supplied behavioral baseline;
image tag-to-commit provenance remains unverified.

External governance sources are CR-0040 (Failure-Triggered Worker Failover) and
ADR-0003 (Autoscaler-Owned Worker Failover) in the `LiveArena/tfapply` repository.
Their source records remain marked proposed; the implementation approval above
does not modify those external records. Refer to
[INF-759](https://linear.app/aiproducer/issue/INF-759) and the
[INF-761 implementation handoff](https://linear.app/aiproducer/document/inf-761-cluster-autoscaler-implementation-handoff-8fadf700b148)
for requirements and source provenance. The handoff identifies external
documentation baseline `96d99e8d`; no unpublished repository URL is assumed.

The ticket requires governance approval before code edits. Owner approval of
this CR and the applicable CR-0040/ADR-0003 proposal **MUST** be recorded before
implementation. Creating this document does not constitute approval. No commit,
push, image publication, live fault injection, or infrastructure mutation is
authorized. The upstream `CONTRIBUTING.md` **MUST NOT** be treated as authoritative
for this fork without consulting Louis Hansen first.

## Problem and Discriminating Check

Terminal primary AzureMachinePoolMachine (AMPM) provisioning failures can leave
desired replicas intact but unfulfilled. Supplied incident evidence records
desired three and Ready one, with repeated failed replacement attempts well
before the current 45-minute provisioning timeout. It does not establish
alternate-zone capacity or prove the live pending pods' scheduling cause.

The source hypothesis is that `ClusterStateRegistry.GetUpcomingNodes` continues
crediting the failed primary desired-to-ready gap. Both static snapshot pod
filtering and scale-up orchestration consume that accounting, potentially hiding
unmet demand behind unreliable placeholders. Pools or priorities alone cannot
fix demand already filtered out.

The discriminating regression **MUST** run production scan paths with an occupied
Ready primary node, primary target three, and two fitting pending pods. Terminal
failure **MUST** produce exactly two secondary requests while preserving primary
target three. Disabling only the accounting adjustment **MUST** break that
assertion. This is a planned test, not a validated implementation claim.

## Scope and Fork Preservation

The implementation **MUST** extend the existing Azure/Cluster API integration,
preserve existing fork changes, and keep Azure schemas/informers out of general
core code. Existing AMPM discovery, provider-ID normalization, resizing,
deletion/marking, and templates **MUST NOT** regress. Disabled/unconfigured groups
and other providers **MUST** retain their current behavior. Existing focused fork
regressions **MUST** run alongside new feature tests.

The autoscaler **MUST** remain the single demand-driven replica writer. CAPZ
**MUST** retain VM allocation, primary retries, and remediation. Primary desired
replicas **MUST NOT** be decreased to reveal demand.

tfapply owns schemas/settings/defaults, separate pools and zone validation,
actual/synthetic templates, chart lifecycle, resolved Spot settings, deployment
RBAC, and image/flag wiring. Absent/null fallback Spot settings inherit the
corresponding primary type's resolved setting; independent boolean overrides
affect only the fallback. The autoscaler **MUST** use actual rendered constraints
and **MUST NOT** assume identical primary/secondary Spot, taints, or topology.

This work **MUST NOT** edit tfapply without coordination, introduce another
replica writer, fix INF-740, perform the INF-731 upgrade, change tenant minima,
substitute GPU SKUs, search a third zone, or force active-workload migration.
The local fork, not DeepWiki, is the source of implementation evidence.

## Intended Solution

1. Observe AMPM `status.provisioningState == "Failed"` through the existing
   informer. Pending, bootstrap-like conditions, or deletion alone **MUST NOT**
   trigger degradation. Follow AMPM ownership to AMP and MachinePool
   infrastructure references, scoped to namespace, Cluster UID, and generation.
   A previously observed failure **MUST** survive deletion and replacement.
2. Reconcile bounded, versioned per-pair state in a management-side Cluster-owned
   ConfigMap. The elected leader **MUST** persist degradation before admission
   and reload/synchronize state before restart scale-up. Conflicts **MUST** cause
   reread/reconciliation. Denied, malformed, stale, or unavailable state **MUST**
   suppress new admissions without hiding real capacity.
3. Adjust shared `GetUpcomingNodes` for degraded primary/secondary groups.
   Unregistered desired gaps **MUST NOT** count as reliable arrival. Independently
   viable registered NotStarted nodes **MUST** remain credited; mapped terminally
   failed instances **MUST NOT**. Registered exclusions **MUST** match retained
   placeholders, and real Ready nodes **MUST** remain usable. Accounting **MUST
   NOT** mutate targets, provider-ID lists, or scale-up records to change simulation.
4. Gate each secondary independently on its primary's degradation and estimate
   only residual fitting demand. Ordinary scheduler predicates, estimator,
   health/backoff, group/global limits, maxima, and max-nodes-per-scaleup **MUST**
   constrain writes. Similar-group balancing **MUST NOT** bypass pair policy.
5. Guard relevant generic timeout/failed-request cleanup and target repair so
   retained primary intent survives beyond 45 minutes. Destructive instance-error
   cleanup **MUST NOT** become the failure signal.
6. Use Healthy, Degraded, Recovering, and Disabled states. Positive post-failure
   primary readiness begins recovery; real Ready capacity meeting the retained
   current target over two failure-free scans restores preference. Deletion,
   silence, and cooldown **MUST NOT** prove recovery. Late arrivals **MUST** count
   before sizing. Recovery **MUST NOT** forcibly migrate active workloads.
7. Hold new secondary increases while its terminally failed requested gap remains
   outstanding. Proposed read-only rechecks start at one scan interval, doubling
   to a 15-minute ceiling, with deadlines persisted across restart. Duplicate
   observations **MUST NOT** advance debt/backoff. Both-zone failure **MUST** retain
   bounded targets rather than request copies of capacity CAPZ is already retrying.
8. Expose state/accounting/admission reasons and support rollback admission freeze
   while preserving existing secondary discovery and Ready/incoming accounting.
   Normal safe scale-down, drain, storage, and PDB protections **MUST** remain.

ConfigMap and MachinePool scale writes are not atomic. Replay safety **MUST**
derive from authoritative targets and residual demand, including delayed cache
observations, not an exactly-once claim based on persisted failure. Attempts
**MUST NOT** accumulate replica debt. State/history **MUST** have finite bounds.

## Integration Decisions Before Interface Freeze

The handoff proposes `aiproducer.com/failover-pair` (`lin` or `win2`) and
`aiproducer.com/failover-role` (`primary` or `secondary`). Physical pool UIDs
identify generations; hashed names **MUST NOT** be treated as ordered versions.
Missing/ambiguous metadata **MUST** suppress new admission while preserving
existing capacity visibility.

The owner and tfapply workstream **MUST** agree the following before integration:

- Annotation placement and explicit current-generation/overlap selection rules.
- Default-off activation, capable-image verification, and old-image rejection.
- Management ConfigMap `<cluster>-autoscaler-failover-state`, schema version,
  Cluster ownership, finite pair/role/attempt bounds, and target/recovery fields.
- Elected-leader fencing, resourceVersion handling, and exact tenant-scoped RBAC.
- Positive-evidence recovery and proposed two-scan/15-minute timing.
- Admission-freeze interface; an old-image downgrade with active secondaries is
  not an approved rollback procedure.
- Optional provider-policy API and cleanup/target-repair guard boundaries.

These remain decisions to resolve, not APIs already implemented for tfapply.

## Local Unit and Production-Path Tests

Reuse nearby Go suites, separate fake management/workload clients, scale reactors,
registry/provider helpers, and real scheduler/estimator paths. A stand-alone
simulator repeating the policy is insufficient. New timing **MUST** use an
injected clock; informer synchronization **MUST** use bounded deadlines rather
than arbitrary sleeps. Tests **MUST** assert actual targets and all replica writes.

Run the canonical trace separately for Linux GPU and Windows, with the other pair
healthy. Primary min=1, target=3, Ready=1; its Ready node is occupied. Secondary
min=0, target=0, max=4. Two pending compatible pods each consume one node workload
slot after daemonset overhead; other groups cannot fit them. Ordinary gates are
eligible. Do not seed an autoscaler-originated primary-gap request.

```gherkin
Scenario: Expose failed demand before the provisioning timeout
  Given the canonical pair with ordinary bootstrap-only primary observations
  When a healthy scan runs
  Then primary upcoming is two and secondary target remains zero
  When terminal primary failure is reconciled before 45 minutes
  Then both incoming-capacity consumers report primary effective upcoming zero
  And one IncreaseSize of two produces secondary target two
  And primary desired replicas remain three

Scenario: Reconcile replay and restart without extra requests
  Given persisted degradation and secondary target two with viable incoming capacity
  When 100 duplicate observations, deletion, replacements, delayed caches, and restart occur
  Then the same demand causes no additional increase and degradation remains latched
  And stale namespace, Cluster UID, or generation state cannot activate another pair
  And the healthy other pair remains on primary preference

Scenario: Use real arrivals and recover safely
  Given two requested secondary nodes and retained primary intent
  When secondary Ready capacity covers fixture demand
  Then no additional capacity is requested
  When two late primary nodes become Ready and two failure-free scans run
  Then preference returns to Healthy with primary target three
  And the policy induces no forced migration or deletion

Scenario: Prove the accounting adjustment is necessary
  Given the same failure fixture through production scan paths
  When only the accounting adjustment is disabled
  Then the expected secondary-request assertion fails
  And healthy and unconfigured controls remain unchanged

Scenario Outline: Enforce fault invariants through production scans
  Given a configured pair with <fault>
  When the scan reconciles observations, state, demand, and targets
  Then <invariant> holds in the target state and replica write ledger

  Examples:
    | fault                                      | invariant                                                  |
    | no fitting demand                          | no secondary increase                                      |
    | one viable registered NotStarted primary   | one arrival and consistent registered exclusions            |
    | registered terminally failed instance      | no viable arrival credited                                 |
    | existing Ready/incoming secondary capacity | only residual fitting demand requested                      |
    | maxima, global limits, health, or backoff   | no ineligible or out-of-limit write                         |
    | secondary failure or both-zone failure     | targets held without copies of outstanding failed requests  |
    | deadline boundary or cooldown restart      | bounded persisted rechecks without replica debt             |
    | state denial/unavailability                | no new admission; real nodes remain visible                 |
    | state resourceVersion conflict             | reread and persist reconciliation before admission          |
    | malformed/unknown-version state            | fail closed with an observable reason                       |
    | stale Cluster UID or another tenant        | no cross-cluster or cross-tenant activation                 |
    | missing generation or hash overlap         | no new admission; old capacity remains visible              |
    | failure while Recovering or silence        | no false recovery or additive request                       |
    | cleanup/target repair beyond 45 minutes    | retained primary target remains intact                      |
    | OS/GPU/taint/affinity/PV mismatch           | incompatible pods do not justify fallback                   |
    | late primary success with secondary work   | no forced eviction; normal drain/PDB protections remain     |
    | disabled/unconfigured/non-Azure groups     | existing accounting, cleanup, and selection remain          |
    | crash before/after persistence or resize   | authoritative targets prevent replay-driven increases       |
```

Every scan **MUST** record policy state, raw/effective incoming counts, registered
exclusions, residual pods, candidate groups, targets, writes/deltas, and deadlines.
Both static-snapshot filtering and orchestration **MUST** be exercised, not merely
the policy helper.

Builds, tests, and tooling **MUST** prioritize Docker containers. If Docker is
unavailable, execution **MUST** pause for owner consultation before installing
or using host-toolchain alternatives. Host execution requires an explicit
exception; implementation approval alone does not grant that exception.

After implementation, execute these commands inside the approved Docker test
container, from the `cluster-autoscaler` module:

```bash
go test -count=1 ./cloudprovider/clusterapi ./clusterstate ./core ./core/scaleup/orchestrator
go test -race -count=1 ./cloudprovider/clusterapi ./clusterstate ./core ./core/scaleup/orchestrator
```

Each slice **MUST** first run its cheapest behavior-scoped check, then relevant
package tests. Formatting/build/lint **MUST** follow verified fork guidance;
the upstream contribution guide requires consultation before use. Race tests
require supported Go/CGO tooling. Missing/unrun gates **MUST** be reported as
unverified, not passing. No unit test result is claimed by this proposal.

## Real Local API Persistence Gate

Fake reactors do not prove API-server CAS or leader semantics. Use existing local
API tooling or an explicitly authorized disposable API, explicit test kubeconfig,
and minimal Cluster CRD/ownership objects. Do not run CAPZ/Azure provisioning or
change the default kubectl context. Cleanup **MUST** affect only test resources.

```gherkin
Scenario: Validate persistence against a real local Kubernetes API
  Given isolated ownership/state resources and two leadership contenders
  When real CAS conflicts, RBAC denial, reload, stale Cluster UID, and handover are exercised
  Then a second writer is fenced and persistence denial prevents new admissions
  And the new leader reloads durable state without replay increases
  And stale state cannot activate a replacement Cluster
```

Report this gate separately from fake-client tests and the Azure pilot. It does
not validate VM allocation, GPU availability, or Windows readiness.

## Deployed-Cluster Acceptance

Run only after approval, paired tfapply integration, capable-image delivery, and
explicit authorization for a controlled pilot. Separate management and workload
clients/kubeconfigs **MUST** be explicit. Live fault injection, rollout, tenant
mutation, and image publication are not authorized by this document.

```gherkin
Scenario: Preserve default-off and ordinary primary provisioning
  Given a disabled tenant and an enabled pilot with healthy primary provisioning
  When compatible Linux GPU and Windows demand is submitted
  Then disabled behavior remains unchanged and healthy secondaries remain zero
  And existing fork resizing and safe removal workflows remain functional

Scenario: Supply independent alternate-zone capacity on terminal failure
  Given approved alternate-zone pools, accurate templates, and fitting demand
  When an authorized terminal primary allocation failure occurs before timeout
  Then only that pair becomes eligible for residual-demand fallback
  And primary targets survive replacement attempts and timeout cleanup
  And alternate VM allocation, Ready nodes, GPU availability, Windows workload startup, and pod placement are separately verified
  And processing-storage access and actual Spot, taint, and topology constraints are verified

Scenario: Bound failure across zones and restart
  Given authorized terminal failure in both zones with outstanding retained requests
  When scans, restart, or leader handover occur
  Then degradation and retry deadlines survive without accumulating replica debt
  And group/global limits, backoff, and tenant isolation remain enforced

Scenario: Recover without migrating active work
  Given secondary workloads and retained primary requests
  When real primary readiness reaches target across two failure-free scans
  Then primary preference resumes without policy-induced migration
  And normal safe scale-down preserves storage and PDB protections

Scenario: Freeze admission for rollback
  Given active secondary workloads on a capable image
  When the approved admission-freeze interface is enabled
  Then no new fallback increase is admitted
  And existing secondary Ready/incoming capacity remains discoverable and accounted for
  And no unsafe downgrade or forced eviction occurs
```

Capture AMPM status, durable state, scale writes, targets, effective incoming
counts, readiness, scheduling, and storage access as separate evidence. Alternate
allocation **MUST NOT** be inferred from SKU eligibility or source reasoning.
Unavailable allocation leaves runtime acceptance unverified.

## Delivery and Decision Log

Return branch/base, changed files, approved interfaces/generation semantics,
schema/bounds, RBAC, recovery/backoff, freeze procedure, exact commands/results,
scan/write traces, and gaps to tfapply. A tested commit or published image **MUST
NOT** be claimed before the respective explicitly authorized operation.

| Item | Status |
|---|---|
| Ticket branch from current fork HEAD | Created; base recorded above |
| Local Change Request | Approved for local implementation by Louis Hansen, 2026-10-07 |
| CR-0040 / ADR-0003 approach | Approved for local implementation in this conversation; canonical tfapply records unchanged |
| Cross-repository interface freeze | Pending decisions listed above |
| Execution environment | Docker-first; execution paused pending owner consultation because Docker was reported unavailable |
| Implementation and unit/race/build tests | Not started |
| Real local API persistence gate | Not run |
| Deployed acceptance / image delivery | Not authorized or run |