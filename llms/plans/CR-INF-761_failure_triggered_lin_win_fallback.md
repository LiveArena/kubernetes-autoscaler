---
id: CR-INF-761
title: Failure-triggered primary/secondary worker failover
status: approved-for-local-implementation
date: 2026-10-07
requestor: Louis Hansen
tracking-issue: INF-761
branch: feature/inf-761-implement-failure-triggered-linwin-fallback-in-cluster
implementation-base: 130a28dbc6e85420ca96b64d1d6d075445eff6f7
---

# Change Request: Failure-Triggered Primary/Secondary Worker Failover

## Status and Authorization

This is the autoscaler implementation proposal for
[INF-761](https://linear.app/aiproducer/issue/INF-761), under
[INF-759](https://linear.app/aiproducer/issue/INF-759).
It records implementation approval, not implementation, test results, or release.

Louis Hansen approved this CR and the worker-failover approach now tracked by
CR-INF-759/ADR-0004 for local
implementation in this conversation on 2026-10-07. Approval does not authorize
commits, pushes, image publication, live fault injection, or infrastructure
mutation. Cross-repository interfaces remain subject to the agreement below.

The ticket branch was created from the existing `livearena/release` HEAD above.
The starting worktree was clean; no fetch, rebase, reset, or upstream substitution
was performed. This base also matches the ticket-supplied behavioral baseline;
image tag-to-commit provenance remains unverified.

External governance sources are CR-INF-759 (Failure-Triggered Worker Failover) and
ADR-0004 (Autoscaler-Owned Worker Failover) in the `LiveArena/tfapply` repository.
The current handoff records approval of the approach; this document does not
modify those external governance records. Refer to
[INF-759](https://linear.app/aiproducer/issue/INF-759) and the
[INF-761 implementation handoff](https://linear.app/aiproducer/document/inf-761-cluster-autoscaler-implementation-handoff-8fadf700b148)
for requirements and source provenance. The handoff identifies external
historical documentation baseline `96d99e8d`; no unpublished repository URL is
assumed. Its 2026-10-08 updates identify the current external records as
`docs/cr/CR-INF-759-failure-triggered-worker-failover.md` and
`docs/adr/ADR-0004-autoscaler-owned-worker-failover.md`. New tfapply Change Requests
use their source ticket ID. The failover records formerly referenced here as
CR-0040/ADR-0003 were renumbered; CR-0040 now belongs to separate VPN work and
**MUST NOT** be cited as the current failover CR. The accepted pairing/activation
contract, scheduling requirements, and ownership are unchanged by renumbering.
This fork's CR-INF-761 already uses its ticket ID and retains its identifier.

The handoff reports a tfapply rebase onto external `origin/main` at
`584ea130b2f0c3300915e37fce4c35a226aa96dc` and replayed temporary implementation
commit `6e46af7d305e07ed74d781880ceafd474d8037d0`. These identify external source
history, not a new autoscaler base or a published image. The approved autoscaler
branch/base remains unchanged; external quality-gate results are not rerun here.

The ticket requires governance approval before code edits. Owner approval of
this CR and the applicable CR-INF-759/ADR-0004 approach **MUST** be recorded before
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

### Existing Leadership and External Retirement Are Out of Scope

Louis Hansen clarified scope on 2026-10-09: INF-761 **MUST** integrate with the
functioning autoscaler's existing leader election, elected-process lifecycle and
shutdown. It **MUST NOT** redesign leadership, lease ownership/renewal, HA or
introduce a feature-specific leadership/fencing system. Feature cleanup continues
to cancel/drain its own operations through the existing lifecycle; that is not a
new retirement protocol.

Retirement orchestration, distributed quiescence acknowledgements, authoritative
old-writer fencing, credential/admission controls, infrastructure removal and
final policy-state deletion are **outside this autoscaler workstream**. They belong
to existing operational/tfapply procedures or a separately authorized change.
Their external safety preconditions remain applicable, but implementing a new
retirement system is not an INF-761 code-completion gate. The autoscaler must not
take over cleanup ownership, widen deletion permissions or recreate state while
disabled. This scope clarification supersedes earlier proposed assignments below.

### Identifier Registration and Reuse

Existing discovery constructs node groups from current scalable resources. The
new failover state instead survives scans/restarts and is keyed by the logical
pair identifier, with physical MachinePool and infrastructure UIDs attached to
each role. It retains failure history, fallback allowance and pending replica
requests. Louis Hansen clarified on 2026-10-09 that a registered identifier
**MUST NOT** be rebound to different physical resources while any prior
operational records of that identifier remain. This applies even without
outstanding requests, failure allowance or surviving old infrastructure.

This satisfies stale-generation isolation and durable-request replay safety:
an old primary's terminal failure must not authorize fallback for a new primary,
and an old `3 -> 4` request must not be retried or treated as committed against a
replacement merely because its target is also 3 or 4. Selection of a unique
current pair alone does not establish that the old durable state belongs to it.

`reconcilePair` checks the registered MachinePool/infrastructure UIDs **before**
interpreting old request targets. A mismatch leaves durable state unchanged and
blocks new growth for both selected roles, with an explicit identifier-conflict
reason. Healthy replacement nodes, waiting and restart do not retire a binding.
This is intentional rejection of unresolved identifier reuse, not an automatic
migration opportunity. The independent pair remains unaffected.

Reuse is permitted only after authorized external retirement fully purges the
identifier's prior operational records and reconciles its outstanding operations
and resource obligations. It then registers as a fresh identifier. Deleting a
pool, changing configuration or observing readiness alone is insufficient. The
autoscaler **MUST NOT** silently reset, prune, migrate or take over retirement.

A distinct identifier creates an independent binding, but does not establish
cleanup of the old identifier or its resources. Retained records still consume
the encoded-state size budget; changing names is not a cleanup workaround.
No new leadership/retirement system, state-delete permission or live purge is
authorized by this clarification. This supersedes the earlier migration proposal.

### Extension-First Architecture

Louis Hansen clarified the fork-maintenance constraint on 2026-10-07 for
[INF-761](https://linear.app/aiproducer/issue/INF-761): implementation **MUST**
prefer extensions of existing modules and established extension points over
replacement or rewriting of upstream behavior. The objective is to keep future
upstream upgrades practical, not merely make the current feature work.

New policy/state/adapter logic **MUST** be isolated in additive extension files
where practical. Shared upstream methods **MUST** retain their existing algorithms
with only narrow opt-in calls or guards where a shared boundary is necessary.
Required cloud-provider interfaces **MUST NOT** be expanded for all providers;
optional interfaces **MUST** preserve default behavior for nonparticipants.
Scheduler, estimator, scale-down safety, and resize modules **MUST NOT** be
replaced with fork-specific implementations. Unrelated refactoring and formatting
churn **MUST NOT** be included.

The current implementation isolates the optional capacity-policy interface in
an additive provider file, delegates viable upcoming-capacity adjustment to an
additive registry file, and delegates paired resize/CAS handling to the Azure
extension file. The original node-group resize path remains the default.
Small shared hooks are still required for consistent upcoming accounting in both
consumers, candidate admission, failed-capacity cleanup protection, and the
secondary full-request limit guard. These integrations **MUST** stay generic and
Azure-schema-free; every required shared hook **MUST** have behavior coverage.

After this isolation, all five touched-package unit and race suites passed in
Linux Docker. The shared provider declaration file has no remaining feature diff;
the node-group implementation retains 16 added hook/guard lines, and the shared
registry retains five added lines and one replacement. Extension files contain
the optional interface, accounting adjustment, and paired resize logic. This is
scope reduction, not a claim that all upstream merges can be conflict-free.

The autoscaler **MUST** remain the single demand-driven replica writer. CAPZ
**MUST** retain VM allocation, primary retries, and remediation. Primary desired
replicas **MUST NOT** be decreased to reveal demand.

tfapply owns schemas/settings/defaults, separate pools and zone validation,
actual/synthetic templates, chart lifecycle, resolved Spot settings, deployment
RBAC, and image/flag wiring. Absent/null fallback Spot settings inherit the
corresponding primary type's resolved setting; independent boolean overrides
affect only the fallback. The autoscaler **MUST** use actual rendered constraints
and **MUST NOT** assume identical primary/secondary Spot, taints, or topology.

### Identifier-Independent Pairing

Louis Hansen clarified the abstraction on 2026-10-09: failover is between a
named primary and named secondary for a configured pair identifier. The
identifier **MUST** be an opaque key, not an OS/workload type or an enumeration
of tfapply's current deployment names. `primary lin` / `secondary lin` and
`primary abc` / `secondary abc` are equally valid role/identifier combinations.
These labels describe roles and pair keys, not required Kubernetes object names;
actual MachinePool names remain explicit configuration references.

`lin` and `win2` are current external tfapply examples, neither reserved values
nor an exhaustive identifier set. Runtime discovery, config/state validation,
disabled/frozen admission, request accounting, failure/recovery and scale-down
preference **MUST NOT** allowlist them, require either literal key, or infer
Linux/Windows/GPU behavior from a key or pool-name prefix. Identifier-format,
one-primary/one-secondary membership and encoded-state bounds **MUST** be explicit
and independent of a deployment-name allowlist. Workload compatibility uses actual rendered
constraints and scheduler predicates. Linux GPU and Windows are compatibility
fixtures for the same algorithm; AMPM observation is the Azure provider adapter.

Every declared identifier **MUST** resolve one named primary and one named
secondary with matching pair/role metadata and actual Cluster/pool ownership.
Equivalent valid configurations under different identifiers **MUST** receive
the same behavior. Renaming live persisted pairing still follows the agreed
identity/generation transition rules; this is not permission to reset state.

The 2026-10-09 implementation removes the `lin`/`win2` allowlist from
config/state validation, discovery and disabled gating. Identifiers use nonempty
Kubernetes label-value syntax (1-63 characters), and configuration declares at
least one complete pair. The requestor subsequently rejected the arbitrary
two-pair cap: configuration and persisted state have no fixed pair-count limit,
while their 64 KiB encoded-size bounds remain. This supersedes earlier fixed-name
and pair-count wording without changing JSON/annotation shape or ownership/safety rules.
No tfapply edit, automatic migration or release is implied.

### Multiple Pools and Graceful Degradation

Pool discovery and admission selection are different. Multiple physical pools
with the same identifier/role can legitimately coexist during rollout or
retirement. Selection is scoped by tenant namespace, actual Cluster UID and
the opaque pair identifier, not by identifier alone across the deployment.
The current configuration must explicitly name one primary and one secondary;
their live pool and infrastructure UIDs establish the selected generations.
Extra discovered pools are not additional primary/secondary admission slots.

| Discovered Pools for an Identifier | Selection | Required Behavior |
|---|---|---|
| One primary, one secondary | Both references/identities valid | Apply normal role-based admission, accounting and safety checks |
| Multiple primaries, one secondary | One primary explicitly selected and live-validated | Admit only the selected primary/secondary; retain nonselected generations for capacity visibility and safe retirement |
| One primary, multiple secondaries | One secondary explicitly selected and live-validated | Admit only the selected primary/secondary; do not fan out or split a fallback request across extra secondaries |
| Multiple primaries and multiple secondaries | Exactly one live-validated reference per role | Treat overlap as generations, not multiple logical pairs; extra pools do not change request bounds or preference |
| Any multiplicity | A role cannot be uniquely selected or its identity cannot be verified | Block new paired replica increases and request replay within the unverifiable scope; preserve existing safe capacity/intent and report the reason |

Multiplicity alone **MUST NOT** crash the autoscaler or invalidate an otherwise
explicit, valid selection. Conversely, ambiguity **MUST NOT** be resolved by
choosing the first informer result, newest timestamp, lexical name/hash, OS,
zone, largest pool, or another inferred preference. Multiple active pools per
role, load balancing among them, or cascading secondary tiers are not this
one-primary/one-secondary contract and need a separately approved design.

For an ambiguous/missing/stale role selection, graceful degradation means:

- Fail closed for new paired primary/secondary scale-up and unwritten request
  replay; do not fall back to an unselected pool or create a new allowance.
- Keep real healthy nodes and demonstrably viable incoming capacity discoverable
  and usable under existing accounting. Do not turn an unverifiable target gap
  into reliable incoming capacity or lose known failed-capacity exclusions.
- Preserve existing replica intent and durable state for reconciliation. Do not
  reset degradation, increment failure epochs for ambiguity, cancel allocations,
  delete pools/nodes/state, or mistake ambiguity for a terminal provisioning event.
- Continue ordinary observation and safe scale-down where identities and normal
  scheduler/PDB/storage/minimum protections remain verifiable. Do not force a
  drain/migration simply because an extra role-matching pool is present.
- Deny admission at the affected pair when its scope and shared configuration
  remain trustworthy. If shared configuration/ownership cannot be validated,
  deny admission across that configuration rather than guessing which pair is
  safe. Unrelated trusted configurations/Clusters must remain operational.
- Report the identifier, unresolved role, candidate names/UIDs and reason with
  bounded diagnostics. Re-evaluate on refresh and before a write; resume only
  after valid explicit selection and outstanding-intent reconciliation. A new
  selected UID still requires the agreed generation transition, not an implicit
  state reset or replay of another generation's failure/allowance.

These are required acceptance behaviors, not a claim that all multiplicity,
pair-isolation and stale-cache permutations are already implemented or tested.
Existing config-selected overlap handling is the foundation; remaining cases
must follow the requested red/green/refactor TDD workflow.

This identifier/multiplicity clarification was recorded on
[INF-761](https://linear.app/aiproducer/issue/INF-761) on 2026-10-09, comment
`9f59c766-5fcc-4568-9678-420aa9ab13ab`, for cross-repository coordination.

```gherkin
Scenario Outline: Pair roles do not depend on deployment identifiers
  Given a complete configured pair named <identifier>
  When its primary encounters the same qualifying failure and fitting residual demand
  Then the same bounded secondary admission and accounting rules apply
  And the identifier does not select an OS or scheduling policy

  Examples:
    | identifier |
    | lin        |
    | win2       |
    | abc        |
    | batch      |

Scenario: Arbitrary identifiers require only their declared pairs
  Given complete pairs with arbitrary valid identifiers within explicit bounds
  When pairing is validated
  Then absence of undeclared lin or win2 keys is not an error
  And missing roles, duplicate references and mismatched identities fail closed

Scenario Outline: Explicit selection handles physical role overlap
  Given identifier abc has <primaries> primary and <secondaries> secondary pools
  And configuration selects one valid named pool and live identity per role
  When failover reconciles residual demand
  Then only the selected pools can receive new paired replica requests
  And other generations remain visible without fallback fan-out or replica debt

  Examples:
    | primaries | secondaries |
    | 2         | 1           |
    | 1         | 2           |
    | 2         | 2           |

Scenario Outline: Unverifiable role selection degrades without destructive repair
  Given identifier abc has multiple pools for role <role>
  And that role has no uniquely resolvable valid configured identity
  When reconciliation or an admitted request is retried
  Then no new paired scale write or destructive repair occurs
  And known safe capacity and retained replica/state intent remain available
  And the reason is reported and trusted unrelated configuration continues
  When explicit valid selection and request reconciliation are restored
  Then normal admission resumes without repeating a committed request

  Examples:
    | role      |
    | primary   |
    | secondary |
```

Louis Hansen clarified the scaling contract on 2026-10-07: fallback pool scaling
settings, including maximum size, **MUST** inherit the corresponding primary
pool's settings. Separate `lin_max_replicas` and `win_max_replicas` configuration
settings **MUST NOT** be introduced. This supersedes those maximum-setting
proposals in the external CR-INF-759 and handoff sources linked through
[INF-761](https://linear.app/aiproducer/issue/INF-761).
After deployment, a cluster can impose a fallback-specific maximum through
`cluster.x-k8s.io/cluster-api-autoscaler-node-group-max-size` on that pool.
The autoscaler **MUST** honor the pool's current annotation via existing
`NodeGroup.MaxSize()` behavior, not a separate failover maximum or a dynamically
recomputed primary maximum that overrides a fallback annotation.
The fallback's initial target and minimum remain zero as specified by the
failure-triggered, demand-driven contract; inheriting a maximum does not create
an always-on secondary minimum.

### Primary Preference for Future Demand

Louis Hansen clarified this behavior on 2026-10-07 for
[INF-761](https://linear.app/aiproducer/issue/INF-761): initial fallback **MUST
NOT** permanently direct subsequent demand to the secondary pool. After a
secondary starts supplying failed primary demand, new residual demand **MUST**
receive a primary provisioning opportunity before any further secondary increase.
An old degraded latch alone **MUST NOT** authorize expansion for new demand.
Existing primary desired intent **MUST** remain intact and replay **MUST NOT**
create duplicate requests. Louis Hansen resolved the request semantics on
2026-10-07: if primary is below its configured maximum, new fitting residual
demand **MUST** first request primary scaling. Relevant failure of that scaling
**MUST** permit an independent secondary request without cancelling primary.
At the primary maximum, the secondary can supply fitting residual demand under
its own limits. Whichever usable capacity arrives first can satisfy the workload;
both requests can succeed and temporarily leave excess capacity.

If the required fallback increment exceeds the secondary's current maximum
headroom, the secondary **MUST NOT** accept that request, including a partial
increment truncated to its remaining slots. Primary desired intent **MUST** remain
unchanged for retries. The implemented optional full-request guard rejects this
case before selecting an expansion and again if balancing cannot supply the full
increment. Linux GPU and Windows scan regressions now assert zero secondary
writes and an unchanged primary target for this case.

For otherwise equivalent safe removals, secondary nodes **MUST** be preferred
within their pair, preserving primary spare capacity when relevant.
This preference **MUST** remain subject to actual pod rescheduling, PDBs, drain
and storage safety, group minima, health, and ordinary scale-down timing. It
**MUST NOT** force eviction or keep unnecessary primary capacity indefinitely.
If secondary capacity gets the workload before a late primary arrival, that unused
primary node **MUST** remain eligible for ordinary safe scale-down. This
supersedes any interpretation that primary spare capacity must be retained
regardless of demand. New-demand primary admission and paired scale-down ordering
were implemented and locally verified on 2026-10-08 using the extension-first
approach below. Deployed behavior has not been validated.

A bounded durable request window replaces blanket primary admission blocking.
After prior fallback is requested, new fitting residual demand can increase
primary while it has headroom. The new increment is temporarily credited as
incoming capacity separately from the old failed gap, and secondary admission
is held while that increment is pending. A different terminal attempt created
strictly after the request window starts permits fallback for that increment;
old or equal-time observations do not. This is conservative pool-level
correlation, not proof that an individual AMPM belonged to a particular pod.

An intent is persisted before the scale write. Restart compares actual targets
with the intent's before/after values: an unwritten intent can retry the same
increment, a completed write cannot be replayed, and an unexpected target change
closes admission. Fallback allowances are consumed by reconciled secondary writes
and released on positive primary success. They do not accumulate per failure.
At primary maximum, secondary admission rechecks the live primary limit/identity.

Pair-scoped ordering extends the existing scale-down planner before simulation
and final selection, without replacing eligibility, removal simulation, PDB,
timing, minima, or deletion logic. Secondary preference is stable within the
pair and does not reorder unrelated pair positions. Empty-versus-occupied
candidates and empty/draining/risky removal classes remain distinct, so an unused
primary can be removed ahead of a secondary hosting work. No forced migration or
indefinite primary spare reservation is added.

```gherkin
Scenario: Try primary capacity for new demand after fallback activation
  Given an active secondary supplying prior failed primary demand
  When additional fitting demand cannot use real or viable incoming capacity
  Then that additional demand receives a new primary provisioning opportunity
  And further secondary scale-up is not justified by the old failure alone
  And duplicate observations and restart do not repeat that opportunity

Scenario: Race independent requests after a new primary failure
  Given primary has room for an additional fitting workload request
  When primary scaling is requested and a relevant terminal failure is observed
  Then secondary may also request the residual capacity without cancelling primary
  And whichever usable capacity arrives first can satisfy the workload
  And any excess late arrival remains eligible for ordinary safe scale-down

Scenario: Reject fallback demand that exceeds the secondary maximum
  Given a retained primary request and secondary headroom of one node
  When the fitting fallback request requires two additional nodes
  Then no secondary increment is issued and its target remains unchanged
  And the request remains solely with primary for continued retries

Scenario: Prefer safe secondary scale-down
  Given excess capacity in a configured pair and equivalent safely removable nodes in both pools
  When the autoscaler chooses scale-down candidates
  Then it prefers secondary removals while retaining relevant primary spare capacity
  And PDBs, storage constraints, minima, and safe drain protections remain enforced
```

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
  A newly attempted primary request window is distinct from the failed gap and
  temporarily receives bounded incoming credit until qualifying failure or
  positive readiness. Registered arrivals **MUST NOT** receive duplicate
  unregistered credit.
4. Gate each secondary independently on its primary's degradation and estimate
   only residual fitting demand. Ordinary scheduler predicates, estimator,
   health/backoff, group/global limits, maxima, and max-nodes-per-scaleup **MUST**
   constrain writes. Similar-group balancing **MUST NOT** bypass pair policy.
  New demand follows the primary-first request-window rule above; primary
  saturation is also an eligible fallback condition, subject to limits and safety.
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

### Accepted External Contract Rechecked 2026-10-08

The current [INF-761 implementation handoff](https://linear.app/aiproducer/document/inf-761-cluster-autoscaler-implementation-handoff-8fadf700b148)
and [INF-761](https://linear.app/aiproducer/issue/INF-761) record an accepted v1
pairing/activation wire contract. This supersedes the provisional annotation and
flag choices made in this implementation. The 2026-10-08 alignment slice now
implements the configuration reader, config-selected generations, runtime modes,
and prepared writer permissions. Deployed-image capability, release provenance,
and controlled rollout verification remain separate gates.

The authoritative management configuration is a read-only Cluster-owned ConfigMap
named `<cluster>-autoscaler-failover-config` in the tenant namespace, with one
`config.json` entry. Its JSON shape is:

```json
{
  "apiVersion": "aiproducer.com/worker-failover/v1",
  "cluster": {
    "name": "managed-cluster",
    "namespace": "tenant-namespace",
    "uid": "11111111-1111-4111-8111-111111111111"
  },
  "pairs": {
    "lin": {
      "primary": { "name": "lin3-abc12" },
      "secondary": { "name": "linfo2-def34" }
    },
    "win2": {
      "primary": { "name": "win23-ab123" },
      "secondary": { "name": "winfo2-de456" }
    }
  }
}
```

These are references from tfapply's current naming scheme, not live objects or
a required identifier set. `abc` is an equally valid `pairs` key with explicit
primary/secondary references. Configuration names select current scale-up
generations. The reader **MUST** validate ConfigMap namespace
and Cluster ownership/UID, JSON Cluster identity, each referenced MachinePool's
`spec.clusterName`, and matching `aiproducer.com/failover-pair` / role annotations.
Actual pool UIDs **MUST** be resolved and stored. Every declared pair must be
complete for active admission; undeclared literal `lin`/`win2` keys are not
required. Identifier-independent validation implements the 2026-10-09
clarification. Unknown versions, missing or mismatched references, and
partially updated upgrades **MUST** suppress new fallback admission. Older pool
generations remain accounted for, but are not extra admission candidates.
The provisional `aiproducer.com/failover-current` annotation is not the accepted
selection mechanism and **MUST NOT** be required from tfapply.

The accepted runtime flag is
`--azure-machinepool-failover-mode=disabled|active|freeze`, with disabled as default.
Mode is not duplicated in configuration JSON. `disabled` **MUST** gate recognized
secondary candidates even when they are discoverable; `active` permits the
failure-triggered policy; `freeze` retains observation, state, discovery, adjusted
accounting, and normal safe scale-down but prevents new secondary requests.
The implementation now exposes this validated mode flag and removes the two
provisional boolean flags. Invalid values fail parsing; disabled is the default.
Recognized secondary candidates are blocked in disabled mode without requiring
policy state or hiding capacity. Active/freeze require AMPM availability and
existing autoscaler leader election.

The external consumer has prepared name-scoped ConfigMap permissions:
configuration `get/list/watch`; state `get/list/watch/update/patch`; tenant-scoped
ConfigMap `create`, because Kubernetes cannot scope create by resource name.
No configuration write or state delete permission is supplied. A list/watch
**MUST** use an exact `metadata.name` field selector, not a namespace-wide request.
Both objects use the management client. The reader uses exact-name GET on each
scan and before admitted writes, so it issues no configuration list/watch and
needs no namespace-wide field-selector exception. The extra management Lease
was removed. Provider construction is inside the existing autoscaler's
`OnStartedLeading` lifecycle, leadership loss exits the process, and active/freeze
with leader election disabled is rejected by the executable. Provider cleanup
cancels its writer context and prevents later refresh/scale writes. The fork
**MUST NOT** silently widen the consumer Role or remove its enablement guard.

Capability identifier `azure-machinepool-failover-v1` is agreed. This fork now
reports compiled support through `--capabilities` and diagnostic HTTP
`GET /capabilities`, using `aiproducer.com/autoscaler-capabilities/v1` JSON.
Consumer acceptance/implementation of this reporting contract and published
image provenance remain pending.
The consumer **MUST** verify a running capable deployment before creating enabled
secondaries. Flags, a guessed tag, a ConfigMap, or a manually supplied capability
boolean are not deployed-image proof.

The v1 configuration API is separate from this fork's v2 internal state schema:
`<cluster>-autoscaler-failover-config` is tfapply-owned read-only pairing data;
`<cluster>-autoscaler-failover-state` is autoscaler-owned durable reconciliation
data. The pre-feature brownfield baseline still has no prior failover state to
migrate. The agreed configuration reader is now implemented; safe consumer
rollout ordering and capable-image verification still require validation.

The bounded primary provisioning deadline now reuses the existing per-group
`MaxNodeProvisionTime` setting, including overrides, rather than introducing a
second timeout flag. The request persists its deadline across restart; expiry
admits only its still-unmet, unregistered increment and retains primary targets.
Unfit-primary fallback now uses production scheduler predicates and hard
resource-limit checks for the current residual demand. Internal errors are not
fit evidence, and freeze, durable windows, config identity and maxima still gate
writes. Dedicated boundary, Linux/Windows and hard-limit regressions pass locally.

The handoff reports prepared eight-role topology, strict SKU/zone catalogue
validation, real/synthetic Linux GPU and Windows Server 2022 build 10.0.20348
templates, and alias-aware fallback keep/cordon hooks. It reports 40 native cases,
15 script tests, full Terragrunt checks, and actionlint passing in Docker. These
are external workstream results, not tests rerun here. Enabled deployment is
still intentionally rejected until the capable-image verification contract is
implemented. No tfapply repository or Linear record was changed by this reread.

The handoff proposes `aiproducer.com/failover-pair` (the opaque configured
identifier; `lin`/`win2` are current examples and `abc` is equally valid) and
`aiproducer.com/failover-role` (`primary` or `secondary`). Physical pool UIDs
identify generations; hashed names **MUST NOT** be treated as ordered versions.
Missing/ambiguous metadata **MUST** suppress new admission while preserving
existing capacity visibility.

The owner and tfapply workstream **MUST** agree the following before integration:

- Annotation placement and explicit current-generation/overlap selection rules.
- Default-off activation, capable-image verification, and old-image rejection.
- Management ConfigMap `<cluster>-autoscaler-failover-state`, schema version,
  Cluster ownership, encoded-size and per-pair role/attempt bounds, and target/recovery fields.
- Reuse of existing elected-leader lifecycle, resourceVersion handling, and exact
  tenant-scoped RBAC; leadership/fencing redesign is outside INF-761 scope.
- Positive-evidence recovery and proposed two-scan/15-minute timing.
- Admission-freeze interface; an old-image downgrade with active secondaries is
  not an approved rollback procedure.
- Optional provider-policy API and cleanup/target-repair guard boundaries.

The accepted external contract above resolves pairing, current-generation input,
and mode names. Capability delivery and external rollout/retirement acceptance
still need alignment before deployment. Identifier reuse follows the explicit
purge-before-reuse contract above; automatic generation migration is not selected.

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
    | unresolved generation or ambiguous overlap | no new admission; known safe capacity/intent remain visible |
    | valid explicit selection with role overlap | selected pools only; extra generations visible, no fan-out  |
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

## Retrospective Baseline Validation

No build or test baseline was run before the first code edit. On 2026-10-07,
the exact base `130a28dbc6e85420ca96b64d1d6d075445eff6f7` was exported with
`git archive` from a read-only source mount into an isolated Docker volume.
The active branch and worktree were not switched, reset, or overwritten.
This is retrospective environment validation, not a pre-edit gate.

Both the pristine base and current checkout passed the full Cluster Autoscaler
executable build and all five touched-package suites in Linux Docker with
`golang:1.24.0`. Each command exited zero. The build used `CGO_ENABLED=0` and
`-p 2`; the current build additionally used `-mod=readonly`. Build outputs stayed
in the disposable comparison volume, not in the worktree.

The package command below was run separately on the pristine base and current
checkout, inside the container's `cluster-autoscaler` module:

```bash
go test -count=1 -vet=atomic,bool,buildtags,directive,errorsas,ifaceassert,nilfunc,slog,stringintconv,tests ./cloudprovider/clusterapi ./clusterstate ./core ./core/scaleup/orchestrator ./config/flags
```

The vet selection matches the existing module Makefile, which intentionally
excludes the printf analyzer. `-count=1` disables test-result reuse.
These results validate the build and scoped suites in this Docker environment;
they do not certify every provider test, every project in the monorepo, race
behavior, the complete planned failure matrix, or deployed-cluster acceptance.

## Delivery and Decision Log

### Five Deployment States

Louis Hansen clarified the agreed five-state lifecycle on 2026-10-08 in the
INF-761 retirement reply. External CR-INF-759 in `LiveArena/tfapply` records
**Agreed Five-State Deployment Lifecycle**, FR-21/FR-22, **Opt-Out Transition and
Teardown**, and **Retained-Object and State Cleanup Ownership**, referenced through
[INF-759](https://linear.app/aiproducer/issue/INF-759) and the
[INF-761 handoff](https://linear.app/aiproducer/document/inf-761-cluster-autoscaler-implementation-handoff-8fadf700b148).
They supersede the earlier ambiguous State-4 wording, not the established
primary-first, secondary-limit, or safe scale-down requirements.

| State | Deployment Situation | Autoscaler Responsibility |
|---|---|---|
| 1 | Not implemented: pre-feature baseline | Existing ordinary autoscaling; no feature-owned fallback topology, configuration or state |
| 2 | Implemented but not activated: topology disabled and mode disabled | Preserve baseline pools/behavior; capable code alone creates no feature state or secondary request |
| 3 | Deployed and activated after capability verification | Resolve actual current identities and initialize absent state on first activation; preserve/reconcile valid state on active redeploy, restart or leadership change |
| 4 | Activated then deactivated: retirement is in progress | Freeze new secondary admission while preserving observation/accounting and safe scale-down; at final disable stop the feature writer; block leftover secondary admission without hiding viable capacity or taking over cleanup |
| 5 | Activated, deactivated, then reactivated after verified completed teardown | Repeat State 2 -> State 3 with fresh state/current identities; do not replay old degradation, intents, retry history or allowances, even with the same Cluster UID or reused names |

These five deployment states are distinct from Healthy/Degraded/Recovering.
State 3's unchanged active redeploy qualification does not suppress normal
autoscaling/recovery. State 2 requires topology disabled, not merely disabled
autoscaler mode with retained resources. State 4 is incomplete until cleanup is
verified; completed teardown returns to State 2. Freeze, Ready=0, flag-off,
configuration absence or Helm uninstall alone do not prove that transition.

CR-INF-759 separates admission freeze from topology opt-out. The required
transition first keeps `enabled=true` with `mode=freeze`, inventories **all**
secondary generations, and permits normal authorized retirement under PDB,
storage, workload-fit, and drain protections. Secondary desired targets **MUST**
be zero with no remaining nodes, viable/failed retry allocations, or dependent
work before applying `enabled=false` and disabled mode. Ready=0 alone does not
prove retirement. The capable image is retained while opt-out and cleanup converge.

On flag-off, tfapply's pairing ConfigMap and added permissions can disappear,
while kept MachinePool/AMP objects and the autoscaler-owned state can remain.
A live Cluster's owner reference does not collect state merely because this
feature is disabled. The uninstall hook's minimum-zero/maximum-current-target
and cordon actions do not themselves set replicas to zero or prove cleanup.
Enc's restored zone/hash effects also remain an external rollout obligation.

The external tfapply retirement workflow, or an explicitly authorized actor
executing it, owns inventory, verified-empty secondary-object removal, and final
CAPI/CAPZ cleanup verification. Primary pools, Cluster, and unrelated tenant
resources **MUST NOT** be cleanup targets. INF-761 owns stopping its writer and
describing state retirement, not adding a cleanup replica writer or state-delete
permission. Completed opt-out requires the separately authorized retirement actor
to delete `<cluster>-autoscaler-failover-state` after verified safe retirement,
retained-resource cleanup and writer quiescence. Inert retention is not a
completed-opt-out alternative. Disabled feature code must not recreate deleted
state. No named automatic retirement job has yet been implemented in tfapply.

On 2026-10-08, the retirement question was posted on
[INF-761](https://linear.app/aiproducer/issue/INF-761), comment
`9641ac88-cca0-47fa-992f-c8676cb8ebbf`. Reply
`c0a4f9f6-b15b-43a8-9742-8956daeba762` at 14:13 UTC records Louis Hansen's
agreed five-state contract in external CR-INF-759, FR-21/FR-22. It resolves
cleanup ownership and deletion versus retention. At that time, a concrete
writer-quiescence acknowledgement/fencing/reporting interface remained proposed
for coordination; the later scope clarification below excludes its implementation
from INF-761. External operational acceptance remains with that workstream.
No live cleanup, deletion, release, permission expansion or migration is
authorized by that clarification.

The 2026-10-09 scope clarification supersedes the earlier request to implement
a new externally consumable writer-quiescence/fencing interface in INF-761.
Existing leader election and process lifecycle remain authoritative. Feature
cleanup closes its operation admission, cancels its context and waits for tracked
operations to return through that lifecycle. This does not prove distributed
quiescence or a cancelled request's remote outcome; those are external operational
concerns, not a new system this feature must implement. Ordinary primary
autoscaling and CAPZ retained-primary retry ownership remain unchanged.

State 5 requires verified retirement and absence of old feature resources/state
before fresh activation. Retained or unverifiable state is incomplete retirement,
not permission to reset/reuse it. Active restart instead preserves valid state.
The clarification selects neither activation-epoch nor ConfigMap-UID protocol;
any such interface change requires coordination before implementation.

#### Superseded Quiescence Proposal

Reply `843e15d0-2c47-41c6-bc98-e7e800d8f966` on the INF-761 question thread
proposed additional retirement evidence on 2026-10-08. The requestor's
2026-10-09 scope clarification excludes implementing that distributed protocol
from this workstream. Any external retirement workflow must still establish
its own safe cleanup preconditions; no new status endpoint, receipt schema,
management Lease, fencing authority or state-delete permission is selected here.

#### Local Lifecycle Evidence

The cancellation-only baseline failed a new controlled Docker race regression
at blocked refresh, intent-persistence and scale API boundaries: cleanup returned
while a feature operation was still in flight. After the local admission/drain
barrier, the same regression passes for all three boundaries. Cleanup cancels
before waiting; later operation admission is rejected, and cancellation after
intent persistence prevents a subsequent new scale call. This does not imply
API-server outcome reconciliation or fencing of a different process.

The fresh-reactivation regression seeds old degradation, failure watermarks,
request intents, retry intervals and allowances, then simulates authorized
external deletion after stopping the old writer. Disabled and stopped code do
not recreate deleted state. With the same Cluster/primary UIDs and reused
secondary names but recreated secondary pool/AMP UIDs, activation resolves those
new identities, initializes healthy state with no old history/intents/allowance,
and produces no old scale replay. The simulated precondition is verified external
teardown; the fixture does not prove that real teardown or distributed fencing.
Existing active-redeploy and incomplete-retirement tests remain passing.

All seven complete affected race suites and the full executable build passed
for this update. One aggregate run reported a provider-package failure without
captured failing-test diagnostics; it was not reproduced in a structured complete
provider rerun or the subsequent unfiltered complete provider run (30.615s).
This unexplained first result is recorded, not treated as a resolved root cause.
The other six package results passed in that aggregate run. The earlier real-API
gate remains separate and was not rerun for this local shutdown/test-only update.
Documentation/code diagnostics and tracked whitespace checks pass; Git still
reports its existing LF/CRLF warnings. No external cleanup or deletion occurred.

The local 2026-10-08 CR marks this retirement sequence and its automation as
required/pending integration work, not a tested live opt-out. This document
records the intended State-4 contract; it does not claim tfapply has completed
that automation or that a real cluster was retired.

Offline lifecycle regressions passed in Linux Docker with `-race`: stable active
redeploy preserves the exact state/pool objects and issues no repeated writes;
opt-out stops the previous writer, simulates external config removal, retains
state without mutation, keeps leftover secondary target/capacity visible but
rejects increases, and makes no autoscaler create/update/patch/delete or scale
writes. A separate simulated external actor removes only zero-target secondary
definitions with no provider IDs, leaving primary pools and retained state intact.
These tests complement initial disabled/unconfigured/v2 initialization tests;
they do not prove Helm keep/GC, real CAPZ instance cleanup, or storage retirement.

```gherkin
Scenario: Reconcile unchanged active redeployment
  Given this capable code already runs with enabled topology and retained valid state
  When deployment inputs, demand, and observations are unchanged across redeploy
  Then existing state and pool identities are reused without reset or replica replay
  And normal autoscaling remains available for subsequent actual changes

Scenario: Preserve cleanup ownership after opt-out
  Given fallback was previously enabled and safe retirement precedes disabled topology
  When tfapply opts out and removes its pairing configuration while kept objects or state remain
  Then the stopped policy writer performs no further state or scale writes
  And recognized secondary increases are blocked without hiding remaining capacity
  And the autoscaler does not delete pools, reset state, or declare retirement complete
  And the authorized external actor verifies cleanup and explicit state disposition
```

### Brownfield Upgrade From the Deployed Baseline

For initial adoption, Louis Hansen clarified on 2026-10-08 that the existing
deployed baseline predates this change. That version has no failover policy
ConfigMap or request schema. Therefore the supported brownfield starting point
is **absent failover state**, not schema version 1. Versions 1 and 2 were internal
development revisions of this unreleased feature; no deployed v1 state is assumed.

Upgrading the executable with failover still disabled preserves ordinary behavior
and creates no policy state. At the first approved opt-in with valid pair metadata,
ownership, synchronized observations, and management RBAC, the elected writer
initializes a new Cluster-owned v2 ConfigMap and reconciles current authoritative
targets and readiness before admitting requests. Pending primary provisioning
alone does not manufacture a failure or fallback allowance. Startup-replayed
terminal observations use the ordinary failure path; no historical event debt is
invented. A second scan reuses the initialized state.

The state initializer **MUST NOT** rewrite Cluster/MachinePool UIDs, replica
targets, workload nodes, or existing processing data. State initialization is not
a Kubernetes/CAPI object schema migration or pool replacement operation.
The 2026-10-08 provider-level regression verifies disabled and unconfigured
no-state behavior, owned v2 initialization for configured pairs, no initial scale
writes, and unchanged existing pool/node objects; it passed in Linux Docker with
the race detector. No production-code change was needed for this starting point.

Consumer rollout remains a coordinated external `LiveArena/tfapply` change tracked
through [INF-761](https://linear.app/aiproducer/issue/INF-761) and
[INF-759](https://linear.app/aiproducer/issue/INF-759). A capable-image rollout and
leadership handover **MUST** complete before old/disabled writers can discover
enabled secondary pools. Management RBAC, accurate templates, explicit pairing,
current-generation metadata, zero-minimum secondaries, and inherited maximum
settings **MUST** be validated before admitting fallback. Existing primary
hash/generation effects of chart updates still require consumer lifecycle tests;
the successful state-initialization test does not prove a disruption-free chart
upgrade. This workstream does not mutate the consumer repository or a tenant.

An unexpected preexisting v1, malformed, foreign-owner, or unknown-version
ConfigMap still causes admission to fail closed. It **MUST NOT** be automatically
deleted or reset to Healthy. Supporting an installation that actually ran a
development v1 image would require a separate explicit migration procedure and
tests; that installation is outside the clarified deployed-baseline assumption.

```gherkin
Scenario: Initialize failover state on a pre-feature brownfield cluster
  Given an existing Cluster running the pre-change fork with no failover state
  When the capable image and approved pairing, templates, and management RBAC are enabled
  Then the policy initializes owned schema-v2 state from current pool and node observations
  And initialization itself changes no existing pool target, identity, or workload node
  And no historical failure or replica request is fabricated

Scenario: Preserve ordinary behavior before brownfield opt-in
  Given a pre-feature cluster with no failover ConfigMap
  When the upgraded image runs with failover disabled or without configured pairs
  Then no failover state or fallback scale request is created
```

### Current Integration Draft

The implementation below is on the approved branch, uncommitted and default-off.
It is not a published image or a frozen tfapply API. Review this draft through
[INF-761](https://linear.app/aiproducer/issue/INF-761) before enabling consumer
configuration. No external tfapply record was edited by this workstream.

**Integration status:** configuration, current-generation selection, modes, and
writer permissions now implement the accepted external contract and have local
unit/race/real-API coverage. Capability reporting, deadline and unfit-primary
extensions are also implemented locally. Consumer preflight acceptance, release
provenance, generation transitions, and remaining placement/concurrency cases
still block a complete deployment claim. No consumer enablement guard was removed.

| Interface | Implemented Draft |
|---|---|
| Activation | `--azure-machinepool-failover-mode=disabled` by default; `active`/`freeze` require AMPM discovery, initial handler synchronization, and autoscaler leader election |
| Rollback freeze | Set `--azure-machinepool-failover-mode=freeze`; observation, durable state, secondary discovery/Ready/incoming accounting, and normal safe scale-down continue without new secondary requests |
| Balancing | Activation rejects `--balance-similar-node-groups=true` |
| Pair metadata | Opaque nonempty label-style identifier plus primary/secondary role annotations; no lin/win2 allowlist or `failover-current` requirement |
| Configuration/selection | Exact-name read-only config GET; at least one complete declared pair, no fixed pair-count limit, 64 KiB encoded-size bound; strict duplicate/unknown JSON-field rejection and live identity/ownership/role checks; nonselected generations remain visible without admission |
| Ownership | Cluster owner UID on MachinePool; MachinePool infrastructureRef to AMP with matching owner UID; terminal AMPM AMP owner identity matches that AMP |
| Maximum | Existing pool max-size annotation, inherited by tfapply from the primary initially; live annotation, target, UID, and generation checks precede each paired scale write |
| State | Management ConfigMap `<cluster>-autoscaler-failover-state`, Cluster-owned, `data.state.json`, schema version 2, maximum encoded size 64 KiB; pre-feature brownfield initializes absent state, while unexpected version 1 is rejected rather than reset |
| State bounds | No fixed pair-count limit; 64 KiB encoded-size bound, two roles per pair, one attempt watermark/fingerprint per role, at most one primary and one secondary intent per pair, and a bounded failed-increment allowance; no per-pod history or cumulative replica debt |
| Observation buffer | At most 64 AMP-owner keys; successfully persisted observations are retired; overflow fails closed |
| Coordination | Existing autoscaler leader election and process lifecycle, plus cancelled/stopped provider writer context; no separate management Lease or additional Lease RBAC |
| Scale write | Persist bounded intent first, then live scale GET and resourceVersion-bearing UPDATE; target/UID/metadata/maximum and durable permission checks reject stale or repeated admission |
| New demand | Primary first below maximum; separately credit its fresh pending increment, then permit secondary only on qualifying later failure or live primary saturation |
| Scale-down preference | Stable secondary-first ordering within equivalent pair/safety classes; original planner safety gates stay authoritative and unused late primary nodes remain removable |
| Recovery | New Ready-set evidence, retained target met on two distinct persisted scan times, no new terminal failure; no forced migration |
| Secondary failure | Outstanding failed requests retained; read-only rechecks double from the scan interval to 15 minutes; deadline persists |
| Identifier reuse | Different pool/infrastructure UIDs cannot reuse a registered identifier until its prior operational records are fully purged through authorized retirement; no automatic migration/reset |
| Diagnostics | Policy reasons enter scale-up skipped reasons; mapping/state/lease errors are logged; no new policy metrics are implemented |

Management-side configuration ConfigMap permissions are exact-name `get/list/watch`;
state ConfigMap permissions are exact-name `get/list/watch/update/patch`, plus
tenant-scoped ConfigMap `create`. No management Lease permission is required.
Ordinary provider permissions include owning Clusters `get`, AzureMachinePools
`get/list/watch`, AMPMs `get/list/watch`, and MachinePools plus their scale
subresource. Existing MachinePool discovery needs `get/list/watch`; paired scale
writes need `get/update` on `machinepools/scale` in addition to the existing
provider's patch permission. State access **MUST NOT** use the workload client.
The consumer owns the final tenant-scoped Role/RoleBinding rendering. No policy
permission to delete AMPMs, Clusters, or workload nodes is introduced.

Old images **MUST NOT** receive enabled pool configuration until a verified
capable image exists. The new flags allow CLI compatibility checks, but no
published tag/digest or independently verified image capability exists yet.
Freezing admissions on the capable image is distinct from disabling the feature
or downgrading to an image that lacks secondary policy support.

### Verified Local Results

On 2026-10-07, Linux Docker with `golang:1.24.0` passed the complete five
touched-package unit suites and the same suites with `-race`, both with
`-count=1`, `-p 2`, and the repository's vet analyzer selection. The full executable
build also passed with `CGO_ENABLED=0`, `-mod=readonly`, and `-p 2`.

The production `RunOnce` fixture now verifies Linux GPU and Windows accounting,
one secondary request of two, no repeated request from viable incoming capacity,
other-pair preference, daemonset overhead, failed registered-node exclusion,
no demand, OS/GPU incompatibility, pool/global limits, real secondary/late-primary
capacity, and retained targets beyond the original 45-minute boundary.
Its accounting-disabled controls return no fallback request, demonstrating that
the fixture detects an accounting-only omission rather than just candidate gating.
Provider/store tests separately cover ownership mapping, durable reload, duplicate
observations, recovery, malformed/unknown/stale state, read/write denial, CAS
retry, sequential lease handover, old-generation visibility, both-zone failure,
annotated maximum changes, and stale or repeated scale-write admission.

The authorized disposable real API gate passed with the race detector against
Kubernetes API server v1.34.0 and etcd v3.5.21 on an internal Docker network with
no host ports. It exercised production state and scale clients with real
resourceVersion conflicts/retry, RBAC read/write denial, reload, Cluster-UID
isolation, sequential lease handover, former-holder rejection, and duplicate
scale-write rejection. It caught and prompted a fix for Lease MicroTime encoding
that fake clients accepted. Only minimal test Cluster/MachinePool CRDs were used;
no Azure or CAPZ provisioning occurred. Test containers and network were removed.
The opt-in test uses deliberately public disposable-test credentials only;
these **MUST NOT** be used on an existing or deployed cluster.

Inside the isolated API test container:

```bash
INF761_TEST_API=https://apiserver:6443 INF761_TEST_API_DISPOSABLE=1 go test -v -race -p 2 -count=1 -vet=atomic,bool,buildtags,directive,errorsas,ifaceassert,nilfunc,slog,stringintconv,tests -run '^TestAzureFailoverRealAPI$' ./cloudprovider/clusterapi
```

The API test skips unless both opt-in variables are supplied. Ordinary unit-suite
success with this test skipped **MUST NOT** be counted as an API contract result.

The combined provider-to-scheduler fixture was corrected on 2026-10-07 after an
invalid direct-orchestrator test exposed missing scan setup. Reading upcoming
counts alone does not inject snapshot capacity or filter pending pods. The
corrected fixture resets the real scheduler snapshot each scan, adds sanitized
effective-upcoming placeholders, applies the production pod-list processor, and
passes only residual demand to the production orchestrator/binpacking estimator.
It supplies the actual processor dependencies rather than bypassing stages.

For both Linux GPU and Windows, the combined fixture now drives failure through
the fake management API and AMPM informer, verifies healthy incoming count two
and zero writes, degraded primary effective incoming zero and exactly one
secondary target-two update, 100 duplicate observations, attempt deletion and
replacement, and reconstruction of controller/informer caches, durable policy,
registry, and scheduler context from retained API objects. Restart introduces no
additional request. It then registers real secondary and late primary nodes,
verifies demand suppression and two-scan positive recovery without migration,
and asserts that the other pair remains on primary preference.
Disabling only primary blocked-gap accounting keeps the primary placeholders and
produces no fallback write, preserving the sensitivity control.

Both complete five-package unit and race suites were rerun after this test repair
and passed. No production-code change was required to repair the test. The
separate production `RunOnce` fixture continues covering the static scan wiring;
the combined provider fixture assembles its snapshot/filtering boundary from
existing production components rather than importing the entire core package.
The real API gate was not rerun for this test-only repair; its earlier separately
recorded result remains distinct from the ordinary suite's skipped API test.

### Primary-First and Scale-Down Verification

On 2026-10-08, before this implementation slice, the five existing touched-package
unit suites were rerun in Docker and passed. The new primary-first regression
then failed at the existing blanket primary admission block, establishing its
sensitivity before implementing the request-window extension.

After implementation, all six complete touched-package unit and race suites
passed, now including `core/scaledown/planner`. The full executable build passed
with `CGO_ENABLED=0`, `-mod=readonly`, and `-p 2`. New logic is isolated in provider
request and planner capacity-policy extension files; the planner requires only
two ordering calls in existing methods. The optional capacity interface and
incoming adjustment remain in additive files.

The Linux GPU and Windows combined production-component traces cover new demand
after secondary readiness, an initial primary target increase, restart with the
new primary request in flight, no repeated write, a genuinely fresh failure,
and subsequent secondary increase with primary target retained. Separate outcomes
cover primary success before fallback, secondary success followed by late primary,
rejection at the secondary maximum, and primary-at-maximum admission. Exact
scale-write names, targets, residual pod counts, and incoming counts are asserted.

Focused tests cover stale/equal-time failure exclusion, an interrupted scale write
after durable intent, denied intent persistence before any scale write, retry
after restart, unexpected authoritative targets, consumed allowance rejection,
positive-recovery allowance release, and no double credit for a fresh registered
NotStarted node. Planner tests use actual eligibility/removal simulation and
cover equivalent empty/occupied preference, unused primary alongside busy or
incompatible secondary work, primary/secondary minima, PDB denial versus permitted
drain, disabled behavior, independent pair positions, and input immutability.

The authorized real API gate was rerun under `-race` against isolated API server
v1.34.0 and etcd v3.5.21 using minimal Cluster, MachinePool, and AzureMachinePool
CRDs. It passed schema-2 intent persistence/reconciliation, real CAS conflicts and
retry, RBAC read/write denial, reload, Cluster-UID isolation, sequential lease
handover, former-holder rejection, and successful scale-write replay rejection.
Only the test's containers and internal network were created, with no host ports,
CAPZ/Azure provisioning, or kubeconfig/context changes; they were removed after
the final successful run.

Inside the ordinary Go test container:

```bash
go test -p 2 -count=1 -vet=atomic,bool,buildtags,directive,errorsas,ifaceassert,nilfunc,slog,stringintconv,tests ./cloudprovider/clusterapi ./clusterstate ./core ./core/scaleup/orchestrator ./core/scaledown/planner ./config/flags
go test -race -p 2 -count=1 -vet=atomic,bool,buildtags,directive,errorsas,ifaceassert,nilfunc,slog,stringintconv,tests ./cloudprovider/clusterapi ./clusterstate ./core ./core/scaleup/orchestrator ./core/scaledown/planner ./config/flags
```

### Consumer Contract Alignment Verification

On 2026-10-08, the six-package Docker unit baseline passed before this slice.
Configuration, generation selection, mode flags, and management permissions were
then aligned with the accepted external handoff without editing tfapply.
New logic lives in an additive provider configuration extension and a validated
flag-value extension; shared methods retain narrow calls/guards. The existing
autoscaler leader-election branch has only a prerequisite check for active/freeze.

Unit and race tests cover exact-name read-only configuration GET, strict API
version/Cluster ownership/identity, complete unique pair references, malformed
or trailing JSON, unknown fields, denied reads, live pool `clusterName`, missing
references, changed configuration after admission, and an incomplete other pair.
Both complete pairs are revalidated before a write. Old/current pools with the
same pair/role annotations are selected solely by configured names and resolved
UIDs; old targets remain discoverable but cannot request new capacity.
Mode tests cover default disabled admission blocking without state creation,
active fallback, frozen admission with existing incoming secondary target, and
invalid CLI values. Production-component request/restart traces remain passing.

All six complete unit and race suites passed after alignment, and the full
executable build passed. Running that executable with
`--azure-machinepool-failover-mode=active --leader-elect=false` failed with the
expected leadership-prerequisite error before starting the autoscaler loop.
Provider cleanup/stopped-writer tests verify no later refresh or scale write;
fake management actions assert no Lease resource access.

The final authorized disposable real-API test passed under `-race` with a
tenant-scoped test identity. Its Role matches the prepared exact-name ConfigMap
read/state-write permissions and ordinary pool/scale/infrastructure reads, without
management Lease permissions. Configuration reads, durable intent/state writes,
ConfigMap resourceVersion conflicts/retry, and actual scale updates succeeded.
Unrelated ConfigMap reads, configuration mutation, state deletion, and management
Lease GET were Forbidden. Reload, stopped former-writer rejection, Cluster-UID
isolation, and scale replay checks also passed. No host ports, CAPZ/Azure
provisioning, or existing kubeconfig changes were used; the two test containers
and internal network were removed.

This real-API gate does not simulate the entire upstream leader-election
callback lifecycle or prove atomic distributed fencing across separate state and
scale transactions. The existing elected-leader construction/process-exit path
was verified in source, and its disabled-election prerequisite was tested on the
real executable. Replacing the old management Lease test does not claim stronger
concurrency proof than that.

### Deadline, Fit and Capability Verification

On 2026-10-08, the six-package Docker unit baseline passed before this slice.
The new provider deadline regression failed before the timeout implementation,
then passed after implementation. Focused race tests cover before/exact expiry,
unwritten scale intents, partial readiness, viable newly registered NotReady
arrivals, success at the deadline, idempotent expiry, healthy-to-degraded
transition without any terminal event, persisted deadlines/restart, per-group
override, and invalid duration rejection. Timeout retains the primary target;
it does not cancel CAPZ allocations or turn old gaps into new allowance.

The additive orchestration extension grants only a scan-local exception when no
current residual pod equivalence group fits the configured primary, or a hard
resource limit excludes its next node. It uses the existing scheduler/resource
checks, rejects internal/unknown errors as evidence, and requires a ready primary
with no outstanding request window. The provider persists
`primaryUnavailableReason` with the secondary intent and independently rechecks
mode, complete live config, UID, request window, scale CAS and maximum headroom.
The exception is cleared before each evaluation, on refresh and on discovery
failure. No allowance or blanket secondary preference is created by fit checks.
Linux GPU/Windows combined production-component traces cover primary-unfit,
both-unfit and frozen-unfit outcomes, bounded writes and restart replay. A direct
production resource-manager check covers the primary hard limit, an admissible
secondary and reset when primary fit returns. Existing maxima/PDB/minima controls
remain passing. Mixed fit batches and all topology/affinity combinations are not
claimed exhaustively covered.

Capability JSON has these separate fields:

- `apiVersion`, `version`, and compiled `capabilities` identifiers.
- `source.status`, `vcs`, `revision`, nullable `modified`, and `goVersion`, from
  Go-embedded build metadata. Missing/incomplete metadata remains `unknown`, not
  implicitly clean; `available` means metadata exists, not trusted attestation.
- `configured.cloudProvider`, `azureMachinePoolFailoverMode`, `leaderElection`,
  and `readiness: "not-evaluated"`. Configuration is not runtime admission or
  writer-ownership evidence.

`--capabilities` prints JSON and exits before Kubernetes connection/writer
construction. The diagnostic listener's `GET /capabilities` returns the same
schema with `Cache-Control: no-store`; other methods return 405. It introduces no
management RBAC permissions and reports no credentials. Unit/race tests cover
CLI/HTTP serialization parity, all three modes, dirty/clean/unknown metadata and
method rejection. The actual full executable's CLI smoke test passed and
reported current repository HEAD `53774afee843cad1fc8995d14d45d5ca123fc235`,
`modified: true`, compiled failover support, and unevaluated readiness. This is
an uncommitted worktree build, not a feature-containing clean release commit.
The original branch/base record above remains unchanged.

Proposed consumer preflight **MUST** target the actual running autoscaler pod(s),
not a separate guessed image or a Service that can conceal old replicas. It must
check the JSON API/compiled identifier, record configured mode separately, bind
each pod's immutable `containerStatuses.imageID` to trusted release source/digest
evidence, and reject missing/contradictory provenance. A tag, capability boolean,
dirty source revision, unknown metadata or this report alone is not release
proof. Embedded source claims still require independent artifact provenance.
Readiness, rollout completion and elected-writer quiescence need their separate
checks. The exact consumer workflow remains subject to tfapply agreement; this
workstream neither edits tfapply nor removes its enablement guard.

All seven complete affected suites passed with `-race`, now including `version`.
The full executable build passed with `CGO_ENABLED=0`, `-mod=readonly` and `-p 2`.
The disposable v1.34.0 API/etcd v3.5.21 scoped-RBAC gate passed again under
`-race`: real state CAS/retry, config reads, denied unrelated/mutation/delete/
Lease access, reload and scale replay remain valid. Its two test containers and
internal network were removed; no host ports, kubeconfig changes, CAPZ/Azure
provisioning or tenant changes were made. Ordinary suites still skip this gate
without its explicit disposable-API environment.

Inside the approved Go 1.24.0 Linux Docker test container:

```bash
go test -race -p 2 -count=1 -vet=atomic,bool,buildtags,directive,errorsas,ifaceassert,nilfunc,slog,stringintconv,tests ./cloudprovider/clusterapi ./clusterstate ./core ./core/scaleup/orchestrator ./core/scaledown/planner ./config/flags ./version
go build -mod=readonly -p 2 -o /baseline/cluster-autoscaler-current ./
/baseline/cluster-autoscaler-current --capabilities --cloud-provider=clusterapi --azure-machinepool-failover-mode=active --leader-elect=false
```

The last command is a reporting-only smoke test, not authorization to run an
active writer without election. Normal active/freeze startup still rejects
disabled election. Publication and deployed acceptance are not performed.

### Identifier and Responsibility Refactor Verification

On 2026-10-09, test-first identifier/state cases failed at the old fixed-name
checks, then passed after generic bounded validation. Primary/secondary/both
physical overlap cases verify explicit selection, ignored nonselected failure
history, unchanged extra targets, retained state during unresolved selection,
diagnostic pair/role/pool/UID context, restoration and committed-request replay
rejection. Configuration duplicate-role and duplicate-pair-key cases failed
before strict Kubernetes JSON parsing, then passed along with existing unknown,
malformed, trailing, identity and missing-role controls.

The implementation now lives in `cloudprovider/clusterapi/failover` with a
`Group`/`Environment` adapter boundary and no import of the parent package.
Modules separate state, persistence, selection, observation/recovery, request
admission/deadlines, snapshots and writer lifecycle. The parent retains thin
controller/node-group/informer/scale adapters and their integration tests. Pure
unit tests move with their owning logic. Approximately 100/300 lines are review
guidelines rather than enforced limits; do not fragment cohesive operations to
meet a number. The largest implementation module is the 111-line membership
lookup; the scheduler scenario test is 237 lines after shared fixture extraction.

The previously paused mixed-demand trace now passes for role-specific primary
and secondary-only demand while retaining both bounded intents and restart
suppression. Its old already-served pods are correctly represented as bound to
real secondary nodes, not left to compete for fresh placeholders after restart.
This does not claim exhaustive flexible-demand scheduling permutations.

Focused race baselines passed before and after declaration/package/fixture moves.
All eight complete affected race suites, including the new failover package,
passed after formatting; the full executable build passed with `CGO_ENABLED=0`,
`-mod=readonly` and `-p 2`. The earlier scoped real-API gate remains distinct and
was not rerun for this extraction. No tfapply edits, state migration, permission
expansion, commit, publication or deployed acceptance was performed.

### Parent-Layer Cleanup Verification

The initial extraction left compatibility aliases and too much test scaffolding
in the parent. A subsequent on-disk check also found five superseded original
files coexisting with extracted declarations; compilation correctly failed on
duplicates. Those five originals were removed, compilation restored, and all
legacy aliases replaced with direct `failover` package references.

Paired scale-write policy now lives in `failover/scaleup.go`. Ten policy-test
modules plus the real-API test moved into the owning package using its existing
Group/Environment interfaces, preserving assertions without exporting private
controller types. The parent now has two wiring files,
`clusterapi_failover.go` and `clusterapi_failover_adapters.go`, and four genuinely
private-controller integration/fixture test files. No `azure_failover*.go`
implementation, monolith or compatibility bridge is intended to remain there.

All eight complete affected race suites pass after formatting. The full
`CGO_ENABLED=0`, `-mod=readonly`, `-p 2` executable build passed synchronously.
The relocated real-API gate also passed under `-race` against disposable API
v1.34.0/etcd v3.5.21, including scoped RBAC, state CAS/reload, scale-write replay
and stopped former-writer rejection. Its two containers and internal network
were removed; no host port, kubeconfig or tenant changes occurred. This does not
prove external retirement/fencing acceptance, authorized identifier purge,
image/provenance or deployed acceptance. Retirement/fencing-system changes are
out of scope rather than remaining local implementation gates. No commit or
publication was performed.

### Disposable API Token Fixture

The [disposable API test](../../cluster-autoscaler/cloudprovider/clusterapi/failover/api_test.go#L33)
uses public, test-only admin, denied and scoped identities from
[api-tokens.csv](../../cluster-autoscaler/cloudprovider/clusterapi/failover/test-fixtures/api-tokens.csv).
Mount the fixture read-only into the disposable API-server container as
`/test-tokens.csv` and set `--token-auth-file=/test-tokens.csv`. The test requires
the authorized disposable endpoint in `INF761_TEST_API` and explicit
`INF761_TEST_API_DISPOSABLE=1`. **Never use these tokens or this authentication
file with a production API server.**

### Remaining Gates and Decisions

- Arbitrary-identifier and multiplicity behavior is locally covered as above;
  unresolved identifier reuse is intentionally rejected. External retirement,
  complete cross-configuration isolation and delayed-cache permutations still
  require their separate acceptance/evidence.
- Full `RunOnce` wiring and the actual-provider combined component trace are
  covered by complementary fixtures. A single fixture containing both the actual
  Azure provider and complete `RunOnce` lifecycle is not claimed.
- Crash boundaries, all delayed-cache permutations, concurrent paused-leader
  interleavings, and all replacement-observation orderings are not exhaustively
  covered. ConfigMap, Lease, and scale writes are separate transactions; the
  successful sequential handover test is not proof of atomic distributed fencing.
- OS/GPU fit and paired PDB/minimum/scale-down controls now exist; dedicated
  failover PV-topology/affinity/taint combinations and one full actual-provider
  scale-up-to-scale-down lifecycle fixture remain additional coverage gaps.
- Primary trials are now bounded by persisted provisioning deadlines.
  Arbitrary multiple demand batches while that window is unresolved, and all
  mixed primary-fit/secondary-only batches, are not exhaustively covered.
- Current-generation references and runtime modes now match the agreed config
  contract. Registered identifiers cannot move across changed pool UIDs without
  completed external purge; automatic state migration is not a remaining code
  gate. Deployed capability verification and exact rollout rendering still require
  external coordination.
- Controlled Azure allocation, GPU/Windows workload readiness, storage access,
  released-image validation, and all deployed acceptance remain unverified.
- No commit, push, image publication, or live tenant mutation was performed by
  this implementation workstream.

Return branch/base, changed files, approved interfaces/generation semantics,
schema/bounds, RBAC, recovery/backoff, freeze procedure, exact commands/results,
scan/write traces, and gaps to tfapply. A tested commit or published image **MUST
NOT** be claimed before the respective explicitly authorized operation.

| Item | Status |
|---|---|
| Ticket branch from current fork HEAD | Created; base recorded above |
| Local Change Request | Approved for local implementation by Louis Hansen, 2026-10-07 |
| CR-INF-759 / ADR-0004 approach | Approved for local implementation in this conversation; current external identifiers rechecked against Linear handoff, 2026-10-08 |
| Cross-repository interface freeze | Accepted config/current-generation/mode/RBAC implemented; identifier reuse requires completed external purge; capability JSON tested locally, consumer preflight/rollout acceptance pending |
| Execution environment | Linux Docker available after explicitly authorized engine switch; Go 1.24.0 container used |
| Pre-edit baseline | Missed; retrospective pristine-base build and five package suites passed, 2026-10-07 |
| Implementation and unit/build tests | Six complete package suites and full executable build passed after consumer configuration/mode/leadership alignment, 2026-10-08 |
| Race tests | Six complete package suites and final schema-2/config-v1 scoped-RBAC real API test passed |
| Combined provider-to-scheduler tests | Corrected snapshot/filtering pipeline; API-to-informer failure, sensitivity, replay/restart, independent pairs, real arrivals, and recovery passed for Linux GPU and Windows |
| Primary-first new demand / paired scale-down | Implemented as extensions; Linux/Windows request-order, restart, fresh-failure, either-arrival, maxima, and safe planner/PDB/minima regressions passed |
| Deadline / unfit primary / capability reporting | Implemented as extensions; boundary, override, hard-limit, Linux/Windows fit/freeze/restart, CLI/HTTP/source metadata checks and seven complete race suites passed |
| Remaining implementation/logic gates | Consumer capability/preflight acceptance and documented placement/coverage gaps; identifier rebinding is deliberately rejected until external purge; automatic generation migration, leadership redesign and retirement-system implementation are out of scope |
| Real local API persistence gate | Authorized Docker-only schema-2/config-v1 intent/CAS/scoped-RBAC/reload/stopped-writer/scale tests passed without management Lease access; test containers/network cleaned up |
| Deployed acceptance / image delivery | Not authorized or run |