# CCE Compliance Service

The time plane of the CCE system. Picks up the SLA transitions the Matcher Service scheduled as their
deadlines pass, writes `step_instance.sla_status` (it is the column's only writer), records the
resulting `OVERDUE` / `MISSED`
deviations, and publishes the intelligence actions they trigger.

It matches no events, enrols no patients and manages no definitions. It **owns no tables** and runs no
migrations. Kafka is **produce-only** — nothing inbound reaches this service.

**Port** `8092` · **Java** 21 · **Spring Boot** 3.4.2 · **Version** 2.0.0

## Quick Start

This service cannot create its own schema. Bring up the Protocol and Matcher services against `ccedb`
first — in that order — then:

```bash
./gradlew build
./gradlew bootRun

curl -s localhost:8092/actuator/health
```

Starting against an unmigrated database fails at boot on `ddl-auto: validate`, by design. Full
sequence: [Developer Setup](docs/developer-setup.md#quick-start).

## Documentation

| Document | Contents |
|---|---|
| [Architecture & Design](docs/architecture-overview.md) | The fetch-and-apply cycle, the applier's behaviour table, retry and backoff, observability, scaling |
| [API Reference](docs/api-reference.md) | The read-only intelligence-event API and the operational endpoints |
| [Developer Setup](docs/developer-setup.md) | Prerequisites, configuration and tuning, project layout, testing, and the invariants to preserve |
| [Deployment Guide](docs/deployment-guide.md) | Docker and Kubernetes, replica scaling, alerts, troubleshooting |

System-wide context lives in **cce-common-util** and is not restated here:

| For | See |
|---|---|
| Why the services are split, and the SLA handoff contract | `cce-common-util` → [docs/architecture-overview.md](../cce-common-util/docs/architecture-overview.md) |
| Schema, columns, enums, table ownership | `cce-common-util` → [docs/data-dictionary.md](../cce-common-util/docs/data-dictionary.md) |
| The shared entities, evaluator and exception handler this service uses | `cce-common-util` → [docs/library-reference.md](../cce-common-util/docs/library-reference.md) |
| Status vocabularies and their FHIR provenance | `cce-common-util` → [docs/fhir-conformance.md](../cce-common-util/docs/fhir-conformance.md) |

Cross-repository links assume the repositories are checked out as siblings, which is also what the
Gradle composite build assumes.

## How it works

```
@Scheduled poll ──> SlaTransitionEvaluator      drives; holds no transaction
                          │
                          ▼
                    SlaTransitionApplier         one transaction per batch
                          │
   fetch: deadline passed, or step already COMPLETED ── FOR UPDATE SKIP LOCKED
                          │
          ┌───────────────┼────────────────┐
          ▼               ▼                ▼
   step_instance      deviation      intelligence_event_log
   .sla_status                              │
                                            ▼
                                  Kafka cce.intelligence.triggers
```

The row lock **is** what reserves the row — no lease table, no heartbeat, no leader election. Every replica can
poll the same table concurrently, and a replica that dies mid-batch releases its rows immediately.

A row is fetched either because its deadline passed or because its step is already completed: the
verdict reads `completed_at` against `process_by` and never the wall clock, so a completion settles its
SLA on the next sweep instead of waiting for a threshold that would only confirm it. Details in
[Architecture §3](docs/architecture-overview.md#3-the-fetch-and-apply-cycle).

## API

```
GET /v1/compliance/intelligence-events?protocolInstanceId=&actionDefinitionId=&published=
GET /v1/compliance/intelligence-events/{id}
```

Read-only. Everything this service writes is driven by its scheduler, never by a request.

## Testing

```bash
./gradlew test              # 54 tests (53 unit + a context-boot test)
./gradlew build             # tests + coverage gate (0.98 instruction coverage)
./gradlew jacocoTestReport  # build/reports/jacoco/test/html/index.html
```

Concurrent-fetch behaviour depends on real `FOR UPDATE SKIP LOCKED` semantics and must be verified
against PostgreSQL, not H2.
