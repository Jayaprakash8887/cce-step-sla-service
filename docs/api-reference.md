# API Reference — Step SLA Service

Base URL: `http://<host>:8092`

This service has **no application API**. Everything it writes is driven by its scheduler, not by a
request — see [Architecture §3](architecture-overview.md#3-the-fetch-and-apply-cycle). Its only HTTP
surface is the operational endpoints below.

---

## Operational endpoints

| Path | Purpose |
|---|---|
| `/actuator/health` | Liveness and readiness probes |
| `/actuator/health/readiness` | Fails while the database is unreachable |
| `/actuator/info` | Build info |
| `/actuator/metrics` | Micrometer metrics |
| `/actuator/prometheus` | Prometheus scrape |

The SLA sweep is not exposed over HTTP — it cannot be triggered, paused or drained by a request. Its
state is observable through the metrics in
[Architecture §6](architecture-overview.md#6-observability) and through
`step_sla_state_transition` itself.
