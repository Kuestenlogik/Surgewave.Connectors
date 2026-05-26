# Testing — Surgewave.Connectors

This repository ships 118 connectors. Each connector lives under
`src/Kuestenlogik.Surgewave.Connector.<Name>/`; the matching test project
(when present) lives under `tests/Kuestenlogik.Surgewave.Connector.<Name>.Tests/`.

## Coverage policy

`.github/workflows/coverage.yml` runs `dotnet test` with Coverlet
(`coverlet.runsettings`) on every PR and `main` push. It generates a
ReportGenerator HTML/Markdown summary plus badges, posts a comment to the PR,
and enforces a coverage **floor**.

Current floor: **40 % line / 30 % branch**.

Baseline measured 2026-05-26: **41.5 % line / 31.6 % branch** across 72
Connector-Assemblies (the 45 connectors without a test project are absent
from the report — they contribute zero to both numerator and denominator).
The floor sits ~1.5 pp under baseline so moderate fluctuation passes but a
real regression fails CI. The floor is ratcheted up alongside the
[backlog](#open-testing-work) below.

Run the same checks locally:

```bash
dotnet test Kuestenlogik.Surgewave.Connectors.slnx -c Release \
  --collect:"XPlat Code Coverage" \
  --results-directory artifacts/coverage/raw \
  --settings coverlet.runsettings \
  --filter "FullyQualifiedName!~IntegrationTests"

reportgenerator \
  -reports:'artifacts/coverage/raw/**/coverage.cobertura.xml' \
  -targetdir:artifacts/coverage/report \
  -reporttypes:'Html;TextSummary'

cat artifacts/coverage/report/Summary.txt
```

## Test-style matrix

The connector test-suite mix is **type-driven**:

- **Self-hostable services → Testcontainers.** Cassandra, PostgreSQL, MongoDB,
  Redis, RabbitMQ, Elasticsearch, Kafka, NATS, etc. Tests pull an upstream
  image, run the connector end-to-end, and assert on real behaviour. Higher
  signal at the cost of `~20-60 s` per connector in CI.

- **Cloud APIs → Mock-based unit tests.** AWS SDK, Azure SDK, GCP SDK,
  Anthropic, OpenAI, Twitter, Discord etc. Config parsing, schema validation,
  request-shape, response-handling are exercised with a mock HTTP/SDK layer
  (Moq + WireMock.Net or Azure.Identity.TestFramework). Hits no external
  service, runs in milliseconds, but cannot catch real-API drift.

- **Hardware/IoT bridges → Pure unit + protocol fakes.** Hue, Matter,
  Google.Home, Alexa. The state-machine and message-framing layer are tested
  in isolation; physical device communication is mocked.

Pick the style that matches the connector. When in doubt:

| Question | If yes | If no |
|---|---|---|
| Can the upstream service run in a Linux container with deterministic startup? | Testcontainers | Mock |
| Is the upstream a public SaaS API with no self-host story? | Mock | Testcontainers |
| Does the connector mostly do schema/transform/auth (not I/O)? | Pure unit | — |

## Open testing work

### Connectors **without** a test project (45)

Today the following connectors have no `*.Tests` project. Each entry is a
backlog item; the priority column reflects how often the connector is exercised
in the wider Surgewave ecosystem (`docs/connectors/*`, sample apps).

| Connector | Test-Strategy | Priority |
|---|---|---|
| Amqp | Testcontainers (RabbitMQ) | High |
| Pulsar | Testcontainers | High |
| Redis.List | Testcontainers | High |
| Redis.Scan | Testcontainers | High |
| Kafka.Bridge | Testcontainers (Kafka) | High |
| Surgewave.Bridge | In-process | High |
| TimescaleDB | Testcontainers (PostgreSQL + extension) | High |
| FileStream | Pure unit | High |
| Flink | Testcontainers | Medium |
| TigerBeetle | Testcontainers | Medium |
| Nsq | Testcontainers | Medium |
| Beanstalkd | Testcontainers | Medium |
| Nats.ObjectStore | Testcontainers | Medium |
| Logic | Pure unit | Medium |
| SignalR | In-process | Medium |
| Sap.Hana | Testcontainers (where licensed) / Mock | Medium |
| Sap.EventMesh | Mock | Medium |
| Sap.OData | Mock + WireMock | Medium |
| Gcp.Bigtable | Mock | Medium |
| Gcp.Spanner | Mock | Medium |
| Aws.Neptune | Mock | Medium |
| Aws.Efs | Mock | Medium |
| ZeroMQ | Pure unit + in-proc loopback | Medium |
| Nanomsg | Pure unit + in-proc loopback | Medium |
| Mattermost | Testcontainers | Low-Medium |
| RocketChat | Testcontainers | Low-Medium |
| Xmpp | Testcontainers (Prosody) | Low-Medium |
| SpaCy | Mock (Python bridge) | Low-Medium |
| Spark | Testcontainers | Low-Medium |
| Telegram | Mock + WireMock | Low |
| Discord | Mock + WireMock | Low |
| WhatsApp | Mock + WireMock | Low |
| FacebookMessenger | Mock + WireMock | Low |
| Facebook | Mock + WireMock | Low |
| Instagram | Mock + WireMock | Low |
| Twitter | Mock + WireMock | Low |
| LinkedIn | Mock + WireMock | Low |
| Reddit | Mock + WireMock | Low |
| Wikipedia | Mock + WireMock (public API) | Low |
| Weather | Mock + WireMock | Low |
| Hue | Pure unit + state-machine | Low |
| Matter | Pure unit + state-machine | Low |
| Google.Home | Mock | Low |
| Google.Photos | Mock | Low |
| Alexa | Mock | Low |

### Connectors **with** thin test coverage

Beyond the 45 missing ones, ~20 existing test projects have only a handful of
Facts (e.g. `Database` has 5 — `Cassandra`, by comparison, has 94). A
follow-up audit should flag those and raise them toward the project-internal
target of ~30 Facts per non-trivial connector.

### Threshold ratchet plan

Baseline (2026-05-26): 41.5 % line / 31.6 % branch on the 72 tested
connectors. Adding test projects for the 45 untested connectors will
initially **lower** the cross-suite percentage (more code in the
denominator before tests catch up), so the ratchet plan keeps a buffer
between current measurement and floor.

| Milestone | Line | Branch | Notes |
|---|---|---|---|
| Today (baseline) | **40 %** | **30 %** | floor sits ~1.5 pp below baseline |
| After thin-test audit (raises Database, etc.) | 45 % | 35 % | tests within existing 72 projects |
| After P0/P1 batch (15 connectors) | 42 % | 32 % | new projects pull denominator up |
| After P2 batch (20 connectors) | 45 % | 35 % | with batch-tests landed |
| After full backlog (all 45 added) | 50 % | 40 % | every connector has at least basic coverage |
| `1.0` ship | 60 % | 45 % | targeted push on hot-path code |
