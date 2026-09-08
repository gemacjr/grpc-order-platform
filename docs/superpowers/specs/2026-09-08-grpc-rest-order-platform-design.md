# Design: gRPC Order Platform with WebFlux REST Edge

**Date:** 2026-09-08
**Status:** Approved

## 1. Purpose

Build two production-ready Spring Boot services:

1. **`order-service`** — a gRPC API owning order state, backed by Postgres.
2. **`order-api`** — a separate Spring WebFlux REST API that calls `order-service`
   over real gRPC and exposes JSON and Server-Sent Events to HTTP clients.

The pair is a reference implementation: every production concern listed here is
implemented for real, not stubbed.

### Premise correction

`RestClient` and `WebClient` cannot speak the gRPC wire protocol (length-prefixed
protobuf over HTTP/2 with `application/grpc` and trailer-based status). The REST
edge therefore calls the gRPC service through generated gRPC stubs. "WebFlux"
applies to the REST edge's own server stack and to the reactive types used to
bridge gRPC calls.

## 2. Success criteria

- `./gradlew build` runs all tests green from a clean checkout.
- `docker compose up` yields a working system: REST on 8080, gRPC on 9443,
  Postgres, Jaeger, Prometheus, Grafana.
- A single HTTP request produces one distributed trace spanning REST -> gRPC -> JDBC.
- Killing `order-service` causes REST reads to return 503 with `Retry-After` via an
  open circuit breaker, and to recover automatically once it returns.
- A duplicate `CreateOrder` with the same idempotency key returns the original order.
- Integration tests prove retry, deadline, circuit-breaking and SSE streaming
  behaviour rather than assuming it.

## 3. Constraints and decisions

| Decision | Choice | Rationale |
|---|---|---|
| Language / runtime | Java 21 (LTS) | Virtual threads for the blocking server path; pattern matching. |
| Framework | Spring Boot 4.0.8 | Verified against Maven Central: `spring-grpc-spring-boot-starter:1.0.3` declares `spring-boot-starter:4.0.2`, so Spring gRPC 1.0.x targets the Boot 4.0 line. An earlier draft of this spec said 3.5.x; that was wrong. |
| gRPC integration | Spring gRPC 1.0.3 (official) | First-party server/client autoconfiguration and Boot lifecycle integration. Its BOM manages grpc 1.77.1, protobuf-java 4.33.4 and reactor-grpc 1.2.4, so those are not pinned independently. |
| Resilience | `resilience4j-spring-boot4:2.4.0` | The Boot 4 variant; the `-spring-boot3` artifact is not used. |
| Build | Gradle multi-module, Kotlin DSL, version catalog | Single-command build, one source of dependency truth. |
| Reactive bridging | `reactor-grpc` 1.2.4 codegen | Emits `Mono`/`Flux` stubs with request-n backpressure wired to gRPC flow control. Hand-rolled `StreamObserver` adapters are subtle and break under load. Managed by the Spring gRPC BOM, so it is a supported combination rather than a bolt-on. |
| Persistence | Spring `JdbcClient` (`spring-boot-starter-jdbc`) + Flyway | Explicit SQL, no lazy-loading traps, predictable with virtual threads. Keyset pagination and the `insert ... returning` used for event sequences are awkward to express through a repository abstraction. |
| Money | `int64` minor units | Never floating point. |
| Pagination | Cursor `(created_at, id)` | Offset pagination silently corrupts under concurrent inserts. |

## 4. Module structure

```
Gprc_SB/
  settings.gradle.kts            # includes the four modules
  gradle/libs.versions.toml      # version catalog
  proto/                         # :proto
    src/main/proto/order/v1/order_service.proto
    buf.yaml, buf.gen.yaml
  order-service/                 # :order-service  (gRPC server)
  order-api/                     # :order-api      (WebFlux REST edge)
  testing-support/               # :testing-support
  docker/                        # Dockerfiles, compose, prometheus, grafana
  k8s/                           # manifests with probes
  docs/superpowers/specs/
```

`:proto` is the only shared artifact. Domain classes are deliberately **not**
shared: each service maps to and from protobuf at its own boundary, so the two
services can evolve independently.

### Module responsibilities

- **`:proto`** — the contract plus generated stubs (protobuf-gradle-plugin,
  `protoc-gen-grpc-java`, `protoc-gen-reactor-grpc`). Depends on nothing.
- **`:order-service`** — gRPC server on 9443 (TLS), Actuator on a separate HTTP
  port 9090 so management traffic is not exposed on the data port. Owns the schema.
- **`:order-api`** — WebFlux on 8080, gRPC client channel, Resilience4j,
  Spring Security resource server. No database access of any kind.
- **`:testing-support`** — Testcontainers Postgres fixture, in-process gRPC server
  harness, JWT test-token minting, fault-injecting stub implementations.

## 5. Contract: `order/v1/order_service.proto`

Package `order.v1`. Five RPCs, each exercising a distinct production concern.

### Messages

- `Order` — `id` (UUID string), `customer_id`, `repeated OrderLine lines`,
  `int64 total_minor`, `string currency` (ISO-4217), `OrderStatus status`,
  `google.protobuf.Timestamp created_at`, `updated_at`, `int64 version`.
- `OrderLine` — `sku`, `int32 quantity`, `int64 unit_price_minor`.
- `OrderStatus` — enum with explicit `ORDER_STATUS_UNSPECIFIED = 0`, then
  `PENDING`, `CONFIRMED`, `SHIPPED`, `CANCELLED`.
- `OrderEvent` — `order_id`, `OrderStatus old_status`, `new_status`,
  `Timestamp occurred_at`, `sequence`.

### RPCs

| RPC | Kind | Concern exercised |
|---|---|---|
| `CreateOrder` | unary | Idempotency key, validation, `FieldViolation` details |
| `GetOrder` | unary | Deadline propagation, circuit breaking, `NOT_FOUND` mapping |
| `ListOrders` | unary | Cursor pagination with an opaque `page_token` |
| `UpdateOrderStatus` | unary | Optimistic locking, illegal-transition rejection, event emission |
| `StreamOrderEvents` | server-streaming | Reactive backpressure, SSE at the REST edge |

`CreateOrderRequest` carries `idempotency_key`. `ListOrdersRequest` carries
`page_size` (server-clamped, default 50, max 500), `page_token`, and an optional
`status` filter; `ListOrdersResponse` returns `repeated Order orders` and
`next_page_token` (empty when exhausted). The page token is a base64-encoded,
opaque `(created_at, id)` cursor; clients must not parse it.

`UpdateOrderStatusRequest` carries `id`, the target `OrderStatus`, and the
`version` the caller last observed. It is what makes the rest of the system real:
it is the only writer of `order_events`, the only source of `FAILED_PRECONDITION`
(illegal transition, e.g. `CANCELLED -> SHIPPED`), and the only trigger of
optimistic-locking conflicts. Legal transitions are `PENDING -> CONFIRMED |
CANCELLED`, `CONFIRMED -> SHIPPED | CANCELLED`; `SHIPPED` and `CANCELLED` are
terminal.

`StreamOrderEventsRequest` carries an optional `customer_id` filter and
`from_sequence` for resumption.

## 6. Data model

Two tables, created by Flyway migrations in `order-service`.

- `orders` — `id uuid pk`, `customer_id text not null`, `total_minor bigint`,
  `currency char(3)`, `status text`, `version bigint`, `created_at timestamptz`,
  `updated_at timestamptz`, `idempotency_key text`.
  - `unique index on (customer_id, idempotency_key) where idempotency_key is not null`
  - `index on (created_at desc, id desc)` to serve the pagination cursor.
- `order_lines` — `id`, `order_id fk`, `sku`, `quantity`, `unit_price_minor`.
- `order_events` — `sequence bigserial pk`, `order_id`, `old_status`,
  `new_status`, `occurred_at`. Written in the same transaction as the status
  change and polled by the streaming RPC, so an event is never emitted for a
  change that rolled back.

Optimistic locking uses the `version` column; a conflicting update raises
`OrderVersionConflict`.

## 7. Error handling

Three hops, each lossless.

1. **Domain to gRPC.** Domain exceptions (`OrderNotFound`, `InvalidOrderState`,
   `OrderVersionConflict`, `IllegalStatusTransition`, `OrderValidationException`)
   are mapped by a
   `ServerInterceptor` to `Status` codes, with `google.rpc.ErrorInfo`
   (`reason`, `domain`, `metadata`) and, for validation failures,
   `google.rpc.BadRequest.FieldViolation` attached to the trailers. Any
   unexpected exception becomes `INTERNAL` carrying a generated error id; the
   full detail is logged server-side and never returned to the caller.

2. **gRPC to domain.** The client adapter in `order-api` unwraps
   `StatusRuntimeException`, reads the trailers, and rethrows typed exceptions.

3. **Domain to HTTP.** A `@RestControllerAdvice` emits RFC 9457 `ProblemDetail`.

| gRPC status | HTTP | Notes |
|---|---|---|
| `NOT_FOUND` | 404 | |
| `INVALID_ARGUMENT` | 400 | `errors[]` populated from `FieldViolation` |
| `ALREADY_EXISTS` | 409 | |
| `FAILED_PRECONDITION` | 409 | Illegal status transition |
| `ABORTED` | 409 | Optimistic-lock version conflict; caller should re-read and retry |
| `DEADLINE_EXCEEDED` | 504 | |
| `UNAVAILABLE` / circuit open | 503 | `Retry-After` header set |
| `PERMISSION_DENIED` | 403 | |
| `UNAUTHENTICATED` | 401 | |
| everything else | 500 | Opaque body, error id echoed for correlation |

Validation runs in explicit request-validator classes rather than annotations on
generated protobuf types, so one violation list feeds both `BadRequest` details
and `ProblemDetail.errors`.

## 8. Reliability

- **Deadlines.** `order-api` sets a per-call deadline from configuration
  (default 2s reads, 5s writes). The deadline propagates as a *budget*, not a
  per-hop timeout. `order-service` checks `Context.current().getDeadline()`
  before expensive work and aborts rather than doing work nobody awaits.
- **Retries.** A gRPC service-config retry policy with exponential backoff and
  jitter, restricted to `UNAVAILABLE` and `RESOURCE_EXHAUSTED`. `GetOrder` and
  `ListOrders` are naturally safe; `CreateOrder` is retried only because the
  idempotency key makes it safe. `UpdateOrderStatus` is retried only because the
  caller-supplied `version` makes a replayed write fail rather than double-apply.
  Hedging is off.
- **Circuit breaking and bulkheads.** Resilience4j wraps the client adapter,
  with counts exported to Micrometer.
- **Keepalive.** Client and server keepalive, idle timeout, and
  `permitKeepAliveWithoutCalls` tuned to survive idle NAT/LB timeouts.
- **Graceful shutdown.** Readiness flips DOWN first so load balancers stop
  sending traffic, then `Server.shutdown()` drains in-flight calls, then
  `awaitTermination` with a bounded grace period before `shutdownNow()`.
- **Idempotency.** `CreateOrder` inserts against the unique idempotency index;
  a duplicate key returns the original order rather than an error or a duplicate.

## 9. Observability

- **Metrics.** Micrometer with a Prometheus endpoint on the management port;
  gRPC server/client metrics, Resilience4j metrics, JDBC pool metrics.
- **Tracing.** `ObservationGrpcClientInterceptor` and
  `ObservationGrpcServerInterceptor` propagate W3C `traceparent` through gRPC
  metadata, producing a single trace spanning REST, gRPC and JDBC, exported over
  OTLP to Jaeger.
- **Logging.** Structured JSON including `traceId`, `spanId`, and a correlation id.
- **Health.** A real `grpc.health.v1.Health` service (so Kubernetes can use a
  native gRPC probe) alongside Actuator health. gRPC server reflection is enabled
  so `grpcurl` works without local descriptors.

## 10. Security

- TLS on the gRPC channel; dev certificates produced by a checked-in generation
  script, never committed.
- JWT bearer tokens validated at the REST edge by Spring Security resource server.
- The token is forwarded in gRPC `authorization` metadata; `order-service`
  re-validates it in a server interceptor and populates a gRPC `Context`
  principal. The gRPC service does not trust the edge.
- Per-method authorization checks in the service layer, driven by scopes.

## 11. Testing

Test-driven throughout.

- **Unit** — domain logic, protobuf mappers, cursor encode/decode, error mapping.
- **gRPC service tests** — against an in-process server (`InProcessServerBuilder`),
  no ports, fast.
- **Persistence and full-service ITs** — Testcontainers Postgres, Flyway applied.
- **REST edge tests** — `WebTestClient` against a real in-process gRPC server,
  including fault-injecting stubs that return `UNAVAILABLE` or hang past the
  deadline, proving retry, circuit-breaker, deadline and SSE-streaming behaviour.
- **Contract** — `buf lint` and `buf breaking` against `main` in CI.

## 12. Delivery

- Multi-stage Dockerfiles per service (JRE 21 base, non-root user).
- `docker compose` bringing up both services, Postgres, Jaeger, Prometheus and
  Grafana with a provisioned dashboard.
- GitHub Actions CI: build, test, buf lint/breaking, image build.
- Kubernetes manifests with startup, readiness and liveness probes (native gRPC
  probe for `order-service`), resource limits, and a `PodDisruptionBudget`.

## 13. Implementation phases

Each phase is independently verifiable and leaves the build green.

1. **Foundation** — settings, version catalog, `:proto` with codegen, buf config.
2. **gRPC service** — schema, repository, domain logic, five RPCs, error model.
3. **REST edge** — reactive client adapter, controllers, ProblemDetail mapping, SSE.
4. **Reliability** — deadlines, retry policy, circuit breaker, idempotency,
   graceful shutdown, keepalive.
5. **Observability and security** — metrics, tracing, structured logging, health,
   TLS, JWT.
6. **Delivery** — Dockerfiles, compose stack, CI, Kubernetes manifests.

## 14. Explicitly out of scope

- A real identity provider; dev JWTs are minted from a local keypair.
- Event publishing to an external broker; `order_events` is polled from the database.
- Multi-tenancy, rate limiting per customer, and horizontal-scale event fan-out.
