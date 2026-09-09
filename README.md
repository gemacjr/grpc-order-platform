# gRPC Order Platform

A reference implementation of two production-shaped Spring Boot services:

- **`order-service`** — a gRPC API owning order state, backed by Postgres.
- **`order-api`** — a separate Spring WebFlux REST API that calls
  `order-service` over real gRPC and exposes JSON and Server-Sent Events (SSE)
  to HTTP clients.

Every production concern below (idempotency, retries, deadlines, circuit
breaking, graceful shutdown, tracing, TLS, JWT) is implemented for real, not
stubbed.

## Why the REST edge calls gRPC through generated stubs, not `RestClient`/`WebClient`

`RestClient` and `WebClient` speak HTTP/1.1 and HTTP/2 as HTTP — they cannot
speak the gRPC wire protocol, which is length-prefixed protobuf framed over
HTTP/2 with `application/grpc` content types and status communicated in
trailers, not response headers. There is no way to get a duplex,
trailer-based, protobuf-framed RPC out of an HTTP client built for
request/response bodies.

So `order-api` depends on the generated Java/gRPC/`reactor-grpc` stubs from
`:proto` and calls `order-service` through a real `ManagedChannel`. "WebFlux"
in this project refers to the REST edge's own server stack (`spring-boot-
starter-webflux`) and to the reactive `Mono`/`Flux` types used to bridge those
gRPC calls into the HTTP response — not to how the two services talk to each
other.

## Module layout

```
proto/            :proto            - the .proto contract plus generated Java/gRPC/reactor-grpc stubs
order-service/     :order-service    - gRPC server on 9443, actuator on 9090, owns the Postgres schema
order-api/          :order-api        - WebFlux REST edge on 8080, actuator on 8081, no database access
testing-support/    :testing-support  - Testcontainers Postgres fixture, in-process gRPC harness, JWT test tokens, fault-injecting stubs
docker/                                Dockerfiles, compose stack, Prometheus/Grafana provisioning
k8s/                                   Kubernetes manifests with startup/readiness/liveness probes
```

`:proto` is the only artifact shared between the two services. Domain classes
are deliberately **not** shared — each service maps to and from protobuf at
its own boundary, so the services can evolve independently.

## Build and test

```
./gradlew build
```

Docker is required: the integration test suites use Testcontainers to spin up
real Postgres instances and in-process gRPC servers. No mocking of the
database or the gRPC transport.

## Running the stack

```
docker compose -f docker/compose.yaml up --build -d
```

| URL | What it is |
|---|---|
| `http://localhost:8080` | `order-api` REST edge |
| `http://localhost:9090/actuator` | `order-service` actuator (health, prometheus, metrics — same HTTP port serves everything; there is no separate management port on this service) |
| `http://localhost:8081/actuator` | `order-api` actuator |
| `http://localhost:9091` | Prometheus |
| `http://localhost:3000` | Grafana (anonymous admin access, dashboard provisioned) |
| `http://localhost:16686` | Jaeger UI |
| `localhost:9443` | `order-service` gRPC port (plaintext by default; see TLS below) |

## REST endpoints

All endpoints are under `/api/v1/orders`.

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/orders` | Create an order. Body carries an `idempotencyKey`; a replayed key returns the original order with `200 OK` instead of a duplicate `201 Created`. |
| `GET` | `/api/v1/orders/{id}` | Fetch a single order. |
| `GET` | `/api/v1/orders` | List orders. Query params: `customerId`, `status`, `pageSize` (default 50), `pageToken` (opaque cursor). |
| `PATCH` | `/api/v1/orders/{id}/status` | Transition status. Body carries the target `status` and the `expectedVersion` observed by the caller (optimistic locking). |
| `GET` | `/api/v1/orders/events` | Server-Sent Events stream of order status changes. Query params: `customerId` (optional filter), `fromSequence` (resume point). |

## gRPC status to HTTP status mapping

Domain exceptions in `order-service` are mapped to gRPC `Status` codes (with
`google.rpc.ErrorInfo` and, for validation failures, `BadRequest.FieldViolation`
attached to the trailers). `order-api` unwraps the `StatusRuntimeException` and
its `@RestControllerAdvice` re-maps it to an RFC 9457 `ProblemDetail`:

| gRPC status | HTTP | Notes |
|---|---|---|
| `NOT_FOUND` | 404 | |
| `INVALID_ARGUMENT` | 400 | `errors[]` populated from `FieldViolation` |
| `FAILED_PRECONDITION` | 409 | Illegal status transition |
| `ABORTED` | 409 | Optimistic-lock version conflict; caller should re-read and retry |
| `DEADLINE_EXCEEDED` | 504 | |
| `UNAVAILABLE` / circuit open | 503 | `Retry-After` header set |
| `PERMISSION_DENIED` | 403 | |
| `UNAUTHENTICATED` | 401 | |
| everything else | 500 | Opaque body, error id echoed for correlation |

Note: `order-service` never emits `ALREADY_EXISTS` — a duplicate
idempotency key resolves into a replay of the original order (see
`POST /api/v1/orders` above), not an error, so that gRPC status has no
corresponding row here.

## Exploring with `grpcurl`

Server reflection is enabled on `order-service`, so `grpcurl` works without
local `.proto` descriptors:

```bash
grpcurl -plaintext localhost:9443 list
grpcurl -plaintext localhost:9443 list order.v1.OrderService
grpcurl -plaintext localhost:9443 grpc.health.v1.Health/Check
grpcurl -plaintext -d '{"customer_id":"c1","lines":[{"sku":"WIDGET","quantity":2,"unit_price_minor":500}],"currency":"USD","idempotency_key":"demo-1"}' \
  localhost:9443 order.v1.OrderService/CreateOrder
```

## TLS and JWT (both off by default)

Both `docker/compose.yaml` runs the stack with **TLS off** (plaintext gRPC)
and **JWT auth off** (`order.security.enabled=false` on both services). This
is deliberate: it keeps the default stack runnable with `docker compose up`
alone — no certificates to generate, no identity provider to stand up — while
every reliability and observability behaviour (idempotency, retries, circuit
breaking, tracing, graceful shutdown) is fully exercised regardless of
whether transport security is on.

**To enable TLS:**

1. Generate dev certificates: `scripts/generate-dev-certs.sh certs` (never
   commit the output — development certificates only).
2. Uncomment the TLS environment blocks in `docker/compose.yaml` for both
   `order-service` (`GRPC_TLS_ENABLED`, `GRPC_TLS_CERT`, `GRPC_TLS_KEY`) and
   `order-api` (`GRPC_TLS_ENABLED`, `GRPC_NEGOTIATION=tls`,
   `GRPC_TRUST_STORE`), and mount `./certs` into both containers.

**To enable JWT:**

Uncomment the security environment blocks in `docker/compose.yaml` for both
services (`ORDER_SECURITY_ENABLED=true`, `ORDER_SECURITY_JWK_SET_URI`,
`ORDER_SECURITY_ISSUER`, `ORDER_SECURITY_AUDIENCE`) — this requires a real
IdP/JWKS endpoint reachable from the containers. `ORDER_SECURITY_JWK_SET_URI`
is required whenever security is enabled: both services fail fast at startup
with a clear `IllegalStateException` if it is left unset. Both services must
agree: `order-api` validates the bearer token at the REST edge and forwards
it as gRPC `authorization` metadata; `order-service` independently
re-validates it in a server interceptor rather than trusting the edge.

## Verified behaviours

The following were checked against the actually-running `docker compose`
stack, not just asserted by unit tests:

- **Idempotent replay.** Sending `CreateOrder` twice with the same
  `idempotencyKey` returns the original order both times — the second
  response is `200 OK`, not a duplicate resource or an error.
- **End-to-end tracing.** A single HTTP request produces exactly one Jaeger
  trace spanning `order-api` (REST), the gRPC call to `order-service`, and the
  JDBC call to Postgres — W3C `traceparent` propagates through gRPC metadata.
- **Graceful degradation and recovery.** Stopping `order-service` causes REST
  reads through `order-api` to return `503` with a `Retry-After` header (the
  circuit breaker opens); once `order-service` is restarted, `order-api`
  recovers automatically without itself being restarted.
