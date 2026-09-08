# gRPC Order Platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build two production-ready Spring Boot services — a gRPC order API backed by Postgres, and a separate WebFlux REST API that calls it over real gRPC — with deadlines, retries, circuit breaking, idempotency, tracing, TLS, JWT, and a full local stack.

**Architecture:** One Gradle multi-module build. `:proto` owns the contract and generates grpc-java + reactor-grpc stubs. `:order-service` is a gRPC server (Spring gRPC server starter) with Spring Data JDBC and Flyway. `:order-api` is a WebFlux REST edge (Spring gRPC client starter) that bridges gRPC to `Mono`/`Flux` and maps every gRPC status to an RFC 9457 `ProblemDetail`. No domain classes are shared between the services — only the proto contract.

**Tech Stack:** Java 21, Spring Boot 4.0.8, Spring gRPC 1.0.3 (manages grpc 1.77.1 / protobuf-java 4.33.4 / reactor-grpc 1.2.4), Gradle 9.6 Kotlin DSL, protobuf-gradle-plugin 0.9.6, Postgres 16, Flyway, Resilience4j 2.4.0 (`-spring-boot4`), Micrometer + OpenTelemetry, Testcontainers 1.21.4, JUnit 5, buf, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-09-08-grpc-rest-order-platform-design.md`

## Global Constraints

- **Java toolchain 21** for every module. Do not use preview features.
- **Spring Boot `4.0.8`**; Spring gRPC `1.0.3`. Do not use `resilience4j-spring-boot3` — the Boot 4 variant is `io.github.resilience4j:resilience4j-spring-boot4:2.4.0`.
- **Never pin `grpc-*`, `protobuf-java`, or `reactor-grpc` versions.** The `org.springframework.grpc:spring-grpc-dependencies:1.0.3` BOM manages all three. Pinning them independently causes codegen/runtime version skew.
- **Spring gRPC configuration lives under `spring.grpc.*`** — `spring.grpc.server.port`, `spring.grpc.client.channels.<name>.address`. A top-level `grpc:` block is silently ignored, which looks like the setting "not working" rather than an error.
- **Money is `int64` minor units.** Floating-point money is a defect anywhere it appears.
- **Every proto enum has an explicit `_UNSPECIFIED = 0` member.**
- **Pagination is cursor-based**, never offset.
- **All timestamps are `google.protobuf.Timestamp` on the wire and `Instant` / `timestamptz` internally.** Never `LocalDateTime`.
- **Package roots:** `com.example.order.proto` (generated), `com.example.order.service` (`:order-service`), `com.example.order.api` (`:order-api`), `com.example.order.testing` (`:testing-support`).
- **Ports:** gRPC 9443, `order-service` management 9090, `order-api` HTTP 8080, `order-api` management 8081.
- **TDD is mandatory.** Every task writes a failing test first, watches it fail, then implements. A step that says "run the test and see it fail" is not optional ceremony — a test you never saw fail proves nothing.
- **Commit at the end of every task**, using the message given in the task's final step.

---

## File Structure

```
settings.gradle.kts                     four modules + toolchain resolver
build.gradle.kts                        root: shared java/test conventions only
gradle/libs.versions.toml               single source of dependency truth

proto/
  build.gradle.kts                      protobuf-gradle-plugin, grpc + reactor codegen
  buf.yaml  buf.gen.yaml                lint + breaking-change config
  src/main/proto/order/v1/order_service.proto

order-service/                          gRPC server
  src/main/java/com/example/order/service/
    OrderServiceApplication.java
    domain/       Order, OrderLine, OrderStatus, OrderEvent, StatusTransitions,
                  and the domain exceptions
    persistence/  OrderRepository (JdbcClient), PageCursor, OrderRowMapper
    application/  OrderApplicationService (transactions, idempotency, locking)
    grpc/         OrderGrpcService, OrderProtoMapper, OrderRequestValidator,
                  GrpcExceptionInterceptor, AuthInterceptor
    config/       GrpcServerConfig, ObservabilityConfig, SecurityConfig
  src/main/resources/db/migration/       V1__orders.sql, V2__order_events.sql
  src/main/resources/application.yml

order-api/                              WebFlux REST edge
  src/main/java/com/example/order/api/
    OrderApiApplication.java
    client/       OrderGrpcClient (reactor stub facade), GrpcStatusTranslator,
                  DeadlineFactory, ResilientOrderClient
    web/          OrderController, OrderEventSseController, ProblemDetailAdvice
    dto/          request/response records + DtoMapper
    config/       GrpcClientConfig, SecurityConfig, ResilienceConfig
  src/main/resources/application.yml

testing-support/
  src/main/java/com/example/order/testing/
    PostgresFixture.java                shared Testcontainers instance
    InProcessGrpc.java                  in-process server/channel harness
    FaultInjectingOrderService.java     programmable failure stub
    JwtTestTokens.java                  local-keypair token minting

docker/  Dockerfile.service Dockerfile.api compose.yaml prometheus.yml grafana/
k8s/     order-service.yaml order-api.yaml
.github/workflows/ci.yml
```

Rationale for the boundaries: `domain` has no Spring or protobuf imports, so status-transition rules are testable with plain JUnit and no context load. `grpc` is the only package that touches generated types on the server side, and `client` is the only one on the edge — protobuf never leaks into controllers or the domain. `testing-support` is a real module rather than duplicated test fixtures because both services need Postgres and JWT fixtures, and duplicated Testcontainers setup drifts.

---

## Phase 1 — Foundation

### Task 1: Gradle multi-module skeleton and version catalog

**Files:**
- Modify: `settings.gradle.kts`
- Modify: `build.gradle.kts`
- Create: `gradle/libs.versions.toml`
- Create: `proto/build.gradle.kts`, `order-service/build.gradle.kts`, `order-api/build.gradle.kts`, `testing-support/build.gradle.kts`
- Test: `order-service/src/test/java/com/example/order/service/ToolchainTest.java`

**Interfaces:**
- Consumes: nothing (first task).
- Produces: version catalog aliases used by every later task — `libs.spring.boot.gradle`, `libs.protobuf.gradle`, `libs.spring.grpc.bom`, `libs.grpc.services`, `libs.grpc.testing`, `libs.reactor.grpc.stub`, `libs.resilience4j`, `libs.testcontainers.bom`, `libs.flyway.postgres`, `libs.micrometer.tracing.otel`, `libs.otel.exporter.otlp`, `libs.logstash.encoder`.

- [ ] **Step 1: Write the failing test**

Create `order-service/src/test/java/com/example/order/service/ToolchainTest.java`:

```java
package com.example.order.service;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class ToolchainTest {

    @Test
    void runsOnJava21OrLater() {
        assertThat(Runtime.version().feature()).isGreaterThanOrEqualTo(21);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :order-service:test`
Expected: FAIL — `Project 'order-service' not found in root project 'Gprc_SB'`.

- [ ] **Step 3: Write the version catalog**

Create `gradle/libs.versions.toml`. Note deliberately absent entries: no `grpc`, `protobuf-java`, or `reactor-grpc` *versions* — the Spring gRPC BOM supplies them.

```toml
[versions]
springBoot = "4.0.8"
springGrpc = "1.0.3"
protobufGradle = "0.9.6"
resilience4j = "2.4.0"
testcontainers = "1.21.4"
logstash = "8.0"

[libraries]
spring-grpc-bom = { module = "org.springframework.grpc:spring-grpc-dependencies", version.ref = "springGrpc" }
spring-grpc-server = { module = "org.springframework.grpc:spring-grpc-server-spring-boot-starter" }
spring-grpc-client = { module = "org.springframework.grpc:spring-grpc-client-spring-boot-starter" }
spring-grpc-test = { module = "org.springframework.grpc:spring-grpc-test" }

grpc-stub = { module = "io.grpc:grpc-stub" }
grpc-protobuf = { module = "io.grpc:grpc-protobuf" }
grpc-services = { module = "io.grpc:grpc-services" }
grpc-testing = { module = "io.grpc:grpc-testing" }
grpc-inprocess = { module = "io.grpc:grpc-inprocess" }
protobuf-java = { module = "com.google.protobuf:protobuf-java" }
protobuf-java-util = { module = "com.google.protobuf:protobuf-java-util" }
reactor-grpc-stub = { module = "com.salesforce.servicelibs:reactor-grpc-stub" }

resilience4j = { module = "io.github.resilience4j:resilience4j-spring-boot4", version.ref = "resilience4j" }
resilience4j-reactor = { module = "io.github.resilience4j:resilience4j-reactor", version.ref = "resilience4j" }

testcontainers-bom = { module = "org.testcontainers:testcontainers-bom", version.ref = "testcontainers" }
testcontainers-postgres = { module = "org.testcontainers:postgresql" }
testcontainers-junit = { module = "org.testcontainers:junit-jupiter" }

flyway-postgres = { module = "org.flywaydb:flyway-database-postgresql" }
micrometer-tracing-otel = { module = "io.micrometer:micrometer-tracing-bridge-otel" }
otel-exporter-otlp = { module = "io.opentelemetry:opentelemetry-exporter-otlp" }
logstash-encoder = { module = "net.logstash.logback:logstash-logback-encoder", version.ref = "logstash" }

[plugins]
spring-boot = { id = "org.springframework.boot", version.ref = "springBoot" }
spring-dependency-management = { id = "io.spring.dependency-management", version = "1.1.7" }
protobuf = { id = "com.google.protobuf", version.ref = "protobufGradle" }
```

- [ ] **Step 4: Write settings and root build**

Replace `settings.gradle.kts`:

```kotlin
rootProject.name = "Gprc_SB"

include("proto", "order-service", "order-api", "testing-support")
```

Replace `build.gradle.kts`:

```kotlin
plugins {
    java
    alias(libs.plugins.spring.boot) apply false
    alias(libs.plugins.spring.dependency.management) apply false
    alias(libs.plugins.protobuf) apply false
}

allprojects {
    group = "com.example.order"
    version = "1.0.0"
    repositories { mavenCentral() }
}

subprojects {
    apply(plugin = "java")

    extensions.configure<JavaPluginExtension> {
        toolchain { languageVersion = JavaLanguageVersion.of(21) }
    }

    dependencies {
        "testImplementation"(platform("org.junit:junit-bom:5.11.4"))
        "testImplementation"("org.junit.jupiter:junit-jupiter")
        "testImplementation"("org.assertj:assertj-core:3.27.3")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        testLogging { events("passed", "failed", "skipped") }
    }
}
```

- [ ] **Step 5: Write the four module build files**

`proto/build.gradle.kts` — left minimal here; Task 2 fills in codegen:

```kotlin
plugins { java }
```

`testing-support/build.gradle.kts`:

```kotlin
plugins { java }
```

`order-service/build.gradle.kts`:

```kotlin
plugins {
    java
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

dependencies {
    implementation(platform(libs.spring.grpc.bom))
    implementation("org.springframework.boot:spring-boot-starter")
}
```

`order-api/build.gradle.kts`:

```kotlin
plugins {
    java
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

dependencies {
    implementation(platform(libs.spring.grpc.bom))
    implementation("org.springframework.boot:spring-boot-starter")
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `./gradlew :order-service:test`
Expected: PASS. If Gradle cannot find a Java 21 toolchain, install one (`brew install openjdk@21`) — do not lower the toolchain version to make the build go green.

- [ ] **Step 7: Verify the whole build wires up**

Run: `./gradlew projects build`
Expected: all four modules listed; BUILD SUCCESSFUL.

- [ ] **Step 8: Commit**

```bash
git add settings.gradle.kts build.gradle.kts gradle/libs.versions.toml \
        proto order-service order-api testing-support
git commit -m "build: multi-module Gradle skeleton with Java 21 and version catalog"
```

---

### Task 2: Proto contract and dual codegen

**Files:**
- Create: `proto/src/main/proto/order/v1/order_service.proto`
- Modify: `proto/build.gradle.kts`
- Create: `proto/buf.yaml`, `proto/buf.gen.yaml`
- Test: `proto/src/test/java/com/example/order/proto/ContractTest.java`

**Interfaces:**
- Consumes: version catalog from Task 1.
- Produces: generated Java in package `com.example.order.proto` — `OrderServiceGrpc` (blocking/async stubs), `ReactorOrderServiceGrpc` (`Mono`/`Flux` stubs), and messages `Order`, `OrderLine`, `OrderEvent`, `OrderStatus`, `CreateOrderRequest/Response`, `GetOrderRequest`, `ListOrdersRequest/Response`, `UpdateOrderStatusRequest/Response`, `StreamOrderEventsRequest`. Every later task depends on these names.

- [ ] **Step 1: Write the failing test**

Create `proto/src/test/java/com/example/order/proto/ContractTest.java`. This test earns its place: it fails loudly if codegen silently stops emitting the reactor stubs, which otherwise shows up much later as a confusing compile error in `:order-api`.

```java
package com.example.order.proto;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class ContractTest {

    @Test
    void orderStatusZeroValueIsUnspecified() {
        assertThat(OrderStatus.forNumber(0)).isEqualTo(OrderStatus.ORDER_STATUS_UNSPECIFIED);
    }

    @Test
    void moneyFieldIsInt64NotDouble() throws Exception {
        var field = Order.getDescriptor().findFieldByName("total_minor");
        assertThat(field.getJavaType().name()).isEqualTo("LONG");
    }

    @Test
    void reactorStubsAreGenerated() {
        assertThat(ReactorOrderServiceGrpc.class).isNotNull();
    }

    @Test
    void serviceExposesAllFiveRpcs() {
        var names = OrderServiceGrpc.getServiceDescriptor().getMethods().stream()
                .map(m -> m.getBareMethodName())
                .toList();
        assertThat(names).containsExactlyInAnyOrder(
                "CreateOrder", "GetOrder", "ListOrders", "UpdateOrderStatus", "StreamOrderEvents");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :proto:test`
Expected: FAIL — compilation error, `package com.example.order.proto does not exist`.

- [ ] **Step 3: Write the proto contract**

Create `proto/src/main/proto/order/v1/order_service.proto`:

```proto
syntax = "proto3";

package order.v1;

import "google/protobuf/timestamp.proto";

option java_package = "com.example.order.proto";
option java_multiple_files = true;
option java_outer_classname = "OrderServiceProto";

service OrderService {
  rpc CreateOrder(CreateOrderRequest) returns (CreateOrderResponse);
  rpc GetOrder(GetOrderRequest) returns (GetOrderResponse);
  rpc ListOrders(ListOrdersRequest) returns (ListOrdersResponse);
  rpc UpdateOrderStatus(UpdateOrderStatusRequest) returns (UpdateOrderStatusResponse);
  rpc StreamOrderEvents(StreamOrderEventsRequest) returns (stream OrderEvent);
}

enum OrderStatus {
  ORDER_STATUS_UNSPECIFIED = 0;
  ORDER_STATUS_PENDING = 1;
  ORDER_STATUS_CONFIRMED = 2;
  ORDER_STATUS_SHIPPED = 3;
  ORDER_STATUS_CANCELLED = 4;
}

message OrderLine {
  string sku = 1;
  int32 quantity = 2;
  int64 unit_price_minor = 3;
}

message Order {
  string id = 1;
  string customer_id = 2;
  repeated OrderLine lines = 3;
  int64 total_minor = 4;
  string currency = 5;
  OrderStatus status = 6;
  int64 version = 7;
  google.protobuf.Timestamp created_at = 8;
  google.protobuf.Timestamp updated_at = 9;
}

message OrderEvent {
  int64 sequence = 1;
  string order_id = 2;
  OrderStatus old_status = 3;
  OrderStatus new_status = 4;
  google.protobuf.Timestamp occurred_at = 5;
}

message CreateOrderRequest {
  string customer_id = 1;
  repeated OrderLine lines = 2;
  string currency = 3;
  // Required. Makes CreateOrder safe to retry: a replay returns the original order.
  string idempotency_key = 4;
}
message CreateOrderResponse {
  Order order = 1;
  // True when this response replayed an existing order rather than creating one.
  bool idempotent_replay = 2;
}

message GetOrderRequest { string id = 1; }
message GetOrderResponse { Order order = 1; }

message ListOrdersRequest {
  int32 page_size = 1;             // server-clamped: default 50, max 500
  string page_token = 2;           // opaque; clients must not parse it
  string customer_id = 3;          // optional filter
  OrderStatus status = 4;          // optional filter; UNSPECIFIED means "any"
}
message ListOrdersResponse {
  repeated Order orders = 1;
  string next_page_token = 2;      // empty when exhausted
}

message UpdateOrderStatusRequest {
  string id = 1;
  OrderStatus new_status = 2;
  // The version the caller last observed. A stale value is rejected with ABORTED,
  // which is what makes this RPC safe to retry.
  int64 expected_version = 3;
}
message UpdateOrderStatusResponse { Order order = 1; }

message StreamOrderEventsRequest {
  string customer_id = 1;          // optional filter
  int64 from_sequence = 2;         // resume point; 0 means "from now"
}
```

- [ ] **Step 4: Configure dual codegen**

Replace `proto/build.gradle.kts`. The `reactor` codegen plugin is what produces `Mono`/`Flux` stubs; without it `:order-api` would have to hand-roll `StreamObserver` bridging.

```kotlin
import com.google.protobuf.gradle.id

plugins {
    java
    alias(libs.plugins.protobuf)
    alias(libs.plugins.spring.dependency.management)
}

dependencyManagement {
    imports { mavenBom(libs.spring.grpc.bom.get().toString()) }
}

dependencies {
    api(libs.protobuf.java)
    api(libs.protobuf.java.util)
    api(libs.grpc.stub)
    api(libs.grpc.protobuf)
    api(libs.reactor.grpc.stub)
    // grpc-java's generated code references javax.annotation.Generated
    compileOnly("org.apache.tomcat:annotations-api:6.0.53")
}

val protobufVersion = dependencyManagement.managedVersions["com.google.protobuf:protobuf-java"]
val grpcVersion = dependencyManagement.managedVersions["io.grpc:grpc-stub"]
val reactorGrpcVersion = dependencyManagement.managedVersions["com.salesforce.servicelibs:reactor-grpc-stub"]

protobuf {
    protoc { artifact = "com.google.protobuf:protoc:$protobufVersion" }
    plugins {
        id("grpc") { artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion" }
        id("reactor") { artifact = "com.salesforce.servicelibs:reactor-grpc:$reactorGrpcVersion:jdk8@jar" }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                id("grpc")
                id("reactor")
            }
        }
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :proto:test`
Expected: PASS, all four tests.

If `reactorStubsAreGenerated` fails to compile, inspect what codegen actually produced before changing the test:
`find proto/build/generated -name "Reactor*"`. The `:jdk8@jar` classifier on the reactor plugin artifact is required — omitting it yields a non-executable artifact and silently produces no reactor output.

- [ ] **Step 6: Add buf configuration**

Create `proto/buf.yaml`:

```yaml
version: v2
modules:
  - path: src/main/proto
lint:
  use:
    - STANDARD
breaking:
  use:
    - FILE
```

Create `proto/buf.gen.yaml` (used by CI checks only; Gradle owns real codegen):

```yaml
version: v2
plugins:
  - local: protoc-gen-java
    out: build/buf-gen
```

- [ ] **Step 7: Verify lint passes**

Run: `cd proto && buf lint`
Expected: no findings. If `buf` is not installed: `brew install bufbuild/buf/buf`. If lint objects to enum value prefixes, fix the proto to satisfy `STANDARD` — do not weaken the lint config.

- [ ] **Step 8: Commit**

```bash
git add proto
git commit -m "feat(proto): order.v1 contract with grpc-java and reactor-grpc codegen"
```

---

## Phase 2 — gRPC service

### Task 3: Service bootstrap, schema, and Testcontainers harness

**Files:**
- Modify: `order-service/build.gradle.kts`
- Create: `order-service/src/main/java/com/example/order/service/OrderServiceApplication.java`
- Create: `order-service/src/main/resources/application.yml`
- Create: `order-service/src/main/resources/db/migration/V1__orders.sql`, `V2__order_events.sql`
- Create: `testing-support/build.gradle.kts` (rewrite), `testing-support/src/main/java/com/example/order/testing/PostgresFixture.java`
- Test: `order-service/src/test/java/com/example/order/service/persistence/SchemaIT.java`

**Interfaces:**
- Consumes: Task 1 catalog.
- Produces: `PostgresFixture.container()` returning a shared started `PostgreSQLContainer<?>`; `PostgresFixture.registerProperties(DynamicPropertyRegistry)`. Schema tables `orders`, `order_lines`, `order_events`.

- [ ] **Step 1: Write the failing test**

Create `order-service/src/test/java/com/example/order/service/persistence/SchemaIT.java`:

```java
package com.example.order.service.persistence;

import com.example.order.testing.PostgresFixture;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class SchemaIT {

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        PostgresFixture.registerProperties(registry);
    }

    @Autowired JdbcClient jdbc;

    @Test
    void flywayCreatesAllTables() {
        var tables = jdbc.sql("select table_name from information_schema.tables where table_schema = 'public'")
                .query(String.class).list();
        assertThat(tables).contains("orders", "order_lines", "order_events");
    }

    @Test
    void idempotencyKeyIsUniquePerCustomer() {
        var indexes = jdbc.sql("select indexname from pg_indexes where tablename = 'orders'")
                .query(String.class).list();
        assertThat(indexes).contains("orders_customer_idempotency_uk");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :order-service:test`
Expected: FAIL — cannot resolve `com.example.order.testing.PostgresFixture`.

- [ ] **Step 3: Build the shared Postgres fixture**

Replace `testing-support/build.gradle.kts`:

```kotlin
plugins {
    java
    alias(libs.plugins.spring.dependency.management)
}

dependencyManagement {
    imports {
        mavenBom("org.springframework.boot:spring-boot-dependencies:${libs.versions.springBoot.get()}")
        mavenBom(libs.testcontainers.bom.get().toString())
        mavenBom(libs.spring.grpc.bom.get().toString())
    }
}

dependencies {
    api(project(":proto"))
    api(libs.testcontainers.postgres)
    api(libs.testcontainers.junit)
    api(libs.grpc.inprocess)
    api(libs.grpc.testing)
    api("org.springframework:spring-test")
    api("org.springframework:spring-context")
}
```

Create `testing-support/src/main/java/com/example/order/testing/PostgresFixture.java`. One container is started per JVM and reused across test classes — starting a container per class turns a 30-second suite into a five-minute one.

```java
package com.example.order.testing;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.PostgreSQLContainer;

public final class PostgresFixture {

    private static final PostgreSQLContainer<?> CONTAINER =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("orders")
                    .withUsername("orders")
                    .withPassword("orders");

    static {
        CONTAINER.start();
    }

    private PostgresFixture() {
    }

    public static PostgreSQLContainer<?> container() {
        return CONTAINER;
    }

    public static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", CONTAINER::getJdbcUrl);
        registry.add("spring.datasource.username", CONTAINER::getUsername);
        registry.add("spring.datasource.password", CONTAINER::getPassword);
    }
}
```

- [ ] **Step 4: Wire the service module dependencies**

Replace `order-service/build.gradle.kts`:

```kotlin
plugins {
    java
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

dependencyManagement {
    imports { mavenBom(libs.spring.grpc.bom.get().toString()) }
}

dependencies {
    implementation(project(":proto"))
    implementation(libs.spring.grpc.server)
    implementation(libs.grpc.services)
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.flywaydb:flyway-core")
    runtimeOnly(libs.flyway.postgres)
    runtimeOnly("org.postgresql:postgresql")

    testImplementation(project(":testing-support"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation(libs.spring.grpc.test)
}
```

- [ ] **Step 5: Write the application class and configuration**

Create `OrderServiceApplication.java`:

```java
package com.example.order.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OrderServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
```

Create `order-service/src/main/resources/application.yml`:

```yaml
spring:
  application:
    name: order-service
  threads:
    virtual:
      enabled: true
  flyway:
    enabled: true
    locations: classpath:db/migration
  grpc:
    server:
      port: 9443
      host: 0.0.0.0

management:
  server:
    port: 9090
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics
  endpoint:
    health:
      probes:
        enabled: true
      show-details: always
```

- [ ] **Step 6: Write the migrations**

Create `V1__orders.sql`:

```sql
create table orders (
    id              uuid primary key,
    customer_id     text        not null,
    total_minor     bigint      not null,
    currency        char(3)     not null,
    status          text        not null,
    version         bigint      not null default 1,
    idempotency_key text,
    created_at      timestamptz not null,
    updated_at      timestamptz not null
);

-- Makes CreateOrder retry-safe: a replayed insert collides instead of duplicating.
create unique index orders_customer_idempotency_uk
    on orders (customer_id, idempotency_key)
    where idempotency_key is not null;

-- Serves the (created_at, id) keyset cursor used by ListOrders.
create index orders_created_at_id_idx on orders (created_at desc, id desc);
create index orders_customer_idx on orders (customer_id);

create table order_lines (
    id               bigserial primary key,
    order_id         uuid   not null references orders (id) on delete cascade,
    sku              text   not null,
    quantity         int    not null check (quantity > 0),
    unit_price_minor bigint not null check (unit_price_minor >= 0)
);

create index order_lines_order_idx on order_lines (order_id);
```

Create `V2__order_events.sql`:

```sql
create table order_events (
    sequence    bigserial primary key,
    order_id    uuid        not null references orders (id) on delete cascade,
    old_status  text        not null,
    new_status  text        not null,
    occurred_at timestamptz not null
);

create index order_events_order_idx on order_events (order_id);
create index order_events_sequence_idx on order_events (sequence);
```

- [ ] **Step 7: Run test to verify it passes**

Run: `./gradlew :order-service:test`
Expected: PASS. Requires a running Docker daemon.

- [ ] **Step 8: Commit**

```bash
git add order-service testing-support
git commit -m "feat(service): bootstrap gRPC service with Flyway schema and Testcontainers fixture"
```

---

### Task 4: Domain model and status transition rules

**Files:**
- Create: `order-service/src/main/java/com/example/order/service/domain/{Order,OrderLine,OrderStatus,OrderEvent,StatusTransitions}.java`
- Create: `order-service/src/main/java/com/example/order/service/domain/{OrderNotFound,IllegalStatusTransition,OrderVersionConflict,OrderValidationException,FieldViolation}.java`
- Test: `order-service/src/test/java/com/example/order/service/domain/StatusTransitionsTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces: `record Order(UUID id, String customerId, List<OrderLine> lines, long totalMinor, String currency, OrderStatus status, long version, Instant createdAt, Instant updatedAt)`; `record OrderLine(String sku, int quantity, long unitPriceMinor)`; `enum OrderStatus { PENDING, CONFIRMED, SHIPPED, CANCELLED }`; `record OrderEvent(long sequence, UUID orderId, OrderStatus oldStatus, OrderStatus newStatus, Instant occurredAt)`; `StatusTransitions.assertAllowed(OrderStatus from, OrderStatus to)`; `StatusTransitions.isAllowed(from, to)`; `record FieldViolation(String field, String description)`; `OrderValidationException.violations()`.

This package must not import Spring or protobuf. That is what lets these rules be tested in milliseconds.

- [ ] **Step 1: Write the failing test**

```java
package com.example.order.service.domain;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static com.example.order.service.domain.OrderStatus.*;

class StatusTransitionsTest {

    @Test
    void pendingMayBeConfirmedOrCancelled() {
        assertThat(StatusTransitions.isAllowed(PENDING, CONFIRMED)).isTrue();
        assertThat(StatusTransitions.isAllowed(PENDING, CANCELLED)).isTrue();
    }

    @Test
    void pendingMayNotSkipStraightToShipped() {
        assertThat(StatusTransitions.isAllowed(PENDING, SHIPPED)).isFalse();
    }

    @Test
    void shippedAndCancelledAreTerminal() {
        for (OrderStatus target : OrderStatus.values()) {
            assertThat(StatusTransitions.isAllowed(SHIPPED, target)).isFalse();
            assertThat(StatusTransitions.isAllowed(CANCELLED, target)).isFalse();
        }
    }

    @Test
    void selfTransitionIsRejected() {
        assertThat(StatusTransitions.isAllowed(PENDING, PENDING)).isFalse();
    }

    @Test
    void assertAllowedThrowsWithBothStatusesNamed() {
        assertThatThrownBy(() -> StatusTransitions.assertAllowed(CANCELLED, SHIPPED))
                .isInstanceOf(IllegalStatusTransition.class)
                .hasMessageContaining("CANCELLED")
                .hasMessageContaining("SHIPPED");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :order-service:test --tests '*StatusTransitionsTest'`
Expected: FAIL — `package com.example.order.service.domain does not exist`.

- [ ] **Step 3: Write the domain types**

```java
package com.example.order.service.domain;

public enum OrderStatus { PENDING, CONFIRMED, SHIPPED, CANCELLED }
```

```java
package com.example.order.service.domain;

public record OrderLine(String sku, int quantity, long unitPriceMinor) {
    public long lineTotalMinor() {
        return (long) quantity * unitPriceMinor;
    }
}
```

```java
package com.example.order.service.domain;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record Order(UUID id, String customerId, List<OrderLine> lines, long totalMinor,
                    String currency, OrderStatus status, long version,
                    Instant createdAt, Instant updatedAt) {

    public Order {
        lines = List.copyOf(lines);
    }

    public static long totalOf(List<OrderLine> lines) {
        return lines.stream().mapToLong(OrderLine::lineTotalMinor).sum();
    }
}
```

```java
package com.example.order.service.domain;

import java.time.Instant;
import java.util.UUID;

public record OrderEvent(long sequence, UUID orderId, OrderStatus oldStatus,
                         OrderStatus newStatus, Instant occurredAt) {
}
```

```java
package com.example.order.service.domain;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public final class StatusTransitions {

    private static final Map<OrderStatus, Set<OrderStatus>> ALLOWED = new EnumMap<>(OrderStatus.class);

    static {
        ALLOWED.put(OrderStatus.PENDING, EnumSet.of(OrderStatus.CONFIRMED, OrderStatus.CANCELLED));
        ALLOWED.put(OrderStatus.CONFIRMED, EnumSet.of(OrderStatus.SHIPPED, OrderStatus.CANCELLED));
        ALLOWED.put(OrderStatus.SHIPPED, EnumSet.noneOf(OrderStatus.class));
        ALLOWED.put(OrderStatus.CANCELLED, EnumSet.noneOf(OrderStatus.class));
    }

    private StatusTransitions() {
    }

    public static boolean isAllowed(OrderStatus from, OrderStatus to) {
        return ALLOWED.getOrDefault(from, Set.of()).contains(to);
    }

    public static void assertAllowed(OrderStatus from, OrderStatus to) {
        if (!isAllowed(from, to)) {
            throw new IllegalStatusTransition(from, to);
        }
    }
}
```

- [ ] **Step 4: Write the domain exceptions**

```java
package com.example.order.service.domain;

import java.util.UUID;

public class OrderNotFound extends RuntimeException {
    private final UUID orderId;

    public OrderNotFound(UUID orderId) {
        super("Order %s not found".formatted(orderId));
        this.orderId = orderId;
    }

    public UUID orderId() {
        return orderId;
    }
}
```

```java
package com.example.order.service.domain;

public class IllegalStatusTransition extends RuntimeException {
    private final OrderStatus from;
    private final OrderStatus to;

    public IllegalStatusTransition(OrderStatus from, OrderStatus to) {
        super("Cannot transition order from %s to %s".formatted(from, to));
        this.from = from;
        this.to = to;
    }

    public OrderStatus from() { return from; }
    public OrderStatus to() { return to; }
}
```

```java
package com.example.order.service.domain;

import java.util.UUID;

public class OrderVersionConflict extends RuntimeException {
    private final UUID orderId;
    private final long expectedVersion;
    private final long actualVersion;

    public OrderVersionConflict(UUID orderId, long expectedVersion, long actualVersion) {
        super("Order %s is at version %d, caller expected %d"
                .formatted(orderId, actualVersion, expectedVersion));
        this.orderId = orderId;
        this.expectedVersion = expectedVersion;
        this.actualVersion = actualVersion;
    }

    public UUID orderId() { return orderId; }
    public long expectedVersion() { return expectedVersion; }
    public long actualVersion() { return actualVersion; }
}
```

```java
package com.example.order.service.domain;

public record FieldViolation(String field, String description) {
}
```

```java
package com.example.order.service.domain;

import java.util.List;

public class OrderValidationException extends RuntimeException {
    private final List<FieldViolation> violations;

    public OrderValidationException(List<FieldViolation> violations) {
        super("Request has %d field violation(s)".formatted(violations.size()));
        this.violations = List.copyOf(violations);
    }

    public List<FieldViolation> violations() {
        return violations;
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :order-service:test --tests '*StatusTransitionsTest'`
Expected: PASS, all five tests.

- [ ] **Step 6: Commit**

```bash
git add order-service/src/main/java/com/example/order/service/domain order-service/src/test
git commit -m "feat(service): domain model and status transition rules"
```

---

### Task 5: Repository with keyset pagination and idempotent insert

**Files:**
- Create: `order-service/src/main/java/com/example/order/service/persistence/{OrderRepository,PageCursor,OrderPage}.java`
- Test: `order-service/src/test/java/com/example/order/service/persistence/OrderRepositoryIT.java`, `.../PageCursorTest.java`

**Interfaces:**
- Consumes: Task 4 domain types, Task 3 schema.
- Produces:
  - `PageCursor` — `record PageCursor(Instant createdAt, UUID id)` with `String encode()` and `static Optional<PageCursor> decode(String token)`.
  - `OrderPage` — `record OrderPage(List<Order> orders, String nextPageToken)`.
  - `OrderRepository` methods: `Optional<Order> findById(UUID)`, `Optional<Order> findByIdempotencyKey(String customerId, String key)`, `Order insert(Order order, String idempotencyKey)`, `OrderPage list(String customerIdOrNull, OrderStatus statusOrNull, int pageSize, PageCursor cursorOrNull)`, `Order updateStatus(UUID id, OrderStatus newStatus, long expectedVersion, Instant now)`, `long appendEvent(UUID orderId, OrderStatus old, OrderStatus next, Instant at)`, `List<OrderEvent> eventsAfter(long sequence, String customerIdOrNull, int limit)`.

- [ ] **Step 1: Write the failing cursor test**

```java
package com.example.order.service.persistence;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;

class PageCursorTest {

    @Test
    void roundTripsThroughEncoding() {
        var cursor = new PageCursor(Instant.parse("2026-09-08T12:00:00Z"), UUID.randomUUID());
        assertThat(PageCursor.decode(cursor.encode())).contains(cursor);
    }

    @Test
    void tokenIsOpaqueBase64WithNoPlaintextTimestamp() {
        var cursor = new PageCursor(Instant.parse("2026-09-08T12:00:00Z"), UUID.randomUUID());
        assertThat(cursor.encode()).doesNotContain("2026");
    }

    @Test
    void garbageTokenDecodesToEmptyRatherThanThrowing() {
        assertThat(PageCursor.decode("not-a-cursor")).isEmpty();
        assertThat(PageCursor.decode("")).isEmpty();
        assertThat(PageCursor.decode(null)).isEmpty();
    }
}
```

A malformed token must not produce a 500. It is caller-supplied input, so it becomes `INVALID_ARGUMENT` at the boundary — decoding returns `Optional.empty()` and the validator turns that into a field violation.

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :order-service:test --tests '*PageCursorTest'`
Expected: FAIL — `PageCursor` does not exist.

- [ ] **Step 3: Implement the cursor**

```java
package com.example.order.service.persistence;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;

public record PageCursor(Instant createdAt, UUID id) {

    public String encode() {
        String raw = createdAt.toEpochMilli() + ":" + id;
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    public static Optional<PageCursor> decode(String token) {
        if (token == null || token.isBlank()) {
            return Optional.empty();
        }
        try {
            String raw = new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
            int split = raw.indexOf(':');
            if (split < 0) {
                return Optional.empty();
            }
            return Optional.of(new PageCursor(
                    Instant.ofEpochMilli(Long.parseLong(raw.substring(0, split))),
                    UUID.fromString(raw.substring(split + 1))));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}
```

- [ ] **Step 4: Run cursor test to verify it passes**

Run: `./gradlew :order-service:test --tests '*PageCursorTest'`
Expected: PASS.

- [ ] **Step 5: Write the failing repository test**

```java
package com.example.order.service.persistence;

import com.example.order.service.domain.*;
import com.example.order.testing.PostgresFixture;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest
class OrderRepositoryIT {

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        PostgresFixture.registerProperties(registry);
    }

    @Autowired OrderRepository repository;

    private Order newOrder(String customerId, Instant createdAt) {
        var lines = List.of(new OrderLine("SKU-1", 2, 1250L));
        return new Order(UUID.randomUUID(), customerId, lines, Order.totalOf(lines),
                "USD", OrderStatus.PENDING, 1L, createdAt, createdAt);
    }

    @Test
    void insertAndFindByIdRoundTrips() {
        var order = newOrder("cust-1", Instant.now().truncatedTo(ChronoUnit.MILLIS));
        repository.insert(order, "key-1");

        var found = repository.findById(order.id()).orElseThrow();
        assertThat(found.customerId()).isEqualTo("cust-1");
        assertThat(found.totalMinor()).isEqualTo(2500L);
        assertThat(found.lines()).hasSize(1);
        assertThat(found.status()).isEqualTo(OrderStatus.PENDING);
    }

    @Test
    void duplicateIdempotencyKeyIsDetectableRatherThanDuplicating() {
        var first = newOrder("cust-dup", Instant.now());
        repository.insert(first, "same-key");

        assertThatThrownBy(() -> repository.insert(newOrder("cust-dup", Instant.now()), "same-key"))
                .isInstanceOf(org.springframework.dao.DuplicateKeyException.class);

        assertThat(repository.findByIdempotencyKey("cust-dup", "same-key"))
                .map(Order::id).contains(first.id());
    }

    @Test
    void sameIdempotencyKeyIsAllowedForDifferentCustomers() {
        repository.insert(newOrder("cust-a", Instant.now()), "shared-key");
        assertThatNoException()
                .isThrownBy(() -> repository.insert(newOrder("cust-b", Instant.now()), "shared-key"));
    }

    @Test
    void keysetPaginationWalksEveryRowExactlyOnce() {
        String customer = "cust-page-" + UUID.randomUUID();
        var base = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        for (int i = 0; i < 7; i++) {
            repository.insert(newOrder(customer, base.plusMillis(i)), null);
        }

        var seen = new java.util.ArrayList<UUID>();
        PageCursor cursor = null;
        do {
            var page = repository.list(customer, null, 3, cursor);
            page.orders().forEach(o -> seen.add(o.id()));
            cursor = PageCursor.decode(page.nextPageToken()).orElse(null);
        } while (cursor != null);

        assertThat(seen).hasSize(7).doesNotHaveDuplicates();
    }

    @Test
    void updateStatusBumpsVersionAndRejectsStaleWriters() {
        var order = newOrder("cust-lock", Instant.now());
        repository.insert(order, null);

        var updated = repository.updateStatus(order.id(), OrderStatus.CONFIRMED, 1L, Instant.now());
        assertThat(updated.version()).isEqualTo(2L);
        assertThat(updated.status()).isEqualTo(OrderStatus.CONFIRMED);

        assertThatThrownBy(() -> repository.updateStatus(order.id(), OrderStatus.SHIPPED, 1L, Instant.now()))
                .isInstanceOf(OrderVersionConflict.class);
    }

    @Test
    void missingOrderUpdateReportsNotFoundNotConflict() {
        assertThatThrownBy(() -> repository.updateStatus(UUID.randomUUID(), OrderStatus.CONFIRMED, 1L, Instant.now()))
                .isInstanceOf(OrderNotFound.class);
    }

    @Test
    void eventsAreReadableInSequenceOrder() {
        var order = newOrder("cust-events", Instant.now());
        repository.insert(order, null);
        long first = repository.appendEvent(order.id(), OrderStatus.PENDING, OrderStatus.CONFIRMED, Instant.now());

        var events = repository.eventsAfter(first - 1, "cust-events", 10);
        assertThat(events).extracting(OrderEvent::newStatus).contains(OrderStatus.CONFIRMED);
    }
}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `./gradlew :order-service:test --tests '*OrderRepositoryIT'`
Expected: FAIL — `OrderRepository` does not exist.

- [ ] **Step 7: Implement the repository**

```java
package com.example.order.service.persistence;

import com.example.order.service.domain.*;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.*;

@Repository
public class OrderRepository {

    private static final int MAX_PAGE_SIZE = 500;

    private final JdbcClient jdbc;

    public OrderRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Order insert(Order order, String idempotencyKey) {
        jdbc.sql("""
                insert into orders (id, customer_id, total_minor, currency, status,
                                    version, idempotency_key, created_at, updated_at)
                values (:id, :customerId, :totalMinor, :currency, :status,
                        :version, :idempotencyKey, :createdAt, :updatedAt)
                """)
                .param("id", order.id())
                .param("customerId", order.customerId())
                .param("totalMinor", order.totalMinor())
                .param("currency", order.currency())
                .param("status", order.status().name())
                .param("version", order.version())
                .param("idempotencyKey", idempotencyKey)
                .param("createdAt", java.sql.Timestamp.from(order.createdAt()))
                .param("updatedAt", java.sql.Timestamp.from(order.updatedAt()))
                .update();

        for (OrderLine line : order.lines()) {
            jdbc.sql("""
                    insert into order_lines (order_id, sku, quantity, unit_price_minor)
                    values (:orderId, :sku, :quantity, :unitPrice)
                    """)
                    .param("orderId", order.id())
                    .param("sku", line.sku())
                    .param("quantity", line.quantity())
                    .param("unitPrice", line.unitPriceMinor())
                    .update();
        }
        return order;
    }

    public Optional<Order> findById(UUID id) {
        return jdbc.sql("select * from orders where id = :id")
                .param("id", id)
                .query(this::mapOrder)
                .optional()
                .map(this::withLines);
    }

    public Optional<Order> findByIdempotencyKey(String customerId, String key) {
        if (key == null || key.isBlank()) {
            return Optional.empty();
        }
        return jdbc.sql("select * from orders where customer_id = :customerId and idempotency_key = :key")
                .param("customerId", customerId)
                .param("key", key)
                .query(this::mapOrder)
                .optional()
                .map(this::withLines);
    }

    public OrderPage list(String customerId, OrderStatus status, int pageSize, PageCursor cursor) {
        int limit = Math.clamp(pageSize <= 0 ? 50 : pageSize, 1, MAX_PAGE_SIZE);

        // Fetch one extra row: its presence is how we know another page exists,
        // without a second COUNT query that would be inconsistent under concurrent writes.
        var sql = new StringBuilder("select * from orders where 1 = 1");
        var params = new HashMap<String, Object>();
        if (customerId != null) {
            sql.append(" and customer_id = :customerId");
            params.put("customerId", customerId);
        }
        if (status != null) {
            sql.append(" and status = :status");
            params.put("status", status.name());
        }
        if (cursor != null) {
            sql.append(" and (created_at, id) < (:cursorCreatedAt, :cursorId)");
            params.put("cursorCreatedAt", java.sql.Timestamp.from(cursor.createdAt()));
            params.put("cursorId", cursor.id());
        }
        sql.append(" order by created_at desc, id desc limit :limit");
        params.put("limit", limit + 1);

        var spec = jdbc.sql(sql.toString());
        for (var entry : params.entrySet()) {
            spec = spec.param(entry.getKey(), entry.getValue());
        }

        List<Order> rows = spec.query(this::mapOrder).list();
        boolean hasMore = rows.size() > limit;
        List<Order> page = hasMore ? rows.subList(0, limit) : rows;
        List<Order> hydrated = page.stream().map(this::withLines).toList();

        String nextToken = "";
        if (hasMore && !hydrated.isEmpty()) {
            Order last = hydrated.get(hydrated.size() - 1);
            nextToken = new PageCursor(last.createdAt(), last.id()).encode();
        }
        return new OrderPage(hydrated, nextToken);
    }

    public Order updateStatus(UUID id, OrderStatus newStatus, long expectedVersion, Instant now) {
        int updated = jdbc.sql("""
                update orders
                   set status = :status, version = version + 1, updated_at = :now
                 where id = :id and version = :expectedVersion
                """)
                .param("status", newStatus.name())
                .param("now", java.sql.Timestamp.from(now))
                .param("id", id)
                .param("expectedVersion", expectedVersion)
                .update();

        if (updated == 0) {
            // Zero rows is ambiguous: the order may be absent, or present at another
            // version. Distinguish them, because they mean different things to callers.
            Order current = findById(id).orElseThrow(() -> new OrderNotFound(id));
            throw new OrderVersionConflict(id, expectedVersion, current.version());
        }
        return findById(id).orElseThrow(() -> new OrderNotFound(id));
    }

    public long appendEvent(UUID orderId, OrderStatus oldStatus, OrderStatus newStatus, Instant at) {
        return jdbc.sql("""
                insert into order_events (order_id, old_status, new_status, occurred_at)
                values (:orderId, :oldStatus, :newStatus, :at)
                returning sequence
                """)
                .param("orderId", orderId)
                .param("oldStatus", oldStatus.name())
                .param("newStatus", newStatus.name())
                .param("at", java.sql.Timestamp.from(at))
                .query(Long.class)
                .single();
    }

    public List<OrderEvent> eventsAfter(long sequence, String customerId, int limit) {
        var sql = new StringBuilder("""
                select e.* from order_events e
                  join orders o on o.id = e.order_id
                 where e.sequence > :sequence
                """);
        if (customerId != null) {
            sql.append(" and o.customer_id = :customerId");
        }
        sql.append(" order by e.sequence asc limit :limit");

        var spec = jdbc.sql(sql.toString())
                .param("sequence", sequence)
                .param("limit", limit);
        if (customerId != null) {
            spec = spec.param("customerId", customerId);
        }
        return spec.query((rs, rowNum) -> new OrderEvent(
                rs.getLong("sequence"),
                rs.getObject("order_id", UUID.class),
                OrderStatus.valueOf(rs.getString("old_status")),
                OrderStatus.valueOf(rs.getString("new_status")),
                rs.getTimestamp("occurred_at").toInstant())).list();
    }

    private Order mapOrder(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        return new Order(
                rs.getObject("id", UUID.class),
                rs.getString("customer_id"),
                List.of(),
                rs.getLong("total_minor"),
                rs.getString("currency"),
                OrderStatus.valueOf(rs.getString("status")),
                rs.getLong("version"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant());
    }

    private Order withLines(Order order) {
        List<OrderLine> lines = jdbc.sql("""
                select sku, quantity, unit_price_minor from order_lines
                 where order_id = :orderId order by id
                """)
                .param("orderId", order.id())
                .query((rs, rowNum) -> new OrderLine(
                        rs.getString("sku"),
                        rs.getInt("quantity"),
                        rs.getLong("unit_price_minor")))
                .list();

        return new Order(order.id(), order.customerId(), lines, order.totalMinor(),
                order.currency(), order.status(), order.version(),
                order.createdAt(), order.updatedAt());
    }
}
```

```java
package com.example.order.service.persistence;

import com.example.order.service.domain.Order;
import java.util.List;

public record OrderPage(List<Order> orders, String nextPageToken) {
}
```

- [ ] **Step 8: Run test to verify it passes**

Run: `./gradlew :order-service:test --tests '*OrderRepositoryIT' --tests '*PageCursorTest'`
Expected: PASS, all ten tests.

- [ ] **Step 9: Commit**

```bash
git add order-service
git commit -m "feat(service): order repository with keyset pagination and optimistic locking"
```

---

### Task 6: Protobuf mappers

**Files:**
- Create: `order-service/src/main/java/com/example/order/service/grpc/OrderProtoMapper.java`
- Test: `order-service/src/test/java/com/example/order/service/grpc/OrderProtoMapperTest.java`

**Interfaces:**
- Consumes: Task 4 domain types, Task 2 generated protos.
- Produces: `OrderProtoMapper.toProto(Order)`, `toProto(OrderEvent)`, `toDomain(OrderStatus protoStatus)` returning `null` for `ORDER_STATUS_UNSPECIFIED`, `toProto(com.example.order.service.domain.OrderStatus)`, `toDomainLines(List<com.example.order.proto.OrderLine>)`.

The `_UNSPECIFIED` to `null` mapping is deliberate: proto3 cannot distinguish "absent" from "zero", so `UNSPECIFIED` is the only honest representation of "the caller did not set a filter". Mapping it to `PENDING` would silently filter every list query.

- [ ] **Step 1: Write the failing test**

```java
package com.example.order.service.grpc;

import com.example.order.service.domain.Order;
import com.example.order.service.domain.OrderLine;
import com.example.order.service.domain.OrderStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class OrderProtoMapperTest {

    @Test
    void mapsAllScalarFieldsAndLines() {
        var lines = List.of(new OrderLine("SKU-9", 3, 500L));
        var id = UUID.randomUUID();
        var now = Instant.parse("2026-09-08T10:15:30Z");
        var order = new Order(id, "cust-7", lines, 1500L, "EUR", OrderStatus.CONFIRMED, 4L, now, now);

        var proto = OrderProtoMapper.toProto(order);

        assertThat(proto.getId()).isEqualTo(id.toString());
        assertThat(proto.getCustomerId()).isEqualTo("cust-7");
        assertThat(proto.getTotalMinor()).isEqualTo(1500L);
        assertThat(proto.getCurrency()).isEqualTo("EUR");
        assertThat(proto.getVersion()).isEqualTo(4L);
        assertThat(proto.getStatus()).isEqualTo(com.example.order.proto.OrderStatus.ORDER_STATUS_CONFIRMED);
        assertThat(proto.getCreatedAt().getSeconds()).isEqualTo(now.getEpochSecond());
        assertThat(proto.getLinesList()).singleElement()
                .satisfies(l -> {
                    assertThat(l.getSku()).isEqualTo("SKU-9");
                    assertThat(l.getQuantity()).isEqualTo(3);
                    assertThat(l.getUnitPriceMinor()).isEqualTo(500L);
                });
    }

    @Test
    void unspecifiedStatusMapsToNullMeaningNoFilter() {
        assertThat(OrderProtoMapper.toDomain(com.example.order.proto.OrderStatus.ORDER_STATUS_UNSPECIFIED)).isNull();
    }

    @Test
    void unrecognizedStatusMapsToNullRatherThanThrowing() {
        assertThat(OrderProtoMapper.toDomain(com.example.order.proto.OrderStatus.UNRECOGNIZED)).isNull();
    }

    @Test
    void everyDomainStatusHasAProtoCounterpart() {
        for (OrderStatus status : OrderStatus.values()) {
            assertThat(OrderProtoMapper.toProto(status))
                    .isNotEqualTo(com.example.order.proto.OrderStatus.ORDER_STATUS_UNSPECIFIED);
        }
    }
}
```

The last test is the one that pays off later: adding a domain status without a proto member fails here rather than shipping an order that reports `UNSPECIFIED` to every client.

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :order-service:test --tests '*OrderProtoMapperTest'`
Expected: FAIL — `OrderProtoMapper` does not exist.

- [ ] **Step 3: Implement the mapper**

```java
package com.example.order.service.grpc;

import com.example.order.service.domain.Order;
import com.example.order.service.domain.OrderEvent;
import com.example.order.service.domain.OrderLine;
import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.util.List;

public final class OrderProtoMapper {

    private OrderProtoMapper() {
    }

    public static com.example.order.proto.Order toProto(Order order) {
        return com.example.order.proto.Order.newBuilder()
                .setId(order.id().toString())
                .setCustomerId(order.customerId())
                .addAllLines(order.lines().stream().map(OrderProtoMapper::toProto).toList())
                .setTotalMinor(order.totalMinor())
                .setCurrency(order.currency())
                .setStatus(toProto(order.status()))
                .setVersion(order.version())
                .setCreatedAt(toProto(order.createdAt()))
                .setUpdatedAt(toProto(order.updatedAt()))
                .build();
    }

    public static com.example.order.proto.OrderLine toProto(OrderLine line) {
        return com.example.order.proto.OrderLine.newBuilder()
                .setSku(line.sku())
                .setQuantity(line.quantity())
                .setUnitPriceMinor(line.unitPriceMinor())
                .build();
    }

    public static com.example.order.proto.OrderEvent toProto(OrderEvent event) {
        return com.example.order.proto.OrderEvent.newBuilder()
                .setSequence(event.sequence())
                .setOrderId(event.orderId().toString())
                .setOldStatus(toProto(event.oldStatus()))
                .setNewStatus(toProto(event.newStatus()))
                .setOccurredAt(toProto(event.occurredAt()))
                .build();
    }

    public static Timestamp toProto(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    public static com.example.order.proto.OrderStatus toProto(com.example.order.service.domain.OrderStatus status) {
        return switch (status) {
            case PENDING -> com.example.order.proto.OrderStatus.ORDER_STATUS_PENDING;
            case CONFIRMED -> com.example.order.proto.OrderStatus.ORDER_STATUS_CONFIRMED;
            case SHIPPED -> com.example.order.proto.OrderStatus.ORDER_STATUS_SHIPPED;
            case CANCELLED -> com.example.order.proto.OrderStatus.ORDER_STATUS_CANCELLED;
        };
    }

    /** Returns null for UNSPECIFIED and UNRECOGNIZED, both of which mean "no status supplied". */
    public static com.example.order.service.domain.OrderStatus toDomain(com.example.order.proto.OrderStatus status) {
        return switch (status) {
            case ORDER_STATUS_PENDING -> com.example.order.service.domain.OrderStatus.PENDING;
            case ORDER_STATUS_CONFIRMED -> com.example.order.service.domain.OrderStatus.CONFIRMED;
            case ORDER_STATUS_SHIPPED -> com.example.order.service.domain.OrderStatus.SHIPPED;
            case ORDER_STATUS_CANCELLED -> com.example.order.service.domain.OrderStatus.CANCELLED;
            case ORDER_STATUS_UNSPECIFIED, UNRECOGNIZED -> null;
        };
    }

    public static List<OrderLine> toDomainLines(List<com.example.order.proto.OrderLine> lines) {
        return lines.stream()
                .map(l -> new OrderLine(l.getSku(), l.getQuantity(), l.getUnitPriceMinor()))
                .toList();
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :order-service:test --tests '*OrderProtoMapperTest'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add order-service
git commit -m "feat(service): protobuf mappers with explicit unspecified handling"
```

---

### Task 7: Application service — idempotency, locking, event emission

**Files:**
- Create: `order-service/src/main/java/com/example/order/service/application/OrderApplicationService.java`
- Test: `order-service/src/test/java/com/example/order/service/application/OrderApplicationServiceIT.java`

**Interfaces:**
- Consumes: Task 5 `OrderRepository`, Task 4 domain.
- Produces: `record CreateResult(Order order, boolean idempotentReplay)`; methods `CreateResult create(String customerId, List<OrderLine> lines, String currency, String idempotencyKey)`, `Order get(UUID id)`, `OrderPage list(String customerId, OrderStatus status, int pageSize, PageCursor cursor)`, `Order updateStatus(UUID id, OrderStatus newStatus, long expectedVersion)`, `List<OrderEvent> eventsAfter(long sequence, String customerId, int limit)`, `long latestSequence()`.

- [ ] **Step 1: Write the failing test**

```java
package com.example.order.service.application;

import com.example.order.service.domain.*;
import com.example.order.testing.PostgresFixture;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest
class OrderApplicationServiceIT {

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        PostgresFixture.registerProperties(registry);
    }

    @Autowired OrderApplicationService service;

    private static final List<OrderLine> LINES = List.of(new OrderLine("SKU-1", 2, 1000L));

    @Test
    void createComputesTotalFromLines() {
        var result = service.create("c1", LINES, "USD", UUID.randomUUID().toString());
        assertThat(result.order().totalMinor()).isEqualTo(2000L);
        assertThat(result.order().status()).isEqualTo(OrderStatus.PENDING);
        assertThat(result.idempotentReplay()).isFalse();
    }

    @Test
    void replayedIdempotencyKeyReturnsTheOriginalOrderNotADuplicate() {
        String key = UUID.randomUUID().toString();
        var first = service.create("c-idem", LINES, "USD", key);
        var second = service.create("c-idem", LINES, "USD", key);

        assertThat(second.order().id()).isEqualTo(first.order().id());
        assertThat(second.idempotentReplay()).isTrue();
    }

    @Test
    void concurrentCreatesWithSameKeyProduceExactlyOneOrder() throws Exception {
        String key = UUID.randomUUID().toString();
        try (var pool = Executors.newFixedThreadPool(8)) {
            var latch = new CountDownLatch(1);
            var futures = new java.util.ArrayList<Future<CreateResult>>();
            for (int i = 0; i < 8; i++) {
                futures.add(pool.submit(() -> {
                    latch.await();
                    return service.create("c-race", LINES, "USD", key);
                }));
            }
            latch.countDown();
            var ids = new java.util.HashSet<UUID>();
            for (var f : futures) {
                ids.add(f.get(10, TimeUnit.SECONDS).order().id());
            }
            assertThat(ids).hasSize(1);
        }
    }

    @Test
    void updateStatusWritesAnEventInTheSameTransaction() {
        var created = service.create("c-evt", LINES, "USD", UUID.randomUUID().toString()).order();
        service.updateStatus(created.id(), OrderStatus.CONFIRMED, created.version());

        var events = service.eventsAfter(0, "c-evt", 100);
        assertThat(events).extracting(OrderEvent::newStatus).contains(OrderStatus.CONFIRMED);
    }

    @Test
    void illegalTransitionIsRejectedAndWritesNoEvent() {
        var created = service.create("c-bad", LINES, "USD", UUID.randomUUID().toString()).order();
        long before = service.eventsAfter(0, "c-bad", 100).size();

        assertThatThrownBy(() -> service.updateStatus(created.id(), OrderStatus.SHIPPED, created.version()))
                .isInstanceOf(IllegalStatusTransition.class);

        assertThat(service.eventsAfter(0, "c-bad", 100)).hasSize((int) before);
    }

    @Test
    void staleVersionIsRejected() {
        var created = service.create("c-stale", LINES, "USD", UUID.randomUUID().toString()).order();
        service.updateStatus(created.id(), OrderStatus.CONFIRMED, created.version());

        assertThatThrownBy(() -> service.updateStatus(created.id(), OrderStatus.SHIPPED, created.version()))
                .isInstanceOf(OrderVersionConflict.class);
    }

    @Test
    void getMissingOrderThrowsOrderNotFound() {
        assertThatThrownBy(() -> service.get(UUID.randomUUID())).isInstanceOf(OrderNotFound.class);
    }
}
```

`concurrentCreatesWithSameKeyProduceExactlyOneOrder` is the test that actually proves idempotency. A single-threaded replay test passes even when the implementation is a check-then-insert race.

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :order-service:test --tests '*OrderApplicationServiceIT'`
Expected: FAIL — `OrderApplicationService` does not exist.

- [ ] **Step 3: Implement the application service**

```java
package com.example.order.service.application;

import com.example.order.service.domain.*;
import com.example.order.service.persistence.OrderPage;
import com.example.order.service.persistence.OrderRepository;
import com.example.order.service.persistence.PageCursor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class OrderApplicationService {

    public record CreateResult(Order order, boolean idempotentReplay) {
    }

    private final OrderRepository repository;
    private final Clock clock;

    public OrderApplicationService(OrderRepository repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
    }

    @Transactional
    public CreateResult create(String customerId, List<OrderLine> lines, String currency, String idempotencyKey) {
        // Check first for the common replay case, then rely on the unique index to
        // settle the race. Checking alone is a check-then-act bug under concurrency;
        // relying on the index alone makes the ordinary replay path throw.
        var existing = repository.findByIdempotencyKey(customerId, idempotencyKey);
        if (existing.isPresent()) {
            return new CreateResult(existing.get(), true);
        }

        Instant now = clock.instant();
        var order = new Order(UUID.randomUUID(), customerId, lines, Order.totalOf(lines),
                currency, OrderStatus.PENDING, 1L, now, now);
        try {
            return new CreateResult(repository.insert(order, idempotencyKey), false);
        } catch (DuplicateKeyException e) {
            return new CreateResult(
                    repository.findByIdempotencyKey(customerId, idempotencyKey)
                            .orElseThrow(() -> e),
                    true);
        }
    }

    @Transactional(readOnly = true)
    public Order get(UUID id) {
        return repository.findById(id).orElseThrow(() -> new OrderNotFound(id));
    }

    @Transactional(readOnly = true)
    public OrderPage list(String customerId, OrderStatus status, int pageSize, PageCursor cursor) {
        return repository.list(customerId, status, pageSize, cursor);
    }

    @Transactional
    public Order updateStatus(UUID id, OrderStatus newStatus, long expectedVersion) {
        Order current = repository.findById(id).orElseThrow(() -> new OrderNotFound(id));
        StatusTransitions.assertAllowed(current.status(), newStatus);

        Instant now = clock.instant();
        Order updated = repository.updateStatus(id, newStatus, expectedVersion, now);
        // Same transaction as the status change, so an event is never visible for a
        // change that rolled back.
        repository.appendEvent(id, current.status(), newStatus, now);
        return updated;
    }

    @Transactional(readOnly = true)
    public List<OrderEvent> eventsAfter(long sequence, String customerId, int limit) {
        return repository.eventsAfter(sequence, customerId, limit);
    }
}
```

- [ ] **Step 4: Register a Clock bean**

Create `order-service/src/main/java/com/example/order/service/config/TimeConfig.java`. Injecting `Clock` rather than calling `Instant.now()` keeps time controllable in tests.

```java
package com.example.order.service.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.time.Clock;

@Configuration
public class TimeConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :order-service:test --tests '*OrderApplicationServiceIT'`
Expected: PASS, all seven tests.

- [ ] **Step 6: Commit**

```bash
git add order-service
git commit -m "feat(service): application service with idempotent create and event emission"
```

---

### Task 8: gRPC endpoint, validation, and the error interceptor

**Files:**
- Create: `order-service/src/main/java/com/example/order/service/grpc/{OrderGrpcService,OrderRequestValidator,GrpcExceptionInterceptor}.java`
- Modify: `order-service/build.gradle.kts` (add `grpc-services` if not already present)
- Test: `order-service/src/test/java/com/example/order/service/grpc/OrderGrpcServiceIT.java`

**Interfaces:**
- Consumes: Task 7 `OrderApplicationService`, Task 6 `OrderProtoMapper`.
- Produces: `OrderGrpcService extends OrderServiceGrpc.OrderServiceImplBase` registered as a Spring bean; `GrpcExceptionInterceptor implements ServerInterceptor` mapping domain exceptions to status + trailers; `OrderRequestValidator` with `validateCreate`, `validateGet`, `validateList`, `validateUpdateStatus`, each throwing `OrderValidationException`.

**Status mapping produced here** (later tasks depend on it exactly):

| Exception | Status | Trailer detail |
|---|---|---|
| `OrderNotFound` | `NOT_FOUND` | `ErrorInfo(reason=ORDER_NOT_FOUND, metadata{orderId})` |
| `OrderValidationException` | `INVALID_ARGUMENT` | `BadRequest` with one `FieldViolation` per violation |
| `IllegalStatusTransition` | `FAILED_PRECONDITION` | `ErrorInfo(reason=ILLEGAL_STATUS_TRANSITION, metadata{from,to})` |
| `OrderVersionConflict` | `ABORTED` | `ErrorInfo(reason=VERSION_CONFLICT, metadata{expectedVersion,actualVersion})` |
| anything else | `INTERNAL` | `ErrorInfo(reason=INTERNAL, metadata{errorId})`; message is the error id only |

- [ ] **Step 1: Write the failing test**

```java
package com.example.order.service.grpc;

import com.example.order.proto.*;
import com.example.order.testing.PostgresFixture;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.BadRequest;
import com.google.rpc.ErrorInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest(properties = "spring.grpc.server.port=0")
class OrderGrpcServiceIT {

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        PostgresFixture.registerProperties(registry);
    }

    @Autowired OrderServiceGrpc.OrderServiceBlockingStub stub;

    private CreateOrderRequest validCreate(String customer, String key) {
        return CreateOrderRequest.newBuilder()
                .setCustomerId(customer)
                .setCurrency("USD")
                .setIdempotencyKey(key)
                .addLines(OrderLine.newBuilder().setSku("SKU-1").setQuantity(2).setUnitPriceMinor(1000).build())
                .build();
    }

    @Test
    void createReturnsPersistedOrder() {
        var response = stub.createOrder(validCreate("g-c1", UUID.randomUUID().toString()));
        assertThat(response.getOrder().getTotalMinor()).isEqualTo(2000L);
        assertThat(response.getOrder().getStatus()).isEqualTo(OrderStatus.ORDER_STATUS_PENDING);
        assertThat(response.getIdempotentReplay()).isFalse();
    }

    @Test
    void replayFlagsIdempotentReplay() {
        String key = UUID.randomUUID().toString();
        stub.createOrder(validCreate("g-c2", key));
        assertThat(stub.createOrder(validCreate("g-c2", key)).getIdempotentReplay()).isTrue();
    }

    @Test
    void missingOrderIsNotFoundWithErrorInfoReason() {
        var thrown = catchThrowableOfType(
                () -> stub.getOrder(GetOrderRequest.newBuilder().setId(UUID.randomUUID().toString()).build()),
                StatusRuntimeException.class);

        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
        assertThat(errorInfo(thrown).getReason()).isEqualTo("ORDER_NOT_FOUND");
    }

    @Test
    void invalidRequestCarriesFieldViolations() {
        var bad = CreateOrderRequest.newBuilder().setCustomerId("").setCurrency("US").build();

        var thrown = catchThrowableOfType(() -> stub.createOrder(bad), StatusRuntimeException.class);
        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);

        var violations = badRequest(thrown).getFieldViolationsList();
        assertThat(violations).extracting(BadRequest.FieldViolation::getField)
                .contains("customer_id", "currency", "lines", "idempotency_key");
    }

    @Test
    void malformedPageTokenIsInvalidArgumentNotInternal() {
        var thrown = catchThrowableOfType(
                () -> stub.listOrders(ListOrdersRequest.newBuilder().setPageToken("!!!not-base64!!!").build()),
                StatusRuntimeException.class);
        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    @Test
    void illegalTransitionIsFailedPrecondition() {
        var created = stub.createOrder(validCreate("g-c3", UUID.randomUUID().toString())).getOrder();
        var thrown = catchThrowableOfType(() -> stub.updateOrderStatus(
                UpdateOrderStatusRequest.newBuilder()
                        .setId(created.getId())
                        .setNewStatus(OrderStatus.ORDER_STATUS_SHIPPED)
                        .setExpectedVersion(created.getVersion()).build()),
                StatusRuntimeException.class);

        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(errorInfo(thrown).getReason()).isEqualTo("ILLEGAL_STATUS_TRANSITION");
    }

    @Test
    void staleVersionIsAborted() {
        var created = stub.createOrder(validCreate("g-c4", UUID.randomUUID().toString())).getOrder();
        stub.updateOrderStatus(UpdateOrderStatusRequest.newBuilder()
                .setId(created.getId()).setNewStatus(OrderStatus.ORDER_STATUS_CONFIRMED)
                .setExpectedVersion(created.getVersion()).build());

        var thrown = catchThrowableOfType(() -> stub.updateOrderStatus(
                UpdateOrderStatusRequest.newBuilder()
                        .setId(created.getId()).setNewStatus(OrderStatus.ORDER_STATUS_SHIPPED)
                        .setExpectedVersion(created.getVersion()).build()),
                StatusRuntimeException.class);

        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
        assertThat(errorInfo(thrown).getReason()).isEqualTo("VERSION_CONFLICT");
    }

    @Test
    void listPaginatesWithOpaqueTokens() {
        String customer = "g-page-" + UUID.randomUUID();
        for (int i = 0; i < 5; i++) {
            stub.createOrder(validCreate(customer, UUID.randomUUID().toString()));
        }
        var first = stub.listOrders(ListOrdersRequest.newBuilder()
                .setCustomerId(customer).setPageSize(2).build());

        assertThat(first.getOrdersList()).hasSize(2);
        assertThat(first.getNextPageToken()).isNotEmpty();

        var second = stub.listOrders(ListOrdersRequest.newBuilder()
                .setCustomerId(customer).setPageSize(2).setPageToken(first.getNextPageToken()).build());
        assertThat(second.getOrdersList()).hasSize(2);
        assertThat(second.getOrders(0).getId()).isNotEqualTo(first.getOrders(0).getId());
    }

    private ErrorInfo errorInfo(StatusRuntimeException e) {
        return unpack(e, ErrorInfo.class);
    }

    private BadRequest badRequest(StatusRuntimeException e) {
        return unpack(e, BadRequest.class);
    }

    private <T extends com.google.protobuf.Message> T unpack(StatusRuntimeException e, Class<T> type) {
        var status = StatusProto.fromThrowable(e);
        assertThat(status).isNotNull();
        for (var any : status.getDetailsList()) {
            if (any.is(type)) {
                try {
                    return any.unpack(type);
                } catch (InvalidProtocolBufferException ex) {
                    throw new AssertionError(ex);
                }
            }
        }
        throw new AssertionError("No " + type.getSimpleName() + " detail in status: " + status);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :order-service:test --tests '*OrderGrpcServiceIT'`
Expected: FAIL — no `OrderServiceBlockingStub` bean, `OrderGrpcService` does not exist.

- [ ] **Step 3: Add the protobuf status dependency**

Add to `order-service/build.gradle.kts` dependencies:

```kotlin
    implementation("com.google.api.grpc:proto-google-common-protos")
```

This supplies `ErrorInfo` and `BadRequest`; `io.grpc:grpc-protobuf` (already present via `:proto`) supplies `StatusProto`.

- [ ] **Step 4: Write the validator**

```java
package com.example.order.service.grpc;

import com.example.order.proto.*;
import com.example.order.service.domain.FieldViolation;
import com.example.order.service.domain.OrderValidationException;
import com.example.order.service.persistence.PageCursor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Component
public class OrderRequestValidator {

    public void validateCreate(CreateOrderRequest request) {
        var violations = new ArrayList<FieldViolation>();
        if (request.getCustomerId().isBlank()) {
            violations.add(new FieldViolation("customer_id", "must not be blank"));
        }
        if (request.getIdempotencyKey().isBlank()) {
            violations.add(new FieldViolation("idempotency_key", "must not be blank"));
        }
        if (request.getCurrency().length() != 3) {
            violations.add(new FieldViolation("currency", "must be a 3-letter ISO-4217 code"));
        }
        if (request.getLinesCount() == 0) {
            violations.add(new FieldViolation("lines", "must contain at least one line"));
        }
        for (int i = 0; i < request.getLinesCount(); i++) {
            OrderLine line = request.getLines(i);
            if (line.getSku().isBlank()) {
                violations.add(new FieldViolation("lines[%d].sku".formatted(i), "must not be blank"));
            }
            if (line.getQuantity() <= 0) {
                violations.add(new FieldViolation("lines[%d].quantity".formatted(i), "must be greater than zero"));
            }
            if (line.getUnitPriceMinor() < 0) {
                violations.add(new FieldViolation("lines[%d].unit_price_minor".formatted(i), "must not be negative"));
            }
        }
        throwIfAny(violations);
    }

    public UUID validateGet(GetOrderRequest request) {
        return parseId(request.getId(), "id");
    }

    public PageCursor validateList(ListOrdersRequest request) {
        var violations = new ArrayList<FieldViolation>();
        if (request.getPageSize() < 0) {
            violations.add(new FieldViolation("page_size", "must not be negative"));
        }
        PageCursor cursor = null;
        if (!request.getPageToken().isBlank()) {
            cursor = PageCursor.decode(request.getPageToken()).orElse(null);
            if (cursor == null) {
                violations.add(new FieldViolation("page_token", "is not a valid page token"));
            }
        }
        throwIfAny(violations);
        return cursor;
    }

    public UUID validateUpdateStatus(UpdateOrderStatusRequest request) {
        var violations = new ArrayList<FieldViolation>();
        UUID id = null;
        try {
            id = UUID.fromString(request.getId());
        } catch (IllegalArgumentException e) {
            violations.add(new FieldViolation("id", "must be a UUID"));
        }
        if (request.getNewStatus() == OrderStatus.ORDER_STATUS_UNSPECIFIED
                || request.getNewStatus() == OrderStatus.UNRECOGNIZED) {
            violations.add(new FieldViolation("new_status", "must be a known, specified status"));
        }
        if (request.getExpectedVersion() <= 0) {
            violations.add(new FieldViolation("expected_version", "must be greater than zero"));
        }
        throwIfAny(violations);
        return id;
    }

    private UUID parseId(String raw, String field) {
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            throw new OrderValidationException(List.of(new FieldViolation(field, "must be a UUID")));
        }
    }

    private void throwIfAny(List<FieldViolation> violations) {
        if (!violations.isEmpty()) {
            throw new OrderValidationException(violations);
        }
    }
}
```

- [ ] **Step 5: Write the exception interceptor**

```java
package com.example.order.service.grpc;

import com.example.order.service.domain.*;
import com.google.protobuf.Any;
import com.google.rpc.BadRequest;
import com.google.rpc.ErrorInfo;
import io.grpc.*;
import io.grpc.protobuf.StatusProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;

/**
 * Translates domain exceptions into gRPC statuses with machine-readable trailers.
 * Wrapping the listener (rather than only the call) is required: exceptions thrown
 * while reading the request message surface through onHalfClose, not through the
 * initial call handler.
 */
@Component
public class GrpcExceptionInterceptor implements ServerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(GrpcExceptionInterceptor.class);
    private static final String DOMAIN = "order.v1";

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        ServerCall.Listener<ReqT> delegate = next.startCall(call, headers);

        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
            @Override
            public void onHalfClose() {
                try {
                    super.onHalfClose();
                } catch (RuntimeException e) {
                    closeWith(call, e);
                }
            }

            @Override
            public void onMessage(ReqT message) {
                try {
                    super.onMessage(message);
                } catch (RuntimeException e) {
                    closeWith(call, e);
                }
            }
        };
    }

    private <ReqT, RespT> void closeWith(ServerCall<ReqT, RespT> call, RuntimeException e) {
        var status = toStatus(e);
        call.close(Status.fromCodeValue(status.getCode()).withDescription(status.getMessage()),
                trailersFor(status));
    }

    public com.google.rpc.Status toStatus(Throwable e) {
        if (e instanceof OrderNotFound notFound) {
            return build(Status.Code.NOT_FOUND, notFound.getMessage(),
                    errorInfo("ORDER_NOT_FOUND", Map.of("orderId", notFound.orderId().toString())));
        }
        if (e instanceof OrderValidationException validation) {
            var badRequest = BadRequest.newBuilder();
            validation.violations().forEach(v -> badRequest.addFieldViolations(
                    BadRequest.FieldViolation.newBuilder()
                            .setField(v.field()).setDescription(v.description()).build()));
            return com.google.rpc.Status.newBuilder()
                    .setCode(Status.Code.INVALID_ARGUMENT.value())
                    .setMessage(validation.getMessage())
                    .addDetails(Any.pack(errorInfoMessage("INVALID_ARGUMENT", Map.of())))
                    .addDetails(Any.pack(badRequest.build()))
                    .build();
        }
        if (e instanceof IllegalStatusTransition transition) {
            return build(Status.Code.FAILED_PRECONDITION, transition.getMessage(),
                    errorInfo("ILLEGAL_STATUS_TRANSITION",
                            Map.of("from", transition.from().name(), "to", transition.to().name())));
        }
        if (e instanceof OrderVersionConflict conflict) {
            return build(Status.Code.ABORTED, conflict.getMessage(),
                    errorInfo("VERSION_CONFLICT", Map.of(
                            "orderId", conflict.orderId().toString(),
                            "expectedVersion", String.valueOf(conflict.expectedVersion()),
                            "actualVersion", String.valueOf(conflict.actualVersion()))));
        }

        // Unexpected: log everything, return an opaque id. Internal detail must not
        // reach the caller, but support needs a handle to correlate against the logs.
        String errorId = UUID.randomUUID().toString();
        log.error("Unhandled exception, errorId={}", errorId, e);
        return build(Status.Code.INTERNAL, "Internal error. errorId=" + errorId,
                errorInfo("INTERNAL", Map.of("errorId", errorId)));
    }

    private com.google.rpc.Status build(Status.Code code, String message, Any detail) {
        return com.google.rpc.Status.newBuilder()
                .setCode(code.value()).setMessage(message).addDetails(detail).build();
    }

    private Any errorInfo(String reason, Map<String, String> metadata) {
        return Any.pack(errorInfoMessage(reason, metadata));
    }

    private ErrorInfo errorInfoMessage(String reason, Map<String, String> metadata) {
        return ErrorInfo.newBuilder().setReason(reason).setDomain(DOMAIN).putAllMetadata(metadata).build();
    }

    private Metadata trailersFor(com.google.rpc.Status status) {
        return StatusProto.toStatusRuntimeException(status).getTrailers();
    }
}
```

- [ ] **Step 6: Write the gRPC endpoint**

```java
package com.example.order.service.grpc;

import com.example.order.proto.*;
import com.example.order.service.application.OrderApplicationService;
import io.grpc.stub.StreamObserver;
import org.springframework.grpc.server.service.GrpcService;

@GrpcService
public class OrderGrpcService extends OrderServiceGrpc.OrderServiceImplBase {

    private final OrderApplicationService service;
    private final OrderRequestValidator validator;

    public OrderGrpcService(OrderApplicationService service, OrderRequestValidator validator) {
        this.service = service;
        this.validator = validator;
    }

    @Override
    public void createOrder(CreateOrderRequest request, StreamObserver<CreateOrderResponse> observer) {
        validator.validateCreate(request);
        var result = service.create(request.getCustomerId(),
                OrderProtoMapper.toDomainLines(request.getLinesList()),
                request.getCurrency(),
                request.getIdempotencyKey());

        observer.onNext(CreateOrderResponse.newBuilder()
                .setOrder(OrderProtoMapper.toProto(result.order()))
                .setIdempotentReplay(result.idempotentReplay())
                .build());
        observer.onCompleted();
    }

    @Override
    public void getOrder(GetOrderRequest request, StreamObserver<GetOrderResponse> observer) {
        var id = validator.validateGet(request);
        observer.onNext(GetOrderResponse.newBuilder()
                .setOrder(OrderProtoMapper.toProto(service.get(id))).build());
        observer.onCompleted();
    }

    @Override
    public void listOrders(ListOrdersRequest request, StreamObserver<ListOrdersResponse> observer) {
        var cursor = validator.validateList(request);
        var page = service.list(
                request.getCustomerId().isBlank() ? null : request.getCustomerId(),
                OrderProtoMapper.toDomain(request.getStatus()),
                request.getPageSize(),
                cursor);

        observer.onNext(ListOrdersResponse.newBuilder()
                .addAllOrders(page.orders().stream().map(OrderProtoMapper::toProto).toList())
                .setNextPageToken(page.nextPageToken())
                .build());
        observer.onCompleted();
    }

    @Override
    public void updateOrderStatus(UpdateOrderStatusRequest request,
                                  StreamObserver<UpdateOrderStatusResponse> observer) {
        var id = validator.validateUpdateStatus(request);
        var updated = service.updateStatus(id,
                OrderProtoMapper.toDomain(request.getNewStatus()),
                request.getExpectedVersion());

        observer.onNext(UpdateOrderStatusResponse.newBuilder()
                .setOrder(OrderProtoMapper.toProto(updated)).build());
        observer.onCompleted();
    }
}
```

- [ ] **Step 7: Register the interceptor globally**

Create `order-service/src/main/java/com/example/order/service/config/GrpcServerConfig.java`:

```java
package com.example.order.service.config;

import com.example.order.service.grpc.GrpcExceptionInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.grpc.server.GlobalServerInterceptor;

@Configuration
public class GrpcServerConfig {

    @Bean
    @GlobalServerInterceptor
    @Order(Ordered.HIGHEST_PRECEDENCE + 100)
    public GrpcExceptionInterceptor grpcExceptionInterceptor() {
        return new GrpcExceptionInterceptor();
    }
}
```

Remove the `@Component` annotation from `GrpcExceptionInterceptor` so it is contributed once, here, with an explicit order.

- [ ] **Step 8: Run test to verify it passes**

Run: `./gradlew :order-service:test --tests '*OrderGrpcServiceIT'`
Expected: PASS, all eight tests.

If the `OrderServiceBlockingStub` bean is not injectable, add a test configuration exposing it from the Spring gRPC test channel factory rather than constructing a raw `ManagedChannel` — the point is to exercise the same interceptor chain production uses.

- [ ] **Step 9: Commit**

```bash
git add order-service
git commit -m "feat(service): gRPC endpoint with validation and rich error trailers"
```

---

### Task 9: Server-streaming order events

**Files:**
- Modify: `order-service/src/main/java/com/example/order/service/grpc/OrderGrpcService.java`
- Create: `order-service/src/main/java/com/example/order/service/application/OrderEventPublisher.java`
- Test: `order-service/src/test/java/com/example/order/service/grpc/OrderEventStreamIT.java`

**Interfaces:**
- Consumes: Task 7 `OrderApplicationService.eventsAfter`.
- Produces: `OrderEventPublisher.poll(long fromSequence, String customerId, int limit)`; `OrderGrpcService.streamOrderEvents` honouring `from_sequence`, the call deadline, and client cancellation.

- [ ] **Step 1: Write the failing test**

```java
package com.example.order.service.grpc;

import com.example.order.proto.*;
import com.example.order.testing.PostgresFixture;
import io.grpc.Context;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "spring.grpc.server.port=0")
class OrderEventStreamIT {

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        PostgresFixture.registerProperties(registry);
    }

    @Autowired OrderServiceGrpc.OrderServiceBlockingStub stub;

    @Test
    void replaysHistoricalEventsFromSequence() {
        String customer = "stream-" + UUID.randomUUID();
        var created = stub.createOrder(CreateOrderRequest.newBuilder()
                .setCustomerId(customer).setCurrency("USD")
                .setIdempotencyKey(UUID.randomUUID().toString())
                .addLines(OrderLine.newBuilder().setSku("S").setQuantity(1).setUnitPriceMinor(100).build())
                .build()).getOrder();

        stub.updateOrderStatus(UpdateOrderStatusRequest.newBuilder()
                .setId(created.getId()).setNewStatus(OrderStatus.ORDER_STATUS_CONFIRMED)
                .setExpectedVersion(created.getVersion()).build());

        var events = stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                .streamOrderEvents(StreamOrderEventsRequest.newBuilder()
                        .setCustomerId(customer).setFromSequence(1).build());

        var first = events.next();
        assertThat(first.getOrderId()).isEqualTo(created.getId());
        assertThat(first.getNewStatus()).isEqualTo(OrderStatus.ORDER_STATUS_CONFIRMED);
        assertThat(first.getOldStatus()).isEqualTo(OrderStatus.ORDER_STATUS_PENDING);
    }

    @Test
    void streamEndsWhenTheDeadlineExpiresRatherThanHangingForever() {
        String customer = "quiet-" + UUID.randomUUID();
        long start = System.nanoTime();

        var events = stub.withDeadlineAfter(2, TimeUnit.SECONDS)
                .streamOrderEvents(StreamOrderEventsRequest.newBuilder()
                        .setCustomerId(customer).setFromSequence(1).build());

        assertThat(events.hasNext()).isFalse();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMs).isLessThan(6_000);
    }
}
```

The second test is the one that catches the classic streaming bug: a polling loop that ignores the deadline and holds a thread and a connection open forever.

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :order-service:test --tests '*OrderEventStreamIT'`
Expected: FAIL — `streamOrderEvents` is unimplemented, so the server returns `UNIMPLEMENTED`.

- [ ] **Step 3: Write the publisher**

```java
package com.example.order.service.application;

import com.example.order.service.domain.OrderEvent;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Polls the order_events table. Polling is deliberate: events are written in the
 * same transaction as the status change, so the table is the single source of
 * truth and no event can be observed for a change that rolled back. An in-memory
 * fan-out would lose events on restart and diverge across replicas.
 */
@Component
public class OrderEventPublisher {

    private final OrderApplicationService service;

    public OrderEventPublisher(OrderApplicationService service) {
        this.service = service;
    }

    public List<OrderEvent> poll(long fromSequence, String customerId, int limit) {
        return service.eventsAfter(fromSequence, customerId, limit);
    }
}
```

- [ ] **Step 4: Implement the streaming RPC**

Add to `OrderGrpcService`. First extend its constructor to take and store the publisher:

```java
    private final OrderEventPublisher publisher;

    public OrderGrpcService(OrderApplicationService service, OrderRequestValidator validator,
                            OrderEventPublisher publisher) {
        this.service = service;
        this.validator = validator;
        this.publisher = publisher;
    }
```

Then add the streaming method:

```java
    private static final int STREAM_BATCH = 100;
    private static final long POLL_INTERVAL_MS = 500L;

    @Override
    public void streamOrderEvents(StreamOrderEventsRequest request, StreamObserver<OrderEvent> observer) {
        String customerId = request.getCustomerId().isBlank() ? null : request.getCustomerId();
        long cursor = Math.max(0L, request.getFromSequence() - 1);

        var serverObserver = (io.grpc.stub.ServerCallStreamObserver<OrderEvent>) observer;
        var context = io.grpc.Context.current();

        while (!context.isCancelled() && !serverObserver.isCancelled()) {
            // Respect the caller's deadline: finishing cleanly beats being killed
            // mid-write, and it releases the thread and connection promptly.
            var deadline = context.getDeadline();
            if (deadline != null && deadline.isExpired()) {
                break;
            }

            var batch = publisher.poll(cursor, customerId, STREAM_BATCH);
            for (var event : batch) {
                // Honour reactive-style backpressure: pause while the transport
                // buffer is full instead of piling messages into memory.
                while (!serverObserver.isReady() && !serverObserver.isCancelled()) {
                    if (!sleepQuietly(50L)) {
                        return;
                    }
                }
                if (serverObserver.isCancelled()) {
                    return;
                }
                observer.onNext(OrderProtoMapper.toProto(event));
                cursor = event.sequence();
            }

            if (batch.isEmpty() && !sleepQuietly(POLL_INTERVAL_MS)) {
                return;
            }
        }
        observer.onCompleted();
    }

    private boolean sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :order-service:test --tests '*OrderEventStreamIT'`
Expected: PASS, both tests.

- [ ] **Step 6: Run the full service suite**

Run: `./gradlew :order-service:test`
Expected: PASS — everything from Tasks 3 through 9.

- [ ] **Step 7: Commit**

```bash
git add order-service
git commit -m "feat(service): server-streaming order events with deadline and backpressure handling"
```

---

## Phase 3 — WebFlux REST edge

### Task 10: gRPC client module and reactive bridging

**Files:**
- Modify: `order-api/build.gradle.kts`
- Create: `order-api/src/main/java/com/example/order/api/OrderApiApplication.java`
- Create: `order-api/src/main/java/com/example/order/api/config/GrpcClientConfig.java`
- Create: `order-api/src/main/java/com/example/order/api/client/{OrderGrpcClient,GrpcStatusTranslator}.java`
- Create: `order-api/src/main/java/com/example/order/api/client/exception/{RemoteOrderNotFound,RemoteValidationFailed,RemoteIllegalTransition,RemoteVersionConflict,RemoteUnavailable,RemoteTimeout,RemoteInternal,RemoteFieldViolation}.java`
- Create: `order-api/src/main/resources/application.yml`
- Create: `testing-support/src/main/java/com/example/order/testing/InProcessGrpc.java`
- Test: `order-api/src/test/java/com/example/order/api/client/GrpcStatusTranslatorTest.java`, `.../OrderGrpcClientTest.java`

**Interfaces:**
- Consumes: Task 2 `ReactorOrderServiceGrpc`, Task 8's exact status/trailer contract.
- Produces:
  - `InProcessGrpc.start(BindableService service)` returning a `record Harness(ManagedChannel channel, Server server) implements AutoCloseable`.
  - `GrpcStatusTranslator.translate(Throwable)` mapping `StatusRuntimeException` to the `Remote*` exceptions; non-gRPC throwables pass through unchanged.
  - `OrderGrpcClient` methods returning reactive types: `Mono<CreateOrderResponse> create(...)`, `Mono<Order> get(UUID)`, `Mono<ListOrdersResponse> list(...)`, `Mono<Order> updateStatus(...)`, `Flux<OrderEvent> streamEvents(String customerId, long fromSequence)`.
  - `record RemoteFieldViolation(String field, String description)`; `RemoteValidationFailed.violations()`.

- [ ] **Step 1: Write the failing translator test**

```java
package com.example.order.api.client;

import com.example.order.api.client.exception.*;
import com.google.protobuf.Any;
import com.google.rpc.BadRequest;
import com.google.rpc.ErrorInfo;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GrpcStatusTranslatorTest {

    private static io.grpc.StatusRuntimeException remote(Status.Code code, String reason, Any... extra) {
        var builder = com.google.rpc.Status.newBuilder()
                .setCode(code.value())
                .setMessage("boom")
                .addDetails(Any.pack(ErrorInfo.newBuilder()
                        .setReason(reason).setDomain("order.v1").build()));
        for (Any any : extra) {
            builder.addDetails(any);
        }
        return StatusProto.toStatusRuntimeException(builder.build());
    }

    @Test
    void notFoundBecomesRemoteOrderNotFound() {
        assertThat(GrpcStatusTranslator.translate(remote(Status.Code.NOT_FOUND, "ORDER_NOT_FOUND")))
                .isInstanceOf(RemoteOrderNotFound.class);
    }

    @Test
    void invalidArgumentCarriesFieldViolationsThrough() {
        var badRequest = Any.pack(BadRequest.newBuilder()
                .addFieldViolations(BadRequest.FieldViolation.newBuilder()
                        .setField("currency").setDescription("must be 3 letters").build())
                .build());

        var translated = GrpcStatusTranslator.translate(
                remote(Status.Code.INVALID_ARGUMENT, "INVALID_ARGUMENT", badRequest));

        assertThat(translated).isInstanceOf(RemoteValidationFailed.class);
        assertThat(((RemoteValidationFailed) translated).violations())
                .extracting(RemoteFieldViolation::field).containsExactly("currency");
    }

    @Test
    void abortedBecomesVersionConflictAndFailedPreconditionBecomesIllegalTransition() {
        assertThat(GrpcStatusTranslator.translate(remote(Status.Code.ABORTED, "VERSION_CONFLICT")))
                .isInstanceOf(RemoteVersionConflict.class);
        assertThat(GrpcStatusTranslator.translate(remote(Status.Code.FAILED_PRECONDITION, "ILLEGAL_STATUS_TRANSITION")))
                .isInstanceOf(RemoteIllegalTransition.class);
    }

    @Test
    void transportFailuresAreDistinguishedFromServerErrors() {
        assertThat(GrpcStatusTranslator.translate(Status.UNAVAILABLE.asRuntimeException()))
                .isInstanceOf(RemoteUnavailable.class);
        assertThat(GrpcStatusTranslator.translate(Status.DEADLINE_EXCEEDED.asRuntimeException()))
                .isInstanceOf(RemoteTimeout.class);
        assertThat(GrpcStatusTranslator.translate(Status.INTERNAL.asRuntimeException()))
                .isInstanceOf(RemoteInternal.class);
    }

    @Test
    void nonGrpcThrowablesPassThroughUnchanged() {
        var original = new IllegalStateException("local");
        assertThat(GrpcStatusTranslator.translate(original)).isSameAs(original);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :order-api:test`
Expected: FAIL — `GrpcStatusTranslator` does not exist.

- [ ] **Step 3: Wire the module**

Replace `order-api/build.gradle.kts`:

```kotlin
plugins {
    java
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

dependencyManagement {
    imports { mavenBom(libs.spring.grpc.bom.get().toString()) }
}

dependencies {
    implementation(project(":proto"))
    implementation(libs.spring.grpc.client)
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("com.google.api.grpc:proto-google-common-protos")

    testImplementation(project(":testing-support"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("io.projectreactor:reactor-test")
}
```

- [ ] **Step 4: Write the exception hierarchy**

Create one file per type in `com.example.order.api.client.exception`:

```java
package com.example.order.api.client.exception;

public class RemoteOrderException extends RuntimeException {
    public RemoteOrderException(String message) { super(message); }
    public RemoteOrderException(String message, Throwable cause) { super(message, cause); }
}
```

```java
package com.example.order.api.client.exception;

public class RemoteOrderNotFound extends RemoteOrderException {
    public RemoteOrderNotFound(String message) { super(message); }
}
```

```java
package com.example.order.api.client.exception;

public record RemoteFieldViolation(String field, String description) {
}
```

```java
package com.example.order.api.client.exception;

import java.util.List;

public class RemoteValidationFailed extends RemoteOrderException {
    private final List<RemoteFieldViolation> violations;

    public RemoteValidationFailed(String message, List<RemoteFieldViolation> violations) {
        super(message);
        this.violations = List.copyOf(violations);
    }

    public List<RemoteFieldViolation> violations() { return violations; }
}
```

```java
package com.example.order.api.client.exception;

public class RemoteIllegalTransition extends RemoteOrderException {
    public RemoteIllegalTransition(String message) { super(message); }
}
```

```java
package com.example.order.api.client.exception;

public class RemoteVersionConflict extends RemoteOrderException {
    public RemoteVersionConflict(String message) { super(message); }
}
```

```java
package com.example.order.api.client.exception;

public class RemoteUnavailable extends RemoteOrderException {
    public RemoteUnavailable(String message, Throwable cause) { super(message, cause); }
}
```

```java
package com.example.order.api.client.exception;

public class RemoteTimeout extends RemoteOrderException {
    public RemoteTimeout(String message, Throwable cause) { super(message, cause); }
}
```

```java
package com.example.order.api.client.exception;

public class RemoteInternal extends RemoteOrderException {
    private final String errorId;

    public RemoteInternal(String message, String errorId, Throwable cause) {
        super(message, cause);
        this.errorId = errorId;
    }

    public String errorId() { return errorId; }
}
```

- [ ] **Step 5: Write the translator**

```java
package com.example.order.api.client;

import com.example.order.api.client.exception.*;
import com.google.rpc.BadRequest;
import com.google.rpc.ErrorInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;

import java.util.List;

public final class GrpcStatusTranslator {

    private GrpcStatusTranslator() {
    }

    public static Throwable translate(Throwable throwable) {
        if (!(throwable instanceof StatusRuntimeException sre)) {
            return throwable;
        }
        var rpcStatus = StatusProto.fromThrowable(sre);
        String message = sre.getStatus().getDescription() == null
                ? sre.getStatus().getCode().name()
                : sre.getStatus().getDescription();

        return switch (sre.getStatus().getCode()) {
            case NOT_FOUND -> new RemoteOrderNotFound(message);
            case INVALID_ARGUMENT -> new RemoteValidationFailed(message, fieldViolations(rpcStatus));
            case FAILED_PRECONDITION -> new RemoteIllegalTransition(message);
            case ABORTED -> new RemoteVersionConflict(message);
            case UNAVAILABLE -> new RemoteUnavailable(message, sre);
            case DEADLINE_EXCEEDED -> new RemoteTimeout(message, sre);
            default -> new RemoteInternal(message, errorId(rpcStatus), sre);
        };
    }

    private static List<RemoteFieldViolation> fieldViolations(com.google.rpc.Status status) {
        if (status == null) {
            return List.of();
        }
        for (var any : status.getDetailsList()) {
            if (any.is(BadRequest.class)) {
                try {
                    return any.unpack(BadRequest.class).getFieldViolationsList().stream()
                            .map(v -> new RemoteFieldViolation(v.getField(), v.getDescription()))
                            .toList();
                } catch (Exception e) {
                    return List.of();
                }
            }
        }
        return List.of();
    }

    private static String errorId(com.google.rpc.Status status) {
        if (status == null) {
            return null;
        }
        for (var any : status.getDetailsList()) {
            if (any.is(ErrorInfo.class)) {
                try {
                    return any.unpack(ErrorInfo.class).getMetadataMap().get("errorId");
                } catch (Exception e) {
                    return null;
                }
            }
        }
        return null;
    }
}
```

- [ ] **Step 6: Run translator test to verify it passes**

Run: `./gradlew :order-api:test --tests '*GrpcStatusTranslatorTest'`
Expected: PASS.

- [ ] **Step 7: Write the in-process harness and the failing client test**

Create `testing-support/src/main/java/com/example/order/testing/InProcessGrpc.java`:

```java
package com.example.order.testing;

import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/** In-process gRPC server plus channel: no ports, no TLS, millisecond startup. */
public final class InProcessGrpc {

    public record Harness(ManagedChannel channel, Server server) implements AutoCloseable {
        @Override
        public void close() {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    private InProcessGrpc() {
    }

    public static Harness start(BindableService service, ServerInterceptor... interceptors) {
        String name = "inproc-" + UUID.randomUUID();
        try {
            Server server = InProcessServerBuilder.forName(name)
                    .directExecutor()
                    .addService(io.grpc.ServerInterceptors.intercept(service, List.of(interceptors)))
                    .build()
                    .start();
            ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            return new Harness(channel, server);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start in-process gRPC server", e);
        }
    }
}
```

Create `order-api/src/test/java/com/example/order/api/client/OrderGrpcClientTest.java`:

```java
package com.example.order.api.client;

import com.example.order.api.client.exception.RemoteOrderNotFound;
import com.example.order.proto.*;
import com.example.order.testing.InProcessGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.UUID;

class OrderGrpcClientTest {

    private static class StubService extends OrderServiceGrpc.OrderServiceImplBase {
        @Override
        public void getOrder(GetOrderRequest request, StreamObserver<GetOrderResponse> observer) {
            if (request.getId().startsWith("00000000")) {
                observer.onError(Status.NOT_FOUND.withDescription("nope").asRuntimeException());
                return;
            }
            observer.onNext(GetOrderResponse.newBuilder()
                    .setOrder(Order.newBuilder().setId(request.getId()).setCustomerId("c").build())
                    .build());
            observer.onCompleted();
        }

        @Override
        public void streamOrderEvents(StreamOrderEventsRequest request, StreamObserver<OrderEvent> observer) {
            for (int i = 1; i <= 3; i++) {
                observer.onNext(OrderEvent.newBuilder().setSequence(i).build());
            }
            observer.onCompleted();
        }
    }

    @Test
    void getReturnsAMonoOfTheOrder() {
        try (var harness = InProcessGrpc.start(new StubService())) {
            var client = new OrderGrpcClient(ReactorOrderServiceGrpc.newReactorStub(harness.channel()),
                    Duration.ofSeconds(2), Duration.ofSeconds(5));
            var id = UUID.randomUUID();

            StepVerifier.create(client.get(id))
                    .assertNext(order -> org.assertj.core.api.Assertions
                            .assertThat(order.getId()).isEqualTo(id.toString()))
                    .verifyComplete();
        }
    }

    @Test
    void remoteErrorsArriveAsTranslatedExceptionsNotStatusRuntimeException() {
        try (var harness = InProcessGrpc.start(new StubService())) {
            var client = new OrderGrpcClient(ReactorOrderServiceGrpc.newReactorStub(harness.channel()),
                    Duration.ofSeconds(2), Duration.ofSeconds(5));

            StepVerifier.create(client.get(UUID.fromString("00000000-0000-0000-0000-000000000000")))
                    .expectError(RemoteOrderNotFound.class)
                    .verify();
        }
    }

    @Test
    void streamingEventsArriveAsAFlux() {
        try (var harness = InProcessGrpc.start(new StubService())) {
            var client = new OrderGrpcClient(ReactorOrderServiceGrpc.newReactorStub(harness.channel()),
                    Duration.ofSeconds(2), Duration.ofSeconds(5));

            StepVerifier.create(client.streamEvents(null, 0).map(OrderEvent::getSequence))
                    .expectNext(1L, 2L, 3L)
                    .verifyComplete();
        }
    }
}
```

- [ ] **Step 8: Run test to verify it fails**

Run: `./gradlew :order-api:test --tests '*OrderGrpcClientTest'`
Expected: FAIL — `OrderGrpcClient` does not exist.

- [ ] **Step 9: Implement the client facade**

```java
package com.example.order.api.client;

import com.example.order.proto.*;
import io.grpc.Deadline;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * The only class in this service that touches generated protobuf types.
 * Deadlines are applied per call: read and write budgets differ, and a streaming
 * call gets none, because a deadline on a long-lived stream is a scheduled failure.
 */
public class OrderGrpcClient {

    private final ReactorOrderServiceGrpc.ReactorOrderServiceStub stub;
    private final Duration readDeadline;
    private final Duration writeDeadline;

    public OrderGrpcClient(ReactorOrderServiceGrpc.ReactorOrderServiceStub stub,
                           Duration readDeadline, Duration writeDeadline) {
        this.stub = stub;
        this.readDeadline = readDeadline;
        this.writeDeadline = writeDeadline;
    }

    public Mono<CreateOrderResponse> create(String customerId, List<OrderLine> lines,
                                            String currency, String idempotencyKey) {
        return write().createOrder(CreateOrderRequest.newBuilder()
                        .setCustomerId(customerId)
                        .addAllLines(lines)
                        .setCurrency(currency)
                        .setIdempotencyKey(idempotencyKey)
                        .build())
                .onErrorMap(GrpcStatusTranslator::translate);
    }

    public Mono<Order> get(UUID id) {
        return read().getOrder(GetOrderRequest.newBuilder().setId(id.toString()).build())
                .map(GetOrderResponse::getOrder)
                .onErrorMap(GrpcStatusTranslator::translate);
    }

    public Mono<ListOrdersResponse> list(String customerId, OrderStatus status,
                                         int pageSize, String pageToken) {
        var request = ListOrdersRequest.newBuilder().setPageSize(pageSize);
        if (customerId != null) {
            request.setCustomerId(customerId);
        }
        if (status != null) {
            request.setStatus(status);
        }
        if (pageToken != null) {
            request.setPageToken(pageToken);
        }
        return read().listOrders(request.build()).onErrorMap(GrpcStatusTranslator::translate);
    }

    public Mono<Order> updateStatus(UUID id, OrderStatus newStatus, long expectedVersion) {
        return write().updateOrderStatus(UpdateOrderStatusRequest.newBuilder()
                        .setId(id.toString())
                        .setNewStatus(newStatus)
                        .setExpectedVersion(expectedVersion)
                        .build())
                .map(UpdateOrderStatusResponse::getOrder)
                .onErrorMap(GrpcStatusTranslator::translate);
    }

    public Flux<OrderEvent> streamEvents(String customerId, long fromSequence) {
        var request = StreamOrderEventsRequest.newBuilder().setFromSequence(fromSequence);
        if (customerId != null) {
            request.setCustomerId(customerId);
        }
        return stub.streamOrderEvents(request.build())
                .onErrorMap(GrpcStatusTranslator::translate);
    }

    private ReactorOrderServiceGrpc.ReactorOrderServiceStub read() {
        return stub.withDeadline(Deadline.after(readDeadline.toMillis(), TimeUnit.MILLISECONDS));
    }

    private ReactorOrderServiceGrpc.ReactorOrderServiceStub write() {
        return stub.withDeadline(Deadline.after(writeDeadline.toMillis(), TimeUnit.MILLISECONDS));
    }
}
```

- [ ] **Step 10: Wire the client configuration and application**

```java
package com.example.order.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OrderApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderApiApplication.class, args);
    }
}
```

```java
package com.example.order.api.config;

import com.example.order.api.client.OrderGrpcClient;
import com.example.order.proto.ReactorOrderServiceGrpc;
import io.grpc.ManagedChannel;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.client.GrpcChannelFactory;

import java.time.Duration;

@Configuration
public class GrpcClientConfig {

    @ConfigurationProperties("order.client")
    public record ClientProperties(Duration readDeadline, Duration writeDeadline) {
        public ClientProperties {
            readDeadline = readDeadline == null ? Duration.ofSeconds(2) : readDeadline;
            writeDeadline = writeDeadline == null ? Duration.ofSeconds(5) : writeDeadline;
        }
    }

    @Bean
    public ManagedChannel orderChannel(GrpcChannelFactory channels) {
        return channels.createChannel("order-service");
    }

    @Bean
    public OrderGrpcClient orderGrpcClient(ManagedChannel orderChannel, ClientProperties properties) {
        return new OrderGrpcClient(ReactorOrderServiceGrpc.newReactorStub(orderChannel),
                properties.readDeadline(), properties.writeDeadline());
    }
}
```

Add `@ConfigurationPropertiesScan` to `OrderApiApplication`.

Create `order-api/src/main/resources/application.yml`:

```yaml
spring:
  application:
    name: order-api

  grpc:
    client:
      channels:
        order-service:
          address: static://localhost:9443
          negotiation-type: plaintext   # replaced with TLS in Task 17

server:
  port: 8080

order:
  client:
    read-deadline: 2s
    write-deadline: 5s

management:
  server:
    port: 8081
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics
  endpoint:
    health:
      probes:
        enabled: true
```

- [ ] **Step 11: Run test to verify it passes**

Run: `./gradlew :order-api:test`
Expected: PASS, all eight tests.

- [ ] **Step 12: Commit**

```bash
git add order-api testing-support
git commit -m "feat(api): reactive gRPC client with status translation"
```

---

### Task 11: REST controllers, DTOs, and ProblemDetail mapping

**Files:**
- Create: `order-api/src/main/java/com/example/order/api/dto/{CreateOrderRequestDto,OrderLineDto,OrderResponseDto,OrderPageDto,UpdateStatusRequestDto,DtoMapper}.java`
- Create: `order-api/src/main/java/com/example/order/api/web/{OrderController,OrderEventSseController,ProblemDetailAdvice}.java`
- Test: `order-api/src/test/java/com/example/order/api/web/OrderControllerTest.java`

**Interfaces:**
- Consumes: Task 10 `OrderGrpcClient` and the `Remote*` exceptions.
- Produces: REST surface —
  - `POST /api/v1/orders` → 201 + `Location`, or 200 when `idempotent_replay` is true
  - `GET /api/v1/orders/{id}` → 200
  - `GET /api/v1/orders?customerId=&status=&pageSize=&pageToken=` → 200 `OrderPageDto`
  - `PATCH /api/v1/orders/{id}/status` → 200
  - `GET /api/v1/orders/events` (`text/event-stream`) → SSE of `OrderEventDto`

- [ ] **Step 1: Write the failing test**

```java
package com.example.order.api.web;

import com.example.order.api.client.OrderGrpcClient;
import com.example.order.api.client.exception.*;
import com.example.order.proto.CreateOrderResponse;
import com.example.order.proto.Order;
import com.example.order.proto.OrderEvent;
import com.example.order.proto.OrderStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@WebFluxTest(controllers = {OrderController.class, OrderEventSseController.class})
class OrderControllerTest {

    @Autowired WebTestClient web;
    @MockBean OrderGrpcClient client;

    private static final String BODY = """
            {"customerId":"c1","currency":"USD","idempotencyKey":"k1",
             "lines":[{"sku":"S1","quantity":2,"unitPriceMinor":1000}]}
            """;

    @Test
    void createReturns201WithLocation() {
        var id = UUID.randomUUID();
        when(client.create(any(), any(), any(), any())).thenReturn(Mono.just(
                CreateOrderResponse.newBuilder()
                        .setOrder(Order.newBuilder().setId(id.toString()).setCustomerId("c1")
                                .setStatus(OrderStatus.ORDER_STATUS_PENDING).setVersion(1).build())
                        .setIdempotentReplay(false).build()));

        web.post().uri("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).bodyValue(BODY)
                .exchange()
                .expectStatus().isCreated()
                .expectHeader().valueEquals("Location", "/api/v1/orders/" + id)
                .expectBody().jsonPath("$.status").isEqualTo("PENDING");
    }

    @Test
    void idempotentReplayReturns200NotCreated() {
        var id = UUID.randomUUID();
        when(client.create(any(), any(), any(), any())).thenReturn(Mono.just(
                CreateOrderResponse.newBuilder()
                        .setOrder(Order.newBuilder().setId(id.toString()).setVersion(1).build())
                        .setIdempotentReplay(true).build()));

        web.post().uri("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).bodyValue(BODY)
                .exchange().expectStatus().isOk();
    }

    @Test
    void notFoundBecomes404ProblemDetail() {
        when(client.get(any())).thenReturn(Mono.error(new RemoteOrderNotFound("gone")));

        web.get().uri("/api/v1/orders/" + UUID.randomUUID()).exchange()
                .expectStatus().isNotFound()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody().jsonPath("$.title").isEqualTo("Order not found");
    }

    @Test
    void validationFailureBecomes400WithFieldErrors() {
        when(client.get(any())).thenReturn(Mono.error(new RemoteValidationFailed("bad",
                List.of(new RemoteFieldViolation("currency", "must be 3 letters")))));

        web.get().uri("/api/v1/orders/" + UUID.randomUUID()).exchange()
                .expectStatus().isBadRequest()
                .expectBody().jsonPath("$.errors[0].field").isEqualTo("currency");
    }

    @Test
    void versionConflictBecomes409AndIllegalTransitionAlsoBecomes409() {
        when(client.get(any())).thenReturn(Mono.error(new RemoteVersionConflict("stale")));
        web.get().uri("/api/v1/orders/" + UUID.randomUUID()).exchange().expectStatus().isEqualTo(409);

        when(client.get(any())).thenReturn(Mono.error(new RemoteIllegalTransition("no")));
        web.get().uri("/api/v1/orders/" + UUID.randomUUID()).exchange().expectStatus().isEqualTo(409);
    }

    @Test
    void unavailableBecomes503WithRetryAfter() {
        when(client.get(any())).thenReturn(Mono.error(new RemoteUnavailable("down", null)));

        web.get().uri("/api/v1/orders/" + UUID.randomUUID()).exchange()
                .expectStatus().isEqualTo(503)
                .expectHeader().exists("Retry-After");
    }

    @Test
    void timeoutBecomes504() {
        when(client.get(any())).thenReturn(Mono.error(new RemoteTimeout("slow", null)));
        web.get().uri("/api/v1/orders/" + UUID.randomUUID()).exchange().expectStatus().isEqualTo(504);
    }

    @Test
    void internalErrorIsOpaqueButKeepsTheErrorId() {
        when(client.get(any())).thenReturn(Mono.error(new RemoteInternal("boom", "err-42", null)));

        web.get().uri("/api/v1/orders/" + UUID.randomUUID()).exchange()
                .expectStatus().isEqualTo(500)
                .expectBody()
                .jsonPath("$.errorId").isEqualTo("err-42")
                .jsonPath("$.detail").value(d -> org.assertj.core.api.Assertions
                        .assertThat((String) d).doesNotContain("boom"));
    }

    @Test
    void eventsAreStreamedAsServerSentEvents() {
        when(client.streamEvents(any(), anyLong())).thenReturn(Flux.just(
                OrderEvent.newBuilder().setSequence(1).setOrderId(UUID.randomUUID().toString()).build()));

        web.get().uri("/api/v1/orders/events").accept(MediaType.TEXT_EVENT_STREAM).exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :order-api:test --tests '*OrderControllerTest'`
Expected: FAIL — controllers do not exist.

- [ ] **Step 3: Write the DTOs**

```java
package com.example.order.api.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.List;

public record CreateOrderRequestDto(
        @NotBlank String customerId,
        @NotBlank @Size(min = 3, max = 3) String currency,
        @NotBlank String idempotencyKey,
        @NotEmpty @Valid List<OrderLineDto> lines) {
}
```

```java
package com.example.order.api.dto;

import jakarta.validation.constraints.*;

public record OrderLineDto(
        @NotBlank String sku,
        @Positive int quantity,
        @PositiveOrZero long unitPriceMinor) {
}
```

```java
package com.example.order.api.dto;

import java.time.Instant;
import java.util.List;

public record OrderResponseDto(String id, String customerId, List<OrderLineDto> lines,
                               long totalMinor, String currency, String status,
                               long version, Instant createdAt, Instant updatedAt) {
}
```

```java
package com.example.order.api.dto;

import java.util.List;

public record OrderPageDto(List<OrderResponseDto> orders, String nextPageToken) {
}
```

```java
package com.example.order.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

public record UpdateStatusRequestDto(@NotBlank String status, @Positive long expectedVersion) {
}
```

```java
package com.example.order.api.dto;

import java.time.Instant;

public record OrderEventDto(long sequence, String orderId, String oldStatus,
                            String newStatus, Instant occurredAt) {
}
```

```java
package com.example.order.api.dto;

import com.example.order.proto.Order;
import com.example.order.proto.OrderEvent;
import com.example.order.proto.OrderStatus;
import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.util.List;

public final class DtoMapper {

    private DtoMapper() {
    }

    public static OrderResponseDto toDto(Order order) {
        return new OrderResponseDto(
                order.getId(),
                order.getCustomerId(),
                order.getLinesList().stream()
                        .map(l -> new OrderLineDto(l.getSku(), l.getQuantity(), l.getUnitPriceMinor()))
                        .toList(),
                order.getTotalMinor(),
                order.getCurrency(),
                shortName(order.getStatus()),
                order.getVersion(),
                toInstant(order.getCreatedAt()),
                toInstant(order.getUpdatedAt()));
    }

    public static OrderEventDto toDto(OrderEvent event) {
        return new OrderEventDto(event.getSequence(), event.getOrderId(),
                shortName(event.getOldStatus()), shortName(event.getNewStatus()),
                toInstant(event.getOccurredAt()));
    }

    public static List<com.example.order.proto.OrderLine> toProtoLines(List<OrderLineDto> lines) {
        return lines.stream()
                .map(l -> com.example.order.proto.OrderLine.newBuilder()
                        .setSku(l.sku()).setQuantity(l.quantity())
                        .setUnitPriceMinor(l.unitPriceMinor()).build())
                .toList();
    }

    /** REST clients see PENDING, not ORDER_STATUS_PENDING: the proto prefix is a wire concern. */
    public static String shortName(OrderStatus status) {
        return status.name().replaceFirst("^ORDER_STATUS_", "");
    }

    public static OrderStatus fromShortName(String name) {
        if (name == null || name.isBlank()) {
            return OrderStatus.ORDER_STATUS_UNSPECIFIED;
        }
        try {
            return OrderStatus.valueOf("ORDER_STATUS_" + name.toUpperCase());
        } catch (IllegalArgumentException e) {
            return OrderStatus.UNRECOGNIZED;
        }
    }

    private static Instant toInstant(Timestamp ts) {
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }
}
```

- [ ] **Step 4: Write the controllers**

```java
package com.example.order.api.web;

import com.example.order.api.client.OrderGrpcClient;
import com.example.order.api.dto.*;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private final OrderGrpcClient client;

    public OrderController(OrderGrpcClient client) {
        this.client = client;
    }

    @PostMapping
    public Mono<ResponseEntity<OrderResponseDto>> create(@Valid @RequestBody CreateOrderRequestDto request) {
        return client.create(request.customerId(), DtoMapper.toProtoLines(request.lines()),
                        request.currency(), request.idempotencyKey())
                .map(response -> {
                    var dto = DtoMapper.toDto(response.getOrder());
                    // A replay is not a creation: 200 tells the caller nothing new happened.
                    return response.getIdempotentReplay()
                            ? ResponseEntity.ok(dto)
                            : ResponseEntity.created(URI.create("/api/v1/orders/" + dto.id())).body(dto);
                });
    }

    @GetMapping("/{id}")
    public Mono<OrderResponseDto> get(@PathVariable UUID id) {
        return client.get(id).map(DtoMapper::toDto);
    }

    @GetMapping
    public Mono<OrderPageDto> list(@RequestParam(required = false) String customerId,
                                   @RequestParam(required = false) String status,
                                   @RequestParam(defaultValue = "50") int pageSize,
                                   @RequestParam(required = false) String pageToken) {
        return client.list(customerId, status == null ? null : DtoMapper.fromShortName(status),
                        pageSize, pageToken)
                .map(response -> new OrderPageDto(
                        response.getOrdersList().stream().map(DtoMapper::toDto).toList(),
                        response.getNextPageToken()));
    }

    @PatchMapping("/{id}/status")
    public Mono<OrderResponseDto> updateStatus(@PathVariable UUID id,
                                               @Valid @RequestBody UpdateStatusRequestDto request) {
        return client.updateStatus(id, DtoMapper.fromShortName(request.status()), request.expectedVersion())
                .map(DtoMapper::toDto);
    }
}
```

```java
package com.example.order.api.web;

import com.example.order.api.client.OrderGrpcClient;
import com.example.order.api.dto.DtoMapper;
import com.example.order.api.dto.OrderEventDto;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/api/v1/orders/events")
public class OrderEventSseController {

    private final OrderGrpcClient client;

    public OrderEventSseController(OrderGrpcClient client) {
        this.client = client;
    }

    /**
     * The gRPC server stream maps one-to-one onto SSE. Backpressure propagates:
     * a slow HTTP consumer slows the Flux, which slows the gRPC request-n signal,
     * which stops the server writing. Nothing buffers unboundedly.
     */
    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<OrderEventDto> stream(@RequestParam(required = false) String customerId,
                                      @RequestParam(defaultValue = "0") long fromSequence) {
        return client.streamEvents(customerId, fromSequence).map(DtoMapper::toDto);
    }
}
```

- [ ] **Step 5: Write the ProblemDetail advice**

```java
package com.example.order.api.web;

import com.example.order.api.client.exception.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.util.List;
import java.util.Map;

@RestControllerAdvice
public class ProblemDetailAdvice {

    private static final String BASE = "https://errors.example.com/orders/";
    private static final String RETRY_AFTER_SECONDS = "5";

    @ExceptionHandler(RemoteOrderNotFound.class)
    public ProblemDetail notFound(RemoteOrderNotFound e) {
        return problem(HttpStatus.NOT_FOUND, "Order not found", e.getMessage(), "not-found");
    }

    @ExceptionHandler(RemoteValidationFailed.class)
    public ProblemDetail validation(RemoteValidationFailed e) {
        var detail = problem(HttpStatus.BAD_REQUEST, "Invalid request", e.getMessage(), "validation");
        detail.setProperty("errors", e.violations().stream()
                .map(v -> Map.of("field", v.field(), "message", v.description()))
                .toList());
        return detail;
    }

    @ExceptionHandler(RemoteIllegalTransition.class)
    public ProblemDetail illegalTransition(RemoteIllegalTransition e) {
        return problem(HttpStatus.CONFLICT, "Illegal status transition", e.getMessage(), "illegal-transition");
    }

    @ExceptionHandler(RemoteVersionConflict.class)
    public ProblemDetail versionConflict(RemoteVersionConflict e) {
        var detail = problem(HttpStatus.CONFLICT, "Version conflict", e.getMessage(), "version-conflict");
        detail.setProperty("remediation", "Re-read the order and retry with its current version");
        return detail;
    }

    @ExceptionHandler(RemoteUnavailable.class)
    public ResponseEntity<ProblemDetail> unavailable(RemoteUnavailable e) {
        var detail = problem(HttpStatus.SERVICE_UNAVAILABLE, "Order service unavailable",
                "The order service is not reachable. Retry shortly.", "unavailable");
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header("Retry-After", RETRY_AFTER_SECONDS)
                .body(detail);
    }

    @ExceptionHandler(RemoteTimeout.class)
    public ProblemDetail timeout(RemoteTimeout e) {
        return problem(HttpStatus.GATEWAY_TIMEOUT, "Order service timed out",
                "The order service did not respond within the deadline.", "timeout");
    }

    @ExceptionHandler(RemoteInternal.class)
    public ProblemDetail internal(RemoteInternal e) {
        // The upstream message may contain internal detail; only the correlation id escapes.
        var detail = problem(HttpStatus.INTERNAL_SERVER_ERROR, "Internal error",
                "An unexpected error occurred.", "internal");
        detail.setProperty("errorId", e.errorId());
        return detail;
    }

    private ProblemDetail problem(HttpStatus status, String title, String detailText, String slug) {
        var detail = ProblemDetail.forStatusAndDetail(status, detailText);
        detail.setTitle(title);
        detail.setType(URI.create(BASE + slug));
        return detail;
    }
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `./gradlew :order-api:test --tests '*OrderControllerTest'`
Expected: PASS, all nine tests.

- [ ] **Step 7: Commit**

```bash
git add order-api
git commit -m "feat(api): REST controllers, SSE stream, and RFC 9457 error mapping"
```

---

### Task 12: End-to-end wiring test across both services

**Files:**
- Create: `order-api/src/test/java/com/example/order/api/EndToEndIT.java`
- Modify: `order-api/build.gradle.kts` (test dependency on `:order-service`)

**Interfaces:**
- Consumes: everything from Tasks 3 through 11.
- Produces: no production code. This task exists because every previous test mocked one side of the boundary; nothing yet has proven the two halves agree.

- [ ] **Step 1: Add the test dependency**

In `order-api/build.gradle.kts`:

```kotlin
    testImplementation(project(":order-service"))
```

- [ ] **Step 2: Write the failing test**

```java
package com.example.order.api;

import com.example.order.testing.PostgresFixture;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.util.UUID;

/**
 * Boots the real gRPC service and the real REST edge in one JVM and drives them
 * over a real channel. Every other test in this build mocks one side; this is the
 * only one that proves the two sides agree about the contract.
 */
@SpringBootTest(classes = {OrderApiApplication.class, com.example.order.service.OrderServiceApplication.class},
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"spring.grpc.server.port=19443",
                      "spring.grpc.client.channels.order-service.address=static://localhost:19443"})
class EndToEndIT {

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        PostgresFixture.registerProperties(registry);
    }

    @Autowired WebTestClient web;

    @Test
    void createReadUpdateAndListRoundTripThroughGrpc() {
        String key = UUID.randomUUID().toString();
        String body = """
                {"customerId":"e2e","currency":"USD","idempotencyKey":"%s",
                 "lines":[{"sku":"S1","quantity":2,"unitPriceMinor":1500}]}
                """.formatted(key);

        var created = web.post().uri("/api/v1/orders")
                .contentType(MediaType.APPLICATION_JSON).bodyValue(body)
                .exchange().expectStatus().isCreated()
                .expectBody()
                .jsonPath("$.totalMinor").isEqualTo(3000)
                .jsonPath("$.status").isEqualTo("PENDING")
                .returnResult();

        String id = com.jayway.jsonpath.JsonPath.read(new String(created.getResponseBody()), "$.id");

        web.get().uri("/api/v1/orders/{id}", id).exchange()
                .expectStatus().isOk().expectBody().jsonPath("$.id").isEqualTo(id);

        web.patch().uri("/api/v1/orders/{id}/status", id)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"status\":\"CONFIRMED\",\"expectedVersion\":1}")
                .exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.status").isEqualTo("CONFIRMED")
                .jsonPath("$.version").isEqualTo(2);

        // Replay: same idempotency key must return 200, not create a second order.
        web.post().uri("/api/v1/orders")
                .contentType(MediaType.APPLICATION_JSON).bodyValue(body)
                .exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.id").isEqualTo(id);

        web.get().uri("/api/v1/orders?customerId=e2e&pageSize=10").exchange()
                .expectStatus().isOk().expectBody().jsonPath("$.orders").isArray();
    }

    @Test
    void staleVersionSurfacesAs409ThroughTheWholeStack() {
        String body = """
                {"customerId":"e2e-conflict","currency":"USD","idempotencyKey":"%s",
                 "lines":[{"sku":"S1","quantity":1,"unitPriceMinor":100}]}
                """.formatted(UUID.randomUUID());

        var created = web.post().uri("/api/v1/orders")
                .contentType(MediaType.APPLICATION_JSON).bodyValue(body)
                .exchange().expectStatus().isCreated().expectBody().returnResult();
        String id = com.jayway.jsonpath.JsonPath.read(new String(created.getResponseBody()), "$.id");

        web.patch().uri("/api/v1/orders/{id}/status", id)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"status\":\"CONFIRMED\",\"expectedVersion\":1}")
                .exchange().expectStatus().isOk();

        web.patch().uri("/api/v1/orders/{id}/status", id)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"status\":\"SHIPPED\",\"expectedVersion\":1}")
                .exchange().expectStatus().isEqualTo(409)
                .expectBody().jsonPath("$.title").isEqualTo("Version conflict");
    }

    @Test
    void unknownOrderSurfacesAs404ProblemDetail() {
        web.get().uri("/api/v1/orders/{id}", UUID.randomUUID()).exchange()
                .expectStatus().isNotFound()
                .expectBody().jsonPath("$.title").isEqualTo("Order not found");
    }
}
```

- [ ] **Step 3: Run test to verify it fails, then passes**

Run: `./gradlew :order-api:test --tests '*EndToEndIT'`
Expected on first run: FAIL if the two application contexts conflict (duplicate `Clock` bean, port clash, or the REST edge's channel pointing at the wrong port). Fix the wiring — not the assertions — until it passes.

- [ ] **Step 4: Run the whole build**

Run: `./gradlew build`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 5: Commit**

```bash
git add order-api
git commit -m "test: end-to-end REST to gRPC integration across both services"
```

---

## Phase 4 — Reliability

### Task 13: Retry policy and deadline enforcement

**Files:**
- Modify: `order-api/src/main/resources/application.yml` (gRPC service config)
- Create: `order-api/src/main/java/com/example/order/api/config/RetryPolicyConfig.java`
- Modify: `order-service/src/main/java/com/example/order/service/grpc/OrderGrpcService.java` (deadline checks)
- Create: `order-service/src/main/java/com/example/order/service/grpc/DeadlineGuard.java`
- Test: `order-api/src/test/java/com/example/order/api/client/RetryPolicyTest.java`, `order-service/src/test/java/com/example/order/service/grpc/DeadlineGuardTest.java`

**Interfaces:**
- Consumes: Task 10 client config.
- Produces: `RetryPolicyConfig.serviceConfig()` returning `Map<String, ?>` for `ManagedChannelBuilder.defaultServiceConfig`; `DeadlineGuard.checkNotExpired()` throwing `DeadlineExceededException` when the caller's budget is already spent.

**Which methods may be retried, and why each is safe:**

| Method | Retryable | Reason |
|---|---|---|
| `GetOrder`, `ListOrders` | yes | Reads; naturally idempotent. |
| `CreateOrder` | yes | The idempotency key makes a replay return the original order. |
| `UpdateOrderStatus` | yes | `expected_version` makes a replayed write fail with `ABORTED` rather than double-apply. |
| `StreamOrderEvents` | no | Retrying a stream restarts it; resumption belongs to the caller via `from_sequence`. |

Retryable status codes are `UNAVAILABLE` and `RESOURCE_EXHAUSTED` only. `DEADLINE_EXCEEDED` is deliberately excluded: the budget is already spent, so retrying only burns the caller's remaining time.

- [ ] **Step 1: Write the failing deadline test**

```java
package com.example.order.service.grpc;

import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

class DeadlineGuardTest {

    @Test
    void passesWhenBudgetRemains() {
        var context = Context.current().withDeadline(
                Deadline.after(5, TimeUnit.SECONDS), Executors.newSingleThreadScheduledExecutor());
        context.run(() -> assertThatNoException().isThrownBy(DeadlineGuard::checkNotExpired));
    }

    @Test
    void throwsDeadlineExceededWhenBudgetIsSpent() {
        var context = Context.current().withDeadline(
                Deadline.after(-1, TimeUnit.SECONDS), Executors.newSingleThreadScheduledExecutor());
        context.run(() -> assertThatThrownBy(DeadlineGuard::checkNotExpired)
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("DEADLINE_EXCEEDED"));
    }

    @Test
    void passesWhenCallerSetNoDeadline() {
        assertThatNoException().isThrownBy(DeadlineGuard::checkNotExpired);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :order-service:test --tests '*DeadlineGuardTest'`
Expected: FAIL — `DeadlineGuard` does not exist.

- [ ] **Step 3: Implement the deadline guard**

```java
package com.example.order.service.grpc;

import io.grpc.Context;
import io.grpc.Status;

/**
 * Checked before expensive work. If the caller's budget is already spent, the
 * response can never be delivered, so doing the work is pure waste — it burns a
 * connection, a thread, and a database transaction for nobody.
 */
public final class DeadlineGuard {

    private DeadlineGuard() {
    }

    public static void checkNotExpired() {
        var deadline = Context.current().getDeadline();
        if (deadline != null && deadline.isExpired()) {
            throw Status.DEADLINE_EXCEEDED
                    .withDescription("Deadline expired before the server began work")
                    .asRuntimeException();
        }
    }
}
```

- [ ] **Step 4: Call the guard from every RPC**

Add `DeadlineGuard.checkNotExpired();` as the first statement of `createOrder`, `getOrder`, `listOrders`, and `updateOrderStatus` in `OrderGrpcService`. The streaming RPC already checks the deadline in its loop (Task 9).

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :order-service:test --tests '*DeadlineGuardTest'`
Expected: PASS.

- [ ] **Step 6: Write the failing retry test**

```java
package com.example.order.api.client;

import com.example.order.api.config.RetryPolicyConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RetryPolicyTest {

    @SuppressWarnings("unchecked")
    private List<Map<String, ?>> methodConfigs() {
        return (List<Map<String, ?>>) RetryPolicyConfig.serviceConfig().get("methodConfig");
    }

    @Test
    void retriesOnlyTransientCodes() {
        var retryPolicy = (Map<String, ?>) methodConfigs().get(0).get("retryPolicy");
        assertThat((List<String>) retryPolicy.get("retryableStatusCodes"))
                .containsExactlyInAnyOrder("UNAVAILABLE", "RESOURCE_EXHAUSTED");
    }

    @Test
    void deadlineExceededIsNotRetried() {
        var retryPolicy = (Map<String, ?>) methodConfigs().get(0).get("retryPolicy");
        assertThat((List<String>) retryPolicy.get("retryableStatusCodes"))
                .doesNotContain("DEADLINE_EXCEEDED");
    }

    @Test
    void streamingMethodIsExcludedFromRetries() {
        var names = methodConfigs().stream()
                .flatMap(c -> ((List<Map<String, String>>) c.get("name")).stream())
                .map(n -> n.get("method"))
                .toList();
        assertThat(names).doesNotContain("StreamOrderEvents");
    }

    @Test
    void backoffGrowsAndIsBounded() {
        var retryPolicy = (Map<String, ?>) methodConfigs().get(0).get("retryPolicy");
        assertThat((Double) retryPolicy.get("backoffMultiplier")).isGreaterThan(1.0);
        assertThat((String) retryPolicy.get("maxBackoff")).isEqualTo("2s");
        assertThat((Double) retryPolicy.get("maxAttempts")).isEqualTo(4.0);
    }
}
```

- [ ] **Step 7: Run test to verify it fails**

Run: `./gradlew :order-api:test --tests '*RetryPolicyTest'`
Expected: FAIL — `RetryPolicyConfig` does not exist.

- [ ] **Step 8: Implement the retry policy**

```java
package com.example.order.api.config;

import java.util.List;
import java.util.Map;

/**
 * gRPC's built-in retry, expressed as a service config. This is preferred over
 * application-level retry because grpc-java applies the backoff below the load
 * balancer, so a retry can land on a different, healthy backend. Jitter is applied
 * by the runtime; the multiplier and cap here bound the worst case.
 *
 * Every listed method is retry-safe by design, not by hope:
 * reads are idempotent, CreateOrder is keyed, UpdateOrderStatus is version-checked.
 */
public final class RetryPolicyConfig {

    private static final String SERVICE = "order.v1.OrderService";

    private RetryPolicyConfig() {
    }

    public static Map<String, ?> serviceConfig() {
        Map<String, ?> retryPolicy = Map.of(
                "maxAttempts", 4.0,
                "initialBackoff", "0.1s",
                "maxBackoff", "2s",
                "backoffMultiplier", 2.0,
                "retryableStatusCodes", List.of("UNAVAILABLE", "RESOURCE_EXHAUSTED"));

        return Map.of("methodConfig", List.of(Map.of(
                "name", List.of(
                        Map.of("service", SERVICE, "method", "GetOrder"),
                        Map.of("service", SERVICE, "method", "ListOrders"),
                        Map.of("service", SERVICE, "method", "CreateOrder"),
                        Map.of("service", SERVICE, "method", "UpdateOrderStatus")),
                "retryPolicy", retryPolicy)));
    }
}
```

- [ ] **Step 9: Apply the policy to the channel**

In `GrpcClientConfig`, replace the `orderChannel` bean with one that customizes the builder:

```java
    @Bean
    public org.springframework.grpc.client.GrpcChannelBuilderCustomizer<?> retryCustomizer() {
        return (name, builder) -> {
            if ("order-service".equals(name)) {
                builder.defaultServiceConfig(RetryPolicyConfig.serviceConfig())
                       .enableRetry()
                       .maxRetryAttempts(4);
            }
        };
    }
```

If the Spring gRPC customizer interface differs in 1.0.3, configure the same three calls directly on the `ManagedChannelBuilder` in the `orderChannel` bean instead. `enableRetry()` is required — the service config alone is inert without it.

- [ ] **Step 10: Run test to verify it passes, then the module suite**

Run: `./gradlew :order-api:test :order-service:test`
Expected: PASS.

- [ ] **Step 11: Commit**

```bash
git add order-api order-service
git commit -m "feat: retry-safe service config and server-side deadline enforcement"
```

---

### Task 14: Circuit breaker and bulkhead

**Files:**
- Modify: `order-api/build.gradle.kts`
- Create: `order-api/src/main/java/com/example/order/api/client/ResilientOrderClient.java`
- Create: `order-api/src/main/java/com/example/order/api/config/ResilienceConfig.java`
- Modify: `order-api/src/main/resources/application.yml`
- Test: `order-api/src/test/java/com/example/order/api/client/ResilientOrderClientTest.java`

**Interfaces:**
- Consumes: Task 10 `OrderGrpcClient`.
- Produces: `ResilientOrderClient` with the same method signatures as `OrderGrpcClient`, injected into the controllers in its place. Opening the breaker surfaces `RemoteUnavailable`, which the existing advice already maps to 503 + `Retry-After`.

- [ ] **Step 1: Add dependencies**

```kotlin
    implementation(libs.resilience4j)
    implementation(libs.resilience4j.reactor)
    implementation("org.springframework.boot:spring-boot-starter-aop")
```

- [ ] **Step 2: Write the failing test**

```java
package com.example.order.api.client;

import com.example.order.api.client.exception.RemoteUnavailable;
import com.example.order.proto.Order;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ResilientOrderClientTest {

    private ResilientOrderClient clientThatAlwaysFails(CircuitBreaker breaker) {
        var delegate = Mockito.mock(OrderGrpcClient.class);
        Mockito.when(delegate.get(Mockito.any()))
                .thenReturn(Mono.error(new RemoteUnavailable("down", null)));
        return new ResilientOrderClient(delegate, breaker,
                Bulkhead.of("test", BulkheadConfig.custom().maxConcurrentCalls(10).build()));
    }

    private CircuitBreaker breaker() {
        return CircuitBreaker.of("test", CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(4)
                .minimumNumberOfCalls(4)
                .failureRateThreshold(50f)
                .waitDurationInOpenState(Duration.ofMillis(200))
                .build());
    }

    @Test
    void breakerOpensAfterRepeatedFailures() {
        var breaker = breaker();
        var client = clientThatAlwaysFails(breaker);

        for (int i = 0; i < 4; i++) {
            StepVerifier.create(client.get(UUID.randomUUID())).expectError().verify();
        }
        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
    }

    @Test
    void openBreakerFailsFastAsRemoteUnavailable() {
        var breaker = breaker();
        var client = clientThatAlwaysFails(breaker);
        for (int i = 0; i < 4; i++) {
            StepVerifier.create(client.get(UUID.randomUUID())).expectError().verify();
        }

        // The point of the breaker: no call reaches the delegate now, and the caller
        // still gets a typed exception the advice can render as 503.
        StepVerifier.create(client.get(UUID.randomUUID()))
                .expectError(RemoteUnavailable.class)
                .verify();
    }

    @Test
    void breakerRecoversAfterTheWaitDuration() throws Exception {
        var breaker = breaker();
        var delegate = Mockito.mock(OrderGrpcClient.class);
        Mockito.when(delegate.get(Mockito.any()))
                .thenReturn(Mono.error(new RemoteUnavailable("down", null)));
        var client = new ResilientOrderClient(delegate, breaker,
                Bulkhead.of("test", BulkheadConfig.ofDefaults()));

        for (int i = 0; i < 4; i++) {
            StepVerifier.create(client.get(UUID.randomUUID())).expectError().verify();
        }
        Mockito.when(delegate.get(Mockito.any()))
                .thenReturn(Mono.just(Order.newBuilder().setId("ok").build()));

        Thread.sleep(300);
        breaker.transitionToHalfOpenState();

        StepVerifier.create(client.get(UUID.randomUUID()))
                .assertNext(o -> assertThat(o.getId()).isEqualTo("ok"))
                .verifyComplete();
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `./gradlew :order-api:test --tests '*ResilientOrderClientTest'`
Expected: FAIL — `ResilientOrderClient` does not exist.

- [ ] **Step 4: Implement the resilient wrapper**

```java
package com.example.order.api.client;

import com.example.order.api.client.exception.RemoteUnavailable;
import com.example.order.proto.*;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;

/**
 * Same surface as OrderGrpcClient, wrapped in a breaker and a bulkhead.
 *
 * The streaming call is deliberately NOT wrapped: a long-lived stream would hold a
 * bulkhead permit for its entire lifetime and starve unary calls, and a single
 * stream ending is not evidence the backend is unhealthy.
 */
public class ResilientOrderClient {

    private final OrderGrpcClient delegate;
    private final CircuitBreaker breaker;
    private final Bulkhead bulkhead;

    public ResilientOrderClient(OrderGrpcClient delegate, CircuitBreaker breaker, Bulkhead bulkhead) {
        this.delegate = delegate;
        this.breaker = breaker;
        this.bulkhead = bulkhead;
    }

    public Mono<CreateOrderResponse> create(String customerId, List<OrderLine> lines,
                                            String currency, String idempotencyKey) {
        return guard(delegate.create(customerId, lines, currency, idempotencyKey));
    }

    public Mono<Order> get(UUID id) {
        return guard(delegate.get(id));
    }

    public Mono<ListOrdersResponse> list(String customerId, OrderStatus status, int pageSize, String pageToken) {
        return guard(delegate.list(customerId, status, pageSize, pageToken));
    }

    public Mono<Order> updateStatus(UUID id, OrderStatus newStatus, long expectedVersion) {
        return guard(delegate.updateStatus(id, newStatus, expectedVersion));
    }

    public Flux<OrderEvent> streamEvents(String customerId, long fromSequence) {
        return delegate.streamEvents(customerId, fromSequence);
    }

    private <T> Mono<T> guard(Mono<T> source) {
        return source
                .transformDeferred(BulkheadOperator.of(bulkhead))
                .transformDeferred(CircuitBreakerOperator.of(breaker))
                // A rejected call is indistinguishable from an unreachable backend
                // as far as the caller is concerned, so it maps to the same 503.
                .onErrorMap(CallNotPermittedException.class,
                        e -> new RemoteUnavailable("Order service circuit is open", e));
    }
}
```

- [ ] **Step 5: Configure the beans**

```java
package com.example.order.api.config;

import com.example.order.api.client.OrderGrpcClient;
import com.example.order.api.client.ResilientOrderClient;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ResilienceConfig {

    @Bean
    public CircuitBreaker orderCircuitBreaker(CircuitBreakerRegistry registry) {
        return registry.circuitBreaker("order-service");
    }

    @Bean
    public Bulkhead orderBulkhead(BulkheadRegistry registry) {
        return registry.bulkhead("order-service");
    }

    @Bean
    public ResilientOrderClient resilientOrderClient(OrderGrpcClient client,
                                                     CircuitBreaker breaker, Bulkhead bulkhead) {
        return new ResilientOrderClient(client, breaker, bulkhead);
    }
}
```

Add to `order-api/src/main/resources/application.yml`:

```yaml
resilience4j:
  circuitbreaker:
    instances:
      order-service:
        sliding-window-type: COUNT_BASED
        sliding-window-size: 20
        minimum-number-of-calls: 10
        failure-rate-threshold: 50
        wait-duration-in-open-state: 10s
        permitted-number-of-calls-in-half-open-state: 3
        automatic-transition-from-open-to-half-open-enabled: true
        register-health-indicator: true
        # Business failures must not trip the breaker: a 404 means the backend is
        # healthy and answering correctly.
        ignore-exceptions:
          - com.example.order.api.client.exception.RemoteOrderNotFound
          - com.example.order.api.client.exception.RemoteValidationFailed
          - com.example.order.api.client.exception.RemoteIllegalTransition
          - com.example.order.api.client.exception.RemoteVersionConflict
  bulkhead:
    instances:
      order-service:
        max-concurrent-calls: 64
        max-wait-duration: 50ms
```

The `ignore-exceptions` list is the subtle part. Without it, a burst of legitimate 404s opens the breaker and takes down a perfectly healthy service.

- [ ] **Step 6: Switch the controllers to the resilient client**

Change the constructor parameter type in `OrderController` and `OrderEventSseController` from `OrderGrpcClient` to `ResilientOrderClient`. Update `OrderControllerTest`'s `@MockBean` type to match.

- [ ] **Step 7: Run tests to verify they pass**

Run: `./gradlew :order-api:test`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add order-api
git commit -m "feat(api): circuit breaker and bulkhead around the gRPC client"
```

---

### Task 15: Graceful shutdown, keepalive, and gRPC health

**Files:**
- Create: `order-service/src/main/java/com/example/order/service/config/{HealthConfig,ShutdownConfig}.java`
- Modify: `order-service/src/main/resources/application.yml`, `order-api/src/main/resources/application.yml`
- Test: `order-service/src/test/java/com/example/order/service/config/GrpcHealthIT.java`

**Interfaces:**
- Consumes: Task 3 application.
- Produces: a registered `grpc.health.v1.Health` service reporting `SERVING` for `order.v1.OrderService`; server reflection enabled; readiness flipped to `DOWN` before the drain starts.

- [ ] **Step 1: Write the failing test**

```java
package com.example.order.service.config;

import com.example.order.testing.PostgresFixture;
import io.grpc.ManagedChannel;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.reflection.v1alpha.ServerReflectionGrpc;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "spring.grpc.server.port=0")
class GrpcHealthIT {

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        PostgresFixture.registerProperties(registry);
    }

    @Autowired ManagedChannel channel;

    @Test
    void healthServiceReportsServing() {
        var response = HealthGrpc.newBlockingStub(channel)
                .check(HealthCheckRequest.newBuilder().setService("").build());
        assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
    }

    @Test
    void perServiceHealthIsReported() {
        var response = HealthGrpc.newBlockingStub(channel)
                .check(HealthCheckRequest.newBuilder().setService("order.v1.OrderService").build());
        assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
    }

    @Test
    void reflectionIsAvailableForGrpcurl() {
        assertThat(ServerReflectionGrpc.newStub(channel)).isNotNull();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :order-service:test --tests '*GrpcHealthIT'`
Expected: FAIL — `UNIMPLEMENTED: Method not found: grpc.health.v1.Health/Check`.

- [ ] **Step 3: Register health and reflection**

```java
package com.example.order.service.config;

import io.grpc.BindableService;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HealthConfig {

    @Bean
    public HealthStatusManager healthStatusManager() {
        var manager = new HealthStatusManager();
        manager.setStatus("", HealthCheckResponse.ServingStatus.SERVING);
        manager.setStatus("order.v1.OrderService", HealthCheckResponse.ServingStatus.SERVING);
        return manager;
    }

    @Bean
    public BindableService healthService(HealthStatusManager manager) {
        return manager.getHealthService();
    }

    /** Lets grpcurl and grpc_cli work without a local copy of the descriptors. */
    @Bean
    public BindableService reflectionService() {
        return ProtoReflectionService.newInstance();
    }
}
```

- [ ] **Step 4: Implement shutdown sequencing**

```java
package com.example.order.service.config;

import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.services.HealthStatusManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;

/**
 * Ordered first on shutdown. Flipping health to NOT_SERVING before the server
 * drains gives load balancers a chance to stop routing new calls; draining first
 * and announcing afterwards means every in-flight arrival during the drain fails.
 */
@Component
public class ShutdownConfig implements ApplicationListener<ContextClosedEvent>, Ordered {

    private static final Logger log = LoggerFactory.getLogger(ShutdownConfig.class);

    private final HealthStatusManager health;

    public ShutdownConfig(HealthStatusManager health) {
        this.health = health;
    }

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        log.info("Shutdown started: marking gRPC health NOT_SERVING before draining");
        health.setStatus("", HealthCheckResponse.ServingStatus.NOT_SERVING);
        health.setStatus("order.v1.OrderService", HealthCheckResponse.ServingStatus.NOT_SERVING);
        try {
            // Give load balancers a moment to observe the change before the drain.
            Thread.sleep(2_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
```

- [ ] **Step 5: Configure shutdown and keepalive**

Add to `order-service/src/main/resources/application.yml`:

```yaml
spring:
  lifecycle:
    timeout-per-shutdown-phase: 30s
  grpc:
    server:
      shutdown-grace-period: 30s
      keep-alive:
        time: 30s
        timeout: 10s
        # Without this an idle client's pings are treated as abuse and the
        # connection is killed - the classic "works until traffic goes quiet" bug.
        permit-without-calls: true
        max-idle: 5m

server:
  shutdown: graceful
```

Add to `order-api/src/main/resources/application.yml` under `spring.grpc.client.channels.order-service`:

```yaml
        health:
          enabled: true
          service-name: order.v1.OrderService
        keep-alive:
          time: 30s
          timeout: 10s
          without-calls: true
```

- [ ] **Step 6: Run test to verify it passes**

Run: `./gradlew :order-service:test --tests '*GrpcHealthIT'`
Expected: PASS. If `ProtoReflectionService` is deprecated in grpc 1.77.1, use `ProtoReflectionServiceV1.newInstance()` and update the test's stub import accordingly.

- [ ] **Step 7: Verify shutdown by hand**

Run `./gradlew :order-service:bootRun`, then in another terminal:
`grpcurl -plaintext localhost:9443 grpc.health.v1.Health/Check`
Expected: `{"status": "SERVING"}`. Then `Ctrl-C` the service and confirm the log line `Shutdown started: marking gRPC health NOT_SERVING before draining` appears before the server stops.

- [ ] **Step 8: Commit**

```bash
git add order-service order-api
git commit -m "feat: gRPC health service, reflection, keepalive, and drain-aware shutdown"
```

---

## Phase 5 — Observability and security

### Task 16: Metrics, tracing, and structured logging

**Files:**
- Modify: both `build.gradle.kts` files and both `application.yml` files
- Create: `order-service/src/main/java/com/example/order/service/config/ObservabilityConfig.java`
- Create: `order-api/src/main/java/com/example/order/api/config/ObservabilityConfig.java`
- Create: `order-service/src/main/resources/logback-spring.xml`, `order-api/src/main/resources/logback-spring.xml`
- Test: `order-api/src/test/java/com/example/order/api/observability/TracePropagationIT.java`

**Interfaces:**
- Consumes: everything so far.
- Produces: `ObservationGrpcServerInterceptor` and `ObservationGrpcClientInterceptor` beans; `/actuator/prometheus` on both management ports; JSON logs carrying `traceId` and `spanId`.

- [ ] **Step 1: Add dependencies to both modules**

```kotlin
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("io.micrometer:micrometer-registry-prometheus")
    implementation(libs.micrometer.tracing.otel)
    implementation(libs.otel.exporter.otlp)
    runtimeOnly(libs.logstash.encoder)
```

- [ ] **Step 2: Write the failing test**

```java
package com.example.order.api.observability;

import com.example.order.testing.PostgresFixture;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = {com.example.order.api.OrderApiApplication.class,
                           com.example.order.service.OrderServiceApplication.class},
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"spring.grpc.server.port=19444",
                      "spring.grpc.client.channels.order-service.address=static://localhost:19444",
                      "management.tracing.sampling.probability=1.0"})
class TracePropagationIT {

    private static final AtomicReference<String> CAPTURED_TRACEPARENT = new AtomicReference<>();

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        PostgresFixture.registerProperties(registry);
    }

    @TestConfiguration
    static class CaptureConfig {
        @Bean
        @org.springframework.grpc.server.GlobalServerInterceptor
        @org.springframework.core.annotation.Order(org.springframework.core.Ordered.LOWEST_PRECEDENCE)
        ServerInterceptor captureTraceparent() {
            return new ServerInterceptor() {
                @Override
                public <Q, S> ServerCall.Listener<Q> interceptCall(
                        ServerCall<Q, S> call, Metadata headers, ServerCallHandler<Q, S> next) {
                    var key = Metadata.Key.of("traceparent", Metadata.ASCII_STRING_MARSHALLER);
                    CAPTURED_TRACEPARENT.set(headers.get(key));
                    return next.startCall(call, headers);
                }
            };
        }
    }

    @Autowired WebTestClient web;

    @Test
    void traceContextCrossesTheGrpcBoundary() {
        web.post().uri("/api/v1/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("""
                        {"customerId":"trace","currency":"USD","idempotencyKey":"%s",
                         "lines":[{"sku":"S","quantity":1,"unitPriceMinor":100}]}
                        """.formatted(UUID.randomUUID()))
                .exchange().expectStatus().isCreated();

        // W3C traceparent must arrive in gRPC metadata. If this is null, the two
        // services will produce two disconnected traces and cross-service debugging
        // stops working - the single most common gap in "instrumented" systems.
        assertThat(CAPTURED_TRACEPARENT.get())
                .as("W3C traceparent propagated into gRPC metadata")
                .isNotNull()
                .startsWith("00-");
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `./gradlew :order-api:test --tests '*TracePropagationIT'`
Expected: FAIL — `traceparent` is null; nothing is instrumenting the gRPC hop.

- [ ] **Step 4: Register observation interceptors**

`order-service/src/main/java/com/example/order/service/config/ObservabilityConfig.java`:

```java
package com.example.order.service.config;

import io.micrometer.core.instrument.binder.grpc.ObservationGrpcServerInterceptor;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.grpc.server.GlobalServerInterceptor;

@Configuration
public class ObservabilityConfig {

    /**
     * Ordered outermost so the span covers the whole call, including the time spent
     * in the exception interceptor. An inner span would omit error handling from
     * the trace - exactly the part you need when debugging failures.
     */
    @Bean
    @GlobalServerInterceptor
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public ObservationGrpcServerInterceptor observationGrpcServerInterceptor(ObservationRegistry registry) {
        return new ObservationGrpcServerInterceptor(registry);
    }
}
```

`order-api/src/main/java/com/example/order/api/config/ObservabilityConfig.java`:

```java
package com.example.order.api.config;

import io.micrometer.core.instrument.binder.grpc.ObservationGrpcClientInterceptor;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.client.GlobalClientInterceptor;

@Configuration
public class ObservabilityConfig {

    @Bean
    @GlobalClientInterceptor
    public ObservationGrpcClientInterceptor observationGrpcClientInterceptor(ObservationRegistry registry) {
        return new ObservationGrpcClientInterceptor(registry);
    }
}
```

- [ ] **Step 5: Configure tracing and metrics**

Add to both `application.yml` files (adjusting `service.name`):

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics
  metrics:
    tags:
      application: ${spring.application.name}
  tracing:
    sampling:
      probability: ${TRACE_SAMPLE_RATE:1.0}
  otlp:
    tracing:
      endpoint: ${OTLP_ENDPOINT:http://localhost:4318/v1/traces}
  observations:
    key-values:
      service: ${spring.application.name}
```

A sample rate of 1.0 is right for local development and wrong for production; it is expressed as an environment variable so the deployment can lower it without a rebuild.

- [ ] **Step 6: Configure structured logging**

Create `logback-spring.xml` in both modules' resources:

```xml
<configuration>
    <springProperty scope="context" name="appName" source="spring.application.name"/>

    <appender name="JSON" class="ch.qos.logback.core.ConsoleAppender">
        <encoder class="net.logstash.logback.encoder.LogstashEncoder">
            <includeMdcKeyName>traceId</includeMdcKeyName>
            <includeMdcKeyName>spanId</includeMdcKeyName>
            <customFields>{"service":"${appName}"}</customFields>
        </encoder>
    </appender>

    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss} %-5level [%X{traceId:-},%X{spanId:-}] %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <!-- Humans read the console locally; log aggregators need JSON everywhere else. -->
    <springProfile name="!local">
        <root level="INFO"><appender-ref ref="JSON"/></root>
    </springProfile>
    <springProfile name="local">
        <root level="INFO"><appender-ref ref="CONSOLE"/></root>
    </springProfile>
</configuration>
```

- [ ] **Step 7: Run test to verify it passes**

Run: `./gradlew :order-api:test --tests '*TracePropagationIT'`
Expected: PASS.

- [ ] **Step 8: Verify metrics are exposed**

Run `./gradlew :order-service:bootRun`, then:
`curl -s localhost:9090/actuator/prometheus | grep -c grpc`
Expected: a non-zero count of gRPC server metrics.

- [ ] **Step 9: Commit**

```bash
git add order-service order-api
git commit -m "feat: metrics, cross-boundary tracing, and structured JSON logging"
```

---

### Task 17: TLS and JWT authentication

**Files:**
- Create: `scripts/generate-dev-certs.sh`
- Create: `testing-support/src/main/java/com/example/order/testing/JwtTestTokens.java`
- Create: `order-service/src/main/java/com/example/order/service/grpc/AuthInterceptor.java`
- Create: `order-api/src/main/java/com/example/order/api/config/SecurityConfig.java`
- Create: `order-api/src/main/java/com/example/order/api/client/AuthPropagatingInterceptor.java`
- Modify: both `application.yml` files, both `build.gradle.kts` files, `.gitignore`
- Test: `order-service/src/test/java/com/example/order/service/grpc/AuthInterceptorIT.java`

**Interfaces:**
- Consumes: Task 8 interceptor chain, Task 10 client.
- Produces: `AuthInterceptor` populating `AuthInterceptor.PRINCIPAL_KEY` (a `Context.Key<String>`) and rejecting bad tokens with `UNAUTHENTICATED`; `AuthPropagatingInterceptor` copying the edge's bearer token into gRPC `authorization` metadata; `JwtTestTokens.signedToken(String subject, List<String> scopes)`.

The gRPC service validates the token itself rather than trusting the edge. An edge that has been compromised, misconfigured, or simply bypassed on the internal network must not be able to assert identity on its own say-so.

- [ ] **Step 1: Add dependencies**

`order-api`: `implementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")` and `implementation("org.springframework.boot:spring-boot-starter-security")`.
`order-service`: `implementation("org.springframework.security:spring-security-oauth2-jose")`.
`testing-support`: `api("com.nimbusds:nimbus-jose-jwt:9.48")`.

- [ ] **Step 2: Write the failing test**

```java
package com.example.order.service.grpc;

import com.example.order.proto.*;
import com.example.order.testing.JwtTestTokens;
import com.example.order.testing.PostgresFixture;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest(properties = {"spring.grpc.server.port=0", "order.security.enabled=true"})
class AuthInterceptorIT {

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        PostgresFixture.registerProperties(registry);
        registry.add("order.security.jwk-set-uri", JwtTestTokens::jwkSetUri);
    }

    @Autowired ManagedChannel channel;

    private OrderServiceGrpc.OrderServiceBlockingStub stubWith(String token) {
        var metadata = new Metadata();
        if (token != null) {
            metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
                    "Bearer " + token);
        }
        return OrderServiceGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    }

    private CreateOrderRequest request() {
        return CreateOrderRequest.newBuilder()
                .setCustomerId("sec").setCurrency("USD")
                .setIdempotencyKey(UUID.randomUUID().toString())
                .addLines(OrderLine.newBuilder().setSku("S").setQuantity(1).setUnitPriceMinor(10).build())
                .build();
    }

    @Test
    void callWithoutTokenIsUnauthenticated() {
        var thrown = catchThrowableOfType(() -> stubWith(null).createOrder(request()),
                StatusRuntimeException.class);
        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    }

    @Test
    void callWithGarbageTokenIsUnauthenticated() {
        var thrown = catchThrowableOfType(() -> stubWith("not.a.jwt").createOrder(request()),
                StatusRuntimeException.class);
        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    }

    @Test
    void callWithValidTokenSucceeds() {
        var token = JwtTestTokens.signedToken("user-1", List.of("orders:write", "orders:read"));
        assertThat(stubWith(token).createOrder(request()).getOrder().getCustomerId()).isEqualTo("sec");
    }

    @Test
    void writeScopeIsRequiredForWrites() {
        var token = JwtTestTokens.signedToken("reader", List.of("orders:read"));
        var thrown = catchThrowableOfType(() -> stubWith(token).createOrder(request()),
                StatusRuntimeException.class);
        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    }

    @Test
    void healthChecksAreExemptFromAuth() {
        var response = io.grpc.health.v1.HealthGrpc.newBlockingStub(channel)
                .check(io.grpc.health.v1.HealthCheckRequest.newBuilder().build());
        assertThat(response.getStatus())
                .isEqualTo(io.grpc.health.v1.HealthCheckResponse.ServingStatus.SERVING);
    }
}
```

The last test matters operationally: if health checks require a token, Kubernetes probes fail and the pod is killed in a loop.

- [ ] **Step 3: Run test to verify it fails**

Run: `./gradlew :order-service:test --tests '*AuthInterceptorIT'`
Expected: FAIL — `JwtTestTokens` does not exist and calls succeed without a token.

- [ ] **Step 4: Write the test token minting fixture**

```java
package com.example.order.testing;

import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.List;

/** Local RSA keypair plus a tiny JWKS endpoint. Development only. */
public final class JwtTestTokens {

    private static final RSAKey KEY;
    private static final HttpServer JWKS_SERVER;

    static {
        try {
            KEY = new RSAKeyGenerator(2048).keyID("test-key").generate();
            JWKS_SERVER = HttpServer.create(new InetSocketAddress(0), 0);
            JWKS_SERVER.createContext("/jwks", exchange -> {
                byte[] body = ("{\"keys\":[" + KEY.toPublicJWK().toJSONString() + "]}")
                        .getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            JWKS_SERVER.start();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to start JWKS fixture", e);
        }
    }

    private JwtTestTokens() {
    }

    public static String jwkSetUri() {
        return "http://localhost:" + JWKS_SERVER.getAddress().getPort() + "/jwks";
    }

    public static String signedToken(String subject, List<String> scopes) {
        try {
            var claims = new JWTClaimsSet.Builder()
                    .subject(subject)
                    .issuer("https://dev.example.com")
                    .audience("order-api")
                    .claim("scope", String.join(" ", scopes))
                    .issueTime(Date.from(Instant.now()))
                    .expirationTime(Date.from(Instant.now().plusSeconds(600)))
                    .build();
            var jwt = new SignedJWT(
                    new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(KEY.getKeyID()).build(), claims);
            jwt.sign(new RSASSASigner(KEY));
            return jwt.serialize();
        } catch (JOSEException e) {
            throw new IllegalStateException("Failed to sign test token", e);
        }
    }
}
```

- [ ] **Step 5: Write the server auth interceptor**

```java
package com.example.order.service.grpc;

import io.grpc.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;

import java.util.List;
import java.util.Set;

/**
 * Validates the bearer token at the gRPC boundary. The REST edge already validated
 * it, but this service does not trust the edge: anything on the network can open a
 * channel, and an internal caller is not automatically an authorized one.
 */
public class AuthInterceptor implements ServerInterceptor {

    public static final Context.Key<String> PRINCIPAL_KEY = Context.key("principal");
    public static final Context.Key<List<String>> SCOPES_KEY = Context.key("scopes");

    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    // Probes must work without credentials or Kubernetes restarts the pod forever.
    private static final Set<String> EXEMPT_SERVICES =
            Set.of("grpc.health.v1.Health", "grpc.reflection.v1alpha.ServerReflection",
                   "grpc.reflection.v1.ServerReflection");

    private static final Set<String> WRITE_METHODS = Set.of("CreateOrder", "UpdateOrderStatus");

    private final JwtDecoder decoder;

    public AuthInterceptor(JwtDecoder decoder) {
        this.decoder = decoder;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        var descriptor = call.getMethodDescriptor();
        if (EXEMPT_SERVICES.contains(descriptor.getServiceName())) {
            return next.startCall(call, headers);
        }

        String header = headers.get(AUTHORIZATION);
        if (header == null || !header.startsWith("Bearer ")) {
            return close(call, Status.UNAUTHENTICATED.withDescription("Missing bearer token"));
        }

        try {
            var jwt = decoder.decode(header.substring("Bearer ".length()));
            List<String> scopes = List.of(
                    String.valueOf(jwt.getClaims().getOrDefault("scope", "")).split(" "));

            String required = WRITE_METHODS.contains(descriptor.getBareMethodName())
                    ? "orders:write" : "orders:read";
            if (!scopes.contains(required)) {
                return close(call, Status.PERMISSION_DENIED
                        .withDescription("Requires scope " + required));
            }

            Context context = Context.current()
                    .withValue(PRINCIPAL_KEY, jwt.getSubject())
                    .withValue(SCOPES_KEY, scopes);
            return Contexts.interceptCall(context, call, headers, next);
        } catch (JwtException e) {
            // The reason is deliberately not echoed: it tells an attacker which part
            // of their forged token to fix.
            return close(call, Status.UNAUTHENTICATED.withDescription("Invalid token"));
        }
    }

    private <ReqT, RespT> ServerCall.Listener<ReqT> close(ServerCall<ReqT, RespT> call, Status status) {
        call.close(status, new Metadata());
        return new ServerCall.Listener<>() {
        };
    }
}
```

Register it in `GrpcServerConfig`, ordered after the observation interceptor and before the exception interceptor, gated on `order.security.enabled`:

```java
    @Bean
    @GlobalServerInterceptor
    @Order(Ordered.HIGHEST_PRECEDENCE + 50)
    @ConditionalOnProperty(name = "order.security.enabled", havingValue = "true")
    public AuthInterceptor authInterceptor(JwtDecoder decoder) {
        return new AuthInterceptor(decoder);
    }

    @Bean
    @ConditionalOnProperty(name = "order.security.enabled", havingValue = "true")
    public JwtDecoder jwtDecoder(@Value("${order.security.jwk-set-uri}") String jwkSetUri) {
        return org.springframework.security.oauth2.jwt.NimbusJwtDecoder
                .withJwkSetUri(jwkSetUri).build();
    }
```

- [ ] **Step 6: Propagate the token from the edge**

```java
package com.example.order.api.client;

import io.grpc.*;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

/**
 * Copies the caller's bearer token into gRPC metadata. Reactor context, not
 * ThreadLocal, is the source of truth in WebFlux, so the token is read from the
 * call options populated per-request rather than from SecurityContextHolder.
 */
public class AuthPropagatingInterceptor implements ClientInterceptor {

    public static final CallOptions.Key<String> BEARER_TOKEN =
            CallOptions.Key.createWithDefault("bearer-token", null);

    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                String token = callOptions.getOption(BEARER_TOKEN);
                if (token != null) {
                    headers.put(AUTHORIZATION, "Bearer " + token);
                }
                super.start(responseListener, headers);
            }
        };
    }
}
```

Add a `withToken(String)` method to `OrderGrpcClient` returning a stub carrying `BEARER_TOKEN`, and have controllers pass the token obtained from the `JwtAuthenticationToken` in the reactive security context.

- [ ] **Step 7: Secure the REST edge**

```java
package com.example.order.api.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;

@Configuration
@EnableWebFluxSecurity
public class SecurityConfig {

    @Bean
    public SecurityWebFilterChain filterChain(ServerHttpSecurity http) {
        return http
                .csrf(ServerHttpSecurity.CsrfSpec::disable)   // stateless bearer-token API
                .authorizeExchange(exchanges -> exchanges
                        .pathMatchers("/actuator/health/**", "/actuator/info").permitAll()
                        .pathMatchers("/actuator/**").authenticated()
                        .anyExchange().authenticated())
                .oauth2ResourceServer(oauth2 -> oauth2.jwt(jwt -> {
                }))
                .build();
    }
}
```

- [ ] **Step 8: Generate development certificates**

Create `scripts/generate-dev-certs.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

# Development certificates only. Never commit the output; never use in production.
OUT="${1:-certs}"
mkdir -p "$OUT"

openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout "$OUT/ca.key" -out "$OUT/ca.crt" \
  -subj "/CN=order-dev-ca"

openssl req -newkey rsa:2048 -nodes \
  -keyout "$OUT/server.key" -out "$OUT/server.csr" \
  -subj "/CN=order-service"

openssl x509 -req -in "$OUT/server.csr" -days 365 \
  -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" -CAcreateserial \
  -out "$OUT/server.crt" \
  -extfile <(printf "subjectAltName=DNS:localhost,DNS:order-service,IP:127.0.0.1")

rm -f "$OUT/server.csr"
echo "Development certificates written to $OUT/"
```

Make it executable (`chmod +x scripts/generate-dev-certs.sh`) and add `certs/` to `.gitignore`.

Add TLS configuration to `order-service/src/main/resources/application.yml`:

```yaml
spring:
  grpc:
    server:
      ssl:
        enabled: ${GRPC_TLS_ENABLED:false}
        certificate: ${GRPC_TLS_CERT:file:certs/server.crt}
        private-key: ${GRPC_TLS_KEY:file:certs/server.key}
```

and to `order-api`'s channel:

```yaml
        negotiation-type: ${GRPC_NEGOTIATION:plaintext}
        ssl:
          trust-store: ${GRPC_TRUST_STORE:file:certs/ca.crt}
```

TLS defaults to off so tests and local runs work without certificates, and the compose stack turns it on explicitly.

- [ ] **Step 9: Run tests to verify they pass**

Run: `./gradlew :order-service:test --tests '*AuthInterceptorIT'`
Expected: PASS, all five tests.

Then run the full build: `./gradlew build`. Existing tests must still pass, because `order.security.enabled` defaults to `false`.

- [ ] **Step 10: Commit**

```bash
git add order-service order-api testing-support scripts .gitignore
git commit -m "feat: JWT authentication at both boundaries and TLS support"
```

---

## Phase 6 — Delivery

### Task 18: Containers and the local stack

**Files:**
- Create: `docker/Dockerfile.service`, `docker/Dockerfile.api`, `docker/compose.yaml`, `docker/prometheus.yml`
- Create: `docker/grafana/provisioning/datasources/prometheus.yml`
- Test: manual verification steps below

**Interfaces:**
- Consumes: everything built so far.
- Produces: `docker compose -f docker/compose.yaml up` bringing up Postgres, Jaeger, Prometheus, Grafana, and both services.

- [ ] **Step 1: Write the service Dockerfile**

Create `docker/Dockerfile.service`:

```dockerfile
# syntax=docker/dockerfile:1
FROM gradle:8-jdk21 AS build
WORKDIR /src
COPY . .
RUN ./gradlew :order-service:bootJar --no-daemon

FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S app && adduser -S app -G app
WORKDIR /app
COPY --from=build /src/order-service/build/libs/*.jar app.jar
USER app
EXPOSE 9443 9090
# Container-aware heap sizing; a fixed -Xmx ignores the cgroup limit.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
```

`docker/Dockerfile.api` is identical except for `:order-api`, `EXPOSE 8080 8081`.

- [ ] **Step 2: Write the compose stack**

Create `docker/compose.yaml`:

```yaml
name: order-platform

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: orders
      POSTGRES_USER: orders
      POSTGRES_PASSWORD: orders
    ports: ["5432:5432"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U orders"]
      interval: 5s
      timeout: 3s
      retries: 10

  jaeger:
    image: jaegertracing/all-in-one:1.62
    environment:
      COLLECTOR_OTLP_ENABLED: "true"
    ports: ["16686:16686", "4318:4318"]

  prometheus:
    image: prom/prometheus:v2.55.1
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
    ports: ["9091:9090"]

  grafana:
    image: grafana/grafana:11.3.0
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
    ports: ["3000:3000"]
    depends_on: [prometheus]

  order-service:
    build:
      context: ..
      dockerfile: docker/Dockerfile.service
    environment:
      SPRING_DATASOURCE_URL: jdbc:postgresql://postgres:5432/orders
      SPRING_DATASOURCE_USERNAME: orders
      SPRING_DATASOURCE_PASSWORD: orders
      OTLP_ENDPOINT: http://jaeger:4318/v1/traces
    ports: ["9443:9443", "9090:9090"]
    depends_on:
      postgres: { condition: service_healthy }
    healthcheck:
      # A native gRPC probe, not a TCP check: an open port proves nothing about
      # whether the service can actually serve.
      test: ["CMD", "wget", "-qO-", "http://localhost:9090/actuator/health/readiness"]
      interval: 10s
      timeout: 3s
      retries: 6
      start_period: 30s

  order-api:
    build:
      context: ..
      dockerfile: docker/Dockerfile.api
    environment:
      SPRING_GRPC_CLIENT_CHANNELS_ORDER-SERVICE_ADDRESS: static://order-service:9443
      OTLP_ENDPOINT: http://jaeger:4318/v1/traces
    ports: ["8080:8080", "8081:8081"]
    depends_on:
      order-service: { condition: service_healthy }
```

Create `docker/prometheus.yml`:

```yaml
global:
  scrape_interval: 10s

scrape_configs:
  - job_name: order-service
    metrics_path: /actuator/prometheus
    static_configs: [{ targets: ["order-service:9090"] }]
  - job_name: order-api
    metrics_path: /actuator/prometheus
    static_configs: [{ targets: ["order-api:8081"] }]
```

Create `docker/grafana/provisioning/datasources/prometheus.yml`:

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

Create `docker/grafana/provisioning/dashboards/dashboards.yml`:

```yaml
apiVersion: 1
providers:
  - name: order-platform
    type: file
    options:
      path: /etc/grafana/provisioning/dashboards
```

Create `docker/grafana/provisioning/dashboards/order-platform.json` — a dashboard with four panels, each built from a metric this build actually emits:

| Panel | PromQL |
|---|---|
| gRPC server request rate by method | `sum by (method) (rate(grpc_server_requests_seconds_count[1m]))` |
| gRPC server p99 latency | `histogram_quantile(0.99, sum by (le, method) (rate(grpc_server_requests_seconds_bucket[5m])))` |
| Error rate by status code | `sum by (statusCode) (rate(grpc_server_requests_seconds_count{statusCode!="OK"}[1m]))` |
| Circuit breaker state | `resilience4j_circuitbreaker_state{name="order-service"}` |

Verify each query returns data in Grafana's explore view before committing the dashboard; a panel wired to a metric name that does not exist is worse than no panel, because it reads as "zero errors".

Add the volume mount to the `grafana` service in compose (already covered by mounting `./grafana/provisioning`).

- [ ] **Step 3: Bring the stack up**

Run: `docker compose -f docker/compose.yaml up --build -d`
Then: `docker compose -f docker/compose.yaml ps`
Expected: all six services healthy or running.

- [ ] **Step 4: Verify the system end to end**

```bash
curl -sS -X POST localhost:8080/api/v1/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"demo","currency":"USD","idempotencyKey":"demo-1",
       "lines":[{"sku":"WIDGET","quantity":3,"unitPriceMinor":499}]}'
```

Expected: `201` with `"totalMinor": 1497` and `"status": "PENDING"`.

Then confirm each production concern actually works:
- Replay the identical command. Expected: `200`, same `id`, no second order.
- `curl -s localhost:8080/api/v1/orders?customerId=demo` returns the order.
- Open `http://localhost:16686` and find a trace spanning `order-api` and `order-service`.
- `curl -s localhost:9091/api/v1/targets | grep -o '"health":"up"'` shows both targets up.
- `docker compose -f docker/compose.yaml stop order-service`, then `curl -i localhost:8080/api/v1/orders/<id>` a dozen times. Expected: `503` with a `Retry-After` header once the breaker opens.
- Restart it and confirm requests succeed again without restarting `order-api`.

- [ ] **Step 5: Commit**

```bash
git add docker
git commit -m "feat(ops): Dockerfiles and full local observability stack"
```

---

### Task 19: CI pipeline and Kubernetes manifests

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `k8s/order-service.yaml`, `k8s/order-api.yaml`
- Create: `README.md`

**Interfaces:**
- Consumes: everything.
- Produces: CI running build, tests, and `buf` checks; manifests with correct probe wiring.

- [ ] **Step 1: Write the CI workflow**

```yaml
name: CI

on:
  push: { branches: [main] }
  pull_request:

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0   # buf breaking needs history to compare against main
      - uses: actions/setup-java@v4
        with:
          java-version: '21'
          distribution: temurin
      - uses: gradle/actions/setup-gradle@v4

      - name: Build and test
        run: ./gradlew build --no-daemon

      - uses: bufbuild/buf-setup-action@v1
      - name: Lint protos
        run: buf lint
        working-directory: proto
      - name: Detect breaking proto changes
        if: github.event_name == 'pull_request'
        run: buf breaking --against "https://github.com/${{ github.repository }}.git#branch=main,subdir=proto"
        working-directory: proto

      - name: Build images
        run: |
          docker build -f docker/Dockerfile.service -t order-service:ci .
          docker build -f docker/Dockerfile.api -t order-api:ci .

      - uses: actions/upload-artifact@v4
        if: failure()
        with:
          name: test-reports
          path: '**/build/reports/tests/**'
```

Testcontainers works on `ubuntu-latest` without extra setup because Docker is preinstalled.

- [ ] **Step 2: Write the Kubernetes manifests**

`k8s/order-service.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-service
spec:
  replicas: 2
  selector:
    matchLabels: { app: order-service }
  template:
    metadata:
      labels: { app: order-service }
    spec:
      # Must exceed the 2s health-flip delay plus the 30s drain from Task 15.
      terminationGracePeriodSeconds: 45
      containers:
        - name: order-service
          image: order-service:latest
          ports:
            - { name: grpc, containerPort: 9443 }
            - { name: mgmt, containerPort: 9090 }
          env:
            - name: OTLP_ENDPOINT
              value: http://jaeger-collector:4318/v1/traces
          resources:
            requests: { cpu: 250m, memory: 512Mi }
            limits: { memory: 1Gi }
          startupProbe:
            grpc: { port: 9443, service: order.v1.OrderService }
            failureThreshold: 30
            periodSeconds: 2
          readinessProbe:
            grpc: { port: 9443, service: order.v1.OrderService }
            periodSeconds: 5
          livenessProbe:
            httpGet: { path: /actuator/health/liveness, port: mgmt }
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: order-service
spec:
  selector: { app: order-service }
  ports:
    - { name: grpc, port: 9443, targetPort: grpc }
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: order-service
spec:
  minAvailable: 1
  selector:
    matchLabels: { app: order-service }
```

The probe split is deliberate: readiness uses the gRPC health service, so a pod leaves the load balancer the moment `ShutdownConfig` flips it to `NOT_SERVING`; liveness uses the separate management port, so a saturated gRPC server is not mistaken for a dead one and restarted.

`k8s/order-api.yaml` follows the same shape with HTTP probes on `/actuator/health/readiness` and `/actuator/health/liveness` (port 8081), `replicas: 2`, and `SPRING_GRPC_CLIENT_CHANNELS_ORDER-SERVICE_ADDRESS: static://order-service:9443`.

- [ ] **Step 3: Validate the manifests**

Run: `kubectl apply --dry-run=client -f k8s/`
Expected: every object validates. Skip if `kubectl` is unavailable; note it in the commit message rather than deleting the files.

- [ ] **Step 4: Write the README**

Cover: what the two services are and why the REST edge uses gRPC stubs rather than `RestClient`; the module layout; how to run tests (`./gradlew build`, Docker required); how to run the stack (`docker compose -f docker/compose.yaml up --build`); the REST endpoint table; the gRPC status to HTTP status mapping table from the spec; how to explore with `grpcurl` (reflection is enabled); where Jaeger, Prometheus and Grafana live; and how to turn on TLS and JWT.

- [ ] **Step 5: Final verification**

Run: `./gradlew clean build`
Expected: BUILD SUCCESSFUL with every test green.

Run: `docker compose -f docker/compose.yaml up --build -d` and repeat the Task 18 Step 4 checks.

- [ ] **Step 6: Commit**

```bash
git add .github k8s README.md
git commit -m "ci: build pipeline with proto breaking-change detection, and k8s manifests"
```

---

## Verification Checklist

Run through this before declaring the plan complete.

- [ ] `./gradlew clean build` passes from scratch.
- [ ] `buf lint` passes; `buf breaking` runs in CI on pull requests.
- [ ] A duplicate `CreateOrder` returns the original order, and the concurrent-create test proves it under contention.
- [ ] `UpdateOrderStatus` with a stale version returns `ABORTED` at the gRPC layer and `409` at the REST layer.
- [ ] An illegal transition returns `FAILED_PRECONDITION` and `409`, and writes no event.
- [ ] Stopping `order-service` yields `503` with `Retry-After`; restarting it recovers without restarting `order-api`.
- [ ] A malformed page token yields `400`, never `500`.
- [ ] One HTTP request produces one Jaeger trace spanning both services and the database.
- [ ] `grpcurl -plaintext localhost:9443 grpc.health.v1.Health/Check` returns `SERVING`, and `NOT_SERVING` during shutdown.
- [ ] Health and reflection remain reachable when `order.security.enabled=true`.
- [ ] `/actuator/prometheus` on both services exposes gRPC and Resilience4j metrics.
- [ ] SSE at `/api/v1/orders/events` delivers events as statuses change.
