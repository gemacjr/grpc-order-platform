plugins {
    java
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

dependencyManagement {
    imports {
        mavenBom(libs.spring.grpc.bom.get().toString())
        mavenBom(libs.testcontainers.bom.get().toString())
    }
}

dependencies {
    implementation(project(":proto"))
    implementation(libs.spring.grpc.client)
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("com.google.api.grpc:proto-google-common-protos")
    implementation(libs.resilience4j)
    implementation(libs.resilience4j.reactor)
    implementation("io.micrometer:micrometer-registry-prometheus")
    implementation(libs.micrometer.tracing.otel)
    implementation(libs.otel.exporter.otlp)
    // See order-service/build.gradle.kts for why this is required in addition to
    // the bridge artifact.
    implementation("org.springframework.boot:spring-boot-micrometer-tracing-opentelemetry")
    runtimeOnly(libs.logstash.encoder)
    // NOTE: spring-boot-starter-aop is not published for Spring Boot 4.0.8 (absent from
    // spring-boot-dependencies-4.0.8's BOM; Central has no such artifact past 4.0.0-M2).
    // It is not needed here anyway: ResilientOrderClient wires CircuitBreakerOperator /
    // BulkheadOperator programmatically rather than via @CircuitBreaker/@Bulkhead
    // annotations, so no AOP proxying is required. Deviation from the brief, documented
    // in task-14-report.md.

    testImplementation(project(":testing-support"))
    testImplementation(project(":order-service"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-webflux-test")
    testImplementation("org.springframework.boot:spring-boot-webtestclient")
    testImplementation("io.projectreactor:reactor-test")
}
