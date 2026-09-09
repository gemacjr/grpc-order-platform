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
    implementation(libs.spring.grpc.server)
    implementation(libs.grpc.services)
    implementation("org.springframework.security:spring-security-oauth2-jose")
    implementation("com.google.api.grpc:proto-google-common-protos")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    implementation("io.micrometer:micrometer-registry-prometheus")
    implementation(libs.micrometer.tracing.otel)
    implementation(libs.otel.exporter.otlp)
    // Boot's BOM manages micrometer-tracing-bridge-otel but the tracing
    // autoconfiguration itself (Tracer/Propagator beans, OTLP span exporter wiring)
    // lives in this separate module and is not pulled in transitively - without it
    // no Tracer bean exists and traceparent is never propagated. Not in the brief;
    // added because the bridge alone left TracePropagationIT failing.
    implementation("org.springframework.boot:spring-boot-micrometer-tracing-opentelemetry")
    runtimeOnly(libs.logstash.encoder)
    runtimeOnly(libs.flyway.postgres)
    runtimeOnly("org.postgresql:postgresql")

    testImplementation(project(":testing-support"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation(libs.spring.grpc.test)
}
