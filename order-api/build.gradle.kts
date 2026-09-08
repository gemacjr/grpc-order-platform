plugins {
    java
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

dependencies {
    implementation(platform(libs.spring.grpc.bom))
    implementation("org.springframework.boot:spring-boot-starter")
}

// TODO(task-10): remove once OrderApiApplication exists. Task 10 replaces this
// file wholesale; the replacement must NOT carry this over, or Task 18's
// `:order-api:bootJar` silently produces no jar and the Docker build fails.
tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    enabled = false
}
