plugins {
    `java-library`
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
    // Not present in spring-boot-dependencies' own BOM (it arrives transitively only
    // through spring-security-oauth2-jose, which this module does not depend on), so
    // it is genuinely absent from managed dependencies here and must be pinned.
    api("com.nimbusds:nimbus-jose-jwt:9.48")
}
