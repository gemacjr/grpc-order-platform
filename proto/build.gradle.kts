import com.google.protobuf.gradle.id

plugins {
    java
    `java-library`
    alias(libs.plugins.protobuf)
    alias(libs.plugins.spring.dependency.management)
}

dependencyManagement {
    imports {
        mavenBom(libs.spring.grpc.bom.get().toString())
        // reactor-grpc-stub declares reactor-core as Maven "provided" scope, which Gradle drops
        // entirely when consuming the POM; pull the version from Spring Boot's BOM instead so it
        // stays aligned with the reactor-core the rest of the platform (order-api/order-service) uses.
        mavenBom("org.springframework.boot:spring-boot-dependencies:${libs.versions.springBoot.get()}")
    }
}

dependencies {
    api(libs.protobuf.java)
    api(libs.protobuf.java.util)
    api(libs.grpc.stub)
    api(libs.grpc.protobuf)
    api(libs.reactor.grpc.stub)
    api("io.projectreactor:reactor-core")
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
        id("reactor") { artifact = "com.salesforce.servicelibs:reactor-grpc:$reactorGrpcVersion:${osdetector.classifier}@exe" }
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
