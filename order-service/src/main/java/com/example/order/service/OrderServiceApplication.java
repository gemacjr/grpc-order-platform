package com.example.order.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.grpc.server.autoconfigure.GrpcServerObservationAutoConfiguration;

// GrpcServerObservationAutoConfiguration registers an ObservationGrpcServerInterceptor
// bean (name "observationGrpcServerInterceptor") at @Order(0) with no
// @ConditionalOnMissingBean guard, which collides with the equivalently-named bean
// com.example.order.service.config.ObservabilityConfig defines at
// Ordered.HIGHEST_PRECEDENCE so the span wraps the whole call including error
// handling. Excluded here so ours is the only one registered.
@SpringBootApplication(exclude = GrpcServerObservationAutoConfiguration.class)
public class OrderServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
