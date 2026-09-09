package com.example.order.service.config;

import io.micrometer.core.instrument.binder.grpc.ObservationGrpcServerInterceptor;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.grpc.server.GlobalServerInterceptor;

// Named explicitly: order-api has its own class of the same simple name
// (com.example.order.api.config.ObservabilityConfig), and tests such as
// TracePropagationIT/EndToEndIT load both application contexts together.
// Spring's default bean-name generator uses the short class name, so without
// distinct names the two @Configuration classes collide with a
// ConflictingBeanDefinitionException when scanned into one context.
@Configuration("orderServiceObservabilityConfig")
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
