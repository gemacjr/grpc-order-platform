package com.example.order.api.config;

import io.micrometer.core.instrument.binder.grpc.ObservationGrpcClientInterceptor;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.client.GlobalClientInterceptor;

// Named explicitly to avoid a bean-name collision with the identically-named
// com.example.order.service.config.ObservabilityConfig class when both app
// contexts are combined (see that class's Javadoc for why).
@Configuration("orderApiObservabilityConfig")
public class ObservabilityConfig {

    @Bean
    @GlobalClientInterceptor
    public ObservationGrpcClientInterceptor observationGrpcClientInterceptor(ObservationRegistry registry) {
        return new ObservationGrpcClientInterceptor(registry);
    }
}
