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
