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
