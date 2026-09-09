package com.example.order.api.config;

import java.util.List;
import java.util.Map;

/**
 * gRPC's built-in retry, expressed as a service config. This is preferred over
 * application-level retry because grpc-java applies the backoff below the load
 * balancer, so a retry can land on a different, healthy backend. Jitter is applied
 * by the runtime; the multiplier and cap here bound the worst case.
 *
 * Every listed method is retry-safe by design, not by hope:
 * reads are idempotent, CreateOrder is keyed, UpdateOrderStatus is version-checked.
 */
public final class RetryPolicyConfig {

    private static final String SERVICE = "order.v1.OrderService";

    private RetryPolicyConfig() {
    }

    public static Map<String, ?> serviceConfig() {
        Map<String, ?> retryPolicy = Map.of(
                "maxAttempts", 4.0,
                "initialBackoff", "0.1s",
                "maxBackoff", "2s",
                "backoffMultiplier", 2.0,
                "retryableStatusCodes", List.of("UNAVAILABLE", "RESOURCE_EXHAUSTED"));

        return Map.of("methodConfig", List.of(Map.of(
                "name", List.of(
                        Map.of("service", SERVICE, "method", "GetOrder"),
                        Map.of("service", SERVICE, "method", "ListOrders"),
                        Map.of("service", SERVICE, "method", "CreateOrder"),
                        Map.of("service", SERVICE, "method", "UpdateOrderStatus")),
                "retryPolicy", retryPolicy)));
    }
}
