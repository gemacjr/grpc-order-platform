package com.example.order.api.client;

import com.example.order.api.client.exception.RemoteUnavailable;
import com.example.order.proto.*;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;

/**
 * Same surface as OrderGrpcClient, wrapped in a breaker and a bulkhead.
 *
 * The streaming call is deliberately NOT wrapped: a long-lived stream would hold a
 * bulkhead permit for its entire lifetime and starve unary calls, and a single
 * stream ending is not evidence the backend is unhealthy.
 */
public class ResilientOrderClient {

    final OrderGrpcClient delegate;
    private final CircuitBreaker breaker;
    private final Bulkhead bulkhead;

    public ResilientOrderClient(OrderGrpcClient delegate, CircuitBreaker breaker, Bulkhead bulkhead) {
        this.delegate = delegate;
        this.breaker = breaker;
        this.bulkhead = bulkhead;
    }

    public Mono<CreateOrderResponse> create(String customerId, List<OrderLine> lines,
                                            String currency, String idempotencyKey) {
        return guard(delegate.create(customerId, lines, currency, idempotencyKey));
    }

    public Mono<Order> get(UUID id) {
        return guard(delegate.get(id));
    }

    public Mono<ListOrdersResponse> list(String customerId, OrderStatus status, int pageSize, String pageToken) {
        return guard(delegate.list(customerId, status, pageSize, pageToken));
    }

    public Mono<Order> updateStatus(UUID id, OrderStatus newStatus, long expectedVersion) {
        return guard(delegate.updateStatus(id, newStatus, expectedVersion));
    }

    public Flux<OrderEvent> streamEvents(String customerId, long fromSequence) {
        return delegate.streamEvents(customerId, fromSequence);
    }

    private <T> Mono<T> guard(Mono<T> source) {
        return source
                .transformDeferred(BulkheadOperator.of(bulkhead))
                .transformDeferred(CircuitBreakerOperator.of(breaker))
                // A rejected call is indistinguishable from an unreachable backend
                // as far as the caller is concerned, so it maps to the same 503.
                .onErrorMap(CallNotPermittedException.class,
                        e -> new RemoteUnavailable("Order service circuit is open", e));
    }
}
