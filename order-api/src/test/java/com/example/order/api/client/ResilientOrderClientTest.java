package com.example.order.api.client;

import com.example.order.api.client.exception.RemoteUnavailable;
import com.example.order.proto.Order;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ResilientOrderClientTest {

    private ResilientOrderClient clientThatAlwaysFails(CircuitBreaker breaker) {
        var delegate = Mockito.mock(OrderGrpcClient.class);
        Mockito.when(delegate.get(Mockito.any()))
                .thenReturn(Mono.error(new RemoteUnavailable("down", null)));
        return new ResilientOrderClient(delegate, breaker,
                Bulkhead.of("test", BulkheadConfig.custom().maxConcurrentCalls(10).build()));
    }

    private CircuitBreaker breaker() {
        return CircuitBreaker.of("test", CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(4)
                .minimumNumberOfCalls(4)
                .failureRateThreshold(50f)
                .waitDurationInOpenState(Duration.ofMillis(200))
                .build());
    }

    @Test
    void breakerOpensAfterRepeatedFailures() {
        var breaker = breaker();
        var client = clientThatAlwaysFails(breaker);

        for (int i = 0; i < 4; i++) {
            StepVerifier.create(client.get(UUID.randomUUID())).expectError().verify();
        }
        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
    }

    @Test
    void openBreakerFailsFastAsRemoteUnavailable() {
        var breaker = breaker();
        var client = clientThatAlwaysFails(breaker);
        for (int i = 0; i < 4; i++) {
            StepVerifier.create(client.get(UUID.randomUUID())).expectError().verify();
        }

        // The point of the breaker: no call reaches the delegate now, and the caller
        // still gets a typed exception the advice can render as 503.
        StepVerifier.create(client.get(UUID.randomUUID()))
                .expectError(RemoteUnavailable.class)
                .verify();
    }

    @Test
    void breakerRecoversAfterTheWaitDuration() throws Exception {
        var breaker = breaker();
        var delegate = Mockito.mock(OrderGrpcClient.class);
        Mockito.when(delegate.get(Mockito.any()))
                .thenReturn(Mono.error(new RemoteUnavailable("down", null)));
        var client = new ResilientOrderClient(delegate, breaker,
                Bulkhead.of("test", BulkheadConfig.ofDefaults()));

        for (int i = 0; i < 4; i++) {
            StepVerifier.create(client.get(UUID.randomUUID())).expectError().verify();
        }
        Mockito.when(delegate.get(Mockito.any()))
                .thenReturn(Mono.just(Order.newBuilder().setId("ok").build()));

        Thread.sleep(300);
        breaker.transitionToHalfOpenState();

        StepVerifier.create(client.get(UUID.randomUUID()))
                .assertNext(o -> assertThat(o.getId()).isEqualTo("ok"))
                .verifyComplete();
    }
}
