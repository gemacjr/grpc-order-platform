package com.example.order.api.observability;

import com.example.order.testing.PostgresFixture;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

// Deviations from the brief's verbatim test, both required to make it actually
// exercise what it claims to test:
//  1. @AutoConfigureWebTestClient - without it, WebTestClient has no bean definition
//     under WebEnvironment.RANDOM_PORT and the context fails to start.
//  2. CaptureConfig.class added to the `classes` attribute - when @SpringBootTest
//     is given an explicit `classes` list, Spring Boot Test does NOT additionally
//     auto-detect a nested @TestConfiguration the way it would with implicit
//     configuration detection. Without this, captureTraceparent() is silently never
//     registered as a bean, CAPTURED_TRACEPARENT never gets set by anything, and the
//     test "passes for the wrong reason" as a false negative (always null, look like
//     a genuine propagation failure) - confirmed by instrumenting the interceptor
//     itself, which never fired until this was added.
@SpringBootTest(classes = {com.example.order.api.OrderApiApplication.class,
                           com.example.order.service.OrderServiceApplication.class,
                           TracePropagationIT.CaptureConfig.class},
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"spring.grpc.server.port=19444",
                      "spring.grpc.client.channels.order-service.address=static://localhost:19444",
                      "management.tracing.sampling.probability=1.0"})
@AutoConfigureWebTestClient
class TracePropagationIT {

    private static final AtomicReference<String> CAPTURED_TRACEPARENT = new AtomicReference<>();

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        PostgresFixture.registerProperties(registry);
    }

    @TestConfiguration
    static class CaptureConfig {
        @Bean
        @org.springframework.grpc.server.GlobalServerInterceptor
        @org.springframework.core.annotation.Order(org.springframework.core.Ordered.LOWEST_PRECEDENCE)
        ServerInterceptor captureTraceparent() {
            return new ServerInterceptor() {
                @Override
                public <Q, S> ServerCall.Listener<Q> interceptCall(
                        ServerCall<Q, S> call, Metadata headers, ServerCallHandler<Q, S> next) {
                    var key = Metadata.Key.of("traceparent", Metadata.ASCII_STRING_MARSHALLER);
                    CAPTURED_TRACEPARENT.set(headers.get(key));
                    return next.startCall(call, headers);
                }
            };
        }
    }

    @Autowired WebTestClient web;

    @Test
    void traceContextCrossesTheGrpcBoundary() {
        web.post().uri("/api/v1/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("""
                        {"customerId":"trace","currency":"USD","idempotencyKey":"%s",
                         "lines":[{"sku":"S","quantity":1,"unitPriceMinor":100}]}
                        """.formatted(UUID.randomUUID()))
                .exchange().expectStatus().isCreated();

        // W3C traceparent must arrive in gRPC metadata. If this is null, the two
        // services will produce two disconnected traces and cross-service debugging
        // stops working - the single most common gap in "instrumented" systems.
        assertThat(CAPTURED_TRACEPARENT.get())
                .as("W3C traceparent propagated into gRPC metadata")
                .isNotNull()
                .startsWith("00-");
    }
}
