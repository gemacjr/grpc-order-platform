package com.example.order.api.web;

import com.example.order.api.client.ResilientOrderClient;
import com.example.order.api.client.exception.*;
import com.example.order.proto.CreateOrderResponse;
import com.example.order.proto.Order;
import com.example.order.proto.OrderEvent;
import com.example.order.proto.OrderStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@WebFluxTest(controllers = {OrderController.class, OrderEventSseController.class})
class OrderControllerTest {

    @Autowired WebTestClient web;
    @MockitoBean ResilientOrderClient client;

    private static final String BODY = """
            {"customerId":"c1","currency":"USD","idempotencyKey":"k1",
             "lines":[{"sku":"S1","quantity":2,"unitPriceMinor":1000}]}
            """;

    @Test
    void createReturns201WithLocation() {
        var id = UUID.randomUUID();
        when(client.create(any(), any(), any(), any())).thenReturn(Mono.just(
                CreateOrderResponse.newBuilder()
                        .setOrder(Order.newBuilder().setId(id.toString()).setCustomerId("c1")
                                .setStatus(OrderStatus.ORDER_STATUS_PENDING).setVersion(1).build())
                        .setIdempotentReplay(false).build()));

        web.post().uri("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).bodyValue(BODY)
                .exchange()
                .expectStatus().isCreated()
                .expectHeader().valueEquals("Location", "/api/v1/orders/" + id)
                .expectBody().jsonPath("$.status").isEqualTo("PENDING");
    }

    @Test
    void idempotentReplayReturns200NotCreated() {
        var id = UUID.randomUUID();
        when(client.create(any(), any(), any(), any())).thenReturn(Mono.just(
                CreateOrderResponse.newBuilder()
                        .setOrder(Order.newBuilder().setId(id.toString()).setVersion(1).build())
                        .setIdempotentReplay(true).build()));

        web.post().uri("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).bodyValue(BODY)
                .exchange().expectStatus().isOk();
    }

    @Test
    void localBeanValidationFailureBecomes400ProblemDetail() {
        // No mocking of `client` needed: @Valid rejects this before the controller
        // method - and therefore the client - is ever invoked.
        web.post().uri("/api/v1/orders").contentType(MediaType.APPLICATION_JSON)
                .bodyValue("""
                        {"customerId":"","currency":"USD","idempotencyKey":"k1","lines":[]}
                        """)
                .exchange()
                .expectStatus().isBadRequest()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON);

        verifyNoInteractions(client);
    }

    @Test
    void notFoundBecomes404ProblemDetail() {
        when(client.get(any())).thenReturn(Mono.error(new RemoteOrderNotFound("gone")));

        web.get().uri("/api/v1/orders/" + UUID.randomUUID()).exchange()
                .expectStatus().isNotFound()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody().jsonPath("$.title").isEqualTo("Order not found");
    }

    @Test
    void validationFailureBecomes400WithFieldErrors() {
        when(client.get(any())).thenReturn(Mono.error(new RemoteValidationFailed("bad",
                List.of(new RemoteFieldViolation("currency", "must be 3 letters")))));

        web.get().uri("/api/v1/orders/" + UUID.randomUUID()).exchange()
                .expectStatus().isBadRequest()
                .expectBody().jsonPath("$.errors[0].field").isEqualTo("currency");
    }

    @Test
    void versionConflictBecomes409AndIllegalTransitionAlsoBecomes409() {
        when(client.get(any())).thenReturn(Mono.error(new RemoteVersionConflict("stale")));
        web.get().uri("/api/v1/orders/" + UUID.randomUUID()).exchange().expectStatus().isEqualTo(409);

        when(client.get(any())).thenReturn(Mono.error(new RemoteIllegalTransition("no")));
        web.get().uri("/api/v1/orders/" + UUID.randomUUID()).exchange().expectStatus().isEqualTo(409);
    }

    @Test
    void unavailableBecomes503WithRetryAfter() {
        when(client.get(any())).thenReturn(Mono.error(new RemoteUnavailable("down", null)));

        web.get().uri("/api/v1/orders/" + UUID.randomUUID()).exchange()
                .expectStatus().isEqualTo(503)
                .expectHeader().exists("Retry-After");
    }

    @Test
    void timeoutBecomes504() {
        when(client.get(any())).thenReturn(Mono.error(new RemoteTimeout("slow", null)));
        web.get().uri("/api/v1/orders/" + UUID.randomUUID()).exchange().expectStatus().isEqualTo(504);
    }

    @Test
    void internalErrorIsOpaqueButKeepsTheErrorId() {
        when(client.get(any())).thenReturn(Mono.error(new RemoteInternal("boom", "err-42", null)));

        web.get().uri("/api/v1/orders/" + UUID.randomUUID()).exchange()
                .expectStatus().isEqualTo(500)
                .expectBody()
                .jsonPath("$.errorId").isEqualTo("err-42")
                .jsonPath("$.detail").value(d -> org.assertj.core.api.Assertions
                        .assertThat((String) d).doesNotContain("boom"));
    }

    @Test
    void unauthenticatedBecomes401() {
        when(client.get(any())).thenReturn(Mono.error(new RemoteUnauthenticated("missing token")));
        web.get().uri("/api/v1/orders/" + UUID.randomUUID()).exchange()
                .expectStatus().isUnauthorized()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody().jsonPath("$.title").isEqualTo("Unauthenticated");
    }

    @Test
    void permissionDeniedBecomes403() {
        when(client.get(any())).thenReturn(Mono.error(new RemotePermissionDenied("missing scope")));
        web.get().uri("/api/v1/orders/" + UUID.randomUUID()).exchange()
                .expectStatus().isForbidden()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody().jsonPath("$.title").isEqualTo("Permission denied");
    }

    @Test
    void unknownStatusFilterReturns400NotServerError() {
        web.get().uri("/api/v1/orders?status=bogus").exchange()
                .expectStatus().isBadRequest()
                .expectBody().jsonPath("$.errors[0].field").isEqualTo("status");

        verifyNoInteractions(client);
    }

    @Test
    void unknownStatusInPatchBodyReturns400() {
        web.patch().uri("/api/v1/orders/" + UUID.randomUUID() + "/status")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("""
                        {"status":"bogus","expectedVersion":1}
                        """)
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void eventsAreStreamedAsServerSentEvents() {
        when(client.streamEvents(any(), anyLong())).thenReturn(Flux.just(
                OrderEvent.newBuilder().setSequence(1).setOrderId(UUID.randomUUID().toString()).build()));

        web.get().uri("/api/v1/orders/events").accept(MediaType.TEXT_EVENT_STREAM).exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM);
    }
}
