package com.example.order.api;

import com.example.order.testing.PostgresFixture;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.util.UUID;

/**
 * Boots the real gRPC service and the real REST edge in one JVM and drives them
 * over a real channel. Every other test in this build mocks one side; this is the
 * only one that proves the two sides agree about the contract.
 */
@SpringBootTest(classes = {OrderApiApplication.class, com.example.order.service.OrderServiceApplication.class},
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"spring.grpc.server.port=19443",
                      "spring.grpc.client.channels.order-service.address=static://localhost:19443"})
@AutoConfigureWebTestClient
class EndToEndIT {

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        PostgresFixture.registerProperties(registry);
    }

    @Autowired WebTestClient web;

    @Test
    void createReadUpdateAndListRoundTripThroughGrpc() {
        String key = UUID.randomUUID().toString();
        String body = """
                {"customerId":"e2e","currency":"USD","idempotencyKey":"%s",
                 "lines":[{"sku":"S1","quantity":2,"unitPriceMinor":1500}]}
                """.formatted(key);

        var created = web.post().uri("/api/v1/orders")
                .contentType(MediaType.APPLICATION_JSON).bodyValue(body)
                .exchange().expectStatus().isCreated()
                .expectBody()
                .jsonPath("$.totalMinor").isEqualTo(3000)
                .jsonPath("$.status").isEqualTo("PENDING")
                .returnResult();

        String id = com.jayway.jsonpath.JsonPath.read(new String(created.getResponseBody()), "$.id");

        web.get().uri("/api/v1/orders/{id}", id).exchange()
                .expectStatus().isOk().expectBody().jsonPath("$.id").isEqualTo(id);

        web.patch().uri("/api/v1/orders/{id}/status", id)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"status\":\"CONFIRMED\",\"expectedVersion\":1}")
                .exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.status").isEqualTo("CONFIRMED")
                .jsonPath("$.version").isEqualTo(2);

        // Replay: same idempotency key must return 200, not create a second order.
        web.post().uri("/api/v1/orders")
                .contentType(MediaType.APPLICATION_JSON).bodyValue(body)
                .exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.id").isEqualTo(id);

        web.get().uri("/api/v1/orders?customerId=e2e&pageSize=10").exchange()
                .expectStatus().isOk().expectBody().jsonPath("$.orders").isArray();
    }

    @Test
    void staleVersionSurfacesAs409ThroughTheWholeStack() {
        String body = """
                {"customerId":"e2e-conflict","currency":"USD","idempotencyKey":"%s",
                 "lines":[{"sku":"S1","quantity":1,"unitPriceMinor":100}]}
                """.formatted(UUID.randomUUID());

        var created = web.post().uri("/api/v1/orders")
                .contentType(MediaType.APPLICATION_JSON).bodyValue(body)
                .exchange().expectStatus().isCreated().expectBody().returnResult();
        String id = com.jayway.jsonpath.JsonPath.read(new String(created.getResponseBody()), "$.id");

        web.patch().uri("/api/v1/orders/{id}/status", id)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"status\":\"CONFIRMED\",\"expectedVersion\":1}")
                .exchange().expectStatus().isOk();

        web.patch().uri("/api/v1/orders/{id}/status", id)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"status\":\"SHIPPED\",\"expectedVersion\":1}")
                .exchange().expectStatus().isEqualTo(409)
                .expectBody().jsonPath("$.title").isEqualTo("Version conflict");
    }

    @Test
    void unknownOrderSurfacesAs404ProblemDetail() {
        web.get().uri("/api/v1/orders/{id}", UUID.randomUUID()).exchange()
                .expectStatus().isNotFound()
                .expectBody().jsonPath("$.title").isEqualTo("Order not found");
    }
}
