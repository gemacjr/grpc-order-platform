package com.example.order.service.grpc;

import com.example.order.proto.*;
import com.example.order.testing.PostgresFixture;
import io.grpc.Context;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "spring.grpc.server.port=0")
@Import(GrpcStubTestConfig.class)
class OrderEventStreamIT {

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        PostgresFixture.registerProperties(registry);
    }

    @Autowired OrderServiceGrpc.OrderServiceBlockingStub stub;

    @Test
    void replaysHistoricalEventsFromSequence() {
        String customer = "stream-" + UUID.randomUUID();
        var created = stub.createOrder(CreateOrderRequest.newBuilder()
                .setCustomerId(customer).setCurrency("USD")
                .setIdempotencyKey(UUID.randomUUID().toString())
                .addLines(OrderLine.newBuilder().setSku("S").setQuantity(1).setUnitPriceMinor(100).build())
                .build()).getOrder();

        stub.updateOrderStatus(UpdateOrderStatusRequest.newBuilder()
                .setId(created.getId()).setNewStatus(OrderStatus.ORDER_STATUS_CONFIRMED)
                .setExpectedVersion(created.getVersion()).build());

        var events = stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                .streamOrderEvents(StreamOrderEventsRequest.newBuilder()
                        .setCustomerId(customer).setFromSequence(1).build());

        var first = events.next();
        assertThat(first.getOrderId()).isEqualTo(created.getId());
        assertThat(first.getNewStatus()).isEqualTo(OrderStatus.ORDER_STATUS_CONFIRMED);
        assertThat(first.getOldStatus()).isEqualTo(OrderStatus.ORDER_STATUS_PENDING);
    }

    @Test
    void streamEndsWhenTheDeadlineExpiresRatherThanHangingForever() {
        String customer = "quiet-" + UUID.randomUUID();
        long start = System.nanoTime();

        var events = stub.withDeadlineAfter(2, TimeUnit.SECONDS)
                .streamOrderEvents(StreamOrderEventsRequest.newBuilder()
                        .setCustomerId(customer).setFromSequence(1).build());

        assertThat(events.hasNext()).isFalse();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMs).isLessThan(6_000);
    }
}
