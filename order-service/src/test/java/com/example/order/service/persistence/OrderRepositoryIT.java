package com.example.order.service.persistence;

import com.example.order.service.domain.*;
import com.example.order.testing.PostgresFixture;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest
class OrderRepositoryIT {

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        PostgresFixture.registerProperties(registry);
    }

    @Autowired OrderRepository repository;

    private Order newOrder(String customerId, Instant createdAt) {
        var lines = List.of(new OrderLine("SKU-1", 2, 1250L));
        return new Order(UUID.randomUUID(), customerId, lines, Order.totalOf(lines),
                "USD", OrderStatus.PENDING, 1L, createdAt, createdAt);
    }

    @Test
    void insertAndFindByIdRoundTrips() {
        var order = newOrder("cust-1", Instant.now().truncatedTo(ChronoUnit.MILLIS));
        repository.insert(order, "key-1");

        var found = repository.findById(order.id()).orElseThrow();
        assertThat(found.customerId()).isEqualTo("cust-1");
        assertThat(found.totalMinor()).isEqualTo(2500L);
        assertThat(found.lines()).hasSize(1);
        assertThat(found.status()).isEqualTo(OrderStatus.PENDING);
    }

    @Test
    void duplicateIdempotencyKeyIsDetectableRatherThanDuplicating() {
        var first = newOrder("cust-dup", Instant.now());
        repository.insert(first, "same-key");

        assertThatThrownBy(() -> repository.insert(newOrder("cust-dup", Instant.now()), "same-key"))
                .isInstanceOf(org.springframework.dao.DuplicateKeyException.class);

        assertThat(repository.findByIdempotencyKey("cust-dup", "same-key"))
                .map(Order::id).contains(first.id());
    }

    @Test
    void sameIdempotencyKeyIsAllowedForDifferentCustomers() {
        repository.insert(newOrder("cust-a", Instant.now()), "shared-key");
        assertThatNoException()
                .isThrownBy(() -> repository.insert(newOrder("cust-b", Instant.now()), "shared-key"));
    }

    @Test
    void keysetPaginationWalksEveryRowExactlyOnce() {
        String customer = "cust-page-" + UUID.randomUUID();
        var base = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        for (int i = 0; i < 7; i++) {
            repository.insert(newOrder(customer, base.plusMillis(i)), null);
        }

        var seen = new java.util.ArrayList<UUID>();
        PageCursor cursor = null;
        do {
            var page = repository.list(customer, null, 3, cursor);
            page.orders().forEach(o -> seen.add(o.id()));
            cursor = PageCursor.decode(page.nextPageToken()).orElse(null);
        } while (cursor != null);

        assertThat(seen).hasSize(7).doesNotHaveDuplicates();
    }

    @Test
    void updateStatusBumpsVersionAndRejectsStaleWriters() {
        var order = newOrder("cust-lock", Instant.now());
        repository.insert(order, null);

        var updated = repository.updateStatus(order.id(), OrderStatus.CONFIRMED, 1L, Instant.now());
        assertThat(updated.version()).isEqualTo(2L);
        assertThat(updated.status()).isEqualTo(OrderStatus.CONFIRMED);

        assertThatThrownBy(() -> repository.updateStatus(order.id(), OrderStatus.SHIPPED, 1L, Instant.now()))
                .isInstanceOf(OrderVersionConflict.class);
    }

    @Test
    void missingOrderUpdateReportsNotFoundNotConflict() {
        assertThatThrownBy(() -> repository.updateStatus(UUID.randomUUID(), OrderStatus.CONFIRMED, 1L, Instant.now()))
                .isInstanceOf(OrderNotFound.class);
    }

    @Test
    void eventsAreReadableInSequenceOrder() {
        var order = newOrder("cust-events", Instant.now());
        repository.insert(order, null);
        long first = repository.appendEvent(order.id(), OrderStatus.PENDING, OrderStatus.CONFIRMED, Instant.now());

        var events = repository.eventsAfter(first - 1, "cust-events", 10);
        assertThat(events).extracting(OrderEvent::newStatus).contains(OrderStatus.CONFIRMED);
    }
}
