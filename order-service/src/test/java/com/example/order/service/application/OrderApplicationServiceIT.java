package com.example.order.service.application;

import com.example.order.service.application.OrderApplicationService.CreateResult;
import com.example.order.service.domain.*;
import com.example.order.testing.PostgresFixture;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest
class OrderApplicationServiceIT {

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        PostgresFixture.registerProperties(registry);
    }

    @Autowired OrderApplicationService service;

    private static final List<OrderLine> LINES = List.of(new OrderLine("SKU-1", 2, 1000L));

    @Test
    void createComputesTotalFromLines() {
        var result = service.create("c1", LINES, "USD", UUID.randomUUID().toString());
        assertThat(result.order().totalMinor()).isEqualTo(2000L);
        assertThat(result.order().status()).isEqualTo(OrderStatus.PENDING);
        assertThat(result.idempotentReplay()).isFalse();
    }

    @Test
    void replayedIdempotencyKeyReturnsTheOriginalOrderNotADuplicate() {
        String key = UUID.randomUUID().toString();
        var first = service.create("c-idem", LINES, "USD", key);
        var second = service.create("c-idem", LINES, "USD", key);

        assertThat(second.order().id()).isEqualTo(first.order().id());
        assertThat(second.idempotentReplay()).isTrue();
    }

    @Test
    void concurrentCreatesWithSameKeyProduceExactlyOneOrder() throws Exception {
        String key = UUID.randomUUID().toString();
        try (var pool = Executors.newFixedThreadPool(8)) {
            var latch = new CountDownLatch(1);
            var futures = new java.util.ArrayList<Future<CreateResult>>();
            for (int i = 0; i < 8; i++) {
                futures.add(pool.submit(() -> {
                    latch.await();
                    return service.create("c-race", LINES, "USD", key);
                }));
            }
            latch.countDown();
            var ids = new java.util.HashSet<UUID>();
            for (var f : futures) {
                ids.add(f.get(10, TimeUnit.SECONDS).order().id());
            }
            assertThat(ids).hasSize(1);
        }
    }

    @Test
    void updateStatusWritesAnEventInTheSameTransaction() {
        var created = service.create("c-evt", LINES, "USD", UUID.randomUUID().toString()).order();
        service.updateStatus(created.id(), OrderStatus.CONFIRMED, created.version());

        var events = service.eventsAfter(0, "c-evt", 100);
        assertThat(events).extracting(OrderEvent::newStatus).contains(OrderStatus.CONFIRMED);
    }

    @Test
    void illegalTransitionIsRejectedAndWritesNoEvent() {
        var created = service.create("c-bad", LINES, "USD", UUID.randomUUID().toString()).order();
        long before = service.eventsAfter(0, "c-bad", 100).size();

        assertThatThrownBy(() -> service.updateStatus(created.id(), OrderStatus.SHIPPED, created.version()))
                .isInstanceOf(IllegalStatusTransition.class);

        assertThat(service.eventsAfter(0, "c-bad", 100)).hasSize((int) before);
    }

    @Test
    void staleVersionIsRejected() {
        var created = service.create("c-stale", LINES, "USD", UUID.randomUUID().toString()).order();
        service.updateStatus(created.id(), OrderStatus.CONFIRMED, created.version());

        assertThatThrownBy(() -> service.updateStatus(created.id(), OrderStatus.SHIPPED, created.version()))
                .isInstanceOf(OrderVersionConflict.class);
    }

    @Test
    void getMissingOrderThrowsOrderNotFound() {
        assertThatThrownBy(() -> service.get(UUID.randomUUID())).isInstanceOf(OrderNotFound.class);
    }
}
