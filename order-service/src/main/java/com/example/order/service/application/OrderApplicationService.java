package com.example.order.service.application;

import com.example.order.service.domain.*;
import com.example.order.service.persistence.OrderPage;
import com.example.order.service.persistence.OrderRepository;
import com.example.order.service.persistence.PageCursor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class OrderApplicationService {

    public record CreateResult(Order order, boolean idempotentReplay) {
    }

    private final OrderRepository repository;
    private final Clock clock;
    private final TransactionTemplate requiresNewTransaction;

    public OrderApplicationService(OrderRepository repository, Clock clock, PlatformTransactionManager txManager) {
        this.repository = repository;
        this.clock = clock;
        this.requiresNewTransaction = new TransactionTemplate(txManager);
        this.requiresNewTransaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    @Transactional
    public CreateResult create(String customerId, List<OrderLine> lines, String currency, String idempotencyKey) {
        // Check first for the common replay case, then rely on the unique index to
        // settle the race. Checking alone is a check-then-act bug under concurrency;
        // relying on the index alone makes the ordinary replay path throw.
        var existing = repository.findByIdempotencyKey(customerId, idempotencyKey);
        if (existing.isPresent()) {
            return new CreateResult(existing.get(), true);
        }

        Instant now = clock.instant();
        var order = new Order(UUID.randomUUID(), customerId, lines, Order.totalOf(lines),
                currency, OrderStatus.PENDING, 1L, now, now);
        try {
            return new CreateResult(repository.insert(order, idempotencyKey), false);
        } catch (DuplicateKeyException e) {
            // Postgres aborts the whole transaction on a unique-constraint violation, so
            // any further statement issued on this same transaction/connection would fail
            // with "current transaction is aborted". Mark this transaction rollback-only
            // (a Spring-side flag, no DB round trip) and do the recovery read in a brand
            // new transaction/connection so it can actually see the winning row.
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            CreateResult recovered = requiresNewTransaction.execute(status ->
                    new CreateResult(
                            repository.findByIdempotencyKey(customerId, idempotencyKey)
                                    .orElseThrow(() -> e),
                            true));
            return recovered;
        }
    }

    @Transactional(readOnly = true)
    public Order get(UUID id) {
        return repository.findById(id).orElseThrow(() -> new OrderNotFound(id));
    }

    @Transactional(readOnly = true)
    public OrderPage list(String customerId, OrderStatus status, int pageSize, PageCursor cursor) {
        return repository.list(customerId, status, pageSize, cursor);
    }

    @Transactional
    public Order updateStatus(UUID id, OrderStatus newStatus, long expectedVersion) {
        Order current = repository.findById(id).orElseThrow(() -> new OrderNotFound(id));
        StatusTransitions.assertAllowed(current.status(), newStatus);

        Instant now = clock.instant();
        Order updated = repository.updateStatus(id, newStatus, expectedVersion, now);
        // Same transaction as the status change, so an event is never visible for a
        // change that rolled back.
        repository.appendEvent(id, current.status(), newStatus, now);
        return updated;
    }

    @Transactional(readOnly = true)
    public List<OrderEvent> eventsAfter(long sequence, String customerId, int limit) {
        return repository.eventsAfter(sequence, customerId, limit);
    }

    @Transactional(readOnly = true)
    public long latestSequence() {
        List<OrderEvent> events = repository.eventsAfter(0, null, Integer.MAX_VALUE);
        return events.isEmpty() ? 0L : events.get(events.size() - 1).sequence();
    }
}
