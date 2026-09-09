package com.example.order.service.application;

import com.example.order.service.domain.OrderEvent;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Polls the order_events table. Polling is deliberate: events are written in the
 * same transaction as the status change, so the table is the single source of
 * truth and no event can be observed for a change that rolled back. An in-memory
 * fan-out would lose events on restart and diverge across replicas.
 */
@Component
public class OrderEventPublisher {

    private final OrderApplicationService service;

    public OrderEventPublisher(OrderApplicationService service) {
        this.service = service;
    }

    public List<OrderEvent> poll(long fromSequence, String customerId, int limit) {
        return service.eventsAfter(fromSequence, customerId, limit);
    }
}
