package com.example.order.service.domain;

import java.util.UUID;

public class OrderNotFound extends RuntimeException {
    private final UUID orderId;

    public OrderNotFound(UUID orderId) {
        super("Order %s not found".formatted(orderId));
        this.orderId = orderId;
    }

    public UUID orderId() {
        return orderId;
    }
}
