package com.example.order.service.domain;

public class IllegalStatusTransition extends RuntimeException {
    private final OrderStatus from;
    private final OrderStatus to;

    public IllegalStatusTransition(OrderStatus from, OrderStatus to) {
        super("Cannot transition order from %s to %s".formatted(from, to));
        this.from = from;
        this.to = to;
    }

    public OrderStatus from() { return from; }
    public OrderStatus to() { return to; }
}
