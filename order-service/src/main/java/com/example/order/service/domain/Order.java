package com.example.order.service.domain;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record Order(UUID id, String customerId, List<OrderLine> lines, long totalMinor,
                    String currency, OrderStatus status, long version,
                    Instant createdAt, Instant updatedAt) {

    public Order {
        lines = List.copyOf(lines);
    }

    public static long totalOf(List<OrderLine> lines) {
        return lines.stream().mapToLong(OrderLine::lineTotalMinor).sum();
    }
}
