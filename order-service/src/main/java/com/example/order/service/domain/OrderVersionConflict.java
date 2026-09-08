package com.example.order.service.domain;

import java.util.UUID;

public class OrderVersionConflict extends RuntimeException {
    private final UUID orderId;
    private final long expectedVersion;
    private final long actualVersion;

    public OrderVersionConflict(UUID orderId, long expectedVersion, long actualVersion) {
        super("Order %s is at version %d, caller expected %d"
                .formatted(orderId, actualVersion, expectedVersion));
        this.orderId = orderId;
        this.expectedVersion = expectedVersion;
        this.actualVersion = actualVersion;
    }

    public UUID orderId() { return orderId; }
    public long expectedVersion() { return expectedVersion; }
    public long actualVersion() { return actualVersion; }
}
