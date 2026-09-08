package com.example.order.service.domain;

import java.time.Instant;
import java.util.UUID;

public record OrderEvent(long sequence, UUID orderId, OrderStatus oldStatus,
                         OrderStatus newStatus, Instant occurredAt) {
}
