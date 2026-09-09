package com.example.order.api.dto;

import java.time.Instant;

public record OrderEventDto(long sequence, String orderId, String oldStatus,
                            String newStatus, Instant occurredAt) {
}
