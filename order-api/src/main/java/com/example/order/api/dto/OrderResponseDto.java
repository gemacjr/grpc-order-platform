package com.example.order.api.dto;

import java.time.Instant;
import java.util.List;

public record OrderResponseDto(String id, String customerId, List<OrderLineDto> lines,
                               long totalMinor, String currency, String status,
                               long version, Instant createdAt, Instant updatedAt) {
}
