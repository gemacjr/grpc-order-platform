package com.example.order.api.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.List;

public record CreateOrderRequestDto(
        @NotBlank String customerId,
        @NotBlank @Size(min = 3, max = 3) String currency,
        @NotBlank String idempotencyKey,
        @NotEmpty @Valid List<OrderLineDto> lines) {
}
