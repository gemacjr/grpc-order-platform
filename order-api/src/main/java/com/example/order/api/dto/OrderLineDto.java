package com.example.order.api.dto;

import jakarta.validation.constraints.*;

public record OrderLineDto(
        @NotBlank String sku,
        @Positive int quantity,
        @PositiveOrZero long unitPriceMinor) {
}
