package com.example.order.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

public record UpdateStatusRequestDto(@NotBlank String status, @Positive long expectedVersion) {
}
