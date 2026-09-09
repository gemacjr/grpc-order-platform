package com.example.order.api.dto;

import java.util.List;

public record OrderPageDto(List<OrderResponseDto> orders, String nextPageToken) {
}
