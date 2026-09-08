package com.example.order.service.persistence;

import com.example.order.service.domain.Order;
import java.util.List;

public record OrderPage(List<Order> orders, String nextPageToken) {
}
