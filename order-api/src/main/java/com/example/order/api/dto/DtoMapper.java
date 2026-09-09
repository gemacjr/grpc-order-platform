package com.example.order.api.dto;

import com.example.order.proto.Order;
import com.example.order.proto.OrderEvent;
import com.example.order.proto.OrderStatus;
import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.util.List;

public final class DtoMapper {

    private DtoMapper() {
    }

    public static OrderResponseDto toDto(Order order) {
        return new OrderResponseDto(
                order.getId(),
                order.getCustomerId(),
                order.getLinesList().stream()
                        .map(l -> new OrderLineDto(l.getSku(), l.getQuantity(), l.getUnitPriceMinor()))
                        .toList(),
                order.getTotalMinor(),
                order.getCurrency(),
                shortName(order.getStatus()),
                order.getVersion(),
                toInstant(order.getCreatedAt()),
                toInstant(order.getUpdatedAt()));
    }

    public static OrderEventDto toDto(OrderEvent event) {
        return new OrderEventDto(event.getSequence(), event.getOrderId(),
                shortName(event.getOldStatus()), shortName(event.getNewStatus()),
                toInstant(event.getOccurredAt()));
    }

    public static List<com.example.order.proto.OrderLine> toProtoLines(List<OrderLineDto> lines) {
        return lines.stream()
                .map(l -> com.example.order.proto.OrderLine.newBuilder()
                        .setSku(l.sku()).setQuantity(l.quantity())
                        .setUnitPriceMinor(l.unitPriceMinor()).build())
                .toList();
    }

    /** REST clients see PENDING, not ORDER_STATUS_PENDING: the proto prefix is a wire concern. */
    public static String shortName(OrderStatus status) {
        return status.name().replaceFirst("^ORDER_STATUS_", "");
    }

    public static OrderStatus fromShortName(String name) {
        if (name == null || name.isBlank()) {
            return OrderStatus.ORDER_STATUS_UNSPECIFIED;
        }
        try {
            return OrderStatus.valueOf("ORDER_STATUS_" + name.toUpperCase());
        } catch (IllegalArgumentException e) {
            return OrderStatus.UNRECOGNIZED;
        }
    }

    private static Instant toInstant(Timestamp ts) {
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }
}
