package com.example.order.service.grpc;

import com.example.order.service.domain.Order;
import com.example.order.service.domain.OrderEvent;
import com.example.order.service.domain.OrderLine;
import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.util.List;

public final class OrderProtoMapper {

    private OrderProtoMapper() {
    }

    public static com.example.order.proto.Order toProto(Order order) {
        return com.example.order.proto.Order.newBuilder()
                .setId(order.id().toString())
                .setCustomerId(order.customerId())
                .addAllLines(order.lines().stream().map(OrderProtoMapper::toProto).toList())
                .setTotalMinor(order.totalMinor())
                .setCurrency(order.currency())
                .setStatus(toProto(order.status()))
                .setVersion(order.version())
                .setCreatedAt(toProto(order.createdAt()))
                .setUpdatedAt(toProto(order.updatedAt()))
                .build();
    }

    public static com.example.order.proto.OrderLine toProto(OrderLine line) {
        return com.example.order.proto.OrderLine.newBuilder()
                .setSku(line.sku())
                .setQuantity(line.quantity())
                .setUnitPriceMinor(line.unitPriceMinor())
                .build();
    }

    public static com.example.order.proto.OrderEvent toProto(OrderEvent event) {
        return com.example.order.proto.OrderEvent.newBuilder()
                .setSequence(event.sequence())
                .setOrderId(event.orderId().toString())
                .setOldStatus(toProto(event.oldStatus()))
                .setNewStatus(toProto(event.newStatus()))
                .setOccurredAt(toProto(event.occurredAt()))
                .build();
    }

    public static Timestamp toProto(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    public static com.example.order.proto.OrderStatus toProto(com.example.order.service.domain.OrderStatus status) {
        return switch (status) {
            case PENDING -> com.example.order.proto.OrderStatus.ORDER_STATUS_PENDING;
            case CONFIRMED -> com.example.order.proto.OrderStatus.ORDER_STATUS_CONFIRMED;
            case SHIPPED -> com.example.order.proto.OrderStatus.ORDER_STATUS_SHIPPED;
            case CANCELLED -> com.example.order.proto.OrderStatus.ORDER_STATUS_CANCELLED;
        };
    }

    /** Returns null for UNSPECIFIED and UNRECOGNIZED, both of which mean "no status supplied". */
    public static com.example.order.service.domain.OrderStatus toDomain(com.example.order.proto.OrderStatus status) {
        return switch (status) {
            case ORDER_STATUS_PENDING -> com.example.order.service.domain.OrderStatus.PENDING;
            case ORDER_STATUS_CONFIRMED -> com.example.order.service.domain.OrderStatus.CONFIRMED;
            case ORDER_STATUS_SHIPPED -> com.example.order.service.domain.OrderStatus.SHIPPED;
            case ORDER_STATUS_CANCELLED -> com.example.order.service.domain.OrderStatus.CANCELLED;
            case ORDER_STATUS_UNSPECIFIED, UNRECOGNIZED -> null;
        };
    }

    public static List<OrderLine> toDomainLines(List<com.example.order.proto.OrderLine> lines) {
        return lines.stream()
                .map(l -> new OrderLine(l.getSku(), l.getQuantity(), l.getUnitPriceMinor()))
                .toList();
    }
}
