package com.example.order.service.grpc;

import com.example.order.service.domain.Order;
import com.example.order.service.domain.OrderLine;
import com.example.order.service.domain.OrderStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class OrderProtoMapperTest {

    @Test
    void mapsAllScalarFieldsAndLines() {
        var lines = List.of(new OrderLine("SKU-9", 3, 500L));
        var id = UUID.randomUUID();
        var now = Instant.parse("2026-09-08T10:15:30Z");
        var order = new Order(id, "cust-7", lines, 1500L, "EUR", OrderStatus.CONFIRMED, 4L, now, now);

        var proto = OrderProtoMapper.toProto(order);

        assertThat(proto.getId()).isEqualTo(id.toString());
        assertThat(proto.getCustomerId()).isEqualTo("cust-7");
        assertThat(proto.getTotalMinor()).isEqualTo(1500L);
        assertThat(proto.getCurrency()).isEqualTo("EUR");
        assertThat(proto.getVersion()).isEqualTo(4L);
        assertThat(proto.getStatus()).isEqualTo(com.example.order.proto.OrderStatus.ORDER_STATUS_CONFIRMED);
        assertThat(proto.getCreatedAt().getSeconds()).isEqualTo(now.getEpochSecond());
        assertThat(proto.getLinesList()).singleElement()
                .satisfies(l -> {
                    assertThat(l.getSku()).isEqualTo("SKU-9");
                    assertThat(l.getQuantity()).isEqualTo(3);
                    assertThat(l.getUnitPriceMinor()).isEqualTo(500L);
                });
    }

    @Test
    void unspecifiedStatusMapsToNullMeaningNoFilter() {
        assertThat(OrderProtoMapper.toDomain(com.example.order.proto.OrderStatus.ORDER_STATUS_UNSPECIFIED)).isNull();
    }

    @Test
    void unrecognizedStatusMapsToNullRatherThanThrowing() {
        assertThat(OrderProtoMapper.toDomain(com.example.order.proto.OrderStatus.UNRECOGNIZED)).isNull();
    }

    @Test
    void everyDomainStatusHasAProtoCounterpart() {
        for (OrderStatus status : OrderStatus.values()) {
            assertThat(OrderProtoMapper.toProto(status))
                    .isNotEqualTo(com.example.order.proto.OrderStatus.ORDER_STATUS_UNSPECIFIED);
        }
    }
}
