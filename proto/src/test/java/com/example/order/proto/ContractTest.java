package com.example.order.proto;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class ContractTest {

    @Test
    void orderStatusZeroValueIsUnspecified() {
        assertThat(OrderStatus.forNumber(0)).isEqualTo(OrderStatus.ORDER_STATUS_UNSPECIFIED);
    }

    @Test
    void moneyFieldIsInt64NotDouble() throws Exception {
        var field = Order.getDescriptor().findFieldByName("total_minor");
        assertThat(field.getJavaType().name()).isEqualTo("LONG");
    }

    @Test
    void reactorStubsAreGenerated() {
        assertThat(ReactorOrderServiceGrpc.class).isNotNull();
    }

    @Test
    void serviceExposesAllFiveRpcs() {
        var names = OrderServiceGrpc.getServiceDescriptor().getMethods().stream()
                .map(m -> m.getBareMethodName())
                .toList();
        assertThat(names).containsExactlyInAnyOrder(
                "CreateOrder", "GetOrder", "ListOrders", "UpdateOrderStatus", "StreamOrderEvents");
    }
}
