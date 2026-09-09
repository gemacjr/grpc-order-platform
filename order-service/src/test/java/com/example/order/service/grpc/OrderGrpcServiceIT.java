package com.example.order.service.grpc;

import com.example.order.proto.*;
import com.example.order.testing.PostgresFixture;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.BadRequest;
import com.google.rpc.ErrorInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest(properties = "spring.grpc.server.port=0")
@Import(GrpcStubTestConfig.class)
class OrderGrpcServiceIT {

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        PostgresFixture.registerProperties(registry);
    }

    @Autowired OrderServiceGrpc.OrderServiceBlockingStub stub;

    private CreateOrderRequest validCreate(String customer, String key) {
        return CreateOrderRequest.newBuilder()
                .setCustomerId(customer)
                .setCurrency("USD")
                .setIdempotencyKey(key)
                .addLines(OrderLine.newBuilder().setSku("SKU-1").setQuantity(2).setUnitPriceMinor(1000).build())
                .build();
    }

    @Test
    void createReturnsPersistedOrder() {
        var response = stub.createOrder(validCreate("g-c1", UUID.randomUUID().toString()));
        assertThat(response.getOrder().getTotalMinor()).isEqualTo(2000L);
        assertThat(response.getOrder().getStatus()).isEqualTo(OrderStatus.ORDER_STATUS_PENDING);
        assertThat(response.getIdempotentReplay()).isFalse();
    }

    @Test
    void replayFlagsIdempotentReplay() {
        String key = UUID.randomUUID().toString();
        stub.createOrder(validCreate("g-c2", key));
        assertThat(stub.createOrder(validCreate("g-c2", key)).getIdempotentReplay()).isTrue();
    }

    @Test
    void missingOrderIsNotFoundWithErrorInfoReason() {
        var thrown = catchThrowableOfType(
                () -> stub.getOrder(GetOrderRequest.newBuilder().setId(UUID.randomUUID().toString()).build()),
                StatusRuntimeException.class);

        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
        assertThat(errorInfo(thrown).getReason()).isEqualTo("ORDER_NOT_FOUND");
    }

    @Test
    void invalidRequestCarriesFieldViolations() {
        var bad = CreateOrderRequest.newBuilder().setCustomerId("").setCurrency("US").build();

        var thrown = catchThrowableOfType(() -> stub.createOrder(bad), StatusRuntimeException.class);
        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);

        var violations = badRequest(thrown).getFieldViolationsList();
        assertThat(violations).extracting(BadRequest.FieldViolation::getField)
                .contains("customer_id", "currency", "lines", "idempotency_key");
    }

    @Test
    void malformedPageTokenIsInvalidArgumentNotInternal() {
        var thrown = catchThrowableOfType(
                () -> stub.listOrders(ListOrdersRequest.newBuilder().setPageToken("!!!not-base64!!!").build()),
                StatusRuntimeException.class);
        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    @Test
    void illegalTransitionIsFailedPrecondition() {
        var created = stub.createOrder(validCreate("g-c3", UUID.randomUUID().toString())).getOrder();
        var thrown = catchThrowableOfType(() -> stub.updateOrderStatus(
                UpdateOrderStatusRequest.newBuilder()
                        .setId(created.getId())
                        .setNewStatus(OrderStatus.ORDER_STATUS_SHIPPED)
                        .setExpectedVersion(created.getVersion()).build()),
                StatusRuntimeException.class);

        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(errorInfo(thrown).getReason()).isEqualTo("ILLEGAL_STATUS_TRANSITION");
    }

    @Test
    void staleVersionIsAborted() {
        var created = stub.createOrder(validCreate("g-c4", UUID.randomUUID().toString())).getOrder();
        stub.updateOrderStatus(UpdateOrderStatusRequest.newBuilder()
                .setId(created.getId()).setNewStatus(OrderStatus.ORDER_STATUS_CONFIRMED)
                .setExpectedVersion(created.getVersion()).build());

        var thrown = catchThrowableOfType(() -> stub.updateOrderStatus(
                UpdateOrderStatusRequest.newBuilder()
                        .setId(created.getId()).setNewStatus(OrderStatus.ORDER_STATUS_SHIPPED)
                        .setExpectedVersion(created.getVersion()).build()),
                StatusRuntimeException.class);

        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
        assertThat(errorInfo(thrown).getReason()).isEqualTo("VERSION_CONFLICT");
    }

    @Test
    void listPaginatesWithOpaqueTokens() {
        String customer = "g-page-" + UUID.randomUUID();
        for (int i = 0; i < 5; i++) {
            stub.createOrder(validCreate(customer, UUID.randomUUID().toString()));
        }
        var first = stub.listOrders(ListOrdersRequest.newBuilder()
                .setCustomerId(customer).setPageSize(2).build());

        assertThat(first.getOrdersList()).hasSize(2);
        assertThat(first.getNextPageToken()).isNotEmpty();

        var second = stub.listOrders(ListOrdersRequest.newBuilder()
                .setCustomerId(customer).setPageSize(2).setPageToken(first.getNextPageToken()).build());
        assertThat(second.getOrdersList()).hasSize(2);
        assertThat(second.getOrders(0).getId()).isNotEqualTo(first.getOrders(0).getId());
    }

    private ErrorInfo errorInfo(StatusRuntimeException e) {
        return unpack(e, ErrorInfo.class);
    }

    private BadRequest badRequest(StatusRuntimeException e) {
        return unpack(e, BadRequest.class);
    }

    private <T extends com.google.protobuf.Message> T unpack(StatusRuntimeException e, Class<T> type) {
        var status = StatusProto.fromThrowable(e);
        assertThat(status).isNotNull();
        for (var any : status.getDetailsList()) {
            if (any.is(type)) {
                try {
                    return any.unpack(type);
                } catch (InvalidProtocolBufferException ex) {
                    throw new AssertionError(ex);
                }
            }
        }
        throw new AssertionError("No " + type.getSimpleName() + " detail in status: " + status);
    }
}
