package com.example.order.api.client;

import com.example.order.proto.*;
import io.grpc.Deadline;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * The only class in this service that touches generated protobuf types.
 * Deadlines are applied per call: read and write budgets differ, and a streaming
 * call gets none, because a deadline on a long-lived stream is a scheduled failure.
 */
public class OrderGrpcClient {

    private final ReactorOrderServiceGrpc.ReactorOrderServiceStub stub;
    private final Duration readDeadline;
    private final Duration writeDeadline;

    public OrderGrpcClient(ReactorOrderServiceGrpc.ReactorOrderServiceStub stub,
                           Duration readDeadline, Duration writeDeadline) {
        this.stub = stub;
        this.readDeadline = readDeadline;
        this.writeDeadline = writeDeadline;
    }

    public Mono<CreateOrderResponse> create(String customerId, List<OrderLine> lines,
                                            String currency, String idempotencyKey) {
        return write().createOrder(CreateOrderRequest.newBuilder()
                        .setCustomerId(customerId)
                        .addAllLines(lines)
                        .setCurrency(currency)
                        .setIdempotencyKey(idempotencyKey)
                        .build())
                .onErrorMap(GrpcStatusTranslator::translate);
    }

    public Mono<Order> get(UUID id) {
        return read().getOrder(GetOrderRequest.newBuilder().setId(id.toString()).build())
                .map(GetOrderResponse::getOrder)
                .onErrorMap(GrpcStatusTranslator::translate);
    }

    public Mono<ListOrdersResponse> list(String customerId, OrderStatus status,
                                         int pageSize, String pageToken) {
        var request = ListOrdersRequest.newBuilder().setPageSize(pageSize);
        if (customerId != null) {
            request.setCustomerId(customerId);
        }
        if (status != null) {
            request.setStatus(status);
        }
        if (pageToken != null) {
            request.setPageToken(pageToken);
        }
        return read().listOrders(request.build()).onErrorMap(GrpcStatusTranslator::translate);
    }

    public Mono<Order> updateStatus(UUID id, OrderStatus newStatus, long expectedVersion) {
        return write().updateOrderStatus(UpdateOrderStatusRequest.newBuilder()
                        .setId(id.toString())
                        .setNewStatus(newStatus)
                        .setExpectedVersion(expectedVersion)
                        .build())
                .map(UpdateOrderStatusResponse::getOrder)
                .onErrorMap(GrpcStatusTranslator::translate);
    }

    public Flux<OrderEvent> streamEvents(String customerId, long fromSequence) {
        var request = StreamOrderEventsRequest.newBuilder().setFromSequence(fromSequence);
        if (customerId != null) {
            request.setCustomerId(customerId);
        }
        return stub.streamOrderEvents(request.build())
                .onErrorMap(GrpcStatusTranslator::translate);
    }

    private ReactorOrderServiceGrpc.ReactorOrderServiceStub read() {
        return stub.withDeadline(Deadline.after(readDeadline.toMillis(), TimeUnit.MILLISECONDS));
    }

    private ReactorOrderServiceGrpc.ReactorOrderServiceStub write() {
        return stub.withDeadline(Deadline.after(writeDeadline.toMillis(), TimeUnit.MILLISECONDS));
    }
}
