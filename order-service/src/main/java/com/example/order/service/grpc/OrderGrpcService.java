package com.example.order.service.grpc;

import com.example.order.proto.*;
import com.example.order.service.application.OrderApplicationService;
import io.grpc.stub.StreamObserver;
import org.springframework.grpc.server.service.GrpcService;

@GrpcService
public class OrderGrpcService extends OrderServiceGrpc.OrderServiceImplBase {

    private final OrderApplicationService service;
    private final OrderRequestValidator validator;

    public OrderGrpcService(OrderApplicationService service, OrderRequestValidator validator) {
        this.service = service;
        this.validator = validator;
    }

    @Override
    public void createOrder(CreateOrderRequest request, StreamObserver<CreateOrderResponse> observer) {
        validator.validateCreate(request);
        var result = service.create(request.getCustomerId(),
                OrderProtoMapper.toDomainLines(request.getLinesList()),
                request.getCurrency(),
                request.getIdempotencyKey());

        observer.onNext(CreateOrderResponse.newBuilder()
                .setOrder(OrderProtoMapper.toProto(result.order()))
                .setIdempotentReplay(result.idempotentReplay())
                .build());
        observer.onCompleted();
    }

    @Override
    public void getOrder(GetOrderRequest request, StreamObserver<GetOrderResponse> observer) {
        var id = validator.validateGet(request);
        observer.onNext(GetOrderResponse.newBuilder()
                .setOrder(OrderProtoMapper.toProto(service.get(id))).build());
        observer.onCompleted();
    }

    @Override
    public void listOrders(ListOrdersRequest request, StreamObserver<ListOrdersResponse> observer) {
        var cursor = validator.validateList(request);
        var page = service.list(
                request.getCustomerId().isBlank() ? null : request.getCustomerId(),
                OrderProtoMapper.toDomain(request.getStatus()),
                request.getPageSize(),
                cursor);

        observer.onNext(ListOrdersResponse.newBuilder()
                .addAllOrders(page.orders().stream().map(OrderProtoMapper::toProto).toList())
                .setNextPageToken(page.nextPageToken())
                .build());
        observer.onCompleted();
    }

    @Override
    public void updateOrderStatus(UpdateOrderStatusRequest request,
                                  StreamObserver<UpdateOrderStatusResponse> observer) {
        var id = validator.validateUpdateStatus(request);
        var updated = service.updateStatus(id,
                OrderProtoMapper.toDomain(request.getNewStatus()),
                request.getExpectedVersion());

        observer.onNext(UpdateOrderStatusResponse.newBuilder()
                .setOrder(OrderProtoMapper.toProto(updated)).build());
        observer.onCompleted();
    }
}
