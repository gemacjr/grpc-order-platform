package com.example.order.service.grpc;

import com.example.order.proto.*;
import com.example.order.service.application.OrderApplicationService;
import com.example.order.service.application.OrderEventPublisher;
import io.grpc.stub.StreamObserver;
import org.springframework.grpc.server.service.GrpcService;

@GrpcService
public class OrderGrpcService extends OrderServiceGrpc.OrderServiceImplBase {

    private static final int STREAM_BATCH = 100;
    private static final long POLL_INTERVAL_MS = 500L;
    // How often the idle-wait below re-checks cancellation/deadline. Must be well
    // below the client's deadline granularity: a single flat sleep for the whole
    // POLL_INTERVAL_MS would let the server notice an expired deadline up to
    // POLL_INTERVAL_MS late, racing (and losing) against the client's own
    // independent, locally-enforced deadline timer.
    private static final long DEADLINE_CHECK_INTERVAL_MS = 50L;
    // The client enforces its own deadline locally and independently of the server -
    // it does not wait for a response. Reacting only once Deadline#isExpired() flips
    // true leaves no time for onCompleted() to reach and be processed by the client
    // before its own timer fires, so the stream loses that race every time. Treating
    // the deadline as "expired" a bit early - once less than this margin remains -
    // gives the completion time to arrive first.
    private static final long DEADLINE_SAFETY_MARGIN_MS = 300L;

    private final OrderApplicationService service;
    private final OrderRequestValidator validator;
    private final OrderEventPublisher publisher;

    public OrderGrpcService(OrderApplicationService service, OrderRequestValidator validator,
                            OrderEventPublisher publisher) {
        this.service = service;
        this.validator = validator;
        this.publisher = publisher;
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

    @Override
    public void streamOrderEvents(StreamOrderEventsRequest request, StreamObserver<OrderEvent> observer) {
        String customerId = request.getCustomerId().isBlank() ? null : request.getCustomerId();
        long cursor = Math.max(0L, request.getFromSequence() - 1);

        var serverObserver = (io.grpc.stub.ServerCallStreamObserver<OrderEvent>) observer;
        var context = io.grpc.Context.current();

        while (!context.isCancelled() && !serverObserver.isCancelled()) {
            // Respect the caller's deadline: finishing cleanly beats being killed
            // mid-write, and it releases the thread and connection promptly.
            var deadline = context.getDeadline();
            if (deadline != null && (deadline.isExpired() || withinSafetyMargin(deadline))) {
                break;
            }

            var batch = publisher.poll(cursor, customerId, STREAM_BATCH);
            for (var event : batch) {
                // Honour reactive-style backpressure: pause while the transport
                // buffer is full instead of piling messages into memory.
                while (!serverObserver.isReady() && !serverObserver.isCancelled()) {
                    if (!sleepQuietly(DEADLINE_CHECK_INTERVAL_MS)) {
                        return;
                    }
                }
                if (serverObserver.isCancelled()) {
                    return;
                }
                observer.onNext(OrderProtoMapper.toProto(event));
                cursor = event.sequence();
            }

            if (batch.isEmpty()) {
                // Wait up to POLL_INTERVAL_MS before polling again, but in short
                // increments so cancellation/deadline expiry is noticed promptly
                // rather than only after the full interval has elapsed.
                long waited = 0L;
                while (waited < POLL_INTERVAL_MS) {
                    if (context.isCancelled() || serverObserver.isCancelled()) {
                        return;
                    }
                    var waitDeadline = context.getDeadline();
                    if (waitDeadline != null && (waitDeadline.isExpired() || withinSafetyMargin(waitDeadline))) {
                        break;
                    }
                    if (!sleepQuietly(DEADLINE_CHECK_INTERVAL_MS)) {
                        return;
                    }
                    waited += DEADLINE_CHECK_INTERVAL_MS;
                }
            }
        }
        observer.onCompleted();
    }

    private boolean withinSafetyMargin(io.grpc.Deadline deadline) {
        return deadline.timeRemaining(java.util.concurrent.TimeUnit.MILLISECONDS) <= DEADLINE_SAFETY_MARGIN_MS;
    }

    private boolean sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
