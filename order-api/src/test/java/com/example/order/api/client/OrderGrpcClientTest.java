package com.example.order.api.client;

import com.example.order.api.client.exception.RemoteOrderNotFound;
import com.example.order.proto.*;
import com.example.order.testing.InProcessGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.UUID;

class OrderGrpcClientTest {

    private static class StubService extends OrderServiceGrpc.OrderServiceImplBase {
        @Override
        public void getOrder(GetOrderRequest request, StreamObserver<GetOrderResponse> observer) {
            if (request.getId().startsWith("00000000")) {
                observer.onError(Status.NOT_FOUND.withDescription("nope").asRuntimeException());
                return;
            }
            observer.onNext(GetOrderResponse.newBuilder()
                    .setOrder(Order.newBuilder().setId(request.getId()).setCustomerId("c").build())
                    .build());
            observer.onCompleted();
        }

        @Override
        public void streamOrderEvents(StreamOrderEventsRequest request, StreamObserver<OrderEvent> observer) {
            for (int i = 1; i <= 3; i++) {
                observer.onNext(OrderEvent.newBuilder().setSequence(i).build());
            }
            observer.onCompleted();
        }
    }

    @Test
    void getReturnsAMonoOfTheOrder() {
        try (var harness = InProcessGrpc.start(new StubService())) {
            var client = new OrderGrpcClient(ReactorOrderServiceGrpc.newReactorStub(harness.channel()),
                    Duration.ofSeconds(2), Duration.ofSeconds(5));
            var id = UUID.randomUUID();

            StepVerifier.create(client.get(id))
                    .assertNext(order -> org.assertj.core.api.Assertions
                            .assertThat(order.getId()).isEqualTo(id.toString()))
                    .verifyComplete();
        }
    }

    @Test
    void remoteErrorsArriveAsTranslatedExceptionsNotStatusRuntimeException() {
        try (var harness = InProcessGrpc.start(new StubService())) {
            var client = new OrderGrpcClient(ReactorOrderServiceGrpc.newReactorStub(harness.channel()),
                    Duration.ofSeconds(2), Duration.ofSeconds(5));

            StepVerifier.create(client.get(UUID.fromString("00000000-0000-0000-0000-000000000000")))
                    .expectError(RemoteOrderNotFound.class)
                    .verify();
        }
    }

    @Test
    void streamingEventsArriveAsAFlux() {
        try (var harness = InProcessGrpc.start(new StubService())) {
            var client = new OrderGrpcClient(ReactorOrderServiceGrpc.newReactorStub(harness.channel()),
                    Duration.ofSeconds(2), Duration.ofSeconds(5));

            StepVerifier.create(client.streamEvents(null, 0).map(OrderEvent::getSequence))
                    .expectNext(1L, 2L, 3L)
                    .verifyComplete();
        }
    }
}
