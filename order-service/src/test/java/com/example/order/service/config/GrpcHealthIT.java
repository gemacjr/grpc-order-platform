package com.example.order.service.config;

import com.example.order.service.grpc.GrpcStubTestConfig;
import com.example.order.testing.PostgresFixture;
import io.grpc.ManagedChannel;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.reflection.v1.ServerReflectionGrpc;
import io.grpc.reflection.v1.ServerReflectionRequest;
import io.grpc.reflection.v1.ServerReflectionResponse;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "spring.grpc.server.port=0")
@Import(GrpcStubTestConfig.class)
class GrpcHealthIT {

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        PostgresFixture.registerProperties(registry);
    }

    @Autowired ManagedChannel channel;

    /**
     * gRPC health now tracks Spring Boot's readiness state (flipped to SERVING
     * only once ACCEPTING_TRAFFIC is published) instead of a hardcoded constant.
     * By the time a @SpringBootTest method runs the context is already fully
     * refreshed and readiness should already be SERVING, but poll with a bounded
     * wait rather than asserting on the first response, in case the readiness
     * event is still being dispatched to listeners.
     */
    private HealthCheckResponse.ServingStatus awaitStatus(String service) {
        var stub = HealthGrpc.newBlockingStub(channel);
        var deadline = Instant.now().plus(Duration.ofSeconds(5));
        HealthCheckResponse.ServingStatus last = null;
        while (Instant.now().isBefore(deadline)) {
            last = stub.check(HealthCheckRequest.newBuilder().setService(service).build()).getStatus();
            if (last == HealthCheckResponse.ServingStatus.SERVING) {
                return last;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return last;
    }

    @Test
    void healthServiceReportsServing() {
        assertThat(awaitStatus("")).isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
    }

    @Test
    void perServiceHealthIsReported() {
        assertThat(awaitStatus("order.v1.OrderService")).isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
    }

    @Test
    void reflectionListsOrderService() throws InterruptedException {
        var stub = ServerReflectionGrpc.newStub(channel);
        var latch = new CountDownLatch(1);
        List<String> services = new CopyOnWriteArrayList<>();
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        StreamObserver<ServerReflectionRequest> requestObserver =
                stub.serverReflectionInfo(new StreamObserver<ServerReflectionResponse>() {
                    @Override
                    public void onNext(ServerReflectionResponse value) {
                        value.getListServicesResponse().getServiceList()
                                .forEach(s -> services.add(s.getName()));
                        latch.countDown();
                    }

                    @Override
                    public void onError(Throwable t) {
                        errors.add(t);
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                });

        requestObserver.onNext(ServerReflectionRequest.newBuilder().setListServices("").build());
        requestObserver.onCompleted();

        assertThat(latch.await(5, TimeUnit.SECONDS)).as("reflection response received within 5s").isTrue();
        assertThat(errors).as("no reflection errors").isEmpty();
        assertThat(services).contains("order.v1.OrderService");
    }
}
