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

    @Test
    void healthServiceReportsServing() {
        var response = HealthGrpc.newBlockingStub(channel)
                .check(HealthCheckRequest.newBuilder().setService("").build());
        assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
    }

    @Test
    void perServiceHealthIsReported() {
        var response = HealthGrpc.newBlockingStub(channel)
                .check(HealthCheckRequest.newBuilder().setService("order.v1.OrderService").build());
        assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
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
