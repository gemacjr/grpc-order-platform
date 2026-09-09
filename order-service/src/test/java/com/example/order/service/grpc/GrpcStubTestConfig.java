package com.example.order.service.grpc;

import com.example.order.proto.OrderServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;

/**
 * order-service has no gRPC client starter on its production classpath (only the
 * server starter). To exercise the real interceptor chain we dial a real channel
 * at the actual ephemeral port the embedded server bound to (spring-grpc-test
 * exposes it via the "local.grpc.port" property once the server has started),
 * rather than building an in-process server that would bypass the production
 * server/interceptor wiring.
 *
 * <p>Shared across gRPC integration tests (see {@code OrderGrpcServiceIT},
 * {@code OrderEventStreamIT}) so every test that just needs a blocking stub
 * imports the same configuration instead of redeclaring it.
 */
@TestConfiguration(proxyBeanMethods = false)
public class GrpcStubTestConfig {

    @Bean(destroyMethod = "shutdown")
    @Lazy
    ManagedChannel orderGrpcChannel(@Value("${local.grpc.port}") int port) {
        return ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
    }

    @Bean
    OrderServiceGrpc.OrderServiceBlockingStub orderServiceBlockingStub(@Lazy ManagedChannel orderGrpcChannel) {
        return OrderServiceGrpc.newBlockingStub(orderGrpcChannel);
    }
}
