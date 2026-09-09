package com.example.order.service.grpc;

import com.example.order.proto.*;
import com.example.order.testing.JwtTestTokens;
import com.example.order.testing.PostgresFixture;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

// order-service has no gRPC client starter on its production classpath; GrpcStubTestConfig
// dials the real embedded server (see its javadoc) so the real interceptor chain runs.
@SpringBootTest(properties = {"spring.grpc.server.port=0", "order.security.enabled=true"})
@Import(GrpcStubTestConfig.class)
class AuthInterceptorIT {

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        PostgresFixture.registerProperties(registry);
        registry.add("order.security.jwk-set-uri", JwtTestTokens::jwkSetUri);
    }

    @Autowired ManagedChannel channel;

    private OrderServiceGrpc.OrderServiceBlockingStub stubWith(String token) {
        var metadata = new Metadata();
        if (token != null) {
            metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
                    "Bearer " + token);
        }
        return OrderServiceGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    }

    private CreateOrderRequest request() {
        return CreateOrderRequest.newBuilder()
                .setCustomerId("sec").setCurrency("USD")
                .setIdempotencyKey(UUID.randomUUID().toString())
                .addLines(OrderLine.newBuilder().setSku("S").setQuantity(1).setUnitPriceMinor(10).build())
                .build();
    }

    @Test
    void callWithoutTokenIsUnauthenticated() {
        var thrown = catchThrowableOfType(() -> stubWith(null).createOrder(request()),
                StatusRuntimeException.class);
        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    }

    @Test
    void callWithGarbageTokenIsUnauthenticated() {
        var thrown = catchThrowableOfType(() -> stubWith("not.a.jwt").createOrder(request()),
                StatusRuntimeException.class);
        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    }

    @Test
    void callWithValidTokenSucceeds() {
        var token = JwtTestTokens.signedToken("user-1", List.of("orders:write", "orders:read"));
        assertThat(stubWith(token).createOrder(request()).getOrder().getCustomerId()).isEqualTo("sec");
    }

    @Test
    void writeScopeIsRequiredForWrites() {
        var token = JwtTestTokens.signedToken("reader", List.of("orders:read"));
        var thrown = catchThrowableOfType(() -> stubWith(token).createOrder(request()),
                StatusRuntimeException.class);
        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    }

    @Test
    void healthChecksAreExemptFromAuth() {
        var response = io.grpc.health.v1.HealthGrpc.newBlockingStub(channel)
                .check(io.grpc.health.v1.HealthCheckRequest.newBuilder().build());
        assertThat(response.getStatus())
                .isEqualTo(io.grpc.health.v1.HealthCheckResponse.ServingStatus.SERVING);
    }
}
