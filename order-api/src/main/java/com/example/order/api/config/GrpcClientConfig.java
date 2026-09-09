package com.example.order.api.config;

import com.example.order.api.client.OrderGrpcClient;
import com.example.order.proto.ReactorOrderServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.client.GrpcChannelBuilderCustomizer;
import org.springframework.grpc.client.GrpcChannelFactory;

import java.time.Duration;

@Configuration
public class GrpcClientConfig {

    @ConfigurationProperties("order.client")
    public record ClientProperties(Duration readDeadline, Duration writeDeadline) {
        public ClientProperties {
            readDeadline = readDeadline == null ? Duration.ofSeconds(2) : readDeadline;
            writeDeadline = writeDeadline == null ? Duration.ofSeconds(5) : writeDeadline;
        }
    }

    @Bean
    public ManagedChannel orderChannel(GrpcChannelFactory channels) {
        return channels.createChannel("order-service");
    }

    // Applies gRPC's built-in retry policy to the order-service channel. Kept
    // additive alongside the orderChannel bean above (rather than replacing it)
    // because OrderGrpcClient depends on that bean directly; GrpcChannelFactory
    // invokes every matching GrpcChannelBuilderCustomizer while building the
    // channel, before orderChannel() receives the built ManagedChannel.
    @Bean
    public GrpcChannelBuilderCustomizer<?> retryCustomizer() {
        return GrpcChannelBuilderCustomizer.matching("order-service", this::applyRetryPolicy);
    }

    private void applyRetryPolicy(ManagedChannelBuilder<?> builder) {
        builder.defaultServiceConfig(RetryPolicyConfig.serviceConfig())
                .enableRetry()
                .maxRetryAttempts(4);
    }

    @Bean
    public OrderGrpcClient orderGrpcClient(ManagedChannel orderChannel, ClientProperties properties) {
        return new OrderGrpcClient(ReactorOrderServiceGrpc.newReactorStub(orderChannel),
                properties.readDeadline(), properties.writeDeadline());
    }
}
