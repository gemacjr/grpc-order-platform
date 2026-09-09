package com.example.order.service.config;

import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.services.HealthStatusManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring gRPC 1.0.3 auto-configures the {@code grpc.health.v1.Health} service AND
 * server reflection on its own (see {@code GrpcServerHealthAutoConfiguration} and
 * {@code GrpcServerReflectionAutoConfiguration} in spring-grpc-server-spring-boot-autoconfigure) -
 * reflection is registered unconditionally once {@code ProtoReflectionServiceV1} is
 * on the classpath (it already is, via the grpc-services dependency), and the health
 * {@code BindableService} is built from whichever {@link HealthStatusManager} bean is
 * in the context (its own default backs off via {@code @ConditionalOnMissingBean}).
 *
 * <p>We therefore only need to supply a customized {@link HealthStatusManager} bean
 * that also reports {@code order.v1.OrderService} as SERVING - defining our own
 * {@code BindableService} beans for health or reflection here would register them a
 * second time (the autoconfigured reflection bean in particular carries no
 * {@code @ConditionalOnMissingBean}) and fail server startup with duplicate service
 * registrations.
 */
@Configuration
public class HealthConfig {

    @Bean
    public HealthStatusManager healthStatusManager() {
        var manager = new HealthStatusManager();
        manager.setStatus("", HealthCheckResponse.ServingStatus.SERVING);
        manager.setStatus("order.v1.OrderService", HealthCheckResponse.ServingStatus.SERVING);
        return manager;
    }
}
