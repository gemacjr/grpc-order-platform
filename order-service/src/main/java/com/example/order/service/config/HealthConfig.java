package com.example.order.service.config;

import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.services.HealthStatusManager;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.context.ApplicationListener;
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
 * that also reports {@code order.v1.OrderService} - defining our own
 * {@code BindableService} beans for health or reflection here would register them a
 * second time (the autoconfigured reflection bean in particular carries no
 * {@code @ConditionalOnMissingBean}) and fail server startup with duplicate service
 * registrations.
 */
@Configuration
public class HealthConfig {

    /**
     * Starts NOT_SERVING: the app is not ready to take traffic until Spring Boot
     * publishes ACCEPTING_TRAFFIC (see {@link #readinessHealthBridge}). A gRPC
     * health check that reports SERVING unconditionally would be indistinguishable
     * from no readiness probe at all - Task 19's Kubernetes manifests point a
     * native gRPC readinessProbe at this exact status, so it must track real
     * readiness (datasource reachability included, via the same actuator
     * readiness group the HTTP probe uses) rather than being hardcoded.
     */
    @Bean
    public HealthStatusManager healthStatusManager() {
        var manager = new HealthStatusManager();
        manager.setStatus("", HealthCheckResponse.ServingStatus.NOT_SERVING);
        manager.setStatus("order.v1.OrderService", HealthCheckResponse.ServingStatus.NOT_SERVING);
        return manager;
    }

    /**
     * Mirrors Spring Boot's readiness state onto gRPC health. ACCEPTING_TRAFFIC is
     * published once the application context is fully refreshed and the readiness
     * actuator group (which includes the datasource health indicator) is green;
     * REFUSING_TRAFFIC is published if that ever regresses. This keeps the gRPC
     * readiness probe and the HTTP readiness probe answering from the same signal.
     */
    @Bean
    public ApplicationListener<AvailabilityChangeEvent<ReadinessState>> readinessHealthBridge(
            HealthStatusManager healthStatusManager) {
        return event -> {
            var status = event.getState() == ReadinessState.ACCEPTING_TRAFFIC
                    ? HealthCheckResponse.ServingStatus.SERVING
                    : HealthCheckResponse.ServingStatus.NOT_SERVING;
            healthStatusManager.setStatus("", status);
            healthStatusManager.setStatus("order.v1.OrderService", status);
        };
    }
}
